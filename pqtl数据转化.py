import importlib
import subprocess
import sys
import os
import ssl
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------------
# 依赖管理与配置
# ----------------------
required_libraries = ["requests", "pandas"]


def install_missing_libraries():
    for lib in required_libraries:
        if not importlib.util.find_spec(lib):
            print(f"安装缺失库: {lib}")
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])


install_missing_libraries()

# 路径配置
input_file = r"E:\gwas\pqtl_e8\merged_pqtl_e8.csv"
output_dir = r"E:\gwas\pqtl_e8\processed_results_pqtl_e8"
standardized_cache = os.path.join(output_dir, "standardized_snp_data.csv")
gene_cache_dir = os.path.join(output_dir, "gene_cache")

# 配置参数
distance_threshold = 100000
MAX_REGION_LENGTH = 4500000
MAX_WORKERS = 4  # 降低线程数减少连接压力
RETRY_COUNT = 3  # 重试次数
CHROMOSOME_LENGTHS = {
    "1": 248956422, "2": 242193529, "3": 198295559, "4": 190214555, "5": 181538259,
    "6": 170805979, "7": 159345973, "8": 145138636, "9": 138394717, "10": 133797422,
    "11": 135086622, "12": 133275309, "13": 114364328, "14": 107043718, "15": 101991189,
    "16": 90338345, "17": 83257441, "18": 80373285, "19": 58617616, "20": 64444167,
    "21": 46709983, "22": 50818468, "X": 156040895, "Y": 57227415, "MT": 16569
}

# 创建目录
os.makedirs(output_dir, exist_ok=True)
os.makedirs(gene_cache_dir, exist_ok=True)


# ----------------------
# 网络请求配置（带重试和SSL处理）
# ----------------------
def create_session_with_retries():
    """创建带有重试机制的请求会话"""
    session = requests.Session()

    # 配置重试策略
    retry_strategy = Retry(
        total=RETRY_COUNT,
        backoff_factor=1,  # 重试间隔：1s, 2s, 4s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


# 全局会话
session = create_session_with_retries()


# ----------------------
# 1. 数据标准化（带缓存）
# ----------------------
def load_or_standardize_data():
    if os.path.exists(standardized_cache):
        print(f"加载缓存的标准化数据: {standardized_cache}")
        return pd.read_csv(standardized_cache, low_memory=False)

    print("读取原始数据并标准化...")
    df = pd.read_csv(
        input_file,
        na_values=["", "NA"],
        keep_default_na=True,
        low_memory=False
    )

    # 必要列检查
    required_cols = ["variant_id", "beta", "p_value", "effect_allele", "other_allele"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"缺少必要列: {', '.join(missing_cols)}")

    # 标准化处理（向量化操作）
    df_clean = df.copy()

    # 统一SNP ID
    df_clean["snp"] = df_clean["rsid"].where(df_clean["rsid"].notna(),
                                             df_clean["hm_rsid"].where(df_clean["hm_rsid"].notna(),
                                                                       df_clean["variant_id"].where(
                                                                           df_clean["variant_id"].notna(),
                                                                           df_clean.get("hm_variant_id", np.nan))))

    # 统一染色体
    df_clean["chrom"] = df_clean["chromosome"].astype(str).str.strip().where(df_clean["chromosome"].notna(),
                                                                             df_clean["hm_chrom"].astype(
                                                                                 str).str.replace("chr",
                                                                                                  "").str.strip().where(
                                                                                 df_clean["hm_chrom"].notna(), np.nan))

    # 统一位置
    df_clean["pos"] = pd.to_numeric(
        df_clean["base_pair_location"].where(df_clean["base_pair_location"].notna(),
                                             df_clean["hm_pos"]), errors="coerce"
    )

    # 统一等位基因及其他字段
    df_clean["effect_allele"] = df_clean["effect_allele"].where(df_clean["effect_allele"].notna(),
                                                                df_clean.get("hm_effect_allele", np.nan))
    df_clean["other_allele"] = df_clean["other_allele"].where(df_clean["other_allele"].notna(),
                                                              df_clean.get("hm_other_allele", np.nan))
    df_clean["beta"] = df_clean["beta"].where(df_clean["beta"].notna(),
                                              df_clean["hm_beta"].where(df_clean["hm_beta"].notna(),
                                                                        np.log(df_clean["odds_ratio"]).where(
                                                                            df_clean["odds_ratio"].notna(),
                                                                            np.log(df_clean["hm_odds_ratio"]).where(
                                                                                df_clean["hm_odds_ratio"].notna(),
                                                                                np.nan))))
    df_clean["se"] = df_clean["standard_error"].where(df_clean["standard_error"].notna(), np.nan)
    df_clean["eaf"] = df_clean["effect_allele_frequency"].where(df_clean["effect_allele_frequency"].notna(),
                                                                df_clean.get("hm_effect_allele_frequency", np.nan))

    if "log(P)" in df_clean.columns:
        df_clean["p_value"] = df_clean["p_value"].where(df_clean["p_value"].notna(),
                                                        10 ** (-df_clean["log(P)"]).where(df_clean["log(P)"].notna(),
                                                                                          np.nan))

    # 过滤和去重
    df_clean = df_clean[
        df_clean["snp"].notna() &
        df_clean["effect_allele"].notna() &
        df_clean["other_allele"].notna() &
        df_clean["beta"].notna() &
        df_clean["p_value"].notna()
        ].sort_values("p_value").drop_duplicates("snp", keep="first")

    df_clean.to_csv(standardized_cache, index=False)
    print(f"标准化完成，保留 {len(df_clean)} 个SNP，已缓存至 {standardized_cache}")
    return df_clean


# ----------------------
# 2. 基因数据获取（增强版错误处理）
# ----------------------
def get_region_genes(chrom, start, end):
    """获取单个区域的基因数据，带SSL错误处理"""
    server = "https://rest.ensembl.org"
    url = f"{server}/overlap/region/human/{chrom}:{start}-{end}?feature=gene;content-type=application/json"

    try:
        # 尝试不同的SSL配置
        for _ in range(RETRY_COUNT):
            try:
                response = session.get(
                    url,
                    headers={"Content-Type": "application/json"},
                    timeout=20,
                    verify=True  # 优先验证证书
                )
                if response.ok:
                    return response.json()
                elif response.status_code == 400:
                    # 区域过大错误，尝试更小的区域
                    mid = (start + end) // 2
                    part1 = get_region_genes(chrom, start, mid)
                    part2 = get_region_genes(chrom, mid + 1, end)
                    return part1 + part2
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError):
                # 失败时重试，最后一次尝试关闭证书验证
                if _ == RETRY_COUNT - 1:
                    response = session.get(
                        url,
                        headers={"Content-Type": "application/json"},
                        timeout=20,
                        verify=False  # 最后尝试关闭证书验证
                    )
                    if response.ok:
                        return response.json()
                continue
        return []
    except Exception as e:
        print(f"区域 {start}-{end} 最终失败: {str(e)[:50]}")
        return []


def get_gene_annotations(chrom):
    """获取染色体基因数据（带缓存）"""
    chrom_clean = str(chrom).strip().replace("chr", "").upper()
    cache_file = os.path.join(gene_cache_dir, f"genes_{chrom_clean}.csv")

    # 检查缓存
    if os.path.exists(cache_file):
        try:
            return pd.read_csv(cache_file, low_memory=False)
        except Exception:
            os.remove(cache_file)  # 删除损坏的缓存

    if chrom_clean not in CHROMOSOME_LENGTHS:
        print(f"跳过无效染色体: {chrom}")
        return pd.DataFrame()

    # 生成区域
    total_length = CHROMOSOME_LENGTHS[chrom_clean]
    regions = []
    start = 1
    while start <= total_length:
        end = min(start + MAX_REGION_LENGTH - 1, total_length)
        regions.append((start, end))
        start = end + 1

    print(f"染色体 {chrom_clean} 分 {len(regions)} 块请求...")

    # 并行请求
    all_genes = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_region_genes, chrom_clean, s, e): (s, e) for s, e in regions}

        for future in as_completed(futures):
            s, e = futures[future]
            try:
                genes = future.result()
                if genes:
                    all_genes.extend(genes)
                    print(f"染色体 {chrom_clean} 区域 {s}-{e} 获取 {len(genes)} 个基因")
            except Exception as e:
                print(f"染色体 {chrom_clean} 区域 {s}-{e} 处理异常: {str(e)[:50]}")

    if not all_genes:
        print(f"染色体 {chrom_clean} 未获取到有效基因数据")
        return pd.DataFrame()

    # 处理基因数据
    df = pd.DataFrame(all_genes)
    required_fields = ["seq_region_name", "start", "end", "external_name", "id"]
    if not all(f in df.columns for f in required_fields):
        return pd.DataFrame()

    df = df.rename(columns={
        "seq_region_name": "chromosome_name",
        "start": "start_position",
        "end": "end_position",
        "external_name": "hgnc_symbol",
        "id": "ensembl_gene_id"
    })

    # 去重和清洗
    df = df.drop_duplicates("ensembl_gene_id")
    df["start_position"] = pd.to_numeric(df["start_position"], errors="coerce")
    df["end_position"] = pd.to_numeric(df["end_position"], errors="coerce")
    df = df.dropna(subset=["start_position", "end_position"])

    # 保存缓存
    df.to_csv(cache_file, index=False)
    print(f"染色体 {chrom_clean} 共获取 {len(df)} 个基因，已缓存")
    return df


# ----------------------
# 3. SNP-基因关联
# ----------------------
def map_snp_to_genes(snp_data):
    results = []
    valid_chrom_mask = snp_data["chrom"].apply(lambda x: str(x).strip().upper() in CHROMOSOME_LENGTHS)
    snp_data_valid = snp_data[valid_chrom_mask].copy()
    snp_data_invalid = snp_data[~valid_chrom_mask].copy()

    # 处理无效染色体
    if not snp_data_invalid.empty:
        snp_data_invalid = snp_data_invalid.assign(
            gene_name=np.nan, gene_id=np.nan,
            gene_start=np.nan, gene_end=np.nan,
            distance_to_gene=np.nan
        )
        results.append(snp_data_invalid)

    # 处理有效染色体
    chroms = snp_data_valid["chrom"].unique()
    print(f"\n开始关联 {len(chroms)} 条染色体...")

    for i, chrom in enumerate(chroms, 1):
        print(f"处理染色体 {i}/{len(chroms)}: {chrom}")
        gene_df = get_gene_annotations(chrom)

        if gene_df.empty:
            snp_chrom = snp_data_valid[snp_data_valid["chrom"] == chrom].copy()
            snp_chrom = snp_chrom.assign(
                gene_name=np.nan, gene_id=np.nan,
                gene_start=np.nan, gene_end=np.nan,
                distance_to_gene=np.nan
            )
            results.append(snp_chrom)
            continue

        # 区间索引加速查找
        gene_df["interval"] = pd.IntervalIndex.from_arrays(
            gene_df["start_position"] - distance_threshold,
            gene_df["end_position"] + distance_threshold,
            closed="both"
        )
        gene_intervals = gene_df.set_index("interval")

        # 处理SNP
        snp_chrom = snp_data_valid[snp_data_valid["chrom"] == chrom].copy()
        snp_chrom["pos"] = pd.to_numeric(snp_chrom["pos"], errors="coerce")
        snp_chrom = snp_chrom.dropna(subset=["pos"])

        chrom_results = []
        for _, snp_row in snp_chrom.iterrows():
            pos = snp_row["pos"]
            overlapping = gene_intervals[gene_intervals.index.contains(pos)]

            if overlapping.empty:
                chrom_results.append({**snp_row.to_dict(),
                                      "gene_name": np.nan, "gene_id": np.nan,
                                      "gene_start": np.nan, "gene_end": np.nan,
                                      "distance_to_gene": np.nan
                                      })
            else:
                for _, gene_row in overlapping.iterrows():
                    chrom_results.append({
                        **snp_row.to_dict(),
                        "gene_name": gene_row["hgnc_symbol"],
                        "gene_id": gene_row["ensembl_gene_id"],
                        "gene_start": gene_row["start_position"],
                        "gene_end": gene_row["end_position"],
                        "distance_to_gene": pos - gene_row["start_position"]
                    })

        results.append(pd.DataFrame(chrom_results))

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


# ----------------------
# 主程序
# ----------------------
if __name__ == "__main__":
    # 禁用不安全的SSL版本警告
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    df_clean = load_or_standardize_data()
    snp_gene_data = map_snp_to_genes(df_clean)

    if snp_gene_data.empty:
        print("未生成有效结果")
    else:
        snp_gene_output = os.path.join(output_dir, "snp_with_gene_annotations.csv")
        snp_gene_data.to_csv(snp_gene_output, index=False)

        mr_core_fields = [
            "snp", "chrom", "pos", "effect_allele", "other_allele",
            "beta", "se", "eaf", "p_value", "gene_name", "gene_id"
        ]
        mr_core_fields = [f for f in mr_core_fields if f in snp_gene_data.columns]
        mr_data = snp_gene_data[mr_core_fields].drop_duplicates()

        mr_output = os.path.join(output_dir, "mr_exposure_data.csv")
        mr_data.to_csv(mr_output, index=False)

        print(f"\n处理完成！共关联 {snp_gene_data['gene_name'].notna().sum()} 个SNP-基因对")
        print(f"带基因注释的数据: {snp_gene_output}")
        print(f"MR分析数据: {mr_output}")
