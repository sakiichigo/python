import pandas as pd
import numpy as np
from pybiomart import Dataset
import os

# ----------------------
# 1. 配置与参数设置
# ----------------------
# 输入文件路径（替换为你的数据文件）
input_file = r"E:\gwas\pqtl_e8\merged_pqtl_e8.csv"
# 输出目录
output_dir = r"E:\gwas\pqtl_e8\processed_results_pqtl_e8.csv"
# 基因关联的距离阈值（上下游100kb）
distance_threshold = 100000


os.makedirs(output_dir, exist_ok=True)

# ----------------------
# 2. 数据读取与标准化
# ----------------------
# 读取原始数据
df = pd.read_csv(
    input_file,
    na_values=["", "NA"],
    keep_default_na=True,
    low_memory=False
)

# 检查必要列
required_cols = ["variant_id", "beta", "p_value", "effect_allele", "other_allele"]
missing_cols = [col for col in required_cols if col not in df.columns]
if missing_cols:
    raise ValueError(f"缺少必要的列: {', '.join(missing_cols)}")


# 数据标准化处理
def standardize_data(df):
    df_clean = df.copy()

    # 统一SNP ID
    df_clean["snp"] = df_clean.apply(
        lambda row: row["rsid"] if pd.notna(row.get("rsid")) else
        row["hm_rsid"] if pd.notna(row.get("hm_rsid")) else
        row["variant_id"] if pd.notna(row.get("variant_id")) else
        row.get("hm_variant_id", np.nan), axis=1
    )

    # 统一染色体
    df_clean["chrom"] = df_clean.apply(
        lambda row: str(row["chromosome"]) if pd.notna(row.get("chromosome")) else
        str(row["hm_chrom"]).replace("chr", "") if pd.notna(row.get("hm_chrom")) else
        np.nan, axis=1
    )

    # 统一位置
    df_clean["pos"] = df_clean.apply(
        lambda row: row["base_pair_location"] if pd.notna(row.get("base_pair_location")) else
        row["hm_pos"] if pd.notna(row.get("hm_pos")) else
        np.nan, axis=1
    )
    df_clean["pos"] = pd.to_numeric(df_clean["pos"], errors="coerce")

    # 统一等位基因
    df_clean["effect_allele"] = df_clean.apply(
        lambda row: row["effect_allele"] if pd.notna(row.get("effect_allele")) else
        row.get("hm_effect_allele", np.nan), axis=1
    )
    df_clean["other_allele"] = df_clean.apply(
        lambda row: row["other_allele"] if pd.notna(row.get("other_allele")) else
        row.get("hm_other_allele", np.nan), axis=1
    )

    # 统一效应量
    df_clean["beta"] = df_clean.apply(
        lambda row: row["beta"] if pd.notna(row.get("beta")) else
        row["hm_beta"] if pd.notna(row.get("hm_beta")) else
        np.log(row["odds_ratio"]) if pd.notna(row.get("odds_ratio")) else
        np.log(row["hm_odds_ratio"]) if pd.notna(row.get("hm_odds_ratio")) else
        np.nan, axis=1
    )

    # 统一标准误
    df_clean["se"] = df_clean.apply(
        lambda row: row["standard_error"] if pd.notna(row.get("standard_error")) else
        np.nan, axis=1
    )

    # 统一等位基因频率
    df_clean["eaf"] = df_clean.apply(
        lambda row: row["effect_allele_frequency"] if pd.notna(row.get("effect_allele_frequency")) else
        row.get("hm_effect_allele_frequency", np.nan), axis=1
    )

    # 统一p值
    if "log(P)" in df_clean.columns:
        df_clean["p_value"] = df_clean.apply(
            lambda row: row["p_value"] if pd.notna(row.get("p_value")) else
            10 ** (-row["log(P)"]) if pd.notna(row.get("log(P)")) else
            np.nan, axis=1
        )

    # 过滤和去重
    df_clean = df_clean[
        df_clean["snp"].notna() &
        df_clean["effect_allele"].notna() &
        df_clean["other_allele"].notna() &
        df_clean["beta"].notna() &
        df_clean["p_value"].notna()
        ]
    df_clean = df_clean.sort_values("p_value").drop_duplicates("snp", keep="first")

    return df_clean


df_clean = standardize_data(df)
print(f"数据标准化完成，保留 {len(df_clean)} 个SNP")


# ----------------------
# 3. SNP关联基因（修复API字段匹配问题）
# ----------------------
def get_gene_annotations(chrom):
    """通过Ensembl REST API获取特定染色体的基因位置信息（兼容字段名）"""
    server = "https://rest.ensembl.org"
    # 确保染色体格式正确（如"1"而非"chr1"）
    chrom_clean = str(chrom).replace("chr", "")
    ext = f"/overlap/region/human/{chrom_clean}?feature=gene;content-type=application/json"

    try:
        r = requests.get(f"{server}{ext}", headers={"Content-Type": "application/json"})
        if not r.ok:
            print(f"API请求失败，状态码: {r.status_code}，染色体: {chrom_clean}")
            return pd.DataFrame()

        gene_data = r.json()
        if not gene_data:
            print(f"染色体 {chrom_clean} 未返回基因数据")
            return pd.DataFrame()

        # 转换为DataFrame并处理字段映射（关键修复：使用API实际返回的字段名）
        df = pd.DataFrame(gene_data)

        # 检查必要字段是否存在（API返回的是start/end而非start_position/end_position）
        required_api_fields = ["seq_region_name", "start", "end", "external_name", "id"]
        missing_api_fields = [f for f in required_api_fields if f not in df.columns]
        if missing_api_fields:
            print(f"API返回数据缺少字段: {missing_api_fields}，染色体: {chrom_clean}")
            return pd.DataFrame()

        # 重命名字段以匹配后续处理
        df = df.rename(columns={
            "seq_region_name": "chromosome_name",
            "start": "start_position",
            "end": "end_position",
            "external_name": "hgnc_symbol",
            "id": "ensembl_gene_id"
        })

        # 确保位置是数值型
        df["start_position"] = pd.to_numeric(df["start_position"], errors="coerce")
        df["end_position"] = pd.to_numeric(df["end_position"], errors="coerce")

        return df.dropna(subset=["start_position", "end_position"])

    except Exception as e:
        print(f"获取染色体 {chrom_clean} 的基因注释失败: {str(e)}")
        return pd.DataFrame()


def map_snp_to_genes(snp_data, distance_threshold):
    results = []
    # 获取数据中存在的所有染色体（去重）
    chroms = [c for c in snp_data["chrom"].dropna().unique() if str(c).strip()]
    total_chroms = len(chroms)

    if total_chroms == 0:
        print("没有有效的染色体数据，无法进行基因关联")
        return pd.DataFrame(results)

    # 逐个处理染色体
    for i, chrom in enumerate(chroms, 1):
        print(f"处理染色体 {i}/{total_chroms}: {chrom}")
        gene_df = get_gene_annotations(chrom)

        if gene_df.empty:
            # 即使该染色体没有基因数据，也保留SNP信息（无基因关联）
            snp_chrom = snp_data[snp_data["chrom"] == chrom].copy()
            for _, snp_row in snp_chrom.iterrows():
                results.append({
                    **snp_row.to_dict(),
                    "gene_name": np.nan,
                    "gene_id": np.nan,
                    "gene_start": np.nan,
                    "gene_end": np.nan,
                    "distance_to_gene": np.nan
                })
            continue

        # 关联该染色体的SNP与基因
        snp_chrom = snp_data[snp_data["chrom"] == chrom].copy()
        for _, snp_row in snp_chrom.iterrows():
            snp_pos = snp_row["pos"]
            if pd.isna(snp_pos):
                results.append({
                    **snp_row.to_dict(),
                    "gene_name": np.nan,
                    "gene_id": np.nan,
                    "gene_start": np.nan,
                    "gene_end": np.nan,
                    "distance_to_gene": np.nan
                })
                continue

            # 查找距离范围内的基因
            nearby_genes = gene_df[
                (gene_df["start_position"] - distance_threshold <= snp_pos) &
                (gene_df["end_position"] + distance_threshold >= snp_pos)
                ]

            if nearby_genes.empty:
                results.append({
                    **snp_row.to_dict(),
                    "gene_name": np.nan,
                    "gene_id": np.nan,
                    "gene_start": np.nan,
                    "gene_end": np.nan,
                    "distance_to_gene": np.nan
                })
            else:
                for _, gene_row in nearby_genes.iterrows():
                    results.append({
                        **snp_row.to_dict(),
                        "gene_name": gene_row["hgnc_symbol"],
                        "gene_id": gene_row["ensembl_gene_id"],
                        "gene_start": gene_row["start_position"],
                        "gene_end": gene_row["end_position"],
                        "distance_to_gene": snp_pos - gene_row["start_position"]
                    })

    return pd.DataFrame(results)


# 执行SNP-基因关联（允许部分染色体失败）
snp_gene_data = map_snp_to_genes(df_clean, distance_threshold)

# 检查是否有有效数据
if snp_gene_data.empty:
    print("警告：未生成任何SNP-基因关联数据，仅保存标准化后的SNP数据")
    # 至少保存标准化后的SNP数据
    snp_only_output = os.path.join(output_dir, "standardized_snp_data.csv")
    df_clean.to_csv(snp_only_output, index=False)
    print(f"标准化后的SNP数据已保存至: {snp_only_output}")
else:
    # 保存带基因注释的SNP数据
    snp_gene_output = os.path.join(output_dir, "snp_with_gene_annotations.csv")
    snp_gene_data.to_csv(snp_gene_output, index=False)

    # 保存MR分析所需数据
    mr_core_fields = [
        "snp", "chrom", "pos", "effect_allele", "other_allele",
        "beta", "se", "eaf", "p_value", "gene_name", "gene_id"
    ]
    # 过滤存在的字段（避免某些情况下字段缺失）
    mr_core_fields = [f for f in mr_core_fields if f in snp_gene_data.columns]
    mr_data = snp_gene_data[mr_core_fields].drop_duplicates()

    mr_output = os.path.join(output_dir, "mr_exposure_data.csv")
    mr_data.to_csv(mr_output, index=False)

    # 输出统计信息
    gene_annotated = snp_gene_data["gene_name"].notna().sum()
    print("\n处理结果统计:")
    print(f" - 带基因注释的SNP数据已保存至: {snp_gene_output}")
    print(f" - MR分析数据已保存至: {mr_output}")
    print(f" - 共注释到 {gene_annotated} 个SNP-基因关联对")

    # 显示部分结果
    print("\n前5条带基因注释的SNP数据:")
    print(mr_data.head())
