import gzip
import re
import subprocess
import os
from pathlib import Path
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed  # 用于并行执行


# -------------------------- 配置参数 --------------------------
vcf_path = r"E:/gwas/other/finn-b-ST19_HYPOTHERMIA.vcf.gz"
gwas_summary_path = r"D:\document\bioInfo\twas\hypothermia\gwas_summary.txt"
fusion_script_path = r"D:/program/Twas/fusion_twas/FUSION.assoc_test.R"
base_weights_dir = r"E:/Twas/gtex/"
ref_ld_chr_path = r"E:/Twas/LDREF/1000G.EUR."  # 更新为LD参考路径前缀（注意末尾的点）
output_base_path = r"D:\document\bioInfo\twas\hypothermia\twas_results_"
max_fusion_workers = 4  # 并行FUSION任务数（根据CPU核心数调整）
# --------------------------------------------------------------


# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def process_vcf_to_summary(vcf_input, summary_output):
    """优化后的VCF处理：逐行写入+固定字段索引，减少内存占用和冗余计算"""
    print("开始处理VCF文件...")
    valid_count = 0
    error_lines = []
    sample_col_index = -1
    # 记录FORMAT字段中ES/SE/LP的固定索引（避免重复创建字典）
    current_format = None
    es_idx, se_idx, lp_idx = -1, -1, -1

    try:
        # 直接打开输出文件，逐行写入（避免大列表占用内存）
        with gzip.open(vcf_input, "rt") as f, \
             open(summary_output, "w", encoding="utf-8") as out_f:
            # 先写入表头
            out_f.write("SNP\tCHR\tBP\tA1\tA2\tBETA\tSE\tP\tZ\n")

            for line_num, line in enumerate(f, 1):  # line_num从1开始计数
                line = line.strip()
                if not line:
                    continue

                # 跳过注释行
                if line.startswith("##"):
                    continue

                # 处理表头行（确定样本列）
                if line.startswith("#CHROM"):
                    header = line.split("\t")
                    try:
                        sample_col_index = header.index("finn-b-ST19_HYPOTHERMIA")
                    except ValueError:
                        sample_col_index = len(header) - 1
                    print(f"已定位样本列索引：{sample_col_index}（列名：{header[sample_col_index]}）")
                    continue

                # 分割数据行
                parts = line.split("\t")
                if len(parts) <= sample_col_index:
                    error_lines.append(f"行 {line_num}: 字段数量不足（{len(parts)}列）")
                    continue

                # 提取基础信息（简化字符串操作）
                chrom = parts[0].replace("chr", "") if parts[0].startswith("chr") else parts[0]
                pos = parts[1]
                rsid = parts[2] if parts[2] != "." else f"chr{chrom}:{pos}"
                # 处理数据行时补充：
                ref = parts[3]  # 参考等位基因（A2）
                alt = parts[4]  # 替代等位基因（A1，通常为效应等位基因）
                # 构建结果行时加入A1和A2：

                format_str = parts[8]
                sample_data = parts[sample_col_index]

                # 解析FORMAT字段（仅当FORMAT变化时重新解析，减少重复计算）
                if format_str != current_format:
                    format_fields = format_str.split(":")
                    # 检查是否包含必要字段
                    required = ["ES", "SE", "LP"]
                    if not all(f in format_fields for f in required):
                        error_lines.append(f"行 {line_num}: FORMAT缺少字段{[f for f in required if f not in format_fields]}（FORMAT:{format_str}）")
                        continue
                    # 记录字段索引
                    es_idx = format_fields.index("ES")
                    se_idx = format_fields.index("SE")
                    lp_idx = format_fields.index("LP")
                    current_format = format_str  # 更新当前FORMAT

                # 提取样本数据（直接用索引，避免创建字典）
                sample_values = sample_data.split(":")
                if len(sample_values) < max(es_idx, se_idx, lp_idx) + 1:
                    error_lines.append(f"行 {line_num}: 样本数据长度不足（需≥{max(es_idx, se_idx, lp_idx)+1}，实际{len(sample_values)}）")
                    continue

                # 数值转换与验证（减少中间变量）
                try:
                    beta = float(sample_values[es_idx])
                    se = float(sample_values[se_idx])
                    lp_val = float(sample_values[lp_idx])
                    es= float(sample_values[es_idx])
                    p_val = 10 **(-lp_val)
                    z=es/se
                    # 过滤异常值
                    if se <= 0:
                        error_lines.append(f"行 {line_num}: SE值无效（{se}≤0）")
                        continue
                    if p_val > 1:
                        error_lines.append(f"行 {line_num}: P值异常（LP={lp_val}→P={p_val}>1）")
                        continue
                except ValueError as ve:
                    error_lines.append(f"行 {line_num}: 数值转换失败（{ve}，数据：{sample_values}）")
                    continue

                # 直接写入结果（无需暂存列表）
                result_line = f"{rsid}\t{chrom}\t{pos}\t{alt}\t{ref}\t{beta}\t{se}\t{p_val}\t{z}\n"
                out_f.write(result_line)
                valid_count += 1

                # 进度反馈（每50000行更新一次，减少IO干扰）
                if valid_count % 50000 == 0:
                    logging.info(f"已处理 {valid_count} 个有效SNP...")

        # 输出总结
        print(f"VCF处理完成！")
        print(f"有效SNP数量: {valid_count}")
        print(f"错误行数: {len(error_lines)}")
        print(f"结果保存至：{summary_output}")

        # 保存错误日志
        if error_lines:
            log_path = f"{summary_output}.errors.log"
            with open(log_path, "w", encoding="utf-8") as log_f:
                log_f.write("\n".join(error_lines))
            print(f"错误详情已保存至：{log_path}")

        return True

    except gzip.BadGzipFile:
        print(f"错误：{vcf_input}不是有效的gzip文件")
        return False
    except FileNotFoundError:
        print(f"错误：未找到文件{vcf_input}")
        return False
    except Exception as e:
        print(f"VCF处理失败：{str(e)}")
        logging.exception("处理异常")
        return False


def run_fusion_analysis(weights_pos_path, weights_dir_path, output_path, chrom):
    """FUSION分析函数（按染色体处理）"""
    print(f"\n开始执行FUSION分析（染色体{chrom}，{os.path.basename(weights_pos_path)}）...")
    # 构建与终端命令完全一致的参数列表
    command = [
        "Rscript",
        fusion_script_path,
        "--sumstats", gwas_summary_path,
        "--weights", weights_pos_path,          # 指向.pos权重位置文件
        "--weights_dir", weights_dir_path,      # 指向权重文件实际目录
        "--ref_ld_chr", ref_ld_chr_path,  # 拼接染色体号
        "--chr", str(chrom),                    # 当前染色体
        "--out", output_path
    ]

    try:
        result = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=3600  # 超时保护（1小时）
        )
        print(f"FUSION执行成功（染色体{chrom}，{os.path.basename(weights_pos_path)}）")
        print(f"结果保存至：{output_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"FUSION执行失败（染色体{chrom}，{os.path.basename(weights_pos_path)}）：{e.stdout[:500]}...")
        return False
    except subprocess.TimeoutExpired:
        print(f"FUSION执行超时（染色体{chrom}，{os.path.basename(weights_pos_path)}，>1小时）")
        return False


def get_target_weights_info(base_dir):
    """获取目标权重的.pos文件和对应的数据目录"""
    base_dir = Path(base_dir)
    if not base_dir.exists():
        print(f"错误：weights根目录{base_dir}不存在！")
        return []

    target_info = []
    # 遍历所有子目录，寻找包含.pos文件的权重目录
    for dir_path in base_dir.iterdir():
        if dir_path.is_dir() and any(k in dir_path.name.lower() for k in ["adipose", "heart", "muscle"]):
            # 查找目录下的.pos文件（假设每个权重目录有且仅有一个.pos文件）
            pos_files = list(dir_path.glob("*.pos"))
            if pos_files:
                pos_file = pos_files[0]  # 取第一个.pos文件
                # 权重数据目录（假设与.pos文件同级的子目录，或当前目录）
                weights_data_dir = dir_path
                if weights_data_dir.exists() and weights_data_dir.is_dir():
                    target_info.append((str(pos_file), str(weights_data_dir)))
                    print(f"找到权重：{pos_file.name} -> 数据目录：{weights_data_dir}")
                else:
                    print(f"警告：{dir_path}下未找到权重数据子目录{weights_data_dir}")

    if not target_info:
        print(f"警告：未找到符合条件的权重.pos文件和数据目录")
    return target_info


if __name__ == "__main__":
    # 步骤1：处理VCF（若文件不存在）
    if os.path.exists(gwas_summary_path):
        print(f"检测到{gwas_summary_path}已存在，跳过VCF处理步骤。")
    else:
        if not process_vcf_to_summary(vcf_path, gwas_summary_path):
            print("VCF处理失败，终止后续步骤。")
            exit(1)

    # 步骤2：确认汇总文件存在
    if not os.path.exists(gwas_summary_path):
        print(f"错误：未找到{gwas_summary_path}，终止分析。")
        exit(1)

    # 步骤3：获取目标权重的.pos文件和数据目录
    target_weights_info = get_target_weights_info(base_weights_dir)
    if not target_weights_info:
        exit(1)

    # 步骤4：为每个染色体和权重组合创建任务
    print(f"\n准备执行1-22号染色体的FUSION分析（最多{max_fusion_workers}个任务同时运行）...")
    tasks = []
    for pos_path, dir_path in target_weights_info:
        pos_basename = os.path.splitext(os.path.basename(pos_path))[0]
        # 循环1-22号染色体
        for chrom in range(1, 23):
            output_twas_path = f"{output_base_path}{pos_basename}_chr{chrom}.txt"
            tasks.append((pos_path, dir_path, output_twas_path, chrom))

    # 用进程池并行处理
    with ProcessPoolExecutor(max_workers=max_fusion_workers) as executor:
        # 提交所有任务
        futures = {executor.submit(run_fusion_analysis, p, d, o, c): (p, o, c) for p, d, o, c in tasks}
        # 跟踪结果
        for future in as_completed(futures):
            pos_path, output_path, chrom = futures[future]
            try:
                success = future.result()
                status = "成功" if success else "失败"
                print(f"{'=' * 20}\n染色体{chrom}分析{status}：{os.path.basename(pos_path)}\n结果路径：{output_path}")
            except Exception as e:
                print(f"{'=' * 20}\n染色体{chrom}分析出错：{os.path.basename(pos_path)}\n错误：{str(e)}")

    print("\n所有染色体分析结束！")


#命令原句
#Rscript "D:/program/Twas/fusion_twas/FUSION.assoc_test.R" ^
#--sumstats "D:/document/bioInfo/twas/hypothermia/gwas_summary.txt" ^
#--weights "E:/Twas/gtex/GTEx.Adipose_Subcutaneous/GTEx.Adipose_Subcutaneous.pos" ^
#--weights_dir "E:/Twas/gtex/GTEx.Adipose_Subcutaneous" ^
#--ref_ld_chr "E:/Twas/LDREF/1000G.EUR." ^
#--chr "1" ^
#--out "D:/document/bioInfo/twas/hypothermia/twas_results_test_chr1.txt"