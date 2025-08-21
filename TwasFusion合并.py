import pandas as pd
import os
import re
from pathlib import Path

# -------------------------- 配置参数 --------------------------
# 源文件所在目录
source_dir = Path(r"D:\document\bioInfo\twas\hypothermia")
# 合并结果输出目录（自动创建）
output_dir = source_dir / "merge"
# 合并后文件名
output_filename = "merged_twas_results.tsv"


# --------------------------------------------------------------


def extract_tissue_from_filename(filename):
    """从文件名中提取组织名称（如Adipose_Subcutaneous）"""
    # 正则表达式匹配文件名模式：twas_results_GTEx.XXX_chrN.txt 或 twas_results_GTEx.XXX_chrN.txt.MHC
    pattern = re.compile(r"twas_results_GTEx\.(.*?)_chr\d+\.txt(?:\.MHC)?$")
    match = pattern.match(filename)
    if match:
        return match.group(1)  # 返回捕获的组织名称
    else:
        return None  # 不匹配则返回None


def main():
    # 创建输出目录（若不存在）
    output_dir.mkdir(exist_ok=True)
    # 存储所有文件的数据框
    all_data = []

    # 遍历源目录下的符合条件的文件，包括.txt和.txt.MHC
    file_patterns = [
        "twas_results_GTEx.*_chr*.txt",
        "twas_results_GTEx.*_chr*.txt.MHC"
    ]

    # 收集所有符合条件的文件
    files = []
    for pattern in file_patterns:
        files.extend(source_dir.glob(pattern))

    # 去重（防止意外重复）
    files = list(set(files))

    # 处理每个文件
    for file in files:
        # 提取文件名（不含路径）
        filename = file.name
        # 提取组织名称
        tissue = extract_tissue_from_filename(filename)
        if not tissue:
            print(f"跳过不符合命名格式的文件：{filename}")
            continue

        try:
            # 读取TSV文件（制表符分隔，NA表示缺失值）
            df = pd.read_csv(
                file,
                sep="\t",  # 明确指定制表符分隔
                na_values="NA",  # 将"NA"识别为缺失值
                dtype=str  # 先按字符串读取，避免数值格式自动转换导致问题
            )

            # 检查列名是否符合预期
            expected_columns = ["PANEL", "FILE", "ID", "CHR", "P0", "P1", "HSQ",
                                "BEST.GWAS.ID", "BEST.GWAS.Z", "EQTL.ID", "EQTL.R2",
                                "EQTL.Z", "EQTL.GWAS.Z", "NSNP", "NWGT", "MODEL",
                                "MODELCV.R2", "MODELCV.PV", "TWAS.Z", "TWAS.P"]

            # 确保所有预期列都存在
            for col in expected_columns:
                if col not in df.columns:
                    print(f"警告：文件 {filename} 缺少列 {col}，已添加空值列")
                    df[col] = pd.NA

            # 部分列需转回数值型
            numeric_cols = ["CHR", "P0", "P1", "HSQ", "BEST.GWAS.Z", "EQTL.R2",
                            "EQTL.Z", "EQTL.GWAS.Z", "NSNP", "NWGT", "TWAS.Z", "TWAS.P"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")  # 转换失败设为NaN

            # 新增TISSUE列（放在最后）
            df["TISSUE"] = tissue
            # 新增SOURCE_FILE列，记录数据来源文件
            df["SOURCE_FILE"] = filename
            all_data.append(df)
            print(f"已读取并处理：{filename}（组织：{tissue}）")

        except Exception as e:
            print(f"处理文件{filename}失败：{str(e)}")
            continue

    # 合并所有数据
    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        # 保存合并结果（制表符分隔，NA用"NA"表示）
        output_path = output_dir / output_filename
        merged_df.to_csv(
            output_path,
            sep="\t",
            index=False,
            na_rep="NA"  # 缺失值显示为"NA"
        )
        print(f"\n合并完成！共处理{len(all_data)}个文件")
        print(f"合并结果保存至：{output_path}")
        print(f"总记录数：{len(merged_df)}")
    else:
        print("\n未找到符合条件的文件，未执行合并。")


if __name__ == "__main__":
    main()
