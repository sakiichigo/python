import pandas as pd
import os
import gzip
import glob

# 定义目录路径
input_dir = r"D:\document\bioInfo\hypothermia-liver\exposure"
output_file = os.path.join(input_dir, "processed_exposure_data.csv")

# 获取目录下所有txt.gz文件
gz_files = glob.glob(os.path.join(input_dir, "*.txt.gz"))

if not gz_files:
    raise ValueError(f"在目录 {input_dir} 中未找到任何txt.gz文件")

print(f"找到 {len(gz_files)} 个文件，开始处理...")

# 存储所有处理后的数据
all_data = []

# 遍历所有文件并处理
for file_path in gz_files:
    try:
        # 读取txt.gz文件
        with gzip.open(file_path, 'rt') as f:
            # 读取数据，指定列名
            df = pd.read_csv(
                f,
                sep='\t',  # 假设是制表符分隔
                header=0,  # 第一行为列名
                low_memory=False
            )

        # 检查是否包含所有必要的列
        required_cols = [
            'variant_id', 'rs_id_dbSNP151_GRCh38p7', 'slope',
            'slope_se', 'maf', 'pval_nominal', 'alt', 'ref'
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"警告: 文件 {os.path.basename(file_path)} 缺少必要的列: {missing_cols}，已跳过")
            continue

        # 映射MR所需的列
        processed_df = df.rename(columns={
            'rs_id_dbSNP151_GRCh38p7': 'rsid',  # 使用已有的rsID列
            'slope': 'beta',
            'slope_se': 'standard_error',
            'maf': 'effect_allele_frequency',
            'pval_nominal': 'p_value',
            'alt': 'effect_allele',
            'ref': 'other_allele',
            'gene_name': 'gene_name'
        })

        # 对于没有rsID的记录，使用variant_id作为备用
        processed_df['snp'] = processed_df.apply(
            lambda row: row['rsid'] if pd.notna(row['rsid']) and row['rsid'] != '' else row['variant_id'],
            axis=1
        )

        # 提取染色体和位置信息（用于后续可能的rsID转换）
        # 从variant_id格式：chr1_64764_C_T_b38中提取
        split_cols = processed_df['variant_id'].str.split("_", expand=True)
        if len(split_cols.columns) >= 2:
            processed_df['chrom'] = split_cols[0].str.replace("chr", "")
            processed_df['pos'] = split_cols[1]
            processed_df['variant_pos'] = processed_df['chrom'] + ":" + processed_df['pos']

        # 保留MR分析所需的列
        final_cols = [
            'snp', 'rsid', 'variant_id', 'variant_pos', 'beta',
            'standard_error', 'effect_allele_frequency', 'p_value',
            'effect_allele', 'other_allele','gene_name'
        ]
        # 过滤存在的列（避免某些文件缺少衍生列的情况）
        final_cols = [col for col in final_cols if col in processed_df.columns]

        all_data.append(processed_df[final_cols])
        print(f"已处理文件: {os.path.basename(file_path)}，记录数: {len(df)}")

    except Exception as e:
        print(f"处理文件 {os.path.basename(file_path)} 时出错: {str(e)}")
        continue

# 合并所有数据
if not all_data:
    raise ValueError("没有成功处理任何文件，无法生成输出数据")

merged_df = pd.concat(all_data, ignore_index=True)

# 保存处理后的CSV文件
merged_df.to_csv(output_file, index=False)
print(f"所有文件处理完成，共合并 {len(merged_df)} 条记录")
print(f"处理后的数据已保存至: {output_file}")
