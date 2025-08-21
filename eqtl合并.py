import pandas as pd
import os
import gzip
import glob
from tqdm import tqdm

# 设置目录路径
input_dir = r"D:\document\bioInfo\hypothermia-liver\exposure"
output_file = os.path.join(input_dir, "merged_significant_results.csv")

# 获取所有.txt.gz文件
gz_files = glob.glob(os.path.join(input_dir, "*.txt.gz"))

if not gz_files:
    raise ValueError("在指定目录中未找到任何.txt.gz文件")

print(f"找到 {len(gz_files)} 个文件，开始处理...")

# 存储所有处理后的结果
all_results = []

# 处理每个文件
for file_path in tqdm(gz_files, desc="处理进度", unit="文件"):
    try:
        # 读取压缩文件
        with gzip.open(file_path, 'rt') as f:
            # 自动检测分隔符，通常是制表符
            df = pd.read_csv(f, sep='\t', header=0, low_memory=False)

        # 查找p值列（包含p_value或pvalue，不区分大小写）
        p_cols = [col for col in df.columns if 'pval_nominal' in col.lower() or 'pvalue' in col.lower()]

        if not p_cols:
            print(f"警告: 文件 {os.path.basename(file_path)} 中未找到p值列，已跳过")
            continue

        # 使用第一个找到的p值列
        p_col = p_cols[0]

        # 确保p值列为数值型
        df[p_col] = pd.to_numeric(df[p_col], errors='coerce')

        # 筛选p值小于5e-8的记录
        significant_df = df[df[p_col] < 5e-8].copy()

        if len(significant_df) == 0:
            print(f"文件 {os.path.basename(file_path)} 中没有符合条件的记录")
            continue

        # 添加来源文件列
        significant_df['source_file'] = os.path.basename(file_path)

        # 添加到结果列表
        all_results.append(significant_df)

        print(f"文件 {os.path.basename(file_path)} 处理完成，保留 {len(significant_df)} 条记录")

    except Exception as e:
        print(f"处理文件 {os.path.basename(file_path)} 时出错: {str(e)}")
        continue

# 检查是否有结果
if not all_results:
    raise ValueError("没有符合条件的记录可合并")

# 合并所有结果
merged_df = pd.concat(all_results, ignore_index=True)

# 保存为CSV文件
merged_df.to_csv(output_file, index=False)

print(f"所有文件处理完成，共合并 {len(merged_df)} 条记录")
print(f"结果已保存到: {output_file}")

