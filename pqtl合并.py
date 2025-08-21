import pandas as pd
import os
import gzip
from glob import glob
from multiprocessing import Pool, cpu_count
from tqdm import tqdm  # 用于显示进度条
import numpy as np

# 定义输入和输出目录
input_dir = r"E:\gwas\pqtl"
output_dir = r"E:\gwas\pqtl_e8"

# 创建输出目录（如果不存在）
os.makedirs(output_dir, exist_ok=True)

# 定义p值阈值
p_threshold = 5e-8


def process_file(file_path):
    """处理单个文件的函数，分块读取以减少内存占用"""
    try:
        # 获取文件名（不包含路径和扩展名）
        file_name = os.path.splitext(os.path.splitext(os.path.basename(file_path))[0])[0]

        with gzip.open(file_path, 'rt') as f:
            # 先读取表头确定p值列
            header = f.readline().strip().split('\t')
            p_columns = [i for i, col in enumerate(header) if 'p_value' in col.lower() or 'pvalue' in col.lower()]

            if not p_columns:
                return None  # 没有p值列，返回空

            p_col_idx = p_columns[0]
            p_col_name = header[p_col_idx]

            # 分块读取文件（每次10,000行），减少内存占用
            chunk_size = 10000
            chunks = []

            # 逐块处理
            for chunk in pd.read_csv(
                    f,
                    sep='\t',
                    names=header,
                    low_memory=False,
                    chunksize=chunk_size
            ):
                # 转换p值列为数值类型
                chunk[p_col_name] = pd.to_numeric(chunk[p_col_name], errors='coerce')
                # 筛选符合条件的记录
                filtered_chunk = chunk[chunk[p_col_name] < p_threshold].copy()
                if not filtered_chunk.empty:
                    chunks.append(filtered_chunk)

            # 如果没有符合条件的记录，返回空
            if not chunks:
                return None

            # 合并当前文件的所有块
            filtered_df = pd.concat(chunks, ignore_index=True)
            # 添加来源文件列
            filtered_df['source_file'] = file_name

            return filtered_df

    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {str(e)}")
        return None


if __name__ == "__main__":
    # 获取所有.h.tsv.gz文件
    gz_files = glob(os.path.join(input_dir, "*.h.tsv.gz"))

    if not gz_files:
        print(f"在目录 {input_dir} 中未找到任何.h.tsv.gz文件")
    else:
        print(f"找到 {len(gz_files)} 个文件，开始处理...")

        # 限制最大进程数（避免内存不足），根据实际内存调整
        max_processes = max(1, cpu_count() // 2)  # 使用一半的CPU核心
        num_processes = min(max_processes, len(gz_files))
        print(f"使用 {num_processes} 个进程并行处理")

        # 并行处理所有文件并显示进度
        with Pool(num_processes) as pool:
            # 使用tqdm显示进度
            results = list(tqdm(
                pool.imap(process_file, gz_files),
                total=len(gz_files),
                desc="处理进度",
                unit="文件"
            ))

        # 过滤掉空结果并合并
        all_filtered_data = [df for df in results if df is not None and not df.empty]

        if all_filtered_data:
            # 合并所有数据
            merged_df = pd.concat(all_filtered_data, ignore_index=True)
            print(f"所有文件处理完成，共合并 {len(merged_df)} 条记录")

            # 保存合并后的数据为CSV文件
            output_file = os.path.join(output_dir, "merged_pqtl_e8.csv")
            merged_df.to_csv(
                output_file,
                sep=',',
                index=False
            )
            print(f"合并后的数据已保存到: {output_file}")
        else:
            print("没有符合条件的记录可合并")
