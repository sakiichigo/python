import os
import pandas as pd

# 输入目录
input_dir = r'E:\hfGwas\HERMES2_GWAS_HF_EUR\tsv'
# 输出目录
output_dir = r'E:\anaFiles\hfGwas\ma'

# 确保输出目录存在
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 获取输入目录下所有的TSV文件
tsv_files = [f for f in os.listdir(input_dir) if f.endswith('.tsv')]

# 定义映射关系
mapping = {
    'rsID': 'SNP',
    'A1': 'A1',
    'A2': 'A2',
    'A1_freq': 'freq',
    'A1_beta': 'b',
    'se': 'se',
    'pval': 'p'
}

for tsv_file in tsv_files:
    # 构建完整的输入文件路径
    input_file_path = os.path.join(input_dir, tsv_file)
    # 读取TSV文件
    df = pd.read_csv(input_file_path, sep='\t')

    # 选择需要的列并进行重命名
    new_df = df[list(mapping.keys())].rename(columns=mapping)

    # 新增一列 n，值为 NA
    new_df['n'] = 'NA'

    # 处理可能为 None 的值
    for col in ['freq', 'b', 'se', 'p']:
        new_df[col] = new_df[col].apply(lambda x: x if x is not None else 'NA')
        if col == 'p':
            new_df[col] = new_df[col].apply(lambda x: str(x) if x is not None else 'NA')

    # 构建输出文件路径，将扩展名改为 txt
    output_file_name = os.path.splitext(tsv_file)[0] + '.txt'
    output_file_path = os.path.join(output_dir, output_file_name)

    # 将新的 DataFrame 保存为 TXT 文件
    new_df.to_csv(output_file_path, sep='\t', na_rep='nan', index=False)
    print(f"已将 {input_file_path} 转换并保存到 {output_file_path}")
