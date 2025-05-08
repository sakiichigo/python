import pandas as pd

# 定义TSV文件路径和输出TXT文件路径
tsv_file = 'C:/Users/22565/Downloads/PMID33875891_studies_export.tsv'  # 替换为你的TSV文件路径
output_txt_file = 'D:/document/生信/downloaded_files/output.txt'

# 读取TSV文件
df = pd.read_csv(tsv_file, sep='\t')

# 确保summaryStatistics和accessionId列存在
if 'summaryStatistics' not in df.columns or 'accessionId' not in df.columns:
    raise ValueError("TSV文件中没有找到summaryStatistics或accessionId列")

# 拼接summaryStatistics和accessionId列
df['combined'] = df['summaryStatistics'] +"/harmonised/"+ df['accessionId']+".h.tsv.gz"

# 将拼接结果保存到TXT文件中，每行一个结果
with open(output_txt_file, 'w') as file:
    for combined_value in df['combined']:
        file.write(combined_value + '\n')

print(f"拼接结果已保存到 {output_txt_file}")