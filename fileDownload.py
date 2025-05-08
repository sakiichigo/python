import pandas as pd
import requests
import os

# 读取TSV文件
tsv_file = 'C:/Users/22565/Downloads/PMID33875891_studies_export.tsv'  # 替换为你的TSV文件路径
df = pd.read_csv(tsv_file, sep='\t')

# 确保summaryStatistics列存在
if 'summaryStatistics' not in df.columns:
    raise ValueError("TSV文件中没有找到summaryStatistics列")

# 创建一个目录来保存下载的文件
download_dir = 'downloaded_files'
os.makedirs(download_dir, exist_ok=True)

# 遍历每一行的summaryStatistics列
for index, row in df.iterrows():
    url = row['summaryStatistics']
    url=url+"/"+os.path.basename(url)+"_buildGRCh37.tsv"
    # 检查URL是否有效
    if pd.isna(url) or not url.startswith('http'):
        print(f"跳过无效的URL: {url}")
        continue

    # 下载文件
    try:
        response = requests.get(url)
        response.raise_for_status()  # 检查请求是否成功

        # 生成文件名（可以使用URL的最后一部分作为文件名）
        file_name = os.path.join(download_dir, os.path.basename(url))

        # 保存文件
        with open(file_name, 'wb') as file:
            file.write(response.content)

        print(f"成功下载文件: {file_name}")

    except requests.exceptions.RequestException as e:
        print(f"下载失败: {url} - {e}")

print("所有文件下载完成。")