import os
import urllib.request
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import csv
import pandas as pd


def getInfomation(id):
    url = 'https://gwas.mrcieu.ac.uk/datasets/' + id + '/'
    while True:
        try:
            page = urllib.request.urlopen(url, timeout=10).read().decode('utf-8')
        except Exception as error:
            print('Error:', error, ' ', url)
        else:
            break
    soup = BeautifulSoup(page, 'html.parser')
    pages = soup.find_all('th', class_='text-nowrap')
    population = "Nome"
    sample_size = "Nome"
    nsp = "Nome"
    year = "Nome"
    pmid = "Nome"
    consortium = "Nome"
    for i in range(0, len(pages), 1):
        temp = pages[i]
        if temp.text == "PMID":
            pmid = temp.findNext("td").text
            continue
        if temp.text == "Year":
            year = temp.findNext("td").text
            continue
        if temp.text == "Population":
            population = temp.findNext("td").text
            continue
        if temp.text == "Sample size":
            sample_size = temp.findNext("td").text
            continue
        if temp.text == "Number of SNPs":
            nsp = temp.findNext("td").text
            continue
        if temp.text == "Consortium":
            consortium = temp.findNext("td").text
            continue
    result = [population, sample_size, nsp, year, pmid, consortium]
    return result


def process_file(input_file_path, output_file_path):
    # 使用 pandas 读取 Excel 文件
    df = pd.read_excel(input_file_path)

    with open(output_file_path, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile)

        # 写入表头
        writer.writerow(['Population', 'Sample size', 'Number of SNPs', 'Year', 'PMID', 'Consortium'])

        for index, row in tqdm(df.iterrows(), desc=f"Processing {input_file_path}", total=len(df)):
            if len(row) > 0:
                id = str(row[0]).strip()
                gwas_info = getInfomation(id)
                writer.writerow(gwas_info)


# 定义输入文件路径和输出文件路径
input_file_path = r'D:\document\bioInfo\sarco-heart-0402\result\combined_ivw_filter.xlsx'
output_dir = os.path.dirname(input_file_path)
output_file_path = os.path.join(output_dir, 'combined_ivw_filter_gwasinfo.csv')

# 处理文件
process_file(input_file_path, output_file_path)