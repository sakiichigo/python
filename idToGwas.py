import os
import urllib.request
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


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


def process_file(filename, input_dir, output_base_dir):
    input_file_path = os.path.join(input_dir, filename)
    output_file_path = os.path.join(output_base_dir, filename)

    with open(input_file_path, 'r', encoding='utf-8') as infile, open(output_file_path, 'w',
                                                                      encoding='utf-8') as outfile:
        # 写入表头
        outfile.write('Population\tSample size\tNumber of SNPs\tYear\tPMID\tConsortium\n')
        lines = infile.readlines()
        for line in tqdm(lines, desc=f"Processing {filename}", unit="lines"):
            parts = line.strip().split(':')
            if len(parts) > 1:
                id = parts[1].strip()
            else:
                id = parts[0]
            gwas_info = getInfomation(id)
            outfile.write('\t'.join(gwas_info) + '\n')


# 定义目录
input_dir = r'D:\document\bioInfo\heart-sarco-0330\outcome'
output_base_dir = os.path.join(input_dir, 'gwasinfo')

# 创建输出目录
if not os.path.exists(output_base_dir):
    os.makedirs(output_base_dir)

# 获取所有txt文件
txt_files = [filename for filename in os.listdir(input_dir) if filename.endswith('.txt')]

# 使用线程池并行处理文件
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(lambda x: process_file(x, input_dir, output_base_dir), txt_files), total=len(txt_files),
              desc="Processing files", unit="files"))
