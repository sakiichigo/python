import requests
import pandas as pd
from bs4 import BeautifulSoup
import concurrent.futures
from tqdm import tqdm


def get_gene_id(gene):
    url = f"https://www.ncbi.nlm.nih.gov/gene/?term={gene}"
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        # 修改为使用 string 参数
        gene_id_element = soup.find('span', string=lambda t: t and 'ID:' in t)
        if gene_id_element:
            gene_id = gene_id_element.get_text().strip().split('ID: ')[1].split(' ')[0]
            # 查找上一个元素的 span 元素内容
            prev_element = gene_id_element.find_previous()
            gene_name = prev_element.get_text().strip()
            return gene_id, gene_name
    return None, None


genes = [
    "AC018720.10", "AP006621.5", "C11orf84", "C1QTNF9B-AS1", "C1orf86", "C6orf106", "C8orf46", "CRIPAK",
    "CTA-212D2.2", "CTD-2008A1.2", "FTSJ2", "ILF3-AS1", "METTL10", "PET112", "PNMAL1", "RP1-228P16.1",
    "RP1-239B22.5", "RP1-283E3.4", "RP11-1017G21.5", "RP11-10L7.1", "RP11-166B2.1", "RP11-231C14.4",
    "RP11-350J20.5", "RP11-375I20.6", "RP11-384P7.7", "RP11-386M24.3", "RP11-408A13.4", "RP11-488P3.1",
    "RP11-532N4.2", "RP11-548H18.2", "RP11-554A11.4", "RP11-624M8.1", "RP11-62H7.2", "RP11-6L6.2",
    "RP11-742D12.2", "RP11-745O10.2", "RP11-791G15.2", "RP11-879F14.2", "RP11-879F14.3", "RP11-96H17.1",
    "RP13-582O9.5", "RP3-508I15.9", "RP4-756G23.5", "RP5-1021I20.1", "RRP7B", "STRA13", "TENC1",
    "TMEM180", "TMEM194A", "TMEM8A", "UHRF1BP1"
]

gene_id_list = []
with concurrent.futures.ThreadPoolExecutor() as executor, tqdm(total=len(genes), desc="Processing genes") as pbar:
    futures = {executor.submit(get_gene_id, gene): gene for gene in genes}
    for future in concurrent.futures.as_completed(futures):
        gene = futures[future]
        gene_id, gene_name = future.result()
        gene_id_list.append({'Gene': gene, 'Gene ID': gene_id, 'Gene Name': gene_name})
        pbar.update(1)

df = pd.DataFrame(gene_id_list)
df.to_excel('gene_id_list.xlsx', index=False)
