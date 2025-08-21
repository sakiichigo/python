import pandas as pd
import os


def generate_links(excel_path):
    # 读取Excel文件
    try:
        df = pd.read_excel(excel_path)
    except Exception as e:
        print(f"读取Excel文件时出错: {e}")
        return False

    # 检查是否存在Edge_Pheno_ID列
    if 'Edge_Pheno_ID' not in df.columns:
        print("Excel文件中未找到'Edge_Pheno_ID'列")
        return False

    # 生成链接列表
    edge_base = "https://zenodo.org/records/5775047/files/ukbiobank_ica_rest_edge_"
    node_base = "https://zenodo.org/records/5775047/files/ukbiobank_ica_rest_node_"
    suffix = "_april2021.zip?download=1"
    links = []

    for id_value in df['Edge_Pheno_ID']:
        # 处理ID值为NaN的情况
        if pd.isna(id_value):
            links.append("")
            continue

        # 将ID转换为小写
        lower_id = str(id_value).lower()

        # 检查是否为pheno1-76
        if lower_id.startswith('pheno'):
            try:
                num = int(lower_id[5:])
                if 1 <= num <= 76:
                    base_url = node_base
                else:
                    base_url = edge_base
            except ValueError:
                base_url = edge_base
        else:
            base_url = edge_base

        link = f"{base_url}{lower_id}{suffix}"
        links.append(link)

    # 获取Excel文件所在目录和文件名
    directory = os.path.dirname(excel_path)
    filename = os.path.basename(excel_path)
    base_filename = os.path.splitext(filename)[0]

    # 保存链接到文本文件
    output_path = os.path.join(directory, f"{base_filename}_links.txt")
    try:
        with open(output_path, 'w') as f:
            for link in links:
                f.write(link + '\n')
        print(f"链接已成功保存到: {output_path}")
        return True
    except Exception as e:
        print(f"保存文件时出错: {e}")
        return False


if __name__ == "__main__":
    # 指定Excel文件路径
    excel_path = r"C:\Users\22565\Documents\Trait_ID_list.xlsx"

    # 生成链接
    generate_links(excel_path)