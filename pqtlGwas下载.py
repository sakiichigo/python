import json
import os

# 定义文件路径
json_file_path = r"D:\document\bioInfo\eqtl-hypothermia\pqtl.txt"
output_file_path = r"D:\document\bioInfo\eqtl-hypothermia\download_links.txt"

try:
    # 读取JSON文件
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 提取parentDocument_accessionId列表
    if 'response' in data and 'docs' in data['response'] and len(data['response']['docs']) > 0:
        docs = data['response']['docs']
        accession_ids = []

        for doc in docs:
            if 'parentDocument_accessionId' in doc:
                # 处理可能是列表或单个值的情况
                if isinstance(doc['parentDocument_accessionId'], list):
                    accession_ids.extend(doc['parentDocument_accessionId'])
                else:
                    accession_ids.append(doc['parentDocument_accessionId'])

        if not accession_ids:
            print("未找到任何parentDocument_accessionId")
        else:
            # 构建下载链接
            base_url = "http://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/"
            links = []

            for acc_id in accession_ids:
                if acc_id.startswith("GCST9024"):
                    # 提取数字部分
                    num_part = acc_id[8:]
                    if num_part.isdigit():
                        num = int(num_part)

                        # 计算范围起始值（每1000为一个范围）
                        # 对于1001-2000范围，起始值是1001，结束值是2000，依此类推
                        range_start = ((num - 1) // 1000) * 1000 + 1
                        range_end = range_start + 999

                        # 格式化范围目录，确保数字部分是4位
                        range_dir = f"GCST9024{range_start:04d}-GCST9024{range_end:04d}"

                        # 构建完整链接
                        link = f"{base_url}{range_dir}/{acc_id}/harmonised/{acc_id}.h.tsv.gz"
                        links.append(link)
                    else:
                        print(f"无效的ID格式: {acc_id}")
                else:
                    print(f"不支持的ID格式: {acc_id}")

            # 保存链接到文件
            with open(output_file_path, 'w', encoding='utf-8') as f:
                for link in links:
                    f.write(link + '\n')

            print(f"成功生成{len(links)}个下载链接，已保存到 {output_file_path}")

    else:
        print("JSON文件结构不符合预期，未找到文档数据")

except FileNotFoundError:
    print(f"错误: 未找到文件 {json_file_path}")
except json.JSONDecodeError:
    print(f"错误: 文件 {json_file_path} 不是有效的JSON格式")
except Exception as e:
    print(f"发生错误: {str(e)}")
