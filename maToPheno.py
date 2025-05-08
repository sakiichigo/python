import os

# 定义相关目录
ma_dir = r'E:\anaFiles\eyeGwas\ma'
outcome_dir = r'D:\document\bioInfo\idp-eye-0228\outcome'
output_dir = r'E:\anaFiles\eyeGwas\ma\pheno'

# 创建输出目录，如果不存在
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 存储ma目录下的id
ma_ids = []
for filename in os.listdir(ma_dir):
    if filename.endswith('.txt'):
        ma_id = os.path.splitext(filename)[0]
        ma_ids.append(ma_id)

# 存储outcome目录下的pheno及其内容
outcome_contents = {}
for filename in os.listdir(outcome_dir):
    if filename.endswith('.txt'):
        pheno = os.path.splitext(filename)[0]
        outcome_file_path = os.path.join(outcome_dir, filename)
        try:
            # 指定使用 UTF-8 编码打开文件
            with open(outcome_file_path, 'r', encoding='utf-8') as outcome_file:
                outcome_contents[pheno] = [line.strip() for line in outcome_file]
        except UnicodeDecodeError:
            print(f"使用 UTF-8 编码读取文件 {outcome_file_path} 时出错，尝试其他编码。")
            try:
                # 若 UTF-8 不行，尝试其他可能的编码，如 GB18030
                with open(outcome_file_path, 'r', encoding='gb18030') as outcome_file:
                    outcome_contents[pheno] = [line.strip() for line in outcome_file]
            except Exception as e:
                print(f"读取文件 {outcome_file_path} 时出错: {e}")
        except Exception as e:
            print(f"读取文件 {outcome_file_path} 时出错: {e}")

# 匹配id和pheno
matched_pairs = []
for ma_id in ma_ids:
    for pheno, lines in outcome_contents.items():
        if any(ma_id in line for line in lines):
            matched_pairs.append(f"{pheno}: {ma_id}")

# 将匹配结果写入文件
output_file_path = os.path.join(output_dir, "matched_pairs.txt")
with open(output_file_path, 'w') as file:
    for pair in matched_pairs:
        file.write(pair + "\n")
