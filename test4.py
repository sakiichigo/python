import os
import shutil

# 定义文件路径
id_file_path = r'E:\anaFiles\eyeGwas\ma\pheno\gwasinfo\allId.txt'
source_dir = r'E:\anaFiles\smrBrainEye'
target_dir = r'E:\anaFiles\smrBrainEye\cataract&glaucoma'

# 确保目标目录存在
if not os.path.exists(target_dir):
    os.makedirs(target_dir)

try:
    # 读取ID文件
    with open(id_file_path, 'r') as f:
        ids = [line.strip() for line in f.readlines()]

    # 遍历源目录
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            for id in ids:
                if id in file:
                    source_file_path = os.path.join(root, file)
                    target_file_path = os.path.join(target_dir, file)
                    # 移动文件
                    shutil.move(source_file_path, target_file_path)
                    print(f"Moved {source_file_path} to {target_file_path}")

except FileNotFoundError:
    print(f"Error: The file {id_file_path} was not found.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
