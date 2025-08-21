import os

# 设置目录路径
directory = r"E:\anaFiles\brain"

# 确保目录存在
if not os.path.exists(directory):
    print(f"错误: 目录 '{directory}' 不存在!")
    exit()

# 获取所有ZIP文件
zip_files = [f for f in os.listdir(directory) if f.lower().endswith('.zip')]

# 处理文件名并保存结果
output_file = os.path.join(directory, "processed_filenames.txt")

with open(output_file, 'w', encoding='utf-8') as f:
    for zip_file in zip_files:
        # 移除 .zip 扩展名
        base_name = os.path.splitext(zip_file)[0]

        # 移除 "Brain" 字符 (不区分大小写)
        processed_name = base_name.replace("Brain_", "").replace("brain", "").replace("BRAIN", "")

        # 移除所有下划线
        processed_name = processed_name.replace("_", " ")

        # 写入结果 (包含原始文件名和处理后的文件名)
        f.write(f"{processed_name}\n")

print(f"处理完成! 结果已保存至: {output_file}")