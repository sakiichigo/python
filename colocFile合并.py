import os
import pandas as pd

# 定义目标目录路径
coloc_dir = r"D:\document\bioInfo\cis-eqtl-hypothermia\coloc"

# 检查目录是否存在
if not os.path.exists(coloc_dir):
    print(f"错误：目录不存在 - {coloc_dir}")
    exit(1)

# 获取目录下所有CSV文件
csv_files = [f for f in os.listdir(coloc_dir) if f.endswith(".csv")]

if not csv_files:
    print(f"目录下没有找到CSV文件 - {coloc_dir}")
    exit(1)

# 初始化一个空列表存储所有数据
all_data = []

# 循环读取每个CSV文件并添加到列表
for file in csv_files:
    file_path = os.path.join(coloc_dir, file)
    try:
        # 读取CSV文件（假设所有文件结构一致）
        df = pd.read_csv(file_path)
        # 可选：添加一列记录数据来源文件名
        df['source_file'] = file
        all_data.append(df)
        print(f"已读取：{file}（{len(df)}行）")
    except Exception as e:
        print(f"读取文件失败 {file}：{str(e)}")

# 合并所有数据
combined_df = pd.concat(all_data, ignore_index=True)

# 保存合并后的结果
output_file = os.path.join(r"D:\document\bioInfo\cis-eqtl-hypothermia", "combined_coloc_results.csv")
combined_df.to_csv(output_file, index=False)

print(f"\n合并完成！共 {len(combined_df)} 行数据")
print(f"结果已保存至：{output_file}")