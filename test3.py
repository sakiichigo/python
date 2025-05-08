import os
import pandas as pd


def merge_txt_to_xlsx(input_dir):
    # 获取指定目录下所有txt文件的路径
    txt_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.txt')]
    # 用于存储所有txt文件的数据
    all_data = []
    # 遍历每个txt文件
    for txt_file in txt_files:
        try:
            # 读取txt文件数据
            data = pd.read_csv(txt_file, sep='\t')
            all_data.append(data)
        except Exception as e:
            print(f"读取文件 {txt_file} 时出现错误: {e}")

    # 如果有数据，则合并数据并保存为xlsx文件
    if all_data:
        # 合并所有数据
        merged_data = pd.concat(all_data, ignore_index=True)

        # 输出文件的路径
        output_file = os.path.join(input_dir, 'merged_files.xlsx')

        try:
            # 将合并后的数据保存为xlsx文件
            merged_data.to_excel(output_file, index=False)
            print(f"已成功将所有txt文件合并为 {output_file}")
        except Exception as e:
            print(f"保存文件 {output_file} 时出现错误: {e}")
    else:
        print("指定目录下没有找到有效的txt文件。")


# 指定目录路径
input_directory = r'D:\document\bioInfo\heart-sarco-0330\outcome\gwasinfo'
# 调用函数
merge_txt_to_xlsx(input_directory)
