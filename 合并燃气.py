import os
import pandas as pd

def merge_excel_files(path, output_file):
    # 初始化一个空的 DataFrame 用于存储合并后的数据
    all_data = pd.DataFrame()
    # 遍历指定目录下的所有文件
    for root, dirs, files in os.walk(path):
        for file in files:
            # 检查文件是否为 .xlsx 格式
            if file.endswith('.xlsx'):
                file_path = os.path.join(root, file)
                try:
                    # 读取 Excel 文件
                    df = pd.read_excel(file_path)
                    # 将读取的数据添加到 all_data 中
                    all_data = pd.concat([all_data, df], ignore_index=True)
                except Exception as e:
                    print(f"读取文件 {file_path} 时出错: {e}")
    # 将合并后的数据保存到新的 Excel 文件中
    all_data.to_excel(output_file, index=False)
    print(f"合并完成，结果已保存到 {output_file}")

if __name__ == "__main__":
    # 指定包含 .xlsx 文件的目录
    path = 'D:\\document\\wechat\\WeChat Files\\wxid_b6u98cui55l622\\FileStorage\\File\\2025-02\\燃气24-26'
    # 指定合并后文件的输出路径和文件名
    output_file = 'D:\\document\\wechat\\WeChat Files\\wxid_b6u98cui55l622\\FileStorage\\File\\2025-02\\merged_excel.xlsx'
    merge_excel_files(path, output_file)