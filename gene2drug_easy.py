

import pandas as pd
import os

# 定义 Excel 文件所在的文件夹路径
folder_path = r'D:\document\wechat\WeChat Files\wxid_b6u98cui55l622\FileStorage\File\2024-09\新建文件夹'  # 替换为你的文件夹路径

# 遍历文件夹中的所有 Excel 文件
for file_name in os.listdir(folder_path):
    if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
        # 构建完整的文件路径
        file_path = os.path.join(folder_path, file_name)

        try:
            # 一次性读取所有分表的数据
            all_sheets = pd.read_excel(file_path, sheet_name=None)

            # 创建一个空的 DataFrame 用于保存所有分表的数据
            combined_data = pd.DataFrame()

            # 用于记录缺少列的分表信息
            missing_column_sheets = []

            # 遍历每个分表
            for sheet_name, data in all_sheets.items():
                # 检查数据是否为有效的 DataFrame 格式
                if not isinstance(data, pd.DataFrame):
                    print(f"警告：文件 {file_name} 的分表 {sheet_name} 读取的数据不是有效的 DataFrame 格式。数据类型为 {type(data)}，跳过该分表。")
                    continue

                # 尝试找到 'Drug' 和 'p' 列，忽略大小写
                drug_col = next((col for col in data.columns if col.lower() == 'drug'), None)
                p_col = next((col for col in data.columns if col.lower() == 'p'), None)

                if drug_col and p_col:
                    # 提取特定的两列
                    extracted_data = data[[drug_col, p_col]]
                    extracted_data.columns = ['Drug', 'p']

                    # 为 B 列赋值 1 到数据长度
                    extracted_data.loc[:, 'p'] = range(1, len(extracted_data) + 1)

                    # 确保 Drug 列的数据类型为字符串
                    extracted_data['Drug'] = extracted_data['Drug'].astype(str)

                    # 对 A 列升序排序，并保持 B 列的顺序
                    sorted_data = extracted_data.sort_values(by='Drug', ascending=True)

                    # 修改 B 列列名为对应 Excel 文件名（不带扩展名）
                    output_column_name = os.path.splitext(file_name)[0]
                    sorted_data = sorted_data[['p']].rename(columns={'p': output_column_name})

                    # 将当前分表的数据添加到合并的 DataFrame 中
                    if combined_data.empty:
                        combined_data = sorted_data
                    else:
                        combined_data = pd.concat([combined_data, sorted_data], axis=1)
                else:
                    missing_column_sheets.append((file_name, sheet_name))
                    print(f"文件 {file_name} 的分表 {sheet_name} 中未找到 'Drug' 或 'p' 列。")

            # 保存合并后的结果到新的 Excel 文件
            if not combined_data.empty:
                output_file_name = f"{os.path.splitext(file_name)[0]}.xlsx"
                output_file_path = os.path.join(folder_path, output_file_name)
                combined_data.to_excel(output_file_path, index=False)
                print(f"已保存所有分表的数据到 {output_file_path}")
            else:
                print(f"文件 {file_name} 中没有有效的 'Drug' 和 'p' 列数据。")

            # 汇总显示缺少列的分表信息
            if missing_column_sheets:
                print(f"文件 {file_name} 中缺少列的分表有: {missing_column_sheets}")

        except Exception as e:
            print(f"无法处理文件 {file_name}: {e}")
