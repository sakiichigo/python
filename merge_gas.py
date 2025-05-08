import pandas as pd
import os
from datetime import datetime


def is_valid_date(date_str):
    # 如果 date_str 是 Timestamp 类型，将其转换为字符串
    if isinstance(date_str, pd.Timestamp):
        date_str = str(date_str.date())
    # 如果 date_str 是 datetime.datetime 类型，将其转换为字符串
    elif isinstance(date_str, datetime):
        date_str = date_str.strftime('%Y/%m/%d')
    try:
        # 尝试以 %Y/%m/%d 格式解析日期
        datetime.strptime(date_str, '%Y/%m/%d')
        return True
    except ValueError:
        try:
            # 若 %Y/%m/%d 格式失败，尝试以 %Y-%m-%d 格式解析日期
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False


def merge_excel_files(folder_path):
    all_data = []
    # 遍历文件夹中的所有文件
    for filename in os.listdir(folder_path):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(folder_path, filename)
            try:
                df = pd.read_excel(file_path)
                # 删除 biz_day 列中值为 int 类型的记录
                df = df[~df['biz_day'].apply(lambda x: isinstance(x, int))]
                all_data.append(df)
            except Exception as e:
                print(f"读取文件 {filename} 时出现错误: {e}")

    # 合并所有 DataFrame
    merged_df = pd.concat(all_data, ignore_index=True)
    # 过滤掉 biz_day 列中无效的日期
    merged_df = merged_df[merged_df['biz_day'].apply(is_valid_date)]
    return merged_df


if __name__ == "__main__":
    folder_path = r'C:\Users\22565\Documents\石油导入数据\燃气'
    result_df = merge_excel_files(folder_path)
    # 构造保存路径
    save_path = os.path.join(folder_path, 'merged_and_filtered.xlsx')
    # 将合并并过滤后的结果保存为新的 Excel 文件
    result_df.to_excel(save_path, index=False)
    print(f"合并并过滤后的文件已保存为 {save_path}")
