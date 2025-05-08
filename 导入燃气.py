import os
import pandas as pd
from datetime import datetime

# 定义列名
columns = [
    'created_at', 'updated_at', 'id', 'created_by_id', 'updated_by_id',
    'biz_day', 'company_name', 'site_code', 'site_name', 'gas_type',
    'sell_num', 'sell_price', 'unit', 'sell_type', 'is_own', 'is_3',
    'inventory', 'buy_num', 'buy_price'
]

# 创建新的 Excel 文件 biz_gas_sale.xlsx
new_df = pd.DataFrame(columns=columns)
new_df.to_excel('C:\\Users\\22565\\Documents\\石油导入数据\\GAS0309.xlsx', index=False)

# 获取当前时间，格式化为 2023-11-1  0:00:00
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# 打开 merge.xlsx 文件
try:
    merge_df = pd.read_excel('C:\\Users\\22565\\Documents\\石油导入数据\\导入\\GAS0309.xlsx')
except FileNotFoundError:
    print("未找到 merged_file.xlsx 文件，请检查文件路径和文件名。")
else:
    # 映射列名
    mapping = {
        '营业日': 'biz_day',
        '市简称': 'company_name',
        '网点编码': 'site_code',
        '网点简称': 'site_name',
        '天然气简称': 'gas_type',
        '计量单位': 'unit',
        '进货数量': 'buy_num',
        '销售数量': 'sell_num',
        '单价': 'sell_price',
        '期末库存': 'inventory'
    }

    # 初始化一个空的 DataFrame 用于存储选中的列
    selected_df = pd.DataFrame()
    for original_col, target_col in mapping.items():
        if original_col in merge_df.columns:
            selected_df[target_col] = merge_df[original_col]
        else:
            # 如果列不存在，用空值填充
            selected_df[target_col] = None

    # 添加其他固定值的列
    selected_df['created_at'] = current_time
    selected_df['updated_at'] = current_time
    selected_df['created_by_id'] = 1
    selected_df['updated_by_id'] = 1
    selected_df['sell_type'] = "纯枪"
    selected_df['is_own'] = 1
    selected_df['is_3'] = 1

    # 生成 id 列，假设为从 1 开始的递增整数
    selected_df['id'] = range(1, len(selected_df) + 1)

    # 确保所有列都存在于 selected_df 中，若不存在则用空值填充
    for col in columns:
        if col not in selected_df.columns:
            selected_df[col] = None

    # 将 biz_day 列中的 - 替换成 /
    if 'biz_day' in selected_df.columns:
        selected_df['biz_day'] = selected_df['biz_day'].astype(str).str.replace('-', '/')

    # 确保列顺序与 columns 列表一致
    selected_df = selected_df[columns]

    # 将数据写入 biz_gas_sale.xlsx 文件
    with pd.ExcelWriter('C:\\Users\\22565\\Documents\\石油导入数据\\GAS0309.xlsx', mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
        selected_df.to_excel(writer, index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)

    print("数据导入完成，已保存到 biz_gas_sale.xlsx 文件。")