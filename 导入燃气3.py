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
new_df.to_excel('C:\\Users\\22565\\Documents\\石油导入数据\\GAS0309_TX.xlsx', index=False)

# 获取当前时间，格式化为 2023-11-1  0:00:00
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# 打开 merge.xlsx 文件
try:
    merge_df = pd.read_excel('C:\\Users\\22565\\Documents\\石油导入数据\\导入\\GAS0309_TX.xlsx')
except FileNotFoundError:
    print("未找到 gas_0309.xlsx 文件，请检查文件路径和文件名。")
else:
    # 映射列名
    mapping = {
        '二级单位': 'company_name',
        '加气站名称': 'site_name',
        '站点性质': 'sell_type',
        '采购量': 'buy_num',
        '期末库存': 'inventory',
        '购进价格': 'buy_price',
        '销售价格': 'sell_price',
        '油气类型': 'gas_type',
        '填报日期': 'biz_day'
    }

    # 初始化一个空的 DataFrame 用于存储选中的列
    selected_df = pd.DataFrame()
    for original_col, target_col in mapping.items():
        if original_col in merge_df.columns:
            selected_df[target_col] = merge_df[original_col]
        else:
            # 如果列不存在，用空值填充
            selected_df[target_col] = None

    # 获取销售数量列
    sell_num_kg = merge_df.get('销售数量（KG）', pd.Series([None] * len(merge_df)))
    sell_num_m3 = merge_df.get('销售数量（方）', pd.Series([None] * len(merge_df)))

    # 初始化 sell_num 和 unit 列
    selected_df['sell_num'] = None
    selected_df['unit'] = None

    # 根据 gas_type 选择销售数量和单位
    lng_mask = selected_df['gas_type'] == 'LNG'
    cng_mask = selected_df['gas_type'] == 'CNG'

    selected_df.loc[lng_mask, 'sell_num'] = sell_num_kg[lng_mask]
    selected_df.loc[lng_mask, 'unit'] = 'KG'

    selected_df.loc[cng_mask, 'sell_num'] = sell_num_m3[cng_mask]
    selected_df.loc[cng_mask, 'unit'] = 'M3'

    # 添加其他固定值的列
    selected_df['created_at'] = current_time
    selected_df['updated_at'] = current_time
    selected_df['created_by_id'] = 1
    selected_df['updated_by_id'] = 1
    #selected_df['sell_type'] = "纯枪"
    selected_df['is_own'] = 1
    selected_df['is_3'] = 0

    # 生成 id 列，假设为从 1 开始的递增整数
    selected_df['id'] = range(1, len(selected_df) + 1)

    # 确保所有列都存在于 selected_df 中，若不存在则用空值填充
    for col in columns:
        if col not in selected_df.columns:
            selected_df[col] = None

    # 将 biz_day 列中的 - 替换成 /
    if 'biz_day' in selected_df.columns:
        selected_df['biz_day'] = selected_df['biz_day'].astype(str).str.replace('-', '/')

    # 定义 site_code 和 site_name 的对应关系
    site_mapping = {
        '裕民加气站': '1-A6501-C012-S105',
        '宏图加气站': '1-A6501-C009-S030',
        '超群加气站': '1-A6501-C009-S098',
        '步缘路加气站': '1-A6501-C012-S109',
        '新丝路加气站': '1-A6501-C009-S042',
        '德胜门CNG加气站': '1-A6501-C015-S121',
        '德胜门LNG加气站': '1-A6501-C015-S121',
        '步缘路LNG加气站': '1-A6501-C012-S109',
        '奎克高速东加气站': '1-A6501-C005-AA01',
        '奎克高速西加气站': '1-A6501-C005-AA02',
        '奎屯天北加气站': '1-A6501-C005-AA03',
        '塔里木母站': '1-A6501-C008-AA01',
        '杜鹃加气母站': '1-A6501-C009-S037',
        '阿克苏CNG直批站点': '1-A6501-C008-BB01',
        '阿勒泰CNG直批站点': '1-A6501-C011-BB01',
        '巴州CNG直批站点': '1-A6501-C009-BB01',
        '博州CNG直批站点': '1-A6501-C010-BB01',
        '昌吉CNG直批站点': '1-A6501-C002-BB01',
        '哈密CNG直批站点': '1-A6501-C015-BB01',
        '和田CNG直批站点': '1-A6501-C013-BB01',
        '喀什CNG直批站点': '1-A6501-C014-BB01',
        '克拉玛依CNG直批站点': '1-A6501-C005-BB01',
        '克州CNG直批站点': '1-A6501-C018-BB01',
        '石河子CNG直批站点': '1-A6501-C018-BB01',
        '塔城CNG直批站点': '1-A6501-C012-BB01',
        '吐鲁番CNG直批站点': '1-A6501-C006-BB01',
        '乌鲁木齐CNG直批站点': '1-A6501-C020-BB01',
        '伊犁CNG直批站点': '1-A6501-C007-BB01',
        '阿克苏LNG直批站点': '1-A6501-C008-BB02',
        '阿勒泰LNG直批站点': '1-A6501-C011-BB02',
        '巴州LNG直批站点': '1-A6501-C009-BB02',
        '博州LNG直批站点': '1-A6501-C010-BB02',
        '昌吉LNG直批站点': '1-A6501-C002-BB02',
        '哈密LNG直批站点': '1-A6501-C015-BB02',
        '和田LNG直批站点': '1-A6501-C013-BB02',
        '喀什LNG直批站点': '1-A6501-C014-BB02',
        '克拉玛依LNG直批站点': '1-A6501-C005-BB02',
        '克州LNG直批站点': '1-A6501-C018-BB02',
        '石河子LNG直批站点': '1-A6501-C018-BB02',
        '塔城LNG直批站点': '1-A6501-C012-BB02',
        '吐鲁番LNG直批站点': '1-A6501-C006-BB02',
        '乌鲁木齐LNG直批站点': '1-A6501-C020-BB02',
        '伊犁LNG直批站点': '1-A6501-C007-BB02'
    }

    # 根据 site_name 更新 site_code
    selected_df['site_code'] = selected_df['site_name'].map(site_mapping)

    # 确保列顺序与 columns 列表一致
    selected_df = selected_df[columns]

    # 将数据写入 biz_gas_sale.xlsx 文件
    with pd.ExcelWriter('C:\\Users\\22565\\Documents\\石油导入数据\\GAS0309_TX.xlsx', mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
        selected_df.to_excel(writer, index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)

    print("数据导入完成，已保存到 biz_gas_sale_0309.xlsx 文件。")