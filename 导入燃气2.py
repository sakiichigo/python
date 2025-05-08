
import pandas as pd
from datetime import datetime

# 定义 gassales.xlsx 的列名
columns = [
    'created_at', 'updated_at', 'id', 'created_by_id', 'updated_by_id',
    'company_name', 'site_name', 'buy_num', 'sall_num_k_g', 'sall_num_c',
    'inventory', 'buy_price', 'sall_price', 'gastype', 'unit', 'date',
    'gas_no', 'site_no'
]

# 创建一个空的 DataFrame 用于存储 gassales.xlsx 的数据
gassales_df = pd.DataFrame(columns=columns)

# 读取 merged_excel.xlsx 文件
merged_df = pd.read_excel('D:\\document\\wechat\\WeChat Files\\wxid_b6u98cui55l622\\FileStorage\\File\\2025-02\\merged_excel.xlsx')

# 处理市简称列
words_to_remove = ["中石油", "新疆", "销售", "有限公司", "荣吉盛", "能源"]
for word in words_to_remove:
    merged_df['市简称'] = merged_df['市简称'].str.replace(word, '', regex=False)

# 将分公司替换成公司
merged_df['市简称'] = merged_df['市简称'].str.replace('分公司', '公司', regex=False)

# 定义列名映射规则
mapping = {
    '市简称': 'company_name',
    '网点编码': 'site_no',
    '网点简称': 'site_name',
    '天然气编码': 'gas_no',
    '天然气简称': 'gastype',
    '计量单位': 'unit',
    '销售数量': ['sall_num_k_g', 'sall_num_c'],
    '进货数量': 'buy_num',
    '营业日': 'date'
}

# 遍历映射规则，将数据从 merged_df 复制到 gassales_df
for source_col, target_col in mapping.items():
    if isinstance(target_col, list):
        for col in target_col:
            gassales_df[col] = merged_df[source_col]
    else:
        gassales_df[target_col] = merged_df[source_col]

# 获取当前日期并格式化为指定格式
current_date = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

# 填充 created_at 和 updated_at 列
gassales_df['created_at'] = current_date
gassales_df['updated_at'] = current_date

# 填充 created_by_id 和 updated_by_id 列
gassales_df['created_by_id'] = 1
gassales_df['updated_by_id'] = 1

# 保存 gassales_df 到 gassales.xlsx 文件
gassales_df.to_excel('C:\\Users\\22565\\Documents\\石油导入数据\\gassales_0227.xlsx', index=False)

print("数据填充完成，已保存到 gassales.xlsx")