import pandas as pd
from datetime import datetime

# 定义 erpsall.xlsx 的列名
columns = [
    'created_at', 'updated_at', 'id', 'created_by_id', 'updated_by_id',
    'oil_no', 'oil_name', 'evalyation', 'out_date', 'custom',
    'out_quantity', 'unit_name', 'custom_no', 'sall_no', 'sall_name',
    'sall_des', 'sall_order', 'sall'
]

# 创建一个空的 DataFrame 用于存储 erpsall.xlsx 的数据
erpsall_df = pd.DataFrame(columns=columns)

# 读取 erpsall_template.xlsx 文件
try:
    # 读取 erpsall_template.xlsx 文件，手动指定引擎为 openpyxl
    template_df = pd.read_excel('C:\\Users\\22565\\Documents\\石油导入数据\\导入\\ERP0331.xlsx', engine='openpyxl')
except FileNotFoundError:
    print("未找到 erpsall_template.xlsx 文件，请检查文件路径。")
    exit(1)

# 定义列名映射规则
mapping = {
    '油品代码': 'oil_no',
    '名称规格': 'oil_name',
    '评估类型': 'evalyation',
    '出库日期': 'out_date',
    '客户全称': 'custom',
    '出库单数量': 'out_quantity',
    '业务单元名称': 'unit_name',
    '客户代码': 'custom_no',
    '销售流向': 'sall_no',
    '销售流向名称': 'sall_name',
    '销售主体': 'sall',
    '销售主体描述': 'sall_des',
    '销售订单': 'sall_order'
}

# 遍历映射规则，将数据从 template_df 复制到 erpsall_df
for source_col, target_col in mapping.items():
    erpsall_df[target_col] = template_df[source_col]

# 过滤掉 out_date 列为空的记录
erpsall_df = erpsall_df[erpsall_df['out_date'].notna()]

# 将 paytime 列中的 "." 替换为 "/"
if 'out_date' in erpsall_df.columns:
    erpsall_df['out_date'] = erpsall_df['out_date'].astype(str).str.replace('.', '/', regex=False)

# 生成 id 列，从 1313928 开始递增
num_rows = len(erpsall_df)
erpsall_df['id'] = range(167072, 167072 + num_rows)

# 获取当前日期并格式化为指定格式
current_date = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

# 填充 created_at 和 updated_at 列
erpsall_df['created_at'] = current_date
erpsall_df['updated_at'] = current_date

# 填充 created_by_id 和 updated_by_id 列
erpsall_df['created_by_id'] = 1
erpsall_df['updated_by_id'] = 1

# 保存 erpsall_df 到 erpsall.xlsx 文件
erpsall_df.to_excel('C:\\Users\\22565\\Documents\\石油导入数据\\ERP0331.xlsx', index=False)

print("数据填充完成，已保存到 erpsall.xlsx")
