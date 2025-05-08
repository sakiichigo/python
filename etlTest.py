import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime

# =====================
# 数据库配置
# =====================
DB_CONFIG = {
    "user": "zorkdata",
    "host": "10.204.20.18",
    "dbname": "biz_data",
    "password": "Zorkdata@2025",
    "port": 54321
}

# =====================
# 1. 数据库连接与数据读取
# =====================
def get_db_data(query, params=None):
    """执行 SQL 查询并返回 DataFrame（支持参数化查询）"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        query_str = query.as_string(conn)  # 转换为字符串
        return pd.read_sql(query_str, conn, params=params)  # 传递参数
    finally:
        conn.close()

# =====================
# 2. 核心数据处理逻辑
# =====================

# ----------
# 2.1 获取当前月份信息
# ----------
current_month_info = pd.DataFrame({
    'curr_year': [datetime.today().year],
    'curr_month': [datetime.today().month],
    'days_in_month': [(datetime(datetime.today().year, datetime.today().month + 1, 1) - datetime(datetime.today().year, datetime.today().month, 1)).days],
    'days_passed': [datetime.today().day]
})
cmi = current_month_info.iloc[0]
days_in_month = cmi['days_in_month']
days_passed = cmi['days_passed']
# ----------
# 2.2 读取基础销售数据
# ----------
# 读取 biz_sales_plan 和 biz_sales_and_plan 表
biz_sales_plan_query = sql.SQL("""
    SELECT * FROM biz_sales_plan
""")
biz_sales_plan = get_db_data(biz_sales_plan_query)
current_date = datetime.today()
start_of_month = current_date.replace(day=1)
# 读取 biz_sales_and_plan 表，并筛选当前月数据
biz_sales_and_plan_query = sql.SQL("""
    SELECT * FROM biz_sales_and_plan
    WHERE product IN ('汽油', '柴油')
      AND date_time BETWEEN %s AND %s  -- 筛选当前月 1 日至今日
""")
biz_sales_and_plan = get_db_data(
    biz_sales_and_plan_query,
    params=(start_of_month.strftime('%Y-%m-%d'), current_date.strftime('%Y-%m-%d'))
)

# ----------
# 2.3 计算销售计划（sales_plan）
# ----------
cmi = current_month_info.iloc[0]
sales_plan = pd.DataFrame({
    'refined_oil_sales': (biz_sales_plan['refined_oil_sales'] * 10000 / cmi['days_in_month']).astype(float),
    'gasoline_sales': (biz_sales_plan['gasoline_sales'] * 10000 / cmi['days_in_month']).astype(float),
    'diesel_sales': (biz_sales_plan['diesel_sales'] * 10000 / cmi['days_in_month']).astype(float),
    'direct_bulk_sales': (biz_sales_plan['direct_bulk_sales'] * 10000 / cmi['days_in_month']).astype(float),
    'direct_bulk_gasoline_sales': (biz_sales_plan['direct_bulk_gasoline_sales'] * 10000 / cmi['days_in_month']).astype(float),
    'direct_bulk_diesel_sales': (biz_sales_plan['direct_bulk_diesel_sales'] * 10000 / cmi['days_in_month']).astype(float),
    'mutual_supply_sales': (biz_sales_plan['mutual_supply_sales'] * 10000 / cmi['days_in_month']).astype(float),
    'mutual_supply_gasoline_sales': (biz_sales_plan['mutual_supply_gasoline_sales'] * 10000 / cmi['days_in_month']).astype(float),
    'mutual_supply_diesel_sales': (biz_sales_plan['mutual_supply_diesel_sales'] * 10000 / cmi['days_in_month']).astype(float),
    'self_operated_pure_gun_sales': (biz_sales_plan['self_operated_pure_gun_sales'] * 10000 / cmi['days_in_month']).astype(float),
    'self_operated_pure_gun_gasoline_sales': (biz_sales_plan['self_operated_pure_gun_gasoline_sales'] * 10000 / cmi['days_in_month']).astype(float),
    'self_operated_pure_gun_diesel_sales': (biz_sales_plan['self_operated_pure_gun_diesel_sales'] * 10000 / cmi['days_in_month']).astype(float),
    'lng_sales': (biz_sales_plan['lng_sales'] * 10000 / cmi['days_in_month']).astype(float),
}, index=[0])  # 假设单条销售计划记录

# ----------
# 2.4 处理销量数据（sales_volume_plan）
# ----------
biz_sales_and_plan['分类'] = biz_sales_and_plan['company_name'].apply(
    lambda x: '互供' if x == '中石化' else '自营'
)
sales_volume_plan = biz_sales_and_plan.groupby([
    'date_time', 'company_name', 'product', 'sales_method', '分类'
]).agg({
    'current_period': 'sum',
    'yoy_value': 'sum',
    'plan_value': 'sum'
}).reset_index()
sales_volume_plan['超欠数值'] = sales_volume_plan['current_period'] - sales_volume_plan['plan_value']

# 新增汽柴油数据到 sales_volume_plan
gas_diesel_volume = sales_volume_plan[sales_volume_plan['product'].isin(['汽油', '柴油'])].groupby([
    'date_time', 'company_name', 'sales_method', '分类'
]).agg({
    'current_period': 'sum',
    'yoy_value': 'sum',
    'plan_value': 'sum'
}).reset_index()
gas_diesel_volume['product'] = '汽柴油'
sales_volume_plan = pd.concat([sales_volume_plan, gas_diesel_volume], ignore_index=True)

# ----------
# 2.5 通用板块数据计算函数
# ----------
def process_plate_data(base_df, sales_plan_row, days_passed, exclude_project=None, sales_method=None):
    """
    计算板块计划完成度数据
    :param exclude_project: 排除的项目（如 '油田自用'）
    :param sales_method: 限定销售方式（如 '纯枪'）
    """
    df = base_df.copy()
    # 过滤条件
    if exclude_project:
        df = df[df['company_name'] != exclude_project]
    if sales_method:
        df = df[df['sales_method'] == sales_method]

    # 计算板块计划进度值（与 SQL 中的 CASE 逻辑对应）
    def calculate_plan_progress(row):
        if row['product'] == '汽油':
            if row['分类'] == '自营' and row['sales_method'] == '直批':
                return sales_plan_row['direct_bulk_gasoline_sales'] * days_passed
            elif row['分类'] == '自营' and row['sales_method'] == '纯枪':
                return sales_plan_row['self_operated_pure_gun_gasoline_sales'] * days_passed
            elif row['分类'] == '互供':
                return sales_plan_row['mutual_supply_gasoline_sales'] * days_passed
        elif row['product'] == '柴油':
            if row['分类'] == '自营' and row['sales_method'] == '直批':
                return sales_plan_row['direct_bulk_diesel_sales'] * days_passed
            elif row['分类'] == '自营' and row['sales_method'] == '纯枪':
                return sales_plan_row['self_operated_pure_gun_diesel_sales'] * days_passed
            elif row['分类'] == '互供':
                return sales_plan_row['mutual_supply_diesel_sales'] * days_passed
        elif row['product'] == '汽柴油':
            # 避免递归调用，直接计算
            gasoline_row = row.copy()
            gasoline_row['product'] = '汽油'
            gasoline_plan = calculate_plan_progress(gasoline_row)

            diesel_row = row.copy()
            diesel_row['product'] = '柴油'
            diesel_plan = calculate_plan_progress(diesel_row)

            return gasoline_plan + diesel_plan
        return 0.0

    df['板块计划进度值'] = df.apply(calculate_plan_progress, axis=1)

    # 计算聚合指标（计划吨数、完成吨数等）
    df['计划吨数'] = df.groupby('product')['板块计划进度值'].transform('max')
    df['完成吨数'] = df.groupby('product')['current_period'].transform('sum')
    df['欠进度'] = df.apply(lambda x: (x['完成吨数'] - x['计划吨数']) / x['计划吨数'] if x['计划吨数'] != 0 else None, axis=1)
    df['欠量吨数'] = df['完成吨数'] - df['计划吨数']

    # 去重并保留最终指标
    return df[['product', '计划吨数', '完成吨数', '欠进度', '欠量吨数']].drop_duplicates()

# ----------
# 2.6 通用公司自排计划数据计算函数
# ----------
def process_company_self_plan_data(base_df, exclude_project=None, sales_method=None):
    """
    计算公司自排计划完成度数据
    :param exclude_project: 排除的项目（如 '油田自用'）
    :param sales_method: 限定销售方式（如 '纯枪'）
    """
    df = base_df.copy()
    # 过滤条件
    if exclude_project:
        df = df[df['company_name'] != exclude_project]
    if sales_method:
        df = df[df['sales_method'] == sales_method]

    # 计算聚合指标（计划吨数、完成吨数等）
    df['计划吨数'] = df.groupby('product')['plan_value'].transform('sum')
    df['完成吨数'] = df.groupby('product')['current_period'].transform('sum')
    df['欠进度'] = df.apply(lambda x: (x['完成吨数'] - x['计划吨数']) / x['计划吨数'] if x['计划吨数'] != 0 else None, axis=1)
    df['欠量吨数'] = df['完成吨数'] - df['计划吨数']

    # 去重并保留最终指标
    return df[['product', '计划吨数', '完成吨数', '欠进度', '欠量吨数']].drop_duplicates()

## 2.7 生成各板块结果
sales_plan_row = sales_plan.iloc[0]
days_passed = cmi['days_passed']

# 板块月度计划含串换总量（含所有项目）
plate_include = process_plate_data(sales_volume_plan, sales_plan_row, days_passed)

# 检查 plate_include 数据框是否包含三种油品的数据
required_products = ['汽油', '柴油', '汽柴油']
if plate_include.empty or not all(product in plate_include['product'].values for product in required_products):
    plate_include_result = pd.DataFrame({
        '项目分类': ['板块月度计划含串换总量'] * 3,
        '油品': ['汽油', '柴油', '汽柴油'],
        '计划吨数': [0] * 3,
        '完成吨数': [0] * 3,
        '欠进度': [None] * 3,
        '欠量吨数': [0] * 3
    })
else:
    plate_include_result = pd.DataFrame({
        '项目分类': ['板块月度计划含串换总量'] * 3,
        '油品': ['汽油', '柴油', '汽柴油'],
        **plate_include.set_index('product').to_dict('list')
    })

# 板块月度计划不含串换总量（排除油田自用）
plate_exclude = process_plate_data(sales_volume_plan, sales_plan_row, days_passed, exclude_project='油田自用')
if plate_exclude.empty:
    plate_exclude_result = pd.DataFrame({
        '项目分类': ['板块月度计划不含串换总量'] * 3,
        '油品': ['汽油', '柴油', '汽柴油'],
        '计划吨数': [0] * 3,
        '完成吨数': [0] * 3,
        '欠进度': [None] * 3,
        '欠量吨数': [0] * 3
    })
else:
    plate_exclude_result = pd.DataFrame({
        '项目分类': ['板块月度计划不含串换总量'] * 3,
        '油品': ['汽油', '柴油', '汽柴油'],
        **plate_exclude.set_index('product').to_dict('list')
    })

# 板块月度计划纯枪数据（仅限纯枪销售方式）
plate_pure_gun = process_plate_data(sales_volume_plan, sales_plan_row, days_passed, sales_method='纯枪')
if plate_pure_gun.empty:
    plate_pure_gun_result = pd.DataFrame({
        '项目分类': ['板块月度计划纯枪'] * 3,
        '油品': ['汽油', '柴油', '汽柴油'],
        '计划吨数': [0] * 3,
        '完成吨数': [0] * 3,
        '欠进度': [None] * 3,
        '欠量吨数': [0] * 3
    })
else:
    plate_pure_gun_result = pd.DataFrame({
        '项目分类': ['板块月度计划纯枪'] * 3,
        '油品': ['汽油', '柴油', '汽柴油'],
        **plate_pure_gun.set_index('product').to_dict('list')
    })

# 公司自排含串换总量
company_self_include = process_company_self_plan_data(sales_volume_plan)
if company_self_include.empty:
    company_self_include_result = pd.DataFrame({
        '项目分类': ['公司自排含串换总量'] * 3,
        '油品': ['汽油', '柴油', '汽柴油'],
        '计划吨数': [0] * 3,
        '完成吨数': [0] * 3,
        '欠进度': [None] * 3,
        '欠量吨数': [0] * 3
    })
else:
    company_self_include_result = pd.DataFrame({
        '项目分类': ['公司自排含串换总量'] * 3,
        '油品': ['汽油', '柴油', '汽柴油'],
        **company_self_include.set_index('product').to_dict('list')
    })

# 公司自排不含串换总量（排除油田自用）
company_self_exclude = process_company_self_plan_data(sales_volume_plan, exclude_project='油田自用')
if company_self_exclude.empty:
    company_self_exclude_result = pd.DataFrame({
        '项目分类': ['公司自排不含串换总量'] * 3,
        '油品': ['汽油', '柴油', '汽柴油'],
        '计划吨数': [0] * 3,
        '完成吨数': [0] * 3,
        '欠进度': [None] * 3,
        '欠量吨数': [0] * 3
    })
else:
    company_self_exclude_result = pd.DataFrame({
        '项目分类': ['公司自排不含串换总量'] * 3,
        '油品': ['汽油', '柴油', '汽柴油'],
        **company_self_exclude.set_index('product').to_dict('list')
    })

# 公司自排纯枪数据（仅限纯枪销售方式）
company_self_pure_gun = process_company_self_plan_data(sales_volume_plan, sales_method='纯枪')
if company_self_pure_gun.empty:
    company_self_pure_gun_result = pd.DataFrame({
        '项目分类': ['公司自排纯枪'] * 3,
        '油品': ['汽油', '柴油', '汽柴油'],
        '计划吨数': [0] * 3,
        '完成吨数': [0] * 3,
        '欠进度': [None] * 3,
        '欠量吨数': [0] * 3
    })
else:
    company_self_pure_gun_result = pd.DataFrame({
        '项目分类': ['公司自排纯枪'] * 3,
        '油品': ['汽油', '柴油', '汽柴油'],
        **company_self_pure_gun.set_index('product').to_dict('list')
    })

# =====================
# 3. 社会总量数据
# =====================
# =====================
# 3. 社会总量数据（新增社会总量纯枪统计）
# =====================
# 原始社会总量（含所有销售方式）
social_data_all = sales_volume_plan.groupby('product').agg({
    'current_period': 'sum',
    'yoy_value': 'sum',
    'plan_value': 'sum'
}).reset_index()

# 新增：社会总量纯枪（仅纯枪销售方式）
social_data_pure_gun = sales_volume_plan[sales_volume_plan['sales_method'] == '纯枪'].groupby('product').agg({
    'current_period': 'sum',
    'yoy_value': 'sum',
    'plan_value': 'sum'
}).reset_index()
social_data_pure_gun['项目分类'] = '社会总量纯枪'

# 合并原始社会总量和纯枪数据
social_data = pd.concat([
    social_data_all.rename(columns={'current_period': '完成吨数', 'yoy_value': '上年同期吨数', 'plan_value': '计划吨数'}),
    social_data_pure_gun.rename(columns={'current_period': '完成吨数', 'yoy_value': '上年同期吨数', 'plan_value': '计划吨数'})
], ignore_index=True)

# 统一处理指标（同比、欠量等）
social_data['同比增加吨数'] = social_data['完成吨数'] - social_data['上年同期吨数']
social_data['同比增加百分比'] = social_data.apply(
    lambda x: (x['同比增加吨数'] / x['上年同期吨数']) if x['上年同期吨数'] != 0 else None, axis=1
)
social_data['项目分类'] = social_data['项目分类'].fillna('社会总量')  # 补全原始社会总量的分类名
social_data['欠量吨数'] = social_data['完成吨数'] - social_data['计划吨数']
social_data = social_data[['项目分类', 'product', '计划吨数', '完成吨数', '同比增加吨数', '同比增加百分比', '欠量吨数']]
social_data.columns = ['项目分类', '油品', '计划吨数', '完成吨数', '同比增加吨数', '同比增加百分比', '欠量吨数']
# ... 前面代码保持不变，直到社会总量数据统计部分修改后 ...

# =====================
# 4. 合并所有结果（模拟 UNION ALL）
# =====================
final_result = pd.concat([
    # 社会总量结果
    social_data,

    # 板块结果（含串换、不含串换、纯枪）
    plate_include_result,
    plate_exclude_result,
    plate_pure_gun_result,

    # 公司自排结果（含串换、不含串换、纯枪）
    company_self_include_result,
    company_self_exclude_result,
    company_self_pure_gun_result
], ignore_index=True)

# 提取汽油、柴油和汽柴油数据
gasoline_data = final_result[final_result['油品'] == '汽油']
diesel_data = final_result[final_result['油品'] == '柴油']
gas_diesel_data = final_result[final_result['油品'] == '汽柴油']
# 新增：提取社会总量纯枪数据
social_pure_gun_gasoline_data = final_result[(final_result['油品'] == '汽油') & (final_result['项目分类'] == '社会总量纯枪')]
social_pure_gun_diesel_data = final_result[(final_result['油品'] == '柴油') & (final_result['项目分类'] == '社会总量纯枪')]
social_pure_gun_gas_diesel_data = final_result[(final_result['油品'] == '汽柴油') & (final_result['项目分类'] == '社会总量纯枪')]

# 计算时间进度（假设这里根据 days_passed 和 days_in_month 计算，需根据实际情况修改）
time_progress = (days_passed / days_in_month) * 100

# 定义输出函数

def print_plan_completion(section_name, include_data, exclude_data, pure_gun_data):
    content = []
    content.append(f"{section_name}：时间进度{time_progress:.1f}%")
    counter = 1  # 初始化计数器

    def print_subsection(subsection_name, data):
        nonlocal counter  # 使用 nonlocal 关键字来修改外部函数的变量
        # 添加空值和 None 判断
        if data.empty:
            total_plan = 0
            total_complete = 0
            total_shortfall_rate = 0.0
            total_shortfall_amount = 0
        else:
            total_plan = data[data['项目分类'].str.contains(subsection_name)]['计划吨数'].values[0]
            total_complete = data[data['项目分类'].str.contains(subsection_name)]['完成吨数'].values[0]
            # 判断欠进度是否为 None
            shortfall_rate = data[data['项目分类'].str.contains(subsection_name)]['欠进度'].values[0]
            total_shortfall_rate = shortfall_rate * 100 if shortfall_rate is not None else 0.0
            total_shortfall_amount = data[data['项目分类'].str.contains(subsection_name)]['欠量吨数'].values[0]

        # 汽油部分同理添加 None 判断
        gasoline_data_sub = gasoline_data[gasoline_data['项目分类'].str.contains(subsection_name)]
        if gasoline_data_sub.empty:
            gasoline_plan = 0
            gasoline_complete = 0
            gasoline_shortfall_rate = 0.0
            gasoline_shortfall_amount = 0
        else:
            gasoline_plan = gasoline_data_sub['计划吨数'].values[0]
            gasoline_complete = gasoline_data_sub['完成吨数'].values[0]
            gs = gasoline_data_sub['欠进度'].values[0]
            gasoline_shortfall_rate = gs * 100 if gs is not None else 0.0
            gasoline_shortfall_amount = gasoline_data_sub['欠量吨数'].values[0]

        # 柴油部分同理添加 None 判断
        diesel_data_sub = diesel_data[diesel_data['项目分类'].str.contains(subsection_name)]
        if diesel_data_sub.empty:
            diesel_plan = 0
            diesel_complete = 0
            diesel_shortfall_rate = 0.0
            diesel_shortfall_amount = 0
        else:
            diesel_plan = diesel_data_sub['计划吨数'].values[0]
            diesel_complete = diesel_data_sub['完成吨数'].values[0]
            ds = diesel_data_sub['欠进度'].values[0]
            diesel_shortfall_rate = ds * 100 if ds is not None else 0.0
            diesel_shortfall_amount = diesel_data_sub['欠量吨数'].values[0]

        line = f"1.{counter} {subsection_name}计划{total_plan:.0f}吨,完成{total_complete:.0f}吨,欠进度{total_shortfall_rate:.1f}%、欠量{total_shortfall_amount:.0f}吨：其中汽油计划{gasoline_plan:.0f}吨,完成{gasoline_complete:.0f}吨,欠进度{gasoline_shortfall_rate:.1f}%、欠量{gasoline_shortfall_amount:.0f}吨,柴油计划{diesel_plan:.0f}吨,完成{diesel_complete:.0f}吨,欠进度{diesel_shortfall_rate:.1f}%、欠量{diesel_shortfall_amount:.0f}吨。"
        content.append(line)
        counter += 1  # 计数器自增

    print_subsection("含串换总量", include_data)
    print_subsection("不含串换总量", exclude_data)
    print_subsection("纯枪", pure_gun_data)
    result_df = pd.DataFrame({
        'name': [section_name],
        'content': ["\n".join(content)]
    })
    return result_df



# 板块月度计划完成情况
result = print_plan_completion("一、板块月度计划完成情况", plate_include_result, plate_exclude_result,
                               plate_pure_gun_result)


# 公司自排计划完成情况
company_self_content = []
company_self_result = print_plan_completion("二、公司自排计划完成情况", company_self_include_result, company_self_exclude_result, company_self_pure_gun_result)
company_self_content.extend(company_self_result['content'].values[0].split('\n'))

# 月度销量同比情况
social_total = social_data[social_data['项目分类'] == '社会总量']
social_pure_gun_total = social_data[social_data['项目分类'] == '社会总量纯枪']

# 社会总量
if not social_total.empty:
    total_completion = social_total['完成吨数'].values[0]
    total_shortfall = social_total['欠量吨数'].values[0]
    total_yoy_percentage = social_total['同比增加百分比'].values[0]
    total_yoy_amount = social_total['同比增加吨数'].values[0]
else:
    total_completion = 0
    total_shortfall = 0
    total_yoy_percentage = 0
    total_yoy_amount = 0

gasoline_data_sub = gasoline_data[gasoline_data['项目分类'] == '社会总量']
if not gasoline_data_sub.empty:
    gasoline_completion = gasoline_data_sub['完成吨数'].values[0]
    gasoline_shortfall = gasoline_data_sub['欠量吨数'].values[0]
    gasoline_yoy_percentage = gasoline_data_sub['同比增加百分比'].values[0]
    gasoline_yoy_amount = gasoline_data_sub['同比增加吨数'].values[0]
else:
    gasoline_completion = 0
    gasoline_shortfall = 0
    gasoline_yoy_percentage = 0
    gasoline_yoy_amount = 0

diesel_data_sub = diesel_data[diesel_data['项目分类'] == '社会总量']
if not diesel_data_sub.empty:
    diesel_completion = diesel_data_sub['完成吨数'].values[0]
    diesel_shortfall = diesel_data_sub['欠量吨数'].values[0]
    diesel_yoy_percentage = diesel_data_sub['同比增加百分比'].values[0]
    diesel_yoy_amount = diesel_data_sub['同比增加吨数'].values[0]
else:
    diesel_completion = 0
    diesel_shortfall = 0
    diesel_yoy_percentage = 0
    diesel_yoy_amount = 0

# 新增：社会总量纯枪数据
if not social_pure_gun_total.empty:
    pure_gun_total_completion = social_pure_gun_total['完成吨数'].values[0]
    pure_gun_total_shortfall = social_pure_gun_total['欠量吨数'].values[0]
    pure_gun_total_yoy_percentage = social_pure_gun_total['同比增加百分比'].values[0]
    pure_gun_total_yoy_amount = social_pure_gun_total['同比增加吨数'].values[0]
else:
    pure_gun_total_completion = 0
    pure_gun_total_shortfall = 0
    pure_gun_total_yoy_percentage = 0
    pure_gun_total_yoy_amount = 0

social_pure_gun_gasoline_data_sub = social_pure_gun_gasoline_data[social_pure_gun_gasoline_data['项目分类'] == '社会总量纯枪']
if not social_pure_gun_gasoline_data_sub.empty:
    pure_gun_gasoline_completion = social_pure_gun_gasoline_data_sub['完成吨数'].values[0]
    pure_gun_gasoline_shortfall = social_pure_gun_gasoline_data_sub['欠量吨数'].values[0]
    pure_gun_gasoline_yoy_percentage = social_pure_gun_gasoline_data_sub['同比增加百分比'].values[0]
    pure_gun_gasoline_yoy_amount = social_pure_gun_gasoline_data_sub['同比增加吨数'].values[0]
else:
    pure_gun_gasoline_completion = 0
    pure_gun_gasoline_shortfall = 0
    pure_gun_gasoline_yoy_percentage = 0
    pure_gun_gasoline_yoy_amount = 0

social_pure_gun_diesel_data_sub = social_pure_gun_diesel_data[social_pure_gun_diesel_data['项目分类'] == '社会总量纯枪']
if not social_pure_gun_diesel_data_sub.empty:
    pure_gun_diesel_completion = social_pure_gun_diesel_data_sub['完成吨数'].values[0]
    pure_gun_diesel_shortfall = social_pure_gun_diesel_data_sub['欠量吨数'].values[0]
    pure_gun_diesel_yoy_percentage = social_pure_gun_diesel_data_sub['同比增加百分比'].values[0]
    pure_gun_diesel_yoy_amount = social_pure_gun_diesel_data_sub['同比增加吨数'].values[0]
else:
    pure_gun_diesel_completion = 0
    pure_gun_diesel_shortfall = 0
    pure_gun_diesel_yoy_percentage = 0
    pure_gun_diesel_yoy_amount = 0

monthly_sales_content = []
monthly_sales_content.append("\n三、月度销量同比情况：")
monthly_sales_content.append(
    f"3 社会总量完成{total_completion:.0f}吨,其中汽油{gasoline_completion:.0f}吨,柴油{diesel_completion:.0f}吨。欠计划量{total_shortfall:.0f}吨,其中汽油欠计划{gasoline_shortfall:.0f}吨,柴油欠计划{diesel_shortfall:.0f}吨。同比增{total_yoy_percentage:.1f}%,增{total_yoy_amount:.0f}吨,其中汽油增{gasoline_yoy_percentage:.1f}%,增{gasoline_yoy_amount:.0f}吨,柴油增{diesel_yoy_percentage:.1f}%,增{diesel_yoy_amount:.0f}吨；")
monthly_sales_content.append(
    f"   社会总量纯枪完成{pure_gun_total_completion:.0f}吨,其中汽油{pure_gun_gasoline_completion:.0f}吨,柴油{pure_gun_diesel_completion:.0f}吨。欠计划量{pure_gun_total_shortfall:.0f}吨,其中汽油欠计划{pure_gun_gasoline_shortfall:.0f}吨,柴油欠计划{pure_gun_diesel_shortfall:.0f}吨。同比增{pure_gun_total_yoy_percentage:.1f}%,增{pure_gun_total_yoy_amount:.0f}吨,其中汽油增{pure_gun_gasoline_yoy_percentage:.1f}%,增{pure_gun_gasoline_yoy_amount:.0f}吨,柴油增{pure_gun_diesel_yoy_percentage:.1f}%,增{pure_gun_diesel_yoy_amount:.0f}吨；")

# 生成公司自排计划完成情况的数据行
company_row = pd.DataFrame({
    'name': ['公司自排计划完成情况'],
    'content': ["\n".join(company_self_content)]
})

# 生成月度销量同比情况的数据行
sales_row = pd.DataFrame({
    'name': ['月度销量同比情况'],
    'content': ["\n".join(monthly_sales_content)]
})

# 合并两个数据行
new_row = pd.concat([company_row, sales_row], ignore_index=True)

# 使用 pd.concat 添加新行
result = pd.concat([result, new_row], ignore_index=True)

def insert_or_update_data(result):
    """插入或更新数据到 proofs_text 表"""
    try:
        # 建立数据库连接
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for index, row in result.iterrows():
            name = row['name']
            content = row['content']

            # 检查 name 是否存在
            check_query = "SELECT * FROM proofs_text WHERE name = %s"
            cursor.execute(check_query, (name,))
            existing_record = cursor.fetchone()

            if existing_record:
                # 如果存在，更新 content
                update_query = "UPDATE proofs_text SET content = %s WHERE name = %s"
                cursor.execute(update_query, (content, name))
            else:
                # 如果不存在，插入新记录
                insert_query = "INSERT INTO proofs_text (name, content) VALUES (%s, %s)"
                cursor.execute(insert_query, (name, content))

        # 提交事务
        conn.commit()
        print("数据插入或更新成功")

    except (Exception, psycopg2.Error) as error:
        print(f"插入或更新数据时出现错误: {error}")
    finally:
        # 关闭数据库连接
        if conn:
            cursor.close()
            conn.close()
            print("数据库连接已关闭")

insert_or_update_data(result)
for content in result['content'].values:
    print(content)

