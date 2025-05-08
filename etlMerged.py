import psycopg2
import pandas as pd
from datetime import datetime, timedelta

# 假设的数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'your_database',
    'user': 'your_user',
    'password': 'your_password'
}

# 获取当前日期、前一日、本月第一天、上月第一天、今年第一天、去年第一天
current_date = datetime.now().date()
previous_day = current_date - timedelta(days=1)
first_day_of_month = current_date.replace(day=1)
first_day_of_last_month = (first_day_of_month - timedelta(days=1)).replace(day=1)
first_day_of_year = current_date.replace(month=1, day=1)
first_day_of_last_year = (first_day_of_year - timedelta(days=1)).replace(month=1, day=1)

# 时间进度，假设为一个示例值
time_progress = 50.0

# 假设的汽油和柴油数据
gasoline_data = pd.DataFrame()
diesel_data = pd.DataFrame()


# 执行 SQL 查询并返回 DataFrame（支持参数化查询）
def get_db_data(query, params=None):
    """执行 SQL 查询并返回 DataFrame（支持参数化查询）"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        query_str = query.as_string(conn)  # 转换为字符串
        return pd.read_sql(query_str, conn, params=params)  # 传递参数
    finally:
        conn.close()


# 计算计划进度
def calculate_plan_progress(row, sales_plan_row, days_passed):
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
        gasoline_plan = calculate_plan_progress(gasoline_row, sales_plan_row, days_passed)

        diesel_row = row.copy()
        diesel_row['product'] = '柴油'
        diesel_plan = calculate_plan_progress(diesel_row, sales_plan_row, days_passed)

        return gasoline_plan + diesel_plan
    return 0.0


# 计算板块计划完成度数据
def process_plate_data(base_df, sales_plan_row, days_passed, exclude_project=None, sales_method=None):
    df = base_df.copy()
    # 过滤条件
    if exclude_project:
        df = df[df['company_name'] != exclude_project]
    if sales_method:
        df = df[df['sales_method'] == sales_method]

    # 计算板块计划进度值（与 SQL 中的 CASE 逻辑对应）
    df['板块计划进度值'] = df.apply(lambda row: calculate_plan_progress(row, sales_plan_row, days_passed), axis=1)

    # 计算聚合指标（计划吨数、完成吨数等）
    df['计划吨数'] = df.groupby('product')['板块计划进度值'].transform('max')
    df['完成吨数'] = df.groupby('product')['current_period'].transform('sum')
    df['欠进度'] = df.apply(lambda x: (x['完成吨数'] - x['计划吨数']) / x['计划吨数'] if x['计划吨数'] != 0 else None,
                            axis=1)
    df['欠量吨数'] = df['完成吨数'] - df['计划吨数']

    # 去重并保留最终指标
    return df[['product', '计划吨数', '完成吨数', '欠进度', '欠量吨数']].drop_duplicates()


# 计算公司自排计划完成度数据
def process_company_self_plan_data(base_df, exclude_project=None, sales_method=None):
    df = base_df.copy()
    # 过滤条件
    if exclude_project:
        df = df[df['company_name'] != exclude_project]
    if sales_method:
        df = df[df['sales_method'] == sales_method]

    # 计算聚合指标（计划吨数、完成吨数等）
    df['计划吨数'] = df.groupby('product')['plan_value'].transform('sum')
    df['完成吨数'] = df.groupby('product')['current_period'].transform('sum')
    df['欠进度'] = df.apply(lambda x: (x['完成吨数'] - x['计划吨数']) / x['计划吨数'] if x['计划吨数'] != 0 else None,
                            axis=1)
    df['欠量吨数'] = df['完成吨数'] - df['计划吨数']

    # 去重并保留最终指标
    return df[['product', '计划吨数', '完成吨数', '欠进度', '欠量吨数']].drop_duplicates()


# 打印子部分信息
def print_subsection(subsection_name, data):
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

    print(
        f"1.1 {subsection_name}计划{total_plan:.0f}吨,完成{total_complete:.0f}吨,欠进度{total_shortfall_rate:.1f}%、欠量{total_shortfall_amount:.0f}吨：其中汽油计划{gasoline_plan:.0f}吨,完成{gasoline_complete:.0f}吨,欠进度{gasoline_shortfall_rate:.1f}%、欠量{gasoline_shortfall_amount:.0f}吨,柴油计划{diesel_plan:.0f}吨,完成{diesel_complete:.0f}吨,欠进度{diesel_shortfall_rate:.1f}%、欠量{diesel_shortfall_amount:.0f}吨。")


# 打印计划完成情况
def print_plan_completion(section_name, include_data, exclude_data, pure_gun_data):
    print(f"一、{section_name}：时间进度{time_progress:.1f}%")
    print_subsection("含串换总量", include_data)
    print_subsection("不含串换总量", exclude_data)
    print_subsection("纯枪", pure_gun_data)


# 计算周期数据
def calculate_period_data(data):
    # 处理合并后的列名（油品数值列用_x，燃气数值列用_y）
    result = {
        "当日含串换油气总量": data[(data['日期'] == current_date)][
            ['油品含串换量', '燃气含串换量']].sum().sum(),
        "当日不含串换油气总量": data[(data['日期'] == current_date)][
            ['油品不含串换量', '燃气不含串换量']].sum().sum(),
        "前一日含串换油气总量": data[(data['日期'] == previous_day)][
            ['油品含串换量', '燃气含串换量']].sum().sum(),
        "前一日不含串换油气总量": data[(data['日期'] == previous_day)][
            ['油品不含串换量', '燃气不含串换量']].sum().sum(),
        "当月含串换油气总量": data[(data['日期'] >= first_day_of_month) & (data['日期'] <= current_date)][
            ['油品含串换量', '燃气含串换量']].sum().sum(),
        "上月含串换油气总量": data[(data['日期'] >= first_day_of_last_month) & (
                data['日期'] <= first_day_of_month - timedelta(days=1))][
            ['油品含串换量', '燃气含串换量']].sum().sum(),
        "当月不含串换油气总量": data[(data['日期'] >= first_day_of_month) & (data['日期'] <= current_date)][
            ['油品不含串换量', '燃气不含串换量']].sum().sum(),
        "上月不含串换油气总量": data[(data['日期'] >= first_day_of_last_month) & (
                data['日期'] <= first_day_of_month - timedelta(days=1))][
            ['油品不含串换量', '燃气不含串换量']].sum().sum(),
        "当年含串换油气总量": data[(data['日期'] >= first_day_of_year) & (data['日期'] <= current_date)][
            ['油品含串换量', '燃气含串换量']].sum().sum(),
        "去年含串换油气总量": data[(data['日期'] >= first_day_of_last_year) & (
                data['日期'] <= first_day_of_year - timedelta(days=1))][
            ['油品含串换量', '燃气含串换量']].sum().sum(),
        "当年不含串换油气总量": data[(data['日期'] >= first_day_of_year) & (data['日期'] <= current_date)][
            ['油品不含串换量', '燃气不含串换量']].sum().sum(),
        "去年不含串换油气总量": data[(data['日期'] >= first_day_of_last_year) & (
                data['日期'] <= first_day_of_year - timedelta(days=1))][
            ['油品不含串换量', '燃气不含串换量']].sum().sum(),
        "当月成品油含串换总量": data[(data['日期'] >= first_day_of_month) & (data['日期'] <= current_date)][
            '油品含串换量'].sum(),
        "上月成品油含串换总量": data[(data['日期'] >= first_day_of_last_month) & (
                data['日期'] <= first_day_of_month - timedelta(days=1))]['油品含串换量'].sum(),
        "当月汽油含串换总量": data[(data['日期'] >= first_day_of_month) & (data['日期'] <= current_date) & (
                data['油品名称'] == '汽油')]['油品含串换量'].sum(),
        "上月汽油含串换总量": data[(data['日期'] >= first_day_of_last_month) & (
                data['日期'] <= first_day_of_month - timedelta(days=1)) & (
                                           data['油品名称'] == '汽油')]['油品含串换量'].sum(),
        "当月柴油含串换总量": data[(data['日期'] >= first_day_of_month) & (data['日期'] <= current_date) & (
                data['油品名称'] == '柴油')]['油品含串换量'].sum(),
        "上月柴油含串换总量": data[(data['日期'] >= first_day_of_last_month) & (
                data['日期'] <= first_day_of_month - timedelta(days=1)) & (
                                           data['油品名称'] == '柴油')]['油品含串换量'].sum(),
        "当月天然气含串换总量": data[(data['日期'] >= first_day_of_month) & (data['日期'] <= current_date)][
            '燃气含串换量'].sum(),
        "上月天然气含串换总量": data[(data['日期'] >= first_day_of_last_month) & (
                data['日期'] <= first_day_of_month - timedelta(days=1))]['燃气含串换量'].sum(),
        "当月成品油不含串换总量": data[(data['日期'] >= first_day_of_month) & (data['日期'] <= current_date)][
            '油品不含串换量'].sum(),
        "上月成品油不含串换总量": data[(data['日期'] >= first_day_of_last_month) & (
                data['日期'] <= first_day_of_month - timedelta(days=1))]['油品不含串换量'].sum(),
        "当月汽油不含串换总量": data[(data['日期'] >= first_day_of_month) & (data['日期'] <= current_date) & (
                data['油品名称'] == '汽油')]['油品不含串换量'].sum(),
        "上月汽油不含串换总量": data[(data['日期'] >= first_day_of_last_month) & (
                data['日期'] <= first_day_of_month - timedelta(days=1)) & (
                                             data['油品名称'] == '汽油')]['油品不含串换量'].sum(),
        "当月柴油不含串换总量": data[(data['日期'] >= first_day_of_month) & (data['日期'] <= current_date) & (
                data['油品名称'] == '柴油')]['油品不含串换量'].sum(),
        "上月柴油不含串换总量": data[(data['日期'] >= first_day_of_last_month) & (
                data['日期'] <= first_day_of_month - timedelta(days=1)) & (
                                             data['油品名称'] == '柴油')]['油品不含串换量'].sum(),
        "当月天然气不含串换总量": data[(data['日期'] >= first_day_of_month) & (data['日期'] <= current_date)][
            '燃气不含串换量'].sum(),
        "上月天然气不含串换总量": data[(data['日期'] >= first_day_of_last_month) & (
                data['日期'] <= first_day_of_month - timedelta(days=1))]['燃气不含串换量'].sum(),
        "当年成品油含串换总量": data[(data['日期'] >= first_day_of_year) & (data['日期'] <= current_date)][
            '油品含串换量'].sum(),
        "去年成品油含串换总量": data[(data['日期'] >= first_day_of_last_year) & (
                data['日期'] <= first_day_of_year - timedelta(days=1))]['油品含串换量'].sum(),
        "当年汽油含串换总量": data[(data['日期'] >= first_day_of_year) & (data['日期'] <= current_date) & (
                data['油品名称'] == '汽油')]['油品含串换量'].sum(),
        "去年汽油含串换总量": data[(data['日期'] >= first_day_of_last_year) & (
                data['日期'] <= first_day_of_year - timedelta(days=1)) & (
                                           data['油品名称'] == '汽油')]['油品含串换量'].sum(),
        "当年柴油含串换总量": data[(data['日期'] >= first_day_of_year) & (data['日期'] <= current_date) & (
                data['油品名称'] == '柴油')]['油品含串换量'].sum(),
        "去年柴油含串换总量": data[(data['日期'] >= first_day_of_last_year) & (
                data['日期'] <= first_day_of_year - timedelta(days=1)) & (
                                           data['油品名称'] == '柴油')]['油品含串换量'].sum()
    }
    return result


# 按指定格式输出结果
def print_result(final_result):
    print("五、日、月、年度油气当量同比情况（含串换）：")

    # 当日情况
    daily_with_swap = final_result[final_result['统计周期'] == '当日含串换量'].iloc[0]
    print(
        f"5.1 当日油气总量{int(round(daily_with_swap['油气总量'], 0))}吨,环比{'降' if daily_with_swap['油气总量环比'] < 0 else '增'}{round(abs(daily_with_swap['油气总量环比']), 2)}吨。"
        f"成品油{int(round(daily_with_swap['成品油总量'], 0))}吨,环比{'降' if daily_with_swap['成品油总量环比'] < 0 else '增'}{round(abs(daily_with_swap['成品油总量环比']), 2)}吨,"
        f"其中汽油{int(round(daily_with_swap['汽油吨数'], 0))}吨,环比{'降' if daily_with_swap['汽油环比'] < 0 else '增'}{round(abs(daily_with_swap['汽油环比']), 2)}吨,"
        f"柴油{int(round(daily_with_swap['柴油吨数'], 0))}吨,环比{'降' if daily_with_swap['柴油环比'] < 0 else '增'}{round(abs(daily_with_swap['柴油环比']), 2)}吨；"
        f"天然气{int(round(daily_with_swap['天然气总量'], 0))}吨,环比{'降' if daily_with_swap['天然气总量环比'] < 0 else '增'}{round(abs(daily_with_swap['天然气总量环比']), 2)}吨,"
        f"其中自有站{int(round(daily_with_swap['自有站吨数'], 0))}吨,环比{'降' if daily_with_swap['自有站环比'] < 0 else '增'}{round(abs(daily_with_swap['自有站环比']), 2)}吨,"
        f"划转站{int(round(daily_with_swap['划转站吨数'], 0))}吨,环比{'降' if daily_with_swap['划转站环比'] < 0 else '增'}{round(abs(daily_with_swap['划转站环比']), 2)}吨。")

    # 当月情况
    monthly_with_swap = final_result[final_result['统计周期'] == '当月含串换量'].iloc[0]
    print(
        f"5.2 当月油气总量{int(round(monthly_with_swap['油气总量'], 0))}吨,同比增{round(monthly_with_swap['油气总量环比'], 2)}吨。"
        f"成品油{int(round(monthly_with_swap['成品油总量'], 0))}吨,同比增{round(monthly_with_swap['成品油总量环比'], 2)}吨,"
        f"其中汽油{int(round(monthly_with_swap['汽油吨数'], 0))}吨,同比增{round(monthly_with_swap['汽油环比'], 2)}吨,"
        f"柴油{int(round(monthly_with_swap['柴油吨数'], 0))}吨,同比增{round(monthly_with_swap['柴油环比'], 2)}吨；"
        f"天然气{int(round(monthly_with_swap['天然气总量'], 0))}吨,同比增{round(monthly_with_swap['天然气总量环比'], 2)}吨,"
        f"其中自有站{int(round(monthly_with_swap['自有站吨数'], 0))}吨,同比增{round(monthly_with_swap['自有站环比'], 2)}吨,"
        f"划转站{int(round(monthly_with_swap['划转站吨数'], 0))}吨,同比增{round(monthly_with_swap['划转站环比'], 2)}吨。")

    # 当年情况
    yearly_with_swap = final_result[final_result['统计周期'] == '当年含串换量'].iloc[0]
    print(
        f"5.3 年度油气总量{int(round(yearly_with_swap['油气总量'], 0))}吨,同比增{round(yearly_with_swap['油气总量环比'], 2)}吨。"
        f"成品油{int(round(yearly_with_swap['成品油总量'], 0))}吨,同比增{round(yearly_with_swap['成品油总量环比'], 2)}吨,"
        f"其中汽油{int(round(yearly_with_swap['汽油吨数'], 0))}吨,同比增{round(yearly_with_swap['汽油环比'], 2)}吨,"
        f"柴油{int(round(yearly_with_swap['柴油吨数'], 0))}吨,同比增{round(yearly_with_swap['柴油环比'], 2)}吨；"
        f"天然气{int(round(yearly_with_swap['天然气总量'], 0))}吨,同比增{round(yearly_with_swap['天然气总量环比'], 2)}吨,"
        f"其中自有站{int(round(yearly_with_swap['自有站吨数'], 0))}吨,同比增{round(yearly_with_swap['自有站环比'], 2)}吨,"
        f"划转站{int(round(yearly_with_swap['划转站吨数'], 0))}吨,同比{'降' if yearly_with_swap['划转站环比'] < 0 else '增'}{round(abs(yearly_with_swap['划转站环比']), 2)}吨。")

    print("\n六、日、月、年度油气当量同比情况（不含串换）：")

    # 当日情况
    daily_without_swap = final_result[final_result['统计周期'] == '当日不含串换量'].iloc[0]
    print(
        f"6.1 当日油气总量{int(round(daily_without_swap['油气总量'], 0))}吨,环比{'降' if daily_without_swap['油气总量环比'] < 0 else '增'}{round(abs(daily_without_swap['油气总量环比']), 2)}吨。"
        f"成品油{int(round(daily_without_swap['成品油总量'], 0))}吨,环比{'降' if daily_without_swap['成品油总量环比'] < 0 else '增'}{round(abs(daily_without_swap['成品油总量环比']), 2)}吨,"
        f"其中汽油{int(round(daily_without_swap['汽油吨数'], 0))}吨,环比{'降' if daily_without_swap['汽油环比'] < 0 else '增'}{round(abs(daily_without_swap['汽油环比']), 2)}吨,"
        f"柴油{int(round(daily_without_swap['柴油吨数'], 0))}吨,环比{'降' if daily_without_swap['柴油环比'] < 0 else '增'}{round(abs(daily_without_swap['柴油环比']), 2)}吨；"
        f"天然气{int(round(daily_without_swap['天然气总量'], 0))}吨,环比{'降' if daily_without_swap['天然气总量环比'] < 0 else '增'}{round(abs(daily_without_swap['天然气总量环比']), 2)}吨,"
        f"其中自有站{int(round(daily_without_swap['自有站吨数'], 0))}吨,环比{'降' if daily_without_swap['自有站环比'] < 0 else '增'}{round(abs(daily_without_swap['自有站环比']), 2)}吨,"
        f"划转站{int(round(daily_without_swap['划转站吨数'], 0))}吨,环比{'降' if daily_without_swap['划转站环比'] < 0 else '增'}{round(abs(daily_without_swap['划转站环比']), 2)}吨。")

    # 当月情况
    monthly_without_swap = final_result[final_result['统计周期'] == '当月不含串换量'].iloc[0]
    print(
        f"6.2 当月油气总量{int(round(monthly_without_swap['油气总量'], 0))}吨,同比增{round(monthly_without_swap['油气总量环比'], 2)}吨。"
        f"成品油{int(round(monthly_without_swap['成品油总量'], 0))}吨,同比增{round(monthly_without_swap['成品油总量环比'], 2)}吨,"
        f"其中汽油{int(round(monthly_without_swap['汽油吨数'], 0))}吨,同比{'降' if monthly_without_swap['汽油环比'] < 0 else '增'}{round(abs(monthly_without_swap['汽油环比']), 2)}吨,"
        f"柴油{int(round(monthly_without_swap['柴油吨数'], 0))}吨,同比增{round(monthly_without_swap['柴油环比'], 2)}吨；"
        f"天然气{int(round(monthly_without_swap['天然气总量'], 0))}吨,同比增{round(monthly_without_swap['天然气总量环比'], 2)}吨,"
        f"其中自有站{int(round(monthly_without_swap['自有站吨数'], 0))}吨,同比增{round(monthly_without_swap['自有站环比'], 2)}吨,"
        f"划转站{int(round(monthly_without_swap['划转站吨数'], 0))}吨,同比增{round(monthly_without_swap['划转站环比'], 2)}吨。")

    # 当年情况
    yearly_without_swap = final_result[final_result['统计周期'] == '当年不含串换量'].iloc[0]
    print(
        f"6.3 年度油气总量{int(round(yearly_without_swap['油气总量'], 0))}吨,同比增{round(yearly_without_swap['油气总量环比'], 2)}吨。"
        f"成品油{int(round(yearly_without_swap['成品油总量'], 0))}吨,同比增{round(yearly_without_swap['成品油总量环比'], 2)}吨,"
        f"其中汽油{int(round(yearly_without_swap['汽油吨数'], 0))}吨,同比增{round(yearly_without_swap['汽油环比'], 2)}吨,"
        f"柴油{int(round(yearly_without_swap['柴油吨数'], 0))}吨,同比增{round(yearly_without_swap['柴油环比'], 2)}吨；"
        f"天然气{int(round(yearly_without_swap['天然气总量'], 0))}吨,同比增{round(yearly_without_swap['天然气总量环比'], 2)}吨,"
        f"其中自有站{int(round(yearly_without_swap['自有站吨数'], 0))}吨,同比增{round(yearly_without_swap['自有站环比'], 2)}吨,"
        f"划转站{int(round(yearly_without_swap['划转站吨数'], 0))}吨,同比{'降' if yearly_without_swap['划转站环比'] < 0 else '增'}{round(abs(yearly_without_swap['划转站环比']), 2)}吨。")


# 主程序示例，这里需要你根据实际情况获取数据并调用函数
if __name__ == "__main__":
    # 假设这里获取到了数据
    data = pd.DataFrame()  # 请替换为实际获取的数据
    final_result = calculate_period_data(data)
    # 这里需要将 final_result 转换为合适的 DataFrame 格式，以便后续处理
    final_result_df = pd.DataFrame.from_dict(final_result, orient='index').reset_index()
    final_result_df.columns = ['统计周期', '数值']
    # 处理成包含各种统计周期和指标的 DataFrame，这里需要根据实际情况调整
    # 例如添加油气总量、成品油总量、汽油吨数等列
    # 这里只是示例，你需要根据实际数据和需求来完善
    # 打印计划完成情况，这里需要根据实际数据调整参数
    print_plan_completion("计划完成情况", pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    # 打印最终结果
    print_result(final_result_df)
