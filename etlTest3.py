import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta

# 数据库配置
DB_CONFIG = {
    "user": "zorkdata",
    "host": "10.204.20.18",
    "dbname": "biz_data",
    "password": "Zorkdata@2025",
    "port": 54321
}

# 连接数据库
connection = psycopg2.connect(
    user=DB_CONFIG["user"],
    password=DB_CONFIG["password"],
    host=DB_CONFIG["host"],
    port=DB_CONFIG["port"],
    database=DB_CONFIG["dbname"]
)
cursor = connection.cursor()

# 获取当前日期
current_date = datetime.now().date()
first_day_of_month = current_date.replace(day=1)
first_day_of_year = current_date.replace(month=1, day=1)
first_day_of_last_month = (first_day_of_month - timedelta(days=1)).replace(day=1)
first_day_of_last_year = first_day_of_year.replace(year=first_day_of_year.year - 1)
previous_day = current_date - timedelta(days=1)

# 从数据库中获取数据
query1 = sql.SQL("SELECT * FROM view_hospay_and_erpsall_current")
cursor.execute(query1)
columns = [desc[0] for desc in cursor.description]
view_hospay_and_erpsall_current = pd.DataFrame(cursor.fetchall(), columns=columns)

query2 = sql.SQL("SELECT * FROM view_gassales_current")
cursor.execute(query2)
columns = [desc[0] for desc in cursor.description]
view_gassales_current = pd.DataFrame(cursor.fetchall(), columns=columns)

# 油品预处理（假设油品数据对应项目列）
oil_preprocessing = view_hospay_and_erpsall_current.copy()
oil_preprocessing['油品含串换量'] = oil_preprocessing['本日']
oil_preprocessing['油品不含串换量'] = oil_preprocessing.apply(
    lambda row: row['本日'] if row['项目'] != '油田自用' else 0, axis=1)

# 燃气预处理（假设燃气数据对应项目列）
gas_preprocessing = view_gassales_current.copy()
gas_preprocessing['燃气含串换量'] = gas_preprocessing['本日']
gas_preprocessing['燃气不含串换量'] = gas_preprocessing.apply(
    lambda row: row['本日'] if row['项目'] != '油田自用' else 0, axis=1)

# 合并数据时指定后缀（油品用_x，燃气用_y）
merged_data = pd.merge(
    oil_preprocessing,
    gas_preprocessing,
    on='日期',
    how='outer',
    suffixes=('_oil', '_gas')  # 明确区分油品和燃气的列
)

# 重命名项目列（假设燃气数据的项目列需要保留）
merged_data = merged_data.rename(columns={'项目_gas': '项目'})  # 假设燃气表的项目列是目标列


# 定义计算周期数据的函数
# 定义计算周期数据的函数
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
                                           data['油品名称'] == '柴油')]['油品含串换量'].sum(),
        "当年天然气含串换总量": data[(data['日期'] >= first_day_of_year) & (data['日期'] <= current_date)][
            '燃气含串换量'].sum(),
        "去年天然气含串换总量": data[(data['日期'] >= first_day_of_last_year) & (
                data['日期'] <= first_day_of_year - timedelta(days=1))]['燃气含串换量'].sum(),
        "当年成品油不含串换总量": data[(data['日期'] >= first_day_of_year) & (data['日期'] <= current_date)][
            '油品不含串换量'].sum(),
        "去年成品油不含串换总量": data[(data['日期'] >= first_day_of_last_year) & (
                data['日期'] <= first_day_of_year - timedelta(days=1))]['油品不含串换量'].sum(),
        "当年汽油不含串换总量": data[(data['日期'] >= first_day_of_year) & (data['日期'] <= current_date) & (
                data['油品名称'] == '汽油')]['油品不含串换量'].sum(),
        "去年汽油不含串换总量": data[(data['日期'] >= first_day_of_last_year) & (
                data['日期'] <= first_day_of_year - timedelta(days=1)) & (
                                             data['油品名称'] == '汽油')]['油品不含串换量'].sum(),
        "当年柴油不含串换总量": data[(data['日期'] >= first_day_of_year) & (data['日期'] <= current_date) & (
                data['油品名称'] == '柴油')]['油品不含串换量'].sum(),
        "去年柴油不含串换总量": data[(data['日期'] >= first_day_of_last_year) & (
                data['日期'] <= first_day_of_year - timedelta(days=1)) & (
                                             data['油品名称'] == '柴油')]['油品不含串换量'].sum(),
        "当年天然气不含串换总量": data[(data['日期'] >= first_day_of_year) & (data['日期'] <= current_date)][
            '燃气不含串换量'].sum(),
        "去年天然气不含串换总量": data[(data['日期'] >= first_day_of_last_year) & (
                data['日期'] <= first_day_of_year - timedelta(days=1))]['燃气不含串换量'].sum(),
        "当日成品油含串换总量": data[(data['日期'] == current_date)]['油品含串换量'].sum(),
        "前一日成品油含串换总量": data[(data['日期'] == previous_day)]['油品含串换量'].sum(),
        "当日汽油含串换总量": data[(data['日期'] == current_date) & (data['油品名称'] == '汽油')][
            '油品含串换量'].sum(),
        "前一日汽油含串换总量": data[(data['日期'] == previous_day) & (data['油品名称'] == '汽油')][
            '油品含串换量'].sum(),
        "当日柴油含串换总量": data[(data['日期'] == current_date) & (data['油品名称'] == '柴油')][
            '油品含串换量'].sum(),
        "前一日柴油含串换总量": data[(data['日期'] == previous_day) & (data['油品名称'] == '柴油')][
            '油品含串换量'].sum(),
        "当日天然气含串换总量": data[(data['日期'] == current_date)]['燃气含串换量'].sum(),
        "前一日天然气含串换总量": data[(data['日期'] == previous_day)]['燃气含串换量'].sum(),
        "当日成品油不含串换总量": data[(data['日期'] == current_date)]['油品不含串换量'].sum(),
        "前一日成品油不含串换总量": data[(data['日期'] == previous_day)]['油品不含串换量'].sum(),
        "当日汽油不含串换总量": data[(data['日期'] == current_date) & (data['油品名称'] == '汽油')][
            '油品不含串换量'].sum(),
        "前一日汽油不含串换总量": data[(data['日期'] == previous_day) & (data['油品名称'] == '汽油')][
            '油品不含串换量'].sum(),
        "当日柴油不含串换总量": data[(data['日期'] == current_date) & (data['油品名称'] == '柴油')][
            '油品不含串换量'].sum(),
        "前一日柴油不含串换总量": data[(data['日期'] == previous_day) & (data['油品名称'] == '柴油')][
            '油品不含串换量'].sum(),
        "当日天然气不含串换总量": data[(data['日期'] == current_date)]['燃气不含串换量'].sum(),
        "前一日天然气不含串换总量": data[(data['日期'] == previous_day)]['燃气不含串换量'].sum(),
        # 处理项目列冲突（假设燃气的项目列是有效列，故使用'项目'列）
        "当日自有站含串换总量": data[(data['日期'] == current_date) & (
                data['项目'] != '划转站')]['燃气含串换量'].sum(),
        "前一日自有站含串换总量": data[(data['日期'] == previous_day) & (
                data['项目'] != '划转站')]['燃气含串换量'].sum(),
        "当日自有站不含串换总量": data[(data['日期'] == current_date) & (
                data['项目'] != '划转站')]['燃气含串换量'].sum(),
        "前一日自有站不含串换总量": data[(data['日期'] == previous_day) & (
                data['项目'] != '划转站')]['燃气含串换量'].sum(),
        "当日划转站含串换总量": data[(data['日期'] == current_date) & (
                data['项目'] == '划转站')]['燃气含串换量'].sum(),
        "前一日划转站含串换总量": data[(data['日期'] == previous_day) & (
                data['项目'] == '划转站')]['燃气含串换量'].sum(),
        "当日划转站不含串换总量": data[(data['日期'] == current_date) & (
                data['项目'] == '划转站')]['燃气含串换量'].sum(),
        "前一日划转站不含串换总量": data[(data['日期'] == previous_day) & (
                data['项目'] == '划转站')]['燃气含串换量'].sum()
    }

    # 先将字典的键存储到一个列表中
    keys = list(result.keys())

    # 计算环比
    for key in keys:
        if '当日' in key:
            prev_key = key.replace('当日', '前一日')
        elif '当月' in key:
            prev_key = key.replace('当月', '上月')
        elif '当年' in key:
            prev_key = key.replace('当年', '去年')
        else:
            continue
        if result[prev_key] == 0:
            result[key + '环比'] = 0
        else:
            result[key + '环比'] = (result[key] - result[prev_key]) / result[prev_key]

    return result


# 计算周期数据
period_data = calculate_period_data(merged_data)


# 定义生成最终结果的函数
def generate_final_result():
    periods = ['当日含串换量', '当日不含串换量', '当月含串换量', '当月不含串换量', '当年含串换量', '当年不含串换量']
    result = []
    for period in periods:
        if '当日' in period:
            date_condition = current_date
        elif '当月' in period:
            date_condition = (first_day_of_month, current_date)
        elif '当年' in period:
            date_condition = (first_day_of_year, current_date)

        is_with_swap = '含串换量' in period
        swap_suffix = '含串换量' if is_with_swap else '不含串换量'
        total_key = f"{period[:2]}含串换油气总量" if is_with_swap else f"{period[:2]}不含串换油气总量"
        oil_key = f"{period[:2]}成品油含串换总量" if is_with_swap else f"{period[:2]}成品油不含串换总量"
        gasoline_key = f"{period[:2]}汽油含串换总量" if is_with_swap else f"{period[:2]}汽油不含串换总量"
        diesel_key = f"{period[:2]}柴油含串换总量" if is_with_swap else f"{period[:2]}柴油不含串换总量"
        gas_key = f"{period[:2]}天然气含串换总量" if is_with_swap else f"{period[:2]}天然气不含串换总量"
        self_owned_key = f"{period[:2]}自有站含串换总量" if is_with_swap else f"{period[:2]}自有站不含串换总量"
        transfer_key = f"{period[:2]}划转站含串换总量" if is_with_swap else f"{period[:2]}划转站不含串换总量"

        total_ring_ratio_key = total_key + '环比'
        oil_ring_ratio_key = oil_key + '环比'
        gasoline_ring_ratio_key = gasoline_key + '环比'
        diesel_ring_ratio_key = diesel_key + '环比'
        gas_ring_ratio_key = gas_key + '环比'
        self_owned_ring_ratio_key = self_owned_key + '环比'
        transfer_ring_ratio_key = transfer_key + '环比'

        if isinstance(date_condition, tuple):
            start_date, end_date = date_condition
            oil_total = merged_data[(merged_data['日期'] >= start_date) & (merged_data['日期'] <= end_date)][
                '油品含串换量' if is_with_swap else '油品不含串换量'].sum()
            gasoline_total = merged_data[
                (merged_data['日期'] >= start_date) & (merged_data['日期'] <= end_date) & (
                        merged_data['油品名称'] == '汽油')][
                '油品含串换量' if is_with_swap else '油品不含串换量'].sum()
            diesel_total = merged_data[
                (merged_data['日期'] >= start_date) & (merged_data['日期'] <= end_date) & (
                        merged_data['油品名称'] == '柴油')][
                '油品含串换量' if is_with_swap else '油品不含串换量'].sum()
            gas_total = merged_data[(merged_data['日期'] >= start_date) & (merged_data['日期'] <= end_date)][
                '燃气含串换量' if is_with_swap else '燃气不含串换量'].sum()
            self_owned_total = merged_data[
                (merged_data['日期'] >= start_date) & (merged_data['日期'] <= end_date) & (
                        merged_data['项目'] != '划转站')][
                '燃气含串换量' if is_with_swap else '燃气不含串换量'].sum()
            transfer_total = merged_data[
                (merged_data['日期'] >= start_date) & (merged_data['日期'] <= end_date) & (
                        merged_data['项目'] == '划转站')][
                '燃气含串换量' if is_with_swap else '燃气不含串换量'].sum()
        else:
            oil_total = merged_data[(merged_data['日期'] == date_condition)][
                '油品含串换量' if is_with_swap else '油品不含串换量'].sum()
            gasoline_total = merged_data[
                (merged_data['日期'] == date_condition) & (merged_data['油品名称'] == '汽油')][
                '油品含串换量' if is_with_swap else '油品不含串换量'].sum()
            diesel_total = merged_data[
                (merged_data['日期'] == date_condition) & (merged_data['油品名称'] == '柴油')][
                '油品含串换量' if is_with_swap else '油品不含串换量'].sum()
            gas_total = merged_data[(merged_data['日期'] == date_condition)][
                '燃气含串换量' if is_with_swap else '燃气不含串换量'].sum()
            self_owned_total = merged_data[
                (merged_data['日期'] == date_condition) & (merged_data['项目'] != '划转站')][
                '燃气含串换量' if is_with_swap else '燃气不含串换量'].sum()
            transfer_total = merged_data[
                (merged_data['日期'] == date_condition) & (merged_data['项目'] == '划转站')][
                '燃气含串换量' if is_with_swap else '燃气不含串换量'].sum()

        result.append({
            "统计周期": period,
            "油气总量": period_data[total_key],
            "含串换总量": period_data[total_key],
            "油气总量环比": period_data[total_ring_ratio_key],
            "成品油总量": oil_total,
            "成品油总量环比": period_data[oil_ring_ratio_key],
            "汽油吨数": gasoline_total,
            "汽油环比": period_data[gasoline_ring_ratio_key],
            "柴油吨数": diesel_total,
            "柴油环比": period_data[diesel_ring_ratio_key],
            "天然气总量": gas_total,
            "天然气总量环比": period_data[gas_ring_ratio_key],
            "自有站吨数": self_owned_total,
            "自有站环比": period_data[self_owned_ring_ratio_key] if '当日' in period else 0,
            "划转站吨数": transfer_total,
            "划转站环比": period_data[transfer_ring_ratio_key] if '当日' in period else 0
        })
    return pd.DataFrame(result)


# 生成最终结果
final_result = generate_final_result()

# 打印最终结果
#print(final_result)
# 按指定格式输出结果

# 组合含串换的日、月、年度油气当量同比情况的 print 内容为一个字符串
content2 = "五、日、月、年度油气当量同比情况（含串换）：\n"
# 当日情况
daily_with_swap = final_result[final_result['统计周期'] == '当日含串换量'].iloc[0]
content2 += (
    f"5.1 当日油气总量{int(round(daily_with_swap['油气总量'], 0))}吨,环比{'降' if daily_with_swap['油气总量环比'] < 0 else '增'}{round(abs(daily_with_swap['油气总量环比']), 2)}吨。"
    f"成品油{int(round(daily_with_swap['成品油总量'], 0))}吨,环比{'降' if daily_with_swap['成品油总量环比'] < 0 else '增'}{round(abs(daily_with_swap['成品油总量环比']), 2)}吨,"
    f"其中汽油{int(round(daily_with_swap['汽油吨数'], 0))}吨,环比{'降' if daily_with_swap['汽油环比'] < 0 else '增'}{round(abs(daily_with_swap['汽油环比']), 2)}吨,"
    f"柴油{int(round(daily_with_swap['柴油吨数'], 0))}吨,环比{'降' if daily_with_swap['柴油环比'] < 0 else '增'}{round(abs(daily_with_swap['柴油环比']), 2)}吨；"
    f"天然气{int(round(daily_with_swap['天然气总量'], 0))}吨,环比{'降' if daily_with_swap['天然气总量环比'] < 0 else '增'}{round(abs(daily_with_swap['天然气总量环比']), 2)}吨,"
    f"其中自有站{int(round(daily_with_swap['自有站吨数'], 0))}吨,环比{'降' if daily_with_swap['自有站环比'] < 0 else '增'}{round(abs(daily_with_swap['自有站环比']), 2)}吨,"
    f"划转站{int(round(daily_with_swap['划转站吨数'], 0))}吨,环比{'降' if daily_with_swap['划转站环比'] < 0 else '增'}{round(abs(daily_with_swap['划转站环比']), 2)}吨。\n"
)

# 当月情况
monthly_with_swap = final_result[final_result['统计周期'] == '当月含串换量'].iloc[0]
content2 += (
    f"5.2 当月油气总量{int(round(monthly_with_swap['油气总量'], 0))}吨,同比增{round(monthly_with_swap['油气总量环比'], 2)}吨。"
    f"成品油{int(round(monthly_with_swap['成品油总量'], 0))}吨,同比增{round(monthly_with_swap['成品油总量环比'], 2)}吨,"
    f"其中汽油{int(round(monthly_with_swap['汽油吨数'], 0))}吨,同比增{round(monthly_with_swap['汽油环比'], 2)}吨,"
    f"柴油{int(round(monthly_with_swap['柴油吨数'], 0))}吨,同比增{round(monthly_with_swap['柴油环比'], 2)}吨；"
    f"天然气{int(round(monthly_with_swap['天然气总量'], 0))}吨,同比增{round(monthly_with_swap['天然气总量环比'], 2)}吨,"
    f"其中自有站{int(round(monthly_with_swap['自有站吨数'], 0))}吨,同比增{round(monthly_with_swap['自有站环比'], 2)}吨,"
    f"划转站{int(round(monthly_with_swap['划转站吨数'], 0))}吨,同比增{round(monthly_with_swap['划转站环比'], 2)}吨。\n"
)

# 当年情况
yearly_with_swap = final_result[final_result['统计周期'] == '当年含串换量'].iloc[0]
content2 += (
    f"5.3 年度油气总量{int(round(yearly_with_swap['油气总量'], 0))}吨,同比增{round(yearly_with_swap['油气总量环比'], 2)}吨。"
    f"成品油{int(round(yearly_with_swap['成品油总量'], 0))}吨,同比增{round(yearly_with_swap['成品油总量环比'], 2)}吨,"
    f"其中汽油{int(round(yearly_with_swap['汽油吨数'], 0))}吨,同比增{round(yearly_with_swap['汽油环比'], 2)}吨,"
    f"柴油{int(round(yearly_with_swap['柴油吨数'], 0))}吨,同比增{round(yearly_with_swap['柴油环比'], 2)}吨；"
    f"天然气{int(round(yearly_with_swap['天然气总量'], 0))}吨,同比增{round(yearly_with_swap['天然气总量环比'], 2)}吨,"
    f"其中自有站{int(round(yearly_with_swap['自有站吨数'], 0))}吨,同比增{round(yearly_with_swap['自有站环比'], 2)}吨,"
    f"划转站{int(round(yearly_with_swap['划转站吨数'], 0))}吨,同比{'降' if yearly_with_swap['划转站环比'] < 0 else '增'}{round(abs(yearly_with_swap['划转站环比']), 2)}吨。\n"
)

# 组合不含串换的日、月、年度油气当量同比情况的 print 内容为一个字符串
content3 = "\n六、日、月、年度油气当量同比情况（不含串换）：\n"
# 当日情况
daily_without_swap = final_result[final_result['统计周期'] == '当日不含串换量'].iloc[0]
content3 += (
    f"6.1 当日油气总量{int(round(daily_without_swap['油气总量'], 0))}吨,环比{'降' if daily_without_swap['油气总量环比'] < 0 else '增'}{round(abs(daily_without_swap['油气总量环比']), 2)}吨。"
    f"成品油{int(round(daily_without_swap['成品油总量'], 0))}吨,环比{'降' if daily_without_swap['成品油总量环比'] < 0 else '增'}{round(abs(daily_without_swap['成品油总量环比']), 2)}吨,"
    f"其中汽油{int(round(daily_without_swap['汽油吨数'], 0))}吨,环比{'降' if daily_without_swap['汽油环比'] < 0 else '增'}{round(abs(daily_without_swap['汽油环比']), 2)}吨,"
    f"柴油{int(round(daily_without_swap['柴油吨数'], 0))}吨,环比{'降' if daily_without_swap['柴油环比'] < 0 else '增'}{round(abs(daily_without_swap['柴油环比']), 2)}吨；"
    f"天然气{int(round(daily_without_swap['天然气总量'], 0))}吨,环比{'降' if daily_without_swap['天然气总量环比'] < 0 else '增'}{round(abs(daily_without_swap['天然气总量环比']), 2)}吨,"
    f"其中自有站{int(round(daily_without_swap['自有站吨数'], 0))}吨,环比{'降' if daily_without_swap['自有站环比'] < 0 else '增'}{round(abs(daily_without_swap['自有站环比']), 2)}吨,"
    f"划转站{int(round(daily_without_swap['划转站吨数'], 0))}吨,环比{'降' if daily_without_swap['划转站环比'] < 0 else '增'}{round(abs(daily_without_swap['划转站环比']), 2)}吨。\n"
)

# 当月情况
monthly_without_swap = final_result[final_result['统计周期'] == '当月不含串换量'].iloc[0]
content3 += (
    f"6.2 当月油气总量{int(round(monthly_without_swap['油气总量'], 0))}吨,同比增{round(monthly_without_swap['油气总量环比'], 2)}吨。"
    f"成品油{int(round(monthly_without_swap['成品油总量'], 0))}吨,同比增{round(monthly_without_swap['成品油总量环比'], 2)}吨,"
    f"其中汽油{int(round(monthly_without_swap['汽油吨数'], 0))}吨,同比{'降' if monthly_without_swap['汽油环比'] < 0 else '增'}{round(abs(monthly_without_swap['汽油环比']), 2)}吨,"
    f"柴油{int(round(monthly_without_swap['柴油吨数'], 0))}吨,同比增{round(monthly_without_swap['柴油环比'], 2)}吨；"
    f"天然气{int(round(monthly_without_swap['天然气总量'], 0))}吨,同比增{round(monthly_without_swap['天然气总量环比'], 2)}吨,"
    f"其中自有站{int(round(monthly_without_swap['自有站吨数'], 0))}吨,同比增{round(monthly_without_swap['自有站环比'], 2)}吨,"
    f"划转站{int(round(monthly_without_swap['划转站吨数'], 0))}吨,同比增{round(monthly_without_swap['划转站环比'], 2)}吨。\n"
)

# 当年情况
yearly_without_swap = final_result[final_result['统计周期'] == '当年不含串换量'].iloc[0]
content3 += (
    f"6.3 年度油气总量{int(round(yearly_without_swap['油气总量'], 0))}吨,同比增{round(yearly_without_swap['油气总量环比'], 2)}吨。"
    f"成品油{int(round(yearly_without_swap['成品油总量'], 0))}吨,同比增{round(yearly_without_swap['成品油总量环比'], 2)}吨,"
    f"其中汽油{int(round(yearly_without_swap['汽油吨数'], 0))}吨,同比增{round(yearly_without_swap['汽油环比'], 2)}吨,"
    f"柴油{int(round(yearly_without_swap['柴油吨数'], 0))}吨,同比增{round(yearly_without_swap['柴油环比'], 2)}吨；"
    f"天然气{int(round(yearly_without_swap['天然气总量'], 0))}吨,同比增{round(yearly_without_swap['天然气总量环比'], 2)}吨,"
    f"其中自有站{int(round(yearly_without_swap['自有站吨数'], 0))}吨,同比增{round(yearly_without_swap['自有站环比'], 2)}吨,"
    f"划转站{int(round(yearly_without_swap['划转站吨数'], 0))}吨,同比{'降' if yearly_without_swap['划转站环比'] < 0 else '增'}{round(abs(yearly_without_swap['划转站环比']), 2)}吨。\n"
)

# 创建包含多行数据的 DataFrame
result = pd.DataFrame({
    'name': ['日、月、年度油气当量同比情况（含串换）', '日、月、年度油气当量同比情况（不含串换）'],
    'content': [content2, content3]
})
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
if connection:
    cursor.close()
    connection.close()
