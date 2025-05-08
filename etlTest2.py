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
def get_db_data(query, params=None):
    """执行 SQL 查询并返回 DataFrame"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        # 将 psycopg2.sql.SQL 对象转换为字符串，并传递参数
        query_str = query.as_string(conn)  # 关键修改：转换为字符串
        return pd.read_sql(query_str, conn, params=params)
    finally:
        conn.close()

# 获取当前日期前一天
previous_day = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

# 查询 view_hospay_and_erpsall_current 视图数据，并筛选前一天的数据
query_view_hospay_and_erpsall_current = sql.SQL("""
    SELECT * 
    FROM view_hospay_and_erpsall_current 
    WHERE "日期" = %s
""")
view_hospay_and_erpsall_current = get_db_data(
    query_view_hospay_and_erpsall_current,
    params=(previous_day,)
)

# 查询 view_hospay_current_oil 视图数据，并筛选前一天的数据
query_view_hospay_current_oil = sql.SQL("""
    SELECT * 
    FROM view_hospay_current_oil 
    WHERE "日期" = %s
""")
view_hospay_current_oil = get_db_data(
    query_view_hospay_current_oil,
    params=(previous_day,)
)
# 过滤掉特定项目
filtered_data_1 = view_hospay_and_erpsall_current[
    ~view_hospay_and_erpsall_current['项目'].isin(['中石化', '兵团', '油田自用', '参股公司'])
]

# 计算汽油和柴油的本日销量和昨天销量
gasoline_today_1 = filtered_data_1[filtered_data_1['油品名称'] == '汽油']['本日'].sum()
diesel_today_1 = filtered_data_1[filtered_data_1['油品名称'] == '柴油']['本日'].sum()
gasoline_yesterday_1 = filtered_data_1[filtered_data_1['油品名称'] == '汽油']['昨天'].sum()
diesel_yesterday_1 = filtered_data_1[filtered_data_1['油品名称'] == '柴油']['昨天'].sum()

# 创建 summary_1 数据框
summary_1 = pd.DataFrame({
    '项目分类': ['当日销售同比情况社会总量'],
    '本日销量': [gasoline_today_1 + diesel_today_1],
    '汽油本日销量': [gasoline_today_1],
    '柴油本日销量': [diesel_today_1],
    '汽油环比变化量': [gasoline_today_1 - gasoline_yesterday_1],
    '柴油环比变化量': [diesel_today_1 - diesel_yesterday_1],
    '总环比变化量': [(gasoline_today_1 - gasoline_yesterday_1) + (diesel_today_1 - diesel_yesterday_1)]
})

# 过滤掉特定项目
filtered_data_2 = view_hospay_current_oil[
    ~view_hospay_current_oil['项目'].isin(['中石化', '兵团', '油田自用', '参股公司'])
]

# 定义汽油和柴油的油品名称列表
gasoline_products = ['92#（ⅥB）', '95#（ⅥB）', '98#（ⅥB）']
diesel_products = ['0＃（Ⅵ）', '-10＃（Ⅵ）', '-20＃（Ⅵ）', '-35＃（Ⅵ）', '-50#（Ⅵ）']

# 计算汽油和柴油的本日销量和昨天销量
gasoline_today_2 = filtered_data_2[filtered_data_2['油品名称'].isin(gasoline_products)]['本日'].sum()
diesel_today_2 = filtered_data_2[filtered_data_2['油品名称'].isin(diesel_products)]['本日'].sum()
gasoline_yesterday_2 = filtered_data_2[filtered_data_2['油品名称'].isin(gasoline_products)]['昨天'].sum()
diesel_yesterday_2 = filtered_data_2[filtered_data_2['油品名称'].isin(diesel_products)]['昨天'].sum()

# 创建 summary_2 数据框
summary_2 = pd.DataFrame({
    '项目分类': ['当日销售同比情况社会总量纯枪'],
    '本日销量': [gasoline_today_2 + diesel_today_2],
    '汽油本日销量': [gasoline_today_2],
    '柴油本日销量': [diesel_today_2],
    '汽油环比变化量': [gasoline_today_2 - gasoline_yesterday_2],
    '柴油环比变化量': [diesel_today_2 - diesel_yesterday_2],
    '总环比变化量': [(gasoline_today_2 - gasoline_yesterday_2) + (diesel_today_2 - diesel_yesterday_2)]
})

# 合并两个数据框
final_result = pd.concat([summary_1, summary_2], ignore_index=True)

# 提取社会总量和社会总量纯枪的数据
social_total = final_result[final_result['项目分类'] == '当日销售同比情况社会总量'].iloc[0]
social_pure_gun = final_result[final_result['项目分类'] == '当日销售同比情况社会总量纯枪'].iloc[0]
# 组合 print 内容为一个字符串
content = "四、当日销售同比情况：\n"
content += f"4. 社会总量完成{social_total['本日销量']:.0f}吨,其中汽油{social_total['汽油本日销量']:.0f}吨,柴油{social_total['柴油本日销量']:.0f}吨,环比{'降' if social_total['总环比变化量'] < 0 else '增'}{abs(social_total['总环比变化量']):.0f}吨,其中汽油{'降' if social_total['汽油环比变化量'] < 0 else '增'}{abs(social_total['汽油环比变化量']):.0f}吨,柴油{'降' if social_total['柴油环比变化量'] < 0 else '增'}{abs(social_total['柴油环比变化量']):.0f}吨；\n"
content += f"   纯枪完成{social_pure_gun['本日销量']:.0f}吨,其中汽油{social_pure_gun['汽油本日销量']:.0f}吨,柴油{social_pure_gun['柴油本日销量']:.0f}吨,环比{'降' if social_pure_gun['总环比变化量'] < 0 else '增'}{abs(social_pure_gun['总环比变化量']):.0f}吨,其中汽油{'降' if social_pure_gun['汽油环比变化量'] < 0 else '增'}{abs(social_pure_gun['汽油环比变化量']):.0f}吨,柴油{'降' if social_pure_gun['柴油环比变化量'] < 0 else '增'}{abs(social_pure_gun['柴油环比变化量']):.0f}吨。"

# 创建包含一行数据的 DataFrame
result = pd.DataFrame({
    'name': ['当日销售同比情况'],
    'content': [content]
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