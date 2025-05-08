import pandas as pd
from datetime import datetime
from zipfile import BadZipFile

# 定义 hospay_0227.xlsx 的列名
columns = [
    'created_at', 'updated_at', 'id', 'created_by_id', 'updated_by_id',
    'paytime', 'oil_des', 'site_no', 'oil_no', 'site_name', 'weight'
]

# 创建一个空的 DataFrame 用于存储 hospay_0227.xlsx 的数据
hospay_df = pd.DataFrame(columns=columns)

try:
    # 读取 hospay_template.xlsx 文件，手动指定引擎为 openpyxl
    template_df = pd.read_excel('C:\\Users\\22565\\Documents\\石油导入数据\\导入\\HOS0331.xlsx', engine='openpyxl')
    # 打印 template_df 的所有列名
    print("template_df 的列名：", template_df.columns)
except FileNotFoundError:
    print("未找到 hospay_template.xlsx 文件，请检查文件路径。")
except BadZipFile:
    print("文件 hospay_template.xlsx 可能损坏或不是有效的 .xlsx 文件，请检查或重新获取该文件。")
except Exception as e:
    print(f"读取文件时出现其他错误: {e}")
else:
    # 定义列名映射规则
    mapping = {
        '营业日期': 'paytime',
        '油品品号': 'oil_no',
        '站点': 'site_no',
        '站点名称': 'site_name',
        '油品描述': 'oil_des',
        '换算重量': 'weight'
    }

    # 遍历映射规则，将数据从 template_df 复制到 hospay_df
    for source_col, target_col in mapping.items():
        try:
            hospay_df[target_col] = template_df[source_col]
        except KeyError:
            print(f"未找到列名 {source_col}，请检查文件列名。")

    # 过滤掉 paytime 列为空的记录
    hospay_df = hospay_df[hospay_df['paytime'].notna()]

    # 将 paytime 列中的 "." 替换为 "/"
    if 'paytime' in hospay_df.columns:
        hospay_df['paytime'] = hospay_df['paytime'].astype(str).str.replace('.', '/', regex=False)

    # 生成 id 列，从 1313928 开始递增
    num_rows = len(hospay_df)
    hospay_df['id'] = range(1405813, 1405813 + num_rows)

    # 获取当前日期并格式化为指定格式
    current_date = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    # 填充 created_at 和 updated_at 列
    hospay_df['created_at'] = current_date
    hospay_df['updated_at'] = current_date

    # 填充 created_by_id 和 updated_by_id 列
    hospay_df['created_by_id'] = 1
    hospay_df['updated_by_id'] = 1

    # 保存 hospay_df 到 hospay_0227.xlsx 文件
    hospay_df.to_excel('C:\\Users\\22565\\Documents\\石油导入数据\\HOS0331.xlsx', index=False)

    print("数据填充完成，已保存到 hospay.xlsx")