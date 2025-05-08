import pandas as pd
import re


def count_decimals(num):
    try:
        num_str = str(float(num))
        if '.' in num_str:
            return len(num_str.split('.')[1])
        return 0
    except ValueError:
        return 0


def extract_and_save(file_path):
    try:
        # 读取 Excel 文件
        df = pd.read_excel(file_path)

        # 提取 '||' 之前的部分并覆盖原列
        df['exposure'] = df['exposure'].str.split('||', regex=False).str[0]
        df['outcome'] = df['outcome'].str.split('||', regex=False).str[0]

        # 指定关键词列表
        keywords = ['myopia', 'cataract', 'cataracts', 'diabetic', 'glaucoma', 'Strabismus']

        # 筛选出 exposure 和 outcome 中包含指定关键词的记录
        def contains_keyword(text):
            text = text.lower()
            return any(keyword.lower() in text for keyword in keywords)

        # 分离包含关键词和不包含关键词的记录
        filtered_df = df[df['exposure'].apply(contains_keyword) & df['outcome'].apply(contains_keyword)]
        non_filtered_df = df[~(df['exposure'].apply(contains_keyword) & df['outcome'].apply(contains_keyword))]

        # 对包含关键词的记录进行处理
        if not filtered_df.empty:
            # 对 pval 列计算小数点后的位数
            filtered_df['pval_decimals'] = filtered_df['pval'].apply(count_decimals)

            # 定义一个函数来选择合适的记录
            def select_best_record(group):
                if len(group) == 1:
                    return group
                min_decimals = group['pval_decimals'].min()
                candidates = group[group['pval_decimals'] - min_decimals <= 2]
                if len(candidates) > 1:
                    return candidates[candidates['nsnp'] == candidates['nsnp'].max()]
                return group[group['pval_decimals'] == min_decimals]

            # 按 exposure 和 outcome 分组并选择合适的记录
            filtered_df = filtered_df.groupby(['exposure', 'outcome']).apply(select_best_record).reset_index(drop=True)

            # 去除 outcome 括号及其中内容
            def remove_parentheses(text):
                return re.sub(r'\([^)]*\)', '', text).strip()

            filtered_df['outcome_base'] = filtered_df['outcome'].apply(remove_parentheses)

            # 按 exposure 和 outcome_base 再次分组，每组只保留第一条记录
            filtered_df = filtered_df.groupby(['exposure', 'outcome_base']).first().reset_index()
            filtered_df = filtered_df.drop(columns=['outcome_base'])

            # 删除临时添加的 pval_decimals 列
            filtered_df = filtered_df.drop(columns=['pval_decimals'])

        # 合并处理后的包含关键词的记录和不包含关键词的记录
        final_df = pd.concat([filtered_df, non_filtered_df], ignore_index=True)

        # 保存到新的 Excel 文件
        new_file_path = 'final_eye-eye_filtered.xlsx'
        final_df.to_excel(new_file_path, index=False)
        print(f"已成功保存结果到 {new_file_path}。")
    except FileNotFoundError:
        print("错误: 文件未找到。")
    except Exception as e:
        print(f"错误: 发生了一个未知错误: {e}")


if __name__ == "__main__":
    file_path = r'C:\Users\22565\Desktop\eye-eye_filtered.xlsx'
    extract_and_save(file_path)
