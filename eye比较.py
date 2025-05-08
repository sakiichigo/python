import pandas as pd
import os


def filter_records(input_file, output_file, intersection_file):
    try:
        df = pd.read_excel(input_file)
    except FileNotFoundError:
        print(f"错误: 文件 {input_file} 未找到。")
        return
    except Exception as e:
        print(f"错误: 读取文件时发生未知错误: {e}")
        return

    if 'outcome' not in df.columns or 'exposure' not in df.columns:
        print("错误: 文件中缺少 'outcome' 或 'exposure' 列。")
        return

    idList = []
    folder_path = r'D:\document\bioInfo\eye-eye-0228\outcome'
    if os.path.exists(folder_path):
        for file in os.listdir(folder_path):
            if file.endswith('.txt'):
                # 去除 .txt 后缀
                idList.append(os.path.splitext(file)[0])
    else:
        print(f"错误: 文件夹 {folder_path} 不存在。")
        return

    # 添加额外的关键词
    extra_keywords = ["diabetic", "retin"]
    idList.extend(extra_keywords)

    intersection_keywords = []

    def should_filter(row):
        outcome_str = str(row['outcome']).lower()
        exposure_str = str(row['exposure']).lower()
        id_set = set([x.lower() for x in idList])

        for keyword in id_set:
            if keyword in outcome_str and keyword in exposure_str:
                intersection_keywords.append(keyword)
                return True
        return False

    df = df[~df.apply(should_filter, axis=1)]

    try:
        df.to_excel(output_file, index=False)
        print(f"过滤后的记录已保存到 {output_file}。")
    except Exception as e:
        print(f"错误: 保存文件时发生未知错误: {e}")

    if intersection_keywords:
        try:
            intersection_df = pd.DataFrame({'intersection_keywords': intersection_keywords})
            intersection_df.to_excel(intersection_file, index=False)
            print(f"交集关键词已保存到 {intersection_file}。")
        except Exception as e:
            print(f"错误: 保存交集关键词文件时发生未知错误: {e}")


if __name__ == "__main__":
    input_file = r'C:\Users\22565\Desktop\eye-eye.xlsx'
    output_file = r'C:\Users\22565\Desktop\eye-eye_filtered.xlsx'
    intersection_file = r'C:\Users\22565\Desktop\eye-eye_intersection.xlsx'
    filter_records(input_file, output_file, intersection_file)
