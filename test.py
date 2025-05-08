import pandas as pd
def read_and_deduplicate(file_path):
    try:
        df = pd.read_excel(file_path)
        # 根据 Gene ID 和 Gene Name 进行去重
        df = df.drop_duplicates(subset=['Gene ID', 'Gene Name'])
        return df
    except FileNotFoundError:
        print(f"文件 {file_path} 未找到。")
        return None
    except Exception as e:
        print(f"读取文件时出现错误: {e}")
        return None
# 调用新函数读取并去重
new_df = read_and_deduplicate('gene_id_list.xlsx')
if new_df is not None:
    new_df.to_excel('gene_id_list_deduplicated.xlsx', index=False)