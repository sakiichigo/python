import pandas as pd
import os


def process_eqtl_data():
    # 定义文件路径
    file_path = r"D:\document\bioInfo\alps-eye\eqtl\selected_eqtl.xlsx"

    # 读取文件
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"读取文件时出错: {e}")
        return

    # 提取outcome列中包含glaucoma和cataract的行，不区分大小写
    outcome_lower = df['outcome'].str.lower()
    glaucoma_rows = df[outcome_lower.str.contains('glaucoma', na=False)].copy()
    cataract_rows = df[outcome_lower.str.contains('cataract', na=False)].copy()

    # 如果没有找到匹配的行，打印提示信息并返回
    if glaucoma_rows.empty and cataract_rows.empty:
        print("未找到包含'glaucoma'或'cataract'的行")
        return

    # 创建结果DataFrame
    result_df = pd.DataFrame()

    # 处理glaucoma的数据
    if not glaucoma_rows.empty:
        # 处理重复值，选择pval最低的记录
        if glaucoma_rows['exposure'].duplicated().any():
            print("警告: glaucoma数据中exposure列包含重复值，将保留pval最低的记录")
            glaucoma_rows = glaucoma_rows.sort_values('pval').drop_duplicates(subset='exposure', keep='first')

        # 创建格式化后的列
        glaucoma_rows['glaucoma'] = glaucoma_rows.apply(
            lambda row: f"β={row['b']} ({row['lo_ci']} to {row['up_ci']}) pval={row['pval']}",
            axis=1
        )

        # 添加到结果DataFrame
        result_df = pd.concat([result_df, glaucoma_rows.set_index('exposure')['glaucoma']], axis=1)

    # 处理cataract的数据
    if not cataract_rows.empty:
        # 处理重复值，选择pval最低的记录
        if cataract_rows['exposure'].duplicated().any():
            print("警告: cataract数据中exposure列包含重复值，将保留pval最低的记录")
            cataract_rows = cataract_rows.sort_values('pval').drop_duplicates(subset='exposure', keep='first')

        # 创建格式化后的列
        cataract_rows['cataract'] = cataract_rows.apply(
            lambda row: f"β={row['b']} ({row['lo_ci']} to {row['up_ci']}) pval={row['pval']}",
            axis=1
        )

        # 添加到结果DataFrame
        result_df = pd.concat([result_df, cataract_rows.set_index('exposure')['cataract']], axis=1)

    # 将索引重置为列，并命名为"exposure"
    if not result_df.empty:
        result_df = result_df.reset_index().rename(columns={'index': 'exposure'})

    # 获取原文件所在目录
    directory = os.path.dirname(file_path)
    output_path = os.path.join(directory, "processed_eqtl.xlsx")

    # 保存结果
    try:
        result_df.to_excel(output_path, index=False)  # 不保存默认索引
        print(f"处理完成，结果已保存到: {output_path}")
    except Exception as e:
        print(f"保存文件时出错: {e}")


if __name__ == "__main__":
    process_eqtl_data()