import pandas as pd
import os
from itertools import combinations
import datetime


def find_column_intersections(file_path, sheet_name="结果",
                              target_columns=[
                                  "fusion显著410【基于所有组织5683gene】",
                                  "wgcna640【基于geo失温数据】",
                                  "差异分析【基于可成药2532基因】",
                                  "mr显著36",
                                  "共定位【基于mr36基因】",
                                  "机器学习【基于mr36基因】"
                              ]):
    """
    查找Excel文件中指定工作表内特定列之间两两的交集，并将结果保存

    参数:
    file_path: Excel文件路径
    sheet_name: 工作表名称，默认为"结果"
    target_columns: 要比较的列名列表
    """
    try:
        # 读取Excel文件中的指定工作表
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        # 获取列名列表
        all_columns = df.columns.tolist()

        # 检查目标列是否都存在
        missing_columns = [col for col in target_columns if col not in all_columns]
        if missing_columns:
            print(f"错误：找不到以下列 - {', '.join(missing_columns)}")
            return

        print(f"将比较以下列: {target_columns}")

        # 如果没有足够的列进行比较
        if len(target_columns) < 2:
            print("需要至少两列才能进行比较")
            return

        # 读取每列的数据（去除NaN值）
        column_data = {}
        for col in target_columns:
            # 去除NaN值并转换为集合便于计算交集
            column_data[col] = set(df[col].dropna().tolist())
            print(f"{col} 列包含 {len(column_data[col])} 个非空值")

        # 准备保存结果
        results = []

        # 两两比较并找出交集
        print("\n=== 列之间的交集结果 ===")
        for col1, col2 in combinations(target_columns, 2):
            intersection = column_data[col1] & column_data[col2]
            intersection_list = sorted(intersection)
            print(f"{col1} 与 {col2} 的交集包含 {len(intersection_list)} 个元素")
            results.append({
                "比较组合": f"{col1} vs {col2}",
                "交集元素数量": len(intersection_list),
                "交集元素": ", ".join(map(str, intersection_list))
            })

        # 计算所有列的共同交集
        all_intersection = set.intersection(*column_data.values())
        all_intersection_list = sorted(all_intersection)
        print(f"\n=== 所有列的共同交集 ===")
        print(f"所有列的共同交集包含 {len(all_intersection_list)} 个元素")
        results.append({
            "比较组合": "所有列共同交集",
            "交集元素数量": len(all_intersection_list),
            "交集元素": ", ".join(map(str, all_intersection_list))
        })

        # 保存结果到同目录
        dir_path = os.path.dirname(file_path)
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(dir_path, f"{file_name}_交集结果_{timestamp}.xlsx")

        # 创建结果DataFrame并保存
        results_df = pd.DataFrame(results)
        results_df.to_excel(output_file, index=False)
        print(f"\n结果已保存至: {output_file}")

    except FileNotFoundError:
        print(f"错误：找不到文件 {file_path}")
    except Exception as e:
        print(f"处理文件时发生错误: {str(e)}")


if __name__ == "__main__":
    # 指定文件路径
    file_path = r"D:\document\bioInfo\letter\hypothermia-target\hypothermiaTarget.xlsx"  # 假设文件是xlsx格式

    # 检查文件是否存在
    if not os.path.exists(file_path):
        print(f"错误：文件不存在 - {file_path}")
    else:
        # 调用函数进行处理
        find_column_intersections(file_path)
