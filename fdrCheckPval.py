import os
import pandas as pd
from statsmodels.stats.multitest import multipletests

def fdr_correction(file_path):
    try:
        # 读取文件
        df = pd.read_csv(file_path, sep='\t')

        # 提取 p_SMR 列的 p 值
        p_values = df['p_SMR']

        # 进行 FDR 校正
        _, corrected_p_values, _, _ = multipletests(p_values, method='fdr_bh')

        # 将校正后的 p 值添加到 DataFrame 中
        df['p_SMR_fdr'] = corrected_p_values

        # 保存结果到新文件
        new_file_path = file_path.replace('.smr', '_fdr_corrected.smr')
        df.to_csv(new_file_path, sep='\t', index=False)
        print(f"FDR 校正完成，结果已保存到 {new_file_path}")
    except FileNotFoundError:
        print(f"错误: 文件 {file_path} 未找到。")
    except KeyError:
        print(f"错误: 文件 {file_path} 中未找到 'p_SMR' 列。")
    except Exception as e:
        print(f"文件 {file_path} 发生未知错误: {e}")


def process_all_smr_files(folder_path):
    # 遍历文件夹中的所有文件
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.smr'):
                file_path = os.path.join(root, file)
                fdr_correction(file_path)


import os
import pandas as pd

# 定义目录路径
directory = r'E:\anaFiles\smrBrainEye\cataractGlaucoma'

def filter_smr_files(directory):
    """
    该函数用于筛选指定目录下以 corrected.smr 结尾的文件中的记录。
    筛选条件为 p_SMR_fdr < 0.05、p_SMR < 0.05 和 p_HEIDI > 0.05，
    并将筛选结果保存为原路径下后缀为 filter.smr 的文件。

    :param directory: 包含 corrected.smr 文件的目录路径
    """
    # 遍历目录下所有以 corrected.smr 结尾的文件
    for filename in os.listdir(directory):
        if filename.endswith('corrected.smr'):
            # 构建完整的文件路径
            file_path = os.path.join(directory, filename)
            try:
                # 以CSV方式读取文件
                df = pd.read_csv(file_path, sep='\t')  # 假设文件以制表符分隔，若不是请修改分隔符

                # 筛选满足条件的记录
                filtered_df = df[(df['p_SMR_fdr'] < 0.05) & (df['p_SMR'] < 0.05) & (df['p_HEIDI'] > 0.05)]

                # 构建保存文件的路径，将后缀改为 filter.smr
                output_filename = os.path.splitext(filename)[0] + '_filter.smr'
                output_file_path = os.path.join(directory, output_filename)

                # 将筛选结果保存为文件
                filtered_df.to_csv(output_file_path, sep='\t', index=False)
                print(f"已筛选并保存 {file_path} 的结果到 {output_file_path}")
            except Exception as e:
                print(f"处理文件 {file_path} 时出现错误: {e}")

def merge_smr_files(directory):
    """
    该函数用于合并指定目录下以 fdr_corrected_filter.smr 结尾的文件。
    在每个文件的记录后新增一列，列内容为文件名中 _fdr_corrected_filter.smr 前面的部分，
    最后将所有文件合并并输出到同一目录下。

    :param directory: 包含 fdr_corrected_filter.smr 文件的目录路径
    :return: 合并后数据保存的文件路径
    """
    # 用于存储所有合并后的数据
    all_data = []

    # 遍历目录下所有以 fdr_corrected_filter.smr 结尾的文件
    for filename in os.listdir(directory):
        if filename.endswith('fdr_corrected_filter.smr'):
            # 构建完整的文件路径
            file_path = os.path.join(directory, filename)
            try:
                # 以CSV方式读取文件，假设以制表符分隔
                df = pd.read_csv(file_path, sep='\t')

                # 获取文件名中 _fdr_corrected_filter.smr 前面的部分
                prefix = filename.split('_fdr_corrected_filter.smr')[0]

                # 在每个记录后面新增一列，列名为新列名，列内容为前缀
                df['new_column'] = prefix

                # 将当前文件的数据添加到合并数据列表中
                all_data.append(df)
            except Exception as e:
                print(f"处理文件 {file_path} 时出现错误: {e}")

    # 合并所有数据
    merged_df = pd.concat(all_data, ignore_index=True)

    # 输出合并后的数据到同一目录下
    output_file_path = os.path.join(directory, 'merged_fdr_corrected_filter.smr')
    merged_df.to_csv(output_file_path, sep='\t', index=False)

    print(f"合并后的数据已保存到 {output_file_path}")
    return output_file_path

def select_smr_files(directory):
    # 用于存储按 id 分组的文件集合
    id_groups = {}

    # 遍历目录中的文件
    for filename in os.listdir(directory):
        if filename.endswith('_fdr_corrected_filter.smr'):
            # 用 -Heart_ 分割文件名
            parts = filename.split('-Brain_')
            if len(parts) == 2:
                id_ = parts[0]
                # 如果 id 不在字典中，初始化一个空列表
                if id_ not in id_groups:
                    id_groups[id_] = []
                # 将文件路径添加到对应的 id 组中
                id_groups[id_].append(os.path.join(directory, filename))

    # 用于存储最终结果的数据
    final_data = {}

    # 遍历每个 id 组
    for id_, file_paths in id_groups.items():
        gene_sets = []
        for file_path in file_paths:
            try:
                # 读取文件
                df = pd.read_csv(file_path, sep='\t')
                # 检查是否存在 Gene 列
                if 'Gene' in df.columns:
                    # 获取当前文件中 Gene 列的唯一值集合
                    gene_set = set(df['Gene'].dropna())
                    gene_sets.append(gene_set)
                else:
                    print(f"文件 {file_path} 中不存在 'Gene' 列。")
            except Exception as e:
                print(f"读取文件 {file_path} 时出错: {e}")

        # 如果有文件成功读取到 Gene 列
        if gene_sets:
            # 求所有集合的交集，得到该 id 组中所有文件共同的 Gene
            #common_genes = set.intersection(*gene_sets)
            #交集比较少时取并集
            common_genes = set.union(*gene_sets)
            # 将共同基因添加到最终结果数据中，以 id 作为列名
            final_data[id_] = pd.Series(sorted(common_genes))
        else:
            print(f"id 为 {id_} 的文件组中未找到包含 'Gene' 列的有效文件。")

    # 将最终结果转换为 DataFrame
    final_df = pd.DataFrame(final_data)

    # 保存最终结果到一个 xlsx 文件
    output_path = os.path.join(directory, 'all_common_genes.xlsx')
    final_df.to_excel(output_path, index=False)
    print(f"所有 id 对应的共同基因已保存到 {output_path}")


def select_smr_files_txt(directory):
    # 用于存储按 id 分组的文件集合
    id_groups = {}

    # 遍历目录中的文件
    for filename in os.listdir(directory):
        if filename.endswith('_fdr_corrected_filter.smr'):
            # 用 -Heart_ 分割文件名
            parts = filename.split('-Brain_')
            if len(parts) == 2:
                id_ = parts[0]
                # 如果 id 不在字典中，初始化一个空列表
                if id_ not in id_groups:
                    id_groups[id_] = []
                # 将文件路径添加到对应的 id 组中
                id_groups[id_].append(os.path.join(directory, filename))

    # 用于存储所有的共同基因
    all_common_genes = set()

    # 遍历每个 id 组
    for id_, file_paths in id_groups.items():
        gene_sets = []
        for file_path in file_paths:
            try:
                # 读取文件
                df = pd.read_csv(file_path, sep='\t')
                # 检查是否存在 Gene 列
                if 'Gene' in df.columns:
                    # 获取当前文件中 Gene 列的唯一值集合
                    gene_set = set(df['Gene'].dropna())
                    gene_sets.append(gene_set)
                else:
                    print(f"文件 {file_path} 中不存在 'Gene' 列。")
            except Exception as e:
                print(f"读取文件 {file_path} 时出错: {e}")

        # 如果有文件成功读取到 Gene 列
        if gene_sets:
            # 求所有集合的交集，得到该 id 组中所有文件共同的 Gene
            #common_genes = set.intersection(*gene_sets)
            common_genes = set.union(*gene_sets)

            # 将共同基因添加到所有共同基因集合中
            all_common_genes.update(common_genes)
        else:
            print(f"id 为 {id_} 的文件组中未找到包含 'Gene' 列的有效文件。")

    # 保存所有共同基因到一个 txt 文件
    output_path = os.path.join(directory, 'all_common_genes.txt')
    with open(output_path, 'w') as f:
        for gene in sorted(all_common_genes):
            f.write(gene + '\n')
    print(f"所有共同基因已保存到 {output_path}")

if __name__ == "__main__":

    #process_all_smr_files(directory)
    #filter_smr_files(directory)
    #merge_smr_files(directory)

    select_smr_files(directory)
    select_smr_files_txt(directory)