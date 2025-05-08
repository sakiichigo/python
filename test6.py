import csv
import os


def tsv_to_csv(tsv_file_path):
    try:
        # 生成对应的 CSV 文件路径
        csv_file_path = os.path.splitext(tsv_file_path)[0] + '.csv'
        with open(tsv_file_path, 'r', encoding='utf-8', newline='') as tsv_file:
            tsv_reader = csv.reader(tsv_file, delimiter='\t')
            with open(csv_file_path, 'w', encoding='utf-8', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                for row in tsv_reader:
                    csv_writer.writerow(row)
        print(f"已成功将 {tsv_file_path} 转换为 {csv_file_path}")
    except FileNotFoundError:
        print(f"错误：未找到文件 {tsv_file_path}")
    except Exception as e:
        print(f"处理文件 {tsv_file_path} 时发生错误: {e}")


def convert_all_tsv_files(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.tsv'):
                tsv_file_path = os.path.join(root, file)
                tsv_to_csv(tsv_file_path)


if __name__ == "__main__":
    directory = r'E:\anaFiles\atherosclerosisGwas\tsv'
    convert_all_tsv_files(directory)
