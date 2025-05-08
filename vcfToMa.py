import gzip
import os
from tqdm import tqdm
from multiprocessing import Pool

# 定义输入和输出文件夹路径
input_dir = r'E:\sacropeniaGwas'
output_dir = r'E:\sacropeniaGwas\ma'

# 如果输出文件夹不存在则创建
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 定义表头
header = ['SNP', 'A1', 'A2', 'freq', 'b', 'se', 'p', 'n']


def process_file(filename):
    file_path = os.path.join(input_dir, filename)
    # 生成输出文件名
    output_filename = os.path.splitext(os.path.splitext(filename)[0])[0] + '.txt'
    output_file = os.path.join(output_dir, output_filename)

    try:
        # 打开输出文件
        with open(output_file, 'w') as outfile:
            # 写入表头
            outfile.write('\t'.join(header) + '\n')

            # 打开 gzip 文件并获取总行数
            with gzip.open(file_path, 'rt') as temp_infile:
                total_lines = sum(1 for _ in temp_infile)

            # 再次打开 gzip 文件进行处理
            with gzip.open(file_path, 'rt') as infile:
                # 创建行处理的进度条
                line_progress = tqdm(infile, total=total_lines, desc=f"处理 {filename}", unit="行", leave=False)
                for line in line_progress:
                    # 忽略以 # 开头的行
                    if line.startswith('#'):
                        continue
                    # 分割行数据
                    parts = line.strip().split('\t')
                    snp_id = parts[2]
                    ref = parts[3]
                    alt = parts[4]
                    # 获取倒数第二列的 FORMAT 和值
                    format_str = parts[-2].split(':')
                    value_str = parts[-1].split(':')
                    # 创建一个字典，将 FORMAT 和值一一对应
                    data_dict = dict(zip(format_str, value_str))
                    # 提取 ES, SE, LP
                    b = data_dict.get('ES')
                    se = data_dict.get('SE')
                    lp = data_dict.get('LP')
                    if lp:
                        # 还原 LP 的值
                        p = 10 ** (-float(lp))
                    else:
                        p = None
                    # 获取 freq 的值
                    freq = data_dict.get('AF')
                    # n 设置为 NA
                    n = 'NA'

                    # 处理可能为 None 的值
                    freq = freq if freq is not None else 'NA'
                    b = b if b is not None else 'NA'
                    se = se if se is not None else 'NA'
                    p = str(p) if p is not None else 'NA'

                    # 写入数据到输出文件
                    output_line = [snp_id, ref, alt, freq, b, se, p, n]
                    outfile.write('\t'.join(output_line) + '\n')
    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {e}")

if __name__ == '__main__':
    # 获取所有 .vcf.gz 文件
    vcf_files = [filename for filename in os.listdir(input_dir) if filename.endswith('.vcf.gz')]

    # 创建进程池
    with Pool() as pool:
        # 使用进程池并行处理文件
        list(tqdm(pool.imap(process_file, vcf_files), total=len(vcf_files), desc="处理 .vcf.gz 文件", unit="文件"))
