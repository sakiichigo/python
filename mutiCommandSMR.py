import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


def execute_command(command):
    print(f"正在执行命令: {' '.join(command)}")
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout_lines = []
        stderr_lines = []

        # 逐行读取标准输出
        for line in process.stdout:
            print(line, end='')
            stdout_lines.append(line)
        # 逐行读取标准错误
        for line in process.stderr:
            print(line, end='', file=os.sys.stderr)
            stderr_lines.append(line)

        process.wait()
        if process.returncode == 0:
            print("命令执行成功")
            return ''.join(stdout_lines)
        else:
            print("命令执行失败")
            return None
    except Exception as e:
        print(f"执行命令时发生错误: {e}")
        return None


# 定义相关目录和文件路径
# 存放 ma 文件（txt 或者 ma 都可，这里用的 txt，ma 则修改 command
ma_dir = r'E:\anaFiles\eyeGwas\ma'
# 大脑 eqtl 数据(https://yanglab.westlake.edu.cn/data/SMR/GTEx_V8_cis_eqtl_summary.html 下载解压后放进去
brain_dir = r'E:\anaFiles\brain'
# 存放 smr 路径
output_base_dir = r'E:\anaFiles\smrBrainEye'
# 千人参考组文件路径(第二个 eur 是文件
kg_dir = r'D:\document\bioInfo\EUR\EUR'
# 日志文件路径
log_file_path = r'E:\anaFiles\smrBrainEye\log'

# 获取 ma_dir 目录下所有的 txt 文件
all_aaa = [os.path.splitext(filename)[0] for filename in os.listdir(ma_dir) if filename.endswith('.txt')]
if True:
    with open(r'E:\anaFiles\eyeGwas\ma\pheno\gwasinfo\allId.txt', 'r', encoding='utf-8') as f:
        all_aaa = [line.strip() for line in f.readlines()]

# 获取 E:\anaFiles\brain 目录下所有文件的文件名部分
all_bbb = []
for filename in os.listdir(brain_dir):
    full_path = os.path.join(brain_dir, filename)
    if os.path.isdir(full_path):
        all_bbb.append(filename)

# 读取日志文件内容
executed_outputs = set()
if os.path.exists(log_file_path):
    with open(log_file_path, 'r', encoding='utf-8') as log_file:
        for line in log_file:
            executed_outputs.add(line.strip())

# 构建所有命令
all_commands = []
for aaa in all_aaa:
    gwas_file = os.path.join(ma_dir, aaa) + '.txt'
    # 检查 gwas 文件是否存在
    if not os.path.exists(gwas_file):
        print(f"文件 {gwas_file} 不存在，跳过该命令。")
        continue
    for bbb in all_bbb:
        output_file_base = os.path.join(output_base_dir, f'{aaa}-{bbb}')

        # 检查日志中是否已存在该 output_file_base
        if output_file_base in executed_outputs:
            print(f"文件 {output_file_base} 已在日志中记录，跳过该命令。")
            continue

        # 检查是否存在忽略后缀的文件
        found = False
        for root, dirs, files in os.walk(output_base_dir):
            for file in files:
                file_base = os.path.splitext(os.path.join(root, file))[0]
                if file_base == output_file_base:
                    found = True
                    break
            if found:
                break
        if found:
            print(f"文件 {output_file_base} 已存在，跳过该命令。")
            continue

        # 将 output_file_base 写入日志文件
        with open(log_file_path, 'a', encoding='utf-8') as log_file:
            log_file.write(output_file_base + '\n')
            executed_outputs.add(output_file_base)

        command = [
            'smr',
            '--bfile', kg_dir,
            '--gwas-summary', gwas_file,
            '--beqtl-summary', os.path.join(brain_dir, bbb, bbb),
            '--out', output_file_base,
            '--thread-num', '10'
        ]
        all_commands.append(command)

# 使用线程池并发执行命令并显示进度条
max_workers = 3  # 根据系统资源调整线程池大小
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    results = list(tqdm(executor.map(execute_command, all_commands), total=len(all_commands), desc="执行命令进度"))