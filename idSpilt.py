import os


def merge_txt_files(source_dir, output_dir):
    output_file = os.path.join(output_dir, 'merged.txt')
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                if file.endswith('.txt'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as infile:
                            # 去除空行
                            non_empty_lines = [line for line in infile if line.strip()]
                            outfile.writelines(non_empty_lines)
                    except Exception as e:
                        print(f"读取文件 {file_path} 时出错: {e}")
    return output_file


def split_file(input_file, output_dir, num_parts):
    with open(input_file, 'r', encoding='utf-8') as infile:
        lines = infile.readlines()
    total_lines = len(lines)
    part_size = total_lines // num_parts

    for i in range(num_parts):
        start = i * part_size
        end = start + part_size if i < num_parts - 1 else total_lines
        part_lines = lines[start:end]
        output_file = os.path.join(output_dir, f'part_{i + 1}.txt')
        with open(output_file, 'w', encoding='utf-8') as outfile:
            outfile.writelines(part_lines)


if __name__ == "__main__":
    source_directory = r'D:\document\bioInfo\gwasID\Osteoarthritis'
    output_directory = os.path.join(source_directory, 'output')
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    merged_file = merge_txt_files(source_directory, output_directory)
    split_file(merged_file, output_directory, 4)
