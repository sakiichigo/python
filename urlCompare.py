import os
import json
import shutil


def main():
    # 源目录路径
    source_dir = r"E:\rsfMRI"
    # JSON文件路径
    json_file = r"C:\Users\22565\Desktop\新文件 4.txt"
    # 目标目录路径
    target_dir = r"E:\rsfMRI\select"

    # 确保目标目录存在
    os.makedirs(target_dir, exist_ok=True)

    try:
        # 读取JSON文件
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 获取entries数组中的文件名（假设每个条目有一个包含文件名的字段）
        # 这里需要根据实际JSON结构调整获取文件名的方式
        entriesSet=data['hits']['hits'][0]['files']['entries']
        all_keys = []
        for file_name, file_info in entriesSet.items():
            # 取出每个文件信息字典里的 'key' ，并添加到列表
            key_value = file_info.get('key')  # 用 get 避免键不存在时报错，若确定都有也可用 file_info['key']
            if key_value:
                all_keys.append(key_value)
        json_filenames = all_keys

        # 获取源目录中的所有文件名
        source_filenames = set(os.listdir(source_dir))

        # 找出JSON中存在但源目录中不存在的文件名
        missing_filenames = [name for name in json_filenames if name not in source_filenames]

        # 打印结果
        print(f"JSON文件中共有 {len(json_filenames)} 个文件名")
        print(f"源目录中共有 {len(source_filenames)} 个文件")
        print(f"找到 {len(missing_filenames)} 个在JSON中但不在源目录中的文件名")

        # 将缺失的文件名保存到目标目录下的文件中
        output_file = os.path.join(target_dir, "missing_files.txt")
        with open(output_file, 'w', encoding='utf-8') as f:
            for name in missing_filenames:
                f.write(name + '\n')

        print(f"结果已保存到: {output_file}")

    except Exception as e:
        print(f"发生错误: {e}")


if __name__ == "__main__":
    main()