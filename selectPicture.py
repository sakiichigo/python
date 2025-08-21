import os
import shutil
import datetime
from pathlib import Path


def main():
    # 源目录和目标目录路径
    source_dir = r"F:\DCIM\101NCZ_6"
    target_dir = r"C:\Users\22565\Pictures\毕业"

    # 目标日期：2025年6月2日
    target_date = datetime.date(2025, 6, 2)

    # 创建目标目录（如果不存在）
    os.makedirs(target_dir, exist_ok=True)

    # 确保源目录存在
    if not os.path.exists(source_dir):
        print(f"错误：源目录 '{source_dir}' 不存在。")
        return

    # 遍历源目录中的所有文件
    for filename in os.listdir(source_dir):
        file_path = os.path.join(source_dir, filename)

        # 检查文件是否为JPG图片（不区分大小写）
        if not filename.lower().endswith(('.nef')):
            continue

        # 获取文件的创建时间和修改时间
        try:
            # 获取文件修改时间
            modified_time = os.path.getmtime(file_path)
            modified_date = datetime.datetime.fromtimestamp(modified_time).date()

            # 获取文件创建时间（在某些系统上可能不可靠）
            created_time = os.path.getctime(file_path)
            created_date = datetime.datetime.fromtimestamp(created_time).date()

            # 检查文件的修改日期或创建日期是否为目标日期
            if modified_date == target_date or created_date == target_date:
                # 构建目标文件路径
                target_file = os.path.join(target_dir, filename)

                # 复制文件
                shutil.copy2(file_path, target_file)
                print(f"已复制: {filename}")
        except Exception as e:
            print(f"处理文件 {filename} 时出错: {e}")

    print("操作完成！")


if __name__ == "__main__":
    main()