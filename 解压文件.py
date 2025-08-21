import os
import tarfile
from pathlib import Path

# 目标目录（包含.tar.bz2文件的目录）
root_dir = Path(r"E:\Twas\gtex")


def decompress_tar_bz2_files():
    # 遍历目录下所有.tar.bz2文件
    for tar_path in root_dir.glob("*.tar.bz2"):
        if tar_path.is_file():  # 确保是文件（排除目录）
            # 提取文件名（去除.tar.bz2后缀）作为目录名
            dir_name = tar_path.stem.replace(".tar", "")  # 例如："data.tar.bz2" → "data"
            # 新建目录路径（与压缩文件同目录）
            target_dir = root_dir / dir_name

            try:
                # 创建目录（若已存在则忽略）
                target_dir.mkdir(exist_ok=True)
                print(f"已创建目录：{target_dir}")

                # 解压.tar.bz2文件到目标目录
                with tarfile.open(tar_path, "r:bz2") as tar:
                    # 解压所有文件到目标目录
                    tar.extractall(path=target_dir)
                print(f"已解压：{tar_path.name} → {target_dir}")

            except tarfile.TarError as e:
                print(f"解压文件{tar_path.name}失败（文件可能损坏）：{str(e)}")
            except Exception as e:
                print(f"处理文件{tar_path.name}时出错：{str(e)}")


if __name__ == "__main__":
    if not root_dir.exists():
        print(f"错误：目录{root_dir}不存在，请检查路径是否正确。")
    else:
        print(f"开始处理目录：{root_dir}")
        decompress_tar_bz2_files()
        print("所有.tar.bz2文件处理完成！")