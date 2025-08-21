import os
import argparse
from cairosvg import svg2png


def convert_svg_to_png(input_path, output_path=None, dpi=300):
    """
    将单个SVG文件转换为PNG

    参数:
        input_path (str): 输入SVG文件路径
        output_path (str, optional): 输出PNG文件路径，默认为None(自动生成)
        dpi (int, optional): 输出图片的DPI，默认为300

    返回:
        bool: 转换成功返回True，失败返回False
    """
    try:
        # 如果没有指定输出路径，自动生成
        if not output_path:
            base_name = os.path.splitext(os.path.basename(input_path))[0]
            output_dir = os.path.dirname(input_path)
            output_path = os.path.join(output_dir, f"{base_name}.png")

        # 执行转换
        svg2png(url=input_path, write_to=output_path, dpi=dpi)

        print(f"成功: {input_path} → {output_path} (DPI: {dpi})")
        return True
    except Exception as e:
        print(f"错误: {input_path} → {e}")
        return False


def batch_convert_svg_to_png(input_dir, output_dir=None, dpi=300):
    """
    批量转换目录中的所有SVG文件为PNG

    参数:
        input_dir (str): 输入目录路径
        output_dir (str, optional): 输出目录路径，默认为None(使用输入目录)
        dpi (int, optional): 输出图片的DPI，默认为300

    返回:
        int: 成功转换的文件数量
    """
    if not os.path.isdir(input_dir):
        print(f"错误: 输入目录不存在 - {input_dir}")
        return 0

    # 如果没有指定输出目录，使用输入目录
    if not output_dir:
        output_dir = input_dir
    else:
        # 创建输出目录(如果不存在)
        os.makedirs(output_dir, exist_ok=True)

    success_count = 0
    total_count = 0

    # 遍历目录中的所有文件
    for filename in os.listdir(input_dir):
        if filename.lower().endswith('.svg'):
            total_count += 1
            svg_path = os.path.join(input_dir, filename)
            base_name = os.path.splitext(filename)[0]
            png_path = os.path.join(output_dir, f"{base_name}.png")

            if convert_svg_to_png(svg_path, png_path, dpi):
                success_count += 1

    print(f"\n转换完成: {success_count}/{total_count} 个SVG文件成功转换为PNG")
    return success_count


def main():
    """命令行入口函数"""
    parser = argparse.ArgumentParser(description='SVG转PNG转换器')
    parser.add_argument('-i', '--input', required=True,
                        help='输入SVG文件或目录路径')
    parser.add_argument('-o', '--output',
                        help='输出PNG文件或目录路径(默认与输入相同位置)')
    parser.add_argument('-d', '--dpi', type=int, default=300,
                        help='输出图片的DPI值(默认300)')
    parser.add_argument('-b', '--batch', action='store_true',
                        help='批量处理目录中的所有SVG文件')

    args = parser.parse_args()

    # 验证输入路径
    if not os.path.exists(args.input):
        print(f"错误: 路径不存在 - {args.input}")
        return

    # 处理单个文件
    if not args.batch:
        if os.path.isfile(args.input) and args.input.lower().endswith('.svg'):
            convert_svg_to_png(args.input, args.output, args.dpi)
        else:
            print(f"错误: 输入不是有效的SVG文件 - {args.input}")

    # 批量处理目录
    else:
        if os.path.isdir(args.input):
            batch_convert_svg_to_png(args.input, args.output, args.dpi)
        else:
            print(f"错误: 输入不是目录 - {args.input}")


if __name__ == "__main__":
    main()

    #批量转换:
    # python svgToPng.py -i D:/document/bioInfo/alps-eye/figure/gtex/ -o D:/document/bioInfo/alps-eye/figure/gtex/ -d 300 -
