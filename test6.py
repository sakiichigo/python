from PIL import Image, ImageDraw, ImageFont
import os

# 图片合并部分
save_dir = r"D:\document\bioInfo\alps-eye\figure\smr_"
target_files = [
    "glaucoma_Cerebellar_Hemisphere(ukb-a-79)_manhattan_plot.png",
"glaucoma_Cerebellar_Hemisphere(ebi-a-GCST009722)_manhattan_plot.png",
    "glaucoma_Cerebellar_Hemisphere(ukb-a-424)_manhattan_plot.png"
]


def merge_images_vertically(file_list, output_path, title):
    """垂直合并多个图片并添加标题"""
    if not file_list:
        print("没有可合并的图片！")
        return

    # 打开所有图片
    images = [Image.open(os.path.join(save_dir, f)) for f in file_list]
    widths, heights = zip(*(i.size for i in images))

    # 计算合并后图片的尺寸
    max_width = max(widths)
    total_height = sum(heights)

    # 创建新图片
    new_im = Image.new('RGB', (max_width, total_height))

    # 逐个粘贴图片并调整高度
    y_offset = 0
    for im in images:
        new_width = max_width
        new_height = int(im.size[1] * (max_width / im.size[0]))  # 按比例调整高度
        resized_im = im.resize((new_width, new_height), Image.LANCZOS)
        new_im.paste(resized_im, (0, y_offset))
        y_offset += new_height

    # 添加标题文本
    title = title.replace("_", " ")
    font_size = int(max_width / 40)
    draw = ImageDraw.Draw(new_im)

    try:
        font = ImageFont.truetype("arialbd.ttf", font_size)
    except OSError:
        print("无法找到 arialbd.ttf 字体文件，将使用默认字体。")
        font = ImageFont.load_default()

    # 计算文本位置
    text_bbox = draw.textbbox((0, 0), title.upper(), font=font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]

    x = 10
    y = 10
    if x + text_width > max_width:
        x = max_width - text_width - 10
    if y + text_height > total_height:
        y = total_height - text_height - 10

    draw.text((x, y), title.upper(), font=font, fill=(0, 0, 0))

    # 保存合并后的图片
    new_im.save(output_path)
    print(f"合并后的图片已保存至: {output_path}")


# 合并所有目标图片为一个文件
output_path = os.path.join(save_dir, "combined_all_images.png")
merge_images_vertically(target_files, output_path, "Glaucoma Cerebellar Hemisphere")