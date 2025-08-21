import os
from PIL import Image
from fpdf import FPDF
import platform

# 图片目录路径
png_dir = r"D:\document\bioInfo\covid-alps\result\png"
# 输出PDF路径
output_pdf = os.path.join(png_dir, "合并图片.pdf")

# 获取目录下所有png图片并按名称排序
png_files = [f for f in os.listdir(png_dir) if f.lower().endswith(".png")]
png_files.sort()  # 按文件名排序

# 初始化PDF对象，设置单位为毫米，页面大小为A4
pdf = FPDF(unit="mm", format="A4")


# 检测操作系统并尝试定位中文字体
def find_font_path():
    system = platform.system()
    if system == "Windows":
        # Windows系统字体路径
        font_path = "C:/Windows/Fonts/simhei.ttf"
        if os.path.exists(font_path):
            return font_path
        # 尝试其他常见中文字体
        for font_name in ["simfang.ttf", "simsun.ttc", "msyh.ttc"]:
            path = f"C:/Windows/Fonts/{font_name}"
            if os.path.exists(path):
                return path
    elif system == "Darwin":  # macOS
        # macOS系统字体路径
        font_path = "/System/Library/Fonts/PingFang.ttc"
        if os.path.exists(font_path):
            return font_path
    # Linux系统字体路径
    linux_paths = [
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
    ]
    for path in linux_paths:
        if os.path.exists(path):
            return path
    return None


# 查找可用的中文字体
font_path = find_font_path()
if font_path:
    # 获取字体文件名（不含路径）
    font_filename = os.path.basename(font_path)
    # 提取字体名称（不含扩展名）
    font_name = os.path.splitext(font_filename)[0]
    try:
        pdf.add_font(font_name, '', font_path, uni=True)
        print(f"已加载中文字体: {font_name}")
    except Exception as e:
        print(f"加载字体失败: {str(e)}")
        print("将使用默认字体，可能导致中文显示异常")
        font_name = None
else:
    print("未找到中文字体，将使用默认字体，可能导致中文显示异常")
    font_name = None

# 遍历图片并添加到PDF
for i, file_name in enumerate(png_files, 1):
    # 图片完整路径
    img_path = os.path.join(png_dir, file_name)
    # 提取图片名称（去掉.png）
    img_title = os.path.splitext(file_name)[0]
    # 图标题格式：Figure 序号.图片名称
    figure_title = f"Figure {i}.{img_title}"

    try:
        # 打开图片获取尺寸
        with Image.open(img_path) as img:
            img_width, img_height = img.size

        # 添加新页面
        pdf.add_page()

        # 设置标题字体和大小
        if font_name:
            pdf.set_font(font_name, size=12)
        else:
            pdf.set_font("Arial", size=12)  # 使用默认字体

        # 标题位置（距离顶部10mm）
        pdf.text(x=10, y=10, txt=figure_title)

        # 计算图片最大可显示尺寸（A4宽度210mm，减去左右边距20mm）
        max_width = 190  # 210 - 20
        max_height = 250  # 297 - 50（预留标题和底部空间）

        # 计算图片缩放比例
        width_ratio = max_width / img_width
        height_ratio = max_height / img_height
        scale = min(width_ratio, height_ratio)

        # 计算缩放后的图片尺寸
        new_width = img_width * scale
        new_height = img_height * scale

        # 计算图片位置（水平居中）
        x_pos = (210 - new_width) / 2
        y_pos = 20  # 标题下方10mm开始

        # 添加图片
        pdf.image(img_path, x=x_pos, y=y_pos, w=new_width, h=new_height)

        print(f"已添加: {file_name} 到PDF，标题: {figure_title}")

    except Exception as e:
        print(f"处理图片 {file_name} 出错: {str(e)}")

# 保存PDF
pdf.output(output_pdf)
print(f"PDF生成完成，保存路径: {output_pdf}")