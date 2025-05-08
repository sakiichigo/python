import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np
from PIL import Image, ImageDraw, ImageFont

# 设置图片清晰度
plt.rcParams['figure.dpi'] = 300

# 读取 Excel 文件
excel_path = r"D:\document\bioInfo\heart-sarco-0330\smr_restults.xlsx"
df_excel = pd.read_excel(excel_path)
id_list = df_excel.columns.tolist()

# 定义 SMR 文件所在目录
smr_dir = r"E:\sacropeniaGwas\smrResult"

# 定义保存图片的目录
save_dir = r"D:\document\bioInfo\heart-sarco-0330\figure\smr"
# 定义保存显著基因的目录
gene_save_dir = r"D:\document\bioInfo\heart-sarco-0330\smr"
# 检查保存目录是否存在，若不存在则创建
if not os.path.exists(save_dir):
    os.makedirs(save_dir)
if not os.path.exists(gene_save_dir):
    os.makedirs(gene_save_dir)

# 定义 id 对应关系
id_mapping = {
    "ebi-a-GCST002783": "Body mass index",
    "ukb-a-248": "Body mass index",
    "ukb-a-374": "Hand grip strength",
    "ukb-a-379": "Hand grip strength",
    "ukb-a-482": "Physical activity",
    "ukb-a-483": "Physical activity",
    "ukb-a-484": "Physical activity",
    "ukb-a-485": "Physical activity",
    "ukb-a-486": "Physical activity",
    "ukb-a-487": "Physical activity",
    "ukb-a-503": "Physical activity",
    "ukb-a-508": "Physical activity",
    "ukb-a-511": "Physical activity",
    "ukb-a-513": "Usual walking Pace"
}

# 为 22 个染色体分配不同颜色
chrom_colors = {i + 1: plt.cm.tab20(i) for i in range(22)}

# 遍历每个 SMR 文件
for file_name in os.listdir(smr_dir):
    if file_name.endswith('_fdr_corrected.smr'):
        # 提取 id 和 eqtl
        parts = file_name.split("-Heart_")
        if len(parts) == 2:
            id_value = parts[0]
            eqtl = parts[1].replace("_fdr_corrected.smr", "")
            # 替换 id_value
            pheno = id_mapping.get(id_value, id_value)

            # 读取 SMR 文件
            file_path = os.path.join(smr_dir, file_name)
            df_smr = pd.read_csv(file_path, sep='\t')

            # 输出列名，方便查看
            print(f"Columns in {file_name}: {df_smr.columns}")

            # 修改列名
            chr_col = 'ProbeChr'
            bp_col = 'Probe_bp'
            p_col = 'p_SMR_fdr'

            # 检查列名是否存在
            if chr_col not in df_smr.columns or bp_col not in df_smr.columns or p_col not in df_smr.columns:
                print(f"Error: One or more required columns ({chr_col}, {bp_col}, {p_col}) not found in {file_name}.")
                continue

            # 绘制曼哈顿图
            plt.figure(figsize=(12, 6))
            # 删去背景的表格线
            sns.set_style("white")

            # 计算每个染色体上 bp_col 的最大值，用于缩放
            chr_max_bp = df_smr.groupby(chr_col)[bp_col].max()

            # 绘制每个染色体的点
            for chrom in df_smr[chr_col].unique():
                if chrom in chrom_colors:
                    chrom_data = df_smr[df_smr[chr_col] == chrom]
                    # 计算缩放后的横坐标
                    scaled_bp = chrom - 1 + chrom_data[bp_col] / (chr_max_bp[chrom] + 1)
                    plt.scatter(scaled_bp, -np.log10(chrom_data[p_col]),
                                color=chrom_colors[chrom], s=10)

            # 设置标题和坐标轴标签
            plt.xlabel('Chromosome Number')
            # 设置 x 轴刻度
            xticks = list(range(1, 23))
            xticklabels = [f"Chr{i}" for i in xticks]
            plt.xticks(xticks, xticklabels, rotation=45)
            plt.ylabel('-log10(p-value)')

            # 绘制显著性阈值线
            threshold = -np.log10(5e-8)
            plt.axhline(y=threshold, color='r', linestyle='--')

            # 标记超过阈值的点
            significant_genes = []
            for _, row in df_smr.iterrows():
                if -np.log10(row[p_col]) > threshold:
                    scaled_x = row[chr_col] - 1 + row[bp_col] / (chr_max_bp[row[chr_col]] + 1)
                    plt.text(scaled_x, -np.log10(row[p_col]), row['Gene'], fontsize=6, ha='center', va='bottom')
                    significant_genes.append(row)
            significant_df = pd.DataFrame(significant_genes)

            # 删去对染色体颜色和 Genome-wide significance 的提示框
            handles, labels = plt.gca().get_legend_handles_labels()
            new_handles = []
            new_labels = []
            for handle, label in zip(handles, labels):
                # 过滤掉染色体编号和显著性阈值线的标签
                if not (label.isdigit() or label == 'Genome-wide significance'):
                    new_handles.append(handle)
                    new_labels.append(label)
            # 仅当有有效图例项时显示图例
            if new_labels:
                plt.legend(new_handles, new_labels, loc='upper right')

            # 删去右和上的框线
            ax = plt.gca()
            ax.spines['right'].set_visible(False)
            ax.spines['top'].set_visible(False)

            # 重新设置纵坐标刻度，使其取整数
            y_values = df_smr[p_col].apply(lambda x: -np.log10(x))
            y_min = int(np.floor(y_values.min()))
            y_max = int(np.ceil(y_values.max()))
            y_ticks = np.arange(y_min, y_max + 1)
            plt.yticks(y_ticks)

            # 调整左线的范围
            ax.spines['left'].set_bounds(y_min, y_max)
            ax.tick_params(axis='y', direction='in')

            # 使下线长度覆盖点的位置，显示刻度，方向向内
            x_min = df_smr[chr_col].min() - 1
            x_max = df_smr[chr_col].max()
            ax.spines['bottom'].set_bounds(x_min, x_max)
            ax.tick_params(axis='x', direction='in')

            # 将标题放在左上角
            plt.text(0.02, 0.95, pheno, transform=plt.gca().transAxes, fontsize=12, va='top')

            # 保存图片
            save_path = os.path.join(save_dir, f'{pheno}_{eqtl}({id_value})_manhattan_plot.png')
            plt.tight_layout()
            plt.savefig(save_path)
            plt.close()

            # 保存显著基因到 xlsx 文件
            gene_save_path = os.path.join(gene_save_dir, f'{pheno}_{eqtl}({id_value})_significant_genes.xlsx')
            significant_df.to_excel(gene_save_path, index=False)

# 图片合并部分
target_files = [
    "Body mass index_Atrial_Appendage(ukb-a-248)_manhattan_plot.png",
    "Body mass index_Left_Ventricle(ukb-a-248)_manhattan_plot.png",
    "Hand grip strength_Atrial_Appendage(ukb-a-379)_manhattan_plot.png",
    "Hand grip strength_Left_Ventricle(ukb-a-379)_manhattan_plot.png",
    "Usual walking Pace_Atrial_Appendage(ukb-a-513)_manhattan_plot.png",
    "Usual walking Pace_Left_Ventricle(ukb-a-513)_manhattan_plot.png",
    "Physical activity_Atrial_Appendage(ukb-a-482)_manhattan_plot.png",
    "Physical activity_Left_Ventricle(ukb-a-482)_manhattan_plot.png"
]

atrial_appendage_files = [f for f in target_files if "Atrial_Appendage" in f]
left_ventricle_files = [f for f in target_files if "Left_Ventricle" in f]


def merge_images_vertically(file_list, output_path, eqtl):
    images = [Image.open(os.path.join(save_dir, f)) for f in file_list]
    widths, heights = zip(*(i.size for i in images))
    max_width = max(widths)
    total_height = sum(heights)

    new_im = Image.new('RGB', (max_width, total_height))

    y_offset = 0
    for im in images:
        new_width = max_width
        new_height = int(im.size[1] * (max_width / im.size[0]))  # 按比例调整高度
        im = im.resize((new_width, new_height), Image.LANCZOS)
        new_im.paste(im, (0, y_offset))
        y_offset += new_height

    # 把下划线替换成空格
    eqtl = eqtl.replace("_", " ")
    # 根据图片尺寸比率设置字体大小，进一步缩小字体
    font_size = int(max_width / 40)
    draw = ImageDraw.Draw(new_im)
    try:
        font = ImageFont.truetype("arialbd.ttf", font_size)
    except OSError:
        print("无法找到 arialbd.ttf 字体文件，将使用默认字体。")
        font = ImageFont.load_default()

    # 计算文本的宽度和高度
    _, _, text_width, text_height = draw.textbbox((0, 0), eqtl.upper(), font=font)
    # 调整文本位置，避免盖住原图片内容，并让文本上移
    x = 10
    y = 5
    if x + text_width > max_width:
        x = max_width - text_width - 10
    if y + text_height > total_height:
        y = total_height - text_height - 10

    draw.text((x, y), eqtl.upper(), font=font, fill=(0, 0, 0))

    new_im.save(output_path)


# 合并 Atrial_Appendage 组图片
atrial_appendage_output = os.path.join(save_dir, "Atrial_Appendage_combined.png")
merge_images_vertically(atrial_appendage_files, atrial_appendage_output, "Atrial_Appendage")

# 合并 Left_Ventricle 组图片
left_ventricle_output = os.path.join(save_dir, "Left_Ventricle_combined.png")
merge_images_vertically(left_ventricle_files, left_ventricle_output, "Left_Ventricle")
