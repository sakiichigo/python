import matplotlib.pyplot as plt
from matplotlib_venn import venn3, venn3_circles
import matplotlib.patches as mpatches

# 设置字体以确保所有字符正确显示
plt.rcParams["font.family"] = ["Arial", "sans-serif"]
plt.rcParams["axes.unicode_minus"] = False  # 正确显示负号

# 定义各区域的基因集合
smr = {'AFAP1', 'RP11-217B7.2', 'TMCO1'}
eqtl = {'HLA-DRB1', 'DND1P1', 'FIBP', 'CD151', 'AP006621.5', 'AFAP1', 'HLA-DRB5', 'ARHGAP27', 'CRHR1-IT1', 'BAG6'}
ppi = {'ARHGAP27', 'ARL17A', 'SPPL2C', 'LRRC37A', 'PLEKHM1', 'LRRC37A2', 'TMCO1', 'TRIOBP', 'AFAP1', 'CAPZA1', 'PPP1CB',
       'TXNRD2'}

# 计算各区域的大小
subset_sizes = (
    len(smr - eqtl - ppi),  # 001 (SMR only)
    len(eqtl - smr - ppi),  # 010 (eQTL only)
    len(smr & eqtl - ppi),  # 011 (SMR & eQTL)
    len(ppi - smr - eqtl),  # 100 (PPI only)
    len(smr & ppi - eqtl),  # 101 (SMR & PPI)
    len(eqtl & ppi - smr),  # 110 (eQTL & PPI)
    len(smr & eqtl & ppi)  # 111 (All three)
)

# 创建图形和韦恩图
fig, ax = plt.subplots(figsize=(12, 10))

# 绘制韦恩图，使用subset_sizes控制各区域大小
v = venn3(
    subsets=subset_sizes,
    set_labels=('SMR (3)', 'eQTL (10)', 'PPI (12)'),
    ax=ax
)

# 设置各区域的颜色
v.get_patch_by_id('100').set_color('#4a90e2')  # SMR
v.get_patch_by_id('010').set_color('#f16854')  # eQTL
v.get_patch_by_id('001').set_color('#62c68a')  # PPI

# 设置透明度和边框
for patch in v.patches:
    patch.set_alpha(0.6)
    patch.set_edgecolor('none')

# 绘制圆形边框
c = venn3_circles(subsets=subset_sizes, linestyle='solid', linewidth=1)


# 添加基因标签
def add_gene_labels(v, smr, eqtl, ppi):
    # 三重重叠区域 (AFAP1)
    if v.get_label_by_id('111'):
        v.get_label_by_id('111').set_text('AFAP1')
        v.get_label_by_id('111').set_fontsize(13)
        v.get_label_by_id('111').set_weight('bold')
        v.get_label_by_id('111').set_color('white')

    # SMR-eQTL 重叠 (AFAP1)
    if v.get_label_by_id('011'):
        v.get_label_by_id('011').set_text('AFAP1')
        v.get_label_by_id('011').set_fontsize(11)
        v.get_label_by_id('011').set_color('white')

    # SMR-PPI 重叠 (AFAP1, TMCO1)
    if v.get_label_by_id('101'):
        v.get_label_by_id('101').set_text('AFAP1\nTMCO1')
        v.get_label_by_id('101').set_fontsize(11)
        v.get_label_by_id('101').set_color('white')

    # eQTL-PPI 重叠 (ARHGAP27, AFAP1)
    if v.get_label_by_id('110'):
        v.get_label_by_id('110').set_text('ARHGAP27\nAFAP1')
        v.get_label_by_id('110').set_fontsize(11)
        v.get_label_by_id('110').set_color('white')

    # SMR 独有区域
    if v.get_label_by_id('100'):
        unique_smr = smr - eqtl - ppi
        label_text = '\n'.join(sorted(unique_smr))
        v.get_label_by_id('100').set_text(label_text)
        v.get_label_by_id('100').set_fontsize(10)

    # eQTL 独有区域
    if v.get_label_by_id('010'):
        unique_eqtl = eqtl - smr - ppi
        # 优化长基因名的显示
        genes = sorted(unique_eqtl)
        label_lines = []
        for gene in genes:
            if len(gene) > 10:  # 如果基因名超过10个字符，尝试分行
                parts = gene.split('-')
                if len(parts) > 1 and len(parts[-1]) < 8:
                    label_lines.append('-'.join(parts[:-1]) + '-\n' + parts[-1])
                else:
                    mid = len(gene) // 2
                    # 在字母之间插入换行，避免在字符中间断开
                    if gene[mid].isalpha() and gene[mid + 1].isalpha():
                        label_lines.append(gene[:mid + 1] + '\n' + gene[mid + 1:])
                    else:
                        label_lines.append(gene)
            else:
                label_lines.append(gene)
        label_text = '\n'.join(label_lines)
        v.get_label_by_id('010').set_text(label_text)
        v.get_label_by_id('010').set_fontsize(9)

    # PPI 独有区域
    if v.get_label_by_id('001'):
        unique_ppi = ppi - smr - eqtl
        # 优化长基因名的显示
        genes = sorted(unique_ppi)
        label_lines = []
        for gene in genes:
            if len(gene) > 10:  # 如果基因名超过10个字符，尝试分行
                parts = gene.split('-')
                if len(parts) > 1 and len(parts[-1]) < 8:
                    label_lines.append('-'.join(parts[:-1]) + '-\n' + parts[-1])
                else:
                    mid = len(gene) // 2
                    # 在字母之间插入换行，避免在字符中间断开
                    if mid < len(gene) - 1 and gene[mid].isalpha() and gene[mid + 1].isalpha():
                        label_lines.append(gene[:mid + 1] + '\n' + gene[mid + 1:])
                    else:
                        label_lines.append(gene)
            else:
                label_lines.append(gene)
        # 每3个基因一行，提高可读性
        label_text = '\n'.join(['\n'.join(label_lines[i:i + 3]) for i in range(0, len(label_lines), 3)])
        v.get_label_by_id('001').set_text(label_text)
        v.get_label_by_id('001').set_fontsize(9)


# 添加基因标签到图中
add_gene_labels(v, smr, eqtl, ppi)

# 设置标题
plt.title('Gene Overlap Analysis of SMR, eQTL, and PPI Datasets', fontsize=14)

# 创建图例
smr_patch = mpatches.Patch(color='#4a90e2', alpha=0.6, label='SMR')
eqtl_patch = mpatches.Patch(color='#f16854', alpha=0.6, label='eQTL')
ppi_patch = mpatches.Patch(color='#62c68a', alpha=0.6, label='PPI')
plt.legend(handles=[smr_patch, eqtl_patch, ppi_patch], loc='upper right')

# 调整布局
plt.tight_layout()

# 显示图形
plt.show()