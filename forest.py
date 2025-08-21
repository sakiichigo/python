import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
from matplotlib.ticker import MaxNLocator


def generate_forest_plot(file_path, output_path=None):
    """优化后的森林图：增大字体并加粗，支持CSV和XLSX文件"""
    try:
        # 根据文件扩展名选择读取方法
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext == '.csv':
            df = pd.read_csv(file_path)
        elif file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"不支持的文件格式: {file_ext}，请使用CSV或XLSX文件")

        # 检查必要列
        required_columns = ['id.exposure', 'id.outcome', 'exposure','outcome', 'method', 'or', 'or_lci95', 'or_uci95', 'pval',
                            'he','ple','F']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"缺少必要列: {col}")

        # 数据处理（保留两位小数）
        data = df[required_columns].copy()
        for col in ['or', 'or_lci95', 'or_uci95']:
            data[col] = data[col].round(2)
        # p值特殊处理（极小值显示为<0.01）
        data['pval_display'] = data['pval'].apply(lambda x: f"{x:.2e}")

        # 创建图形和轴（加宽画布以容纳更大字体）
        fig, ax = plt.subplots(figsize=(34, 8 + len(data) * 0.6))  # 增加高度以适应更大字体

        # 移除顶部、右侧和左侧边框
        for spine in ['top', 'right', 'left']:
            ax.spines[spine].set_visible(False)

        # 设置Y轴标签（增大字体并加粗）
        y_pos = np.arange(len(data))
        y_labels = [f"{exp_id} - {out_id}" for exp_id, out_id in zip(data['exposure'], data['outcome'])]
        ax.set_yticks(y_pos)
        ax.set_yticklabels(y_labels, fontsize=12, fontweight='bold')  # 增大Y轴标签字体
        ax.tick_params(axis='y', length=0)  # 移除Y轴刻度线
        ax.set_ylim(-1, len(data))  # 顶部预留空间

        # 绘制置信区间和OR点
        for i, (or_val, lci, uci) in enumerate(zip(data['or'], data['or_lci95'], data['or_uci95'])):
            color = 'darkred' if (lci > 1 or uci < 1) else 'royalblue'
            ax.hlines(y=i, xmin=lci, xmax=uci, color=color, linewidth=3, zorder=2)
            ax.scatter(or_val, i, color=color, s=140, edgecolor='white', linewidth=1.5, zorder=3)  # 增大OR点尺寸

        # 添加无效应线（OR=1）
        ax.axvline(x=1, color='gray', linestyle='--', linewidth=2, label='No Effect (OR=1)')

        # 设置X轴（增大字体并加粗）
        ax.set_xlabel('Odds Ratio (95% CI)', fontsize=14, fontweight='bold', labelpad=15)  # 增大X轴标签字体
        x_min = max(0.5, min(data['or_lci95']) - 0.3)
        x_max = max(data['or_uci95']) + 0.3
        ax.set_xlim(x_min, x_max)
        ax.xaxis.set_major_locator(MaxNLocator(integer=False, nbins=8))
        ax.tick_params(axis='x', labelsize=12, width=1.5, pad=8)  # 增大X轴刻度字体

        # 右侧信息栏布局（增大间距以适应更大字体）
        x_range = x_max - x_min
        info_start = x_max + x_range * 0.05
        method_x = info_start
        pval_x = method_x + x_range * 0.55  # 进一步增大method与pvalue间距
        or_x = pval_x + x_range * 0.3

        # 信息栏标题（增大字体并加粗）
        ax.text(method_x, len(data) - 0.5, 'Method', fontsize=13, fontweight='bold', ha='left', va='bottom')
        ax.text(pval_x, len(data) - 0.5, 'p-value', fontsize=13, fontweight='bold', ha='left', va='bottom')
        ax.text(or_x, len(data) - 0.5, 'OR (95% CI)', fontsize=13, fontweight='bold', ha='left', va='bottom')

        # 填充信息内容（增大字体并加粗）
        for i in range(len(data)):
            ax.text(method_x, i, data['method'].iloc[i], fontsize=11, ha='left', va='center', wrap=True,
                    fontweight='bold')
            ax.text(pval_x, i, data['pval_display'].iloc[i],
                    fontsize=11, ha='left', va='center',
                    fontweight='bold')  # 全部p值加粗显示
            or_text = f"{data['or'].iloc[i]:.2f} ({data['or_lci95'].iloc[i]:.2f}, {data['or_uci95'].iloc[i]:.2f})"
            ax.text(or_x, i, or_text, fontsize=11, ha='left', va='center', fontweight='bold')

        # 标题和图例（增大字体并加粗）
        ax.set_title('Association Between Hypothermia and Infection index',
                     fontsize=18, fontweight='bold', pad=25)  # 增大标题字体
        ax.legend(loc='upper right', frameon=False, fontsize=12)  # 增大图例字体

        # 调整布局
        plt.tight_layout()

        # 保存或显示
        if output_path:
            output_dir = os.path.dirname(output_path)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            print(f"森林图已保存至: {output_path}")
        else:
            plt.show()

        return fig, ax

    except Exception as e:
        print(f"生成森林图时出错: {e}")
        return None, None


if __name__ == "__main__":
    # 输入文件路径（支持CSV或XLSX）
    input_file = r"D:\document\bioInfo\hypothermia-pneumonia\hy-po.xlsx"
    # 输出图片路径
    output_image = r"D:\document\bioInfo\hypothermia-pneumonia\hy-po.png"

    # 生成并保存图片
    generate_forest_plot(input_file, output_image)
    # 如需直接显示图片，取消下面一行注释
    # generate_forest_plot(input_file)