import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
from matplotlib.ticker import MaxNLocator


def generate_forest_plot(file_path, output_path=None):
    """优化后的森林图：使用效应值b绘图，增大字体并加粗，支持CSV和XLSX文件"""
    try:
        # 根据文件扩展名选择读取方法
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext == '.csv':
            df = pd.read_csv(file_path)
        elif file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"不支持的文件格式: {file_ext}，请使用CSV或XLSX文件")

        # 检查必要列（更新为效应值相关列）
        required_columns = ['id.exposure', 'id.outcome', 'outcome', 'exposure', 'method',
                           'b', 'lo_ci', 'up_ci', 'pval', 'he', 'ple', 'F']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"缺少必要列: {col}")

        # 数据处理（保留三位小数，效应值通常需要更高精度）
        data = df[required_columns].copy()
        for col in ['b', 'lo_ci', 'up_ci']:
            data[col] = data[col].round(3)
        # p值特殊处理（极小值显示为<0.01）
        data['pval_display'] = data['pval'].apply(lambda x: f"<0.01" if x < 0.01 else f"{x:.2f}")

        # 创建图形和轴（加宽画布以容纳更大字体）
        fig, ax = plt.subplots(figsize=(48, 8 + len(data) * 0.6))  # 增加高度以适应更多条目

        # 移除顶部、右侧和左侧边框
        for spine in ['top', 'right', 'left']:
            ax.spines[spine].set_visible(False)

        # 设置Y轴标签（增大字体并加粗）
        y_pos = np.arange(len(data))
        y_labels = [f"{exp} - {out}" for exp, out in zip(data['exposure'], data['outcome'])]
        ax.set_yticks(y_pos)
        ax.set_yticklabels(y_labels, fontsize=12, fontweight='bold')  # 增大Y轴标签字体
        ax.tick_params(axis='y', length=0)  # 移除Y轴刻度线
        ax.set_ylim(-1, len(data))  # 顶部预留空间

        # 绘制置信区间和效应值点
        for i, (b_val, lo, up) in enumerate(zip(data['b'], data['lo_ci'], data['up_ci'])):
            # 效应值的显著性判断：置信区间不包含0则为显著
            color = 'darkred' if (lo > 0 or up < 0) else 'royalblue'
            ax.hlines(y=i, xmin=lo, xmax=up, color=color, linewidth=3, zorder=2)
            ax.scatter(b_val, i, color=color, s=140, edgecolor='white', linewidth=1.5, zorder=3)

        # 添加无效应线（效应值=0）
        ax.axvline(x=0, color='gray', linestyle='--', linewidth=2, label='No Effect (b=0)')

        # 设置X轴（增大字体并加粗）
        ax.set_xlabel('Effect Size (95% CI)', fontsize=14, fontweight='bold', labelpad=15)
        # 计算合适的X轴范围，留出适当边距
        x_min = min(data['lo_ci']) - abs(min(data['lo_ci']) * 0.1)
        x_max = max(data['up_ci']) + abs(max(data['up_ci']) * 0.1)
        # 确保包含0点并对称显示
        x_range = max(abs(x_min), abs(x_max))
        ax.set_xlim(-x_range, x_range)
        ax.xaxis.set_major_locator(MaxNLocator(integer=False, nbins=10))
        ax.tick_params(axis='x', labelsize=12, width=1.5, pad=8)

        # 右侧信息栏布局（增大间距以适应更大字体）
        x_range_actual = x_max - x_min
        info_start = x_max + x_range_actual * 0.05
        method_x = info_start
        pval_x = method_x + x_range_actual * 0.3  # 调整间距适应效应值布局
        b_x = pval_x + x_range_actual * 0.3
        he_x = b_x + x_range_actual * 0.3
        ple_x = he_x + x_range_actual * 0.3

        # 信息栏标题（增大字体并加粗）
        ax.text(method_x, len(data) - 0.5, 'Method', fontsize=13, fontweight='bold', ha='left', va='bottom')
        ax.text(pval_x, len(data) - 0.5, 'p-value', fontsize=13, fontweight='bold', ha='left', va='bottom')
        ax.text(b_x, len(data) - 0.5, 'Effect Size (95% CI)', fontsize=13, fontweight='bold', ha='left', va='bottom')
        ax.text(he_x, len(data) - 0.5, 'HE', fontsize=13, fontweight='bold', ha='left', va='bottom')
        ax.text(ple_x, len(data) - 0.5, 'PLE', fontsize=13, fontweight='bold', ha='left', va='bottom')

        # 填充信息内容（增大字体并加粗）
        for i in range(len(data)):
            ax.text(method_x, i, data['method'].iloc[i], fontsize=11, ha='left', va='center', wrap=True,
                    fontweight='bold')
            ax.text(pval_x, i, data['pval_display'].iloc[i],
                    fontsize=11, ha='left', va='center',
                    fontweight='bold')
            b_text = f"{data['b'].iloc[i]:.3f} ({data['lo_ci'].iloc[i]:.3f}, {data['up_ci'].iloc[i]:.3f})"
            ax.text(b_x, i, b_text, fontsize=11, ha='left', va='center', fontweight='bold')
            ax.text(he_x, i, f"{data['he'].iloc[i]:.3f}", fontsize=11, ha='left', va='center', fontweight='bold')
            ax.text(ple_x, i, f"{data['ple'].iloc[i]:.3f}", fontsize=11, ha='left', va='center', fontweight='bold')

        # 标题和图例（增大字体并加粗）
        ax.set_title('Association Analysis with Effect Sizes',
                     fontsize=18, fontweight='bold', pad=25)
        ax.legend(loc='upper right', frameon=False, fontsize=12)

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
    input_file = r"D:\document\bioInfo\alps-covid\alps-covid.xlsx"
    # 输出图片路径
    output_image = r"D:\document\bioInfo\alps-covid\effect_size_forest_plot.png"

    # 生成并保存图片
    generate_forest_plot(input_file, output_image)
    # 如需直接显示图片，取消下面一行注释
    # generate_forest_plot(input_file)