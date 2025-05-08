import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os

# 读取 Excel 文件
file_path = r"D:\document\bioInfo\heart-sarco-0330\eqtl\combined_ivw_filtered_selected_2.xlsx"
df = pd.read_excel(file_path)

# 提取数据
exposures = df['exposure'].tolist()
outcomes = df.columns[1:]
num_outcomes = len(outcomes)
num_exposures = len(exposures)

data = []
p_values = []

for index, row in df.iterrows():
    row_data = []
    row_p_values = []
    for outcome in outcomes:
        value_str = row[outcome]
        beta_str = value_str.split('(')[0].split('=')[1].strip()
        ci_str = value_str.split('(')[1].split(')')[0].strip()
        pval_str = value_str.split('pval=')[1].strip()

        beta = float(beta_str)
        lower, upper = map(float, ci_str.split(' to '))
        pval = float(pval_str)

        row_data.extend([beta, lower, upper])
        row_p_values.append(pval)

    data.append(row_data)
    p_values.append(row_p_values)

# 调整图片大小，进一步增加高度以确保有足够空间
plt.figure(figsize=(20, 20))
y_positions = np.arange(num_exposures * num_outcomes)
y_ticks = []
y_labels = []

for i, exposure in enumerate(exposures):
    for j, outcome in enumerate(outcomes):
        index = i * num_outcomes + j
        beta = data[i][j * 3]
        lower = data[i][j * 3 + 1]
        upper = data[i][j * 3 + 2]
        plt.hlines(y_positions[index], lower, upper, color='black')
        plt.plot(beta, y_positions[index], 'o', markersize=5, color='black')
        p = p_values[i][j]
        y_ticks.append(y_positions[index])
        y_labels.append(f'{exposure} - {outcome}    p={p:.2e}')

# 设置图形属性，调大标签字体并加粗
plt.yticks(y_ticks, y_labels, fontsize=12, weight='bold')
plt.xlabel("Effect Size (β)", fontsize=14, weight='bold')
plt.ylabel("Exposure - Outcome", fontsize=14, weight='bold')
plt.title("Forest Plot of Exposure Effects on Outcomes", fontsize=16, weight='bold')

# 添加 Effect Size 为 0 时的虚线
plt.axvline(x=0, linestyle='--', color='gray', alpha=0.7)

# 恢复上框线和下框线
ax = plt.gca()
ax.spines['top'].set_visible(True)
ax.spines['bottom'].set_visible(True)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.tick_params(axis='y', left=False)

# 去除虚线框线
plt.grid(False)

plt.tight_layout()

# 保存图片到指定目录
save_dir = r"D:\document\bioInfo\heart-sarco-0330\eqtl"
os.makedirs(save_dir, exist_ok=True)
save_path = os.path.join(save_dir, "forest_plot.png")
plt.savefig(save_path)

# 显示图片
plt.show()