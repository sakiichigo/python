import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

# 设置中文字体支持
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 读取数据
try:
    df = pd.read_csv(r"C:\Users\22565\Downloads\string_node_degrees.tsv", sep='\t')
    print(f"数据加载成功，共{len(df)}个节点")
except FileNotFoundError:
    print("文件未找到，请检查文件路径是否正确")
    # 创建示例数据用于演示
    np.random.seed(42)
    genes = [f"Gene{i}" for i in range(1, 101)]
    identifiers = [f"9606.ENSP00000{i:05d}" for i in range(10001, 10101)]
    degrees = np.random.poisson(lam=5, size=100)
    df = pd.DataFrame({
        '#node': genes,
        'identifier': identifiers,
        'node_degree': degrees
    })
    print("使用示例数据进行演示")

# 基本统计分析
degree_stats = df['node_degree'].describe()
print("\n度中心性统计:")
print(degree_stats)

# 确定Hub基因（度值大于均值+2倍标准差）
threshold = degree_stats['mean'] + 2 * degree_stats['std']
hub_genes = df[df['node_degree'] > threshold]
print(f"\n发现{len(hub_genes)}个Hub基因（度 > {threshold:.2f}）")
print(hub_genes[['#node', 'node_degree']])

# 可视化 - 度分布直方图
plt.figure(figsize=(10, 6))
sns.histplot(df['node_degree'], kde=True, bins=20)
plt.axvline(x=threshold, color='red', linestyle='--', label=f'Hub阈值: {threshold:.2f}')
plt.axvline(x=degree_stats['mean'], color='green', linestyle='-', label=f'均值: {degree_stats["mean"]:.2f}')
plt.title('蛋白质相互作用网络的度分布')
plt.xlabel('度中心性')
plt.ylabel('频次')
plt.legend()
plt.tight_layout()
plt.savefig('degree_distribution.png', dpi=300)
plt.close()

# 可视化 - 度中心性排名条形图（前20）
top_20 = df.sort_values('node_degree', ascending=False).head(20)
plt.figure(figsize=(12, 8))
sns.barplot(x='node_degree', y='#node', data=top_20)
plt.title('Top 20 高连接度蛋白质')
plt.xlabel('度中心性')
plt.ylabel('蛋白质名称')
plt.tight_layout()
plt.savefig('top_20_degree.png', dpi=300)
plt.close()

# 输出结果到CSV
df.to_csv('degree_analysis_results.csv', index=False)
hub_genes.to_csv('hub_genes.csv', index=False)

print("\n分析完成! 结果已保存为:")
print("- 整体分析: degree_analysis_results.csv")
print("- Hub基因: hub_genes.csv")
print("- 度分布可视化: degree_distribution.png")
print("- 前20高连接度蛋白质: top_20_degree.png")