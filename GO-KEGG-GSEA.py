import openpyxl,math
#从R中获得GO,KEGG,GSEA结果以及使用基因的SYMBOL-ENTREZID转换表,另准备差异表达结果
#################################PARAMETERS#################################
#指定上方所述五个文件的路径与文件名
file_diffexp=open('/Users/ZYP/Downloads/KEGG_GO_GSEA/result_diffexp.txt','r')
file_genes=open('/Users/ZYP/Downloads/KEGG_GO_GSEA/result_genes.txt','r')
file_GO=open('/Users/ZYP/Downloads/KEGG_GO_GSEA/result_GO.txt','r')
file_KEGG=open('/Users/ZYP/Downloads/KEGG_GO_GSEA/result_KEGG.txt','r')
file_GSEA=open('/Users/ZYP/Downloads/KEGG_GO_GSEA/result_GSEA.txt','r')
#指定储存路径
save_path='/Users/ZYP/Downloads/KEGG_GO_GSEA/'
############################################################################
#录入基因ID和上下调表达差异信息
dict_ID={}
dict_exp={}
for line in file_genes:
  if line[0]!='#':
  info=line[:-1].split('\t')
dict_ID[info[0]]=info[1]
file_genes.close()
for line in file_diffexp:
  if line[0]!='#':
  info=line[:-1].split('\t')
SYMBOL=info[0]
Log2FC=float(info[1])
if SYMBOL in dict_ID.keys():
  ENTREZID=dict_ID[SYMBOL]
if Log2FC > 0:
  dict_exp[ENTREZID]='up'
else:
  dict_exp[ENTREZID]='down'
file_diffexp.close()
#录入富集分析结果并与之前的差异表达信息一起写入保存文件
save_GO=open(save_path+'GO.txt','w')
save_GO.write('id'+'\t'+'category'+'\t'+'gene_num.min'+'\t'+'gene_num.max'+'\t'+'gene_num.rich'+'\t'+'-log10Pvalue'+'\t'+'up.regulated'+'\t'+'down.regulated'+'\t'+'rich.factor'+'\n')
for line in file_GO:
  if line[0]!='#':
  info=line[:-1].split('\t')
id=info[1]
category=info[0]
list_counts_max=info[3].split('/')
counts=int(list_counts_max[0])
max=int(list_counts_max[1])
pvalue=-math.log10(float(info[5]))
num_up=0
num_down=0
list_genes=info[-1].split('/')
for gene in list_genes:
  if gene in dict_ID.keys():
  gene=dict_ID[gene]
if gene in dict_exp.keys():
  regulate=dict_exp[gene]
if regulate == 'up':
  num_up+=1
if regulate == 'down':
  num_down+=1
ratio=counts/max
save_GO.write(id+'\t'+category+'\t'+'0'+'\t'+str(max)+'\t'+str(counts)+'\t'+str(pvalue)+'\t'+str(num_up)+'\t'+str(num_down)+'\t'+str(ratio)+'\n')
file_GO.close()
save_GO.close()
save_KEGG=open(save_path+'KEGG.txt','w')
save_KEGG.write('id'+'\t'+'category'+'\t'+'gene_num.min'+'\t'+'gene_num.max'+'\t'+'gene_num.rich'+'\t'+'-log10Pvalue'+'\t'+'up.regulated'+'\t'+'down.regulated'+'\t'+'rich.factor'+'\n')
for line in file_KEGG:
  if line[0] != '#':
  info=line[:-1].split('\t')
id=info[1]
category=info[0]
list_counts_max=info[3].split('/')
counts=int(list_counts_max[0])
max=int(list_counts_max[1])
pvalue=-math.log10(float(info[5]))
num_up=0
num_down=0
list_genes=info[-1].split('/')
for gene in list_genes:
  if gene in dict_exp.keys():
  regulate=dict_exp[gene]
if regulate == 'up':
  num_up+=1
if regulate == 'down':
  num_down+=1
ratio=counts/max
save_KEGG.write(id+'\t'+category+'\t'+'0'+'\t'+str(max)+'\t'+str(counts)+'\t'+str(pvalue)+'\t'+str(num_up)+'\t'+str(num_down)+'\t'+str(ratio)+'\n')
file_KEGG.close()
save_KEGG.close()
save_GSEA=open(save_path+'GSEA.txt','w')
save_GSEA.write('id'+'\t'+'category'+'\t'+'gene_num.min'+'\t'+'gene_num.max'+'\t'+'gene_num.rich'+'\t'+'-log10Pvalue'+'\t'+'up.regulated'+'\t'+'down.regulated'+'\t'+'rich.factor'+'\n')
for line in file_GSEA:
  if line[0] != '#':
  info=line[:-1].split('\t')
id=info[1]
category=info[0]
max=info[3]
pvalue=-math.log10(float(info[6]))
ratio=info[4]
counts=0
num_up=0
num_down=0
list_genes=info[-1].split('/')
for gene in list_genes:
  counts+=1
if gene in dict_exp.keys():
  regulate=dict_exp[gene]
if regulate == 'up':
  num_up+=1
if regulate == 'down':
  num_down+=1
save_GSEA.write(id+'\t'+category+'\t'+'0'+'\t'+str(max)+'\t'+str(counts)+'\t'+str(pvalue)+'\t'+str(num_up)+'\t'+str(num_down)+'\t'+str(ratio)+'\n')
file_GSEA.close()
save_GSEA.close()
print('finished!')
#得到用于富集圈图绘制的输入文件，信息如下图所示。
