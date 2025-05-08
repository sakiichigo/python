import pandas as pd


def extract_keywords(phenotype):
    keywords = []
    # 定义疾病关键词列表，将包含空格的关键词放在前面
    disease_keywords = ['diabetic background retinopathy','diabetic retinopathy','Retinitis pigmentosa', 'age - related macular degeneration', 'macular degeneration','cataract', 'choroid',
                        'eye', 'glaucoma', 'hypermetropia',
                        'lens','macular','myopia', 'ocular', 'optic', 'presbyopia','refraction','retina',
                        'Retinitis','sight', 'Strabismus','strabismus', 'vision', 'visual', 'anopsia']
    # 去除重复关键词
    disease_keywords = list(set(disease_keywords))

    phenotype_lower = phenotype.lower()
    for keyword in disease_keywords:
        keyword_lower = keyword.lower()
        if keyword_lower in phenotype_lower:
            if keyword_lower not in [kw.lower() for kw in keywords]:
                keywords.append(keyword.capitalize())

    return keywords


def process_phenotype_combination(combination):
    if ' vs ' in combination:
        exposure, outcome = combination.split(' vs ')
        exposure_keywords = extract_keywords(exposure)
        outcome_keywords = extract_keywords(outcome)

        exposure_str =' '.join(exposure_keywords)
        outcome_str =' '.join(outcome_keywords)

        return exposure_str +' vs '+ outcome_str
    else:
        # 若无法拆分，返回一个默认值或者提示信息
        return "Invalid combination: no ' vs ' found"


# 读取文件
file_path = r'C:\Users\22565\Desktop\eye-eye_filtered - 副本.xlsx'
df = pd.read_excel(file_path)

# 提取 exposure 表型和 outcome 表型列中 "||" 前面部分
df['exposure_phenotype'] = df['exposure'].apply(lambda x: x.split(' || ')[0].strip())
df['outcome_phenotype'] = df['outcome'].apply(lambda x: x.split(' || ')[0].strip())

# 创建表型组合列
df['phenotype_combination'] = df['exposure_phenotype'] + ' vs ' + df['outcome_phenotype']
# 剔除包含 "Prothrombin " 的表型组合
df = df[~df['phenotype_combination'].str.contains('Prothrombin')]
df = df[~df['phenotype_combination'].str.contains('liver')]

# 对表型组合进行处理
df['abstract_keyword_combination'] = df['phenotype_combination'].apply(process_phenotype_combination)

# 保存为新的 Excel 文件
new_file_path = r'C:\Users\22565\Desktop\eye-eye_filtered_processed.xlsx'
df.to_excel(new_file_path, index=False)
