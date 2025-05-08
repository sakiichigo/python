import pandas as pd
import os
from tqdm import tqdm
from multiprocessing import Pool, cpu_count, freeze_support
import requests
import json
import concurrent.futures
import time

# 保存筛选后的 DataFrame 为 XLSX 文件
# 指定 CSV 文件所在的目录

directory = r"D:\document\bioInfo\alps-eye\eqtl\result"

ivw_output_file = r"D:\document\bioInfo\alps-eye\eqtl\combined_ivw.xlsx"

# 保存合并后的 DataFrame 为一个新的 CSV 文件
output_file = r"D:\document\bioInfo\alps-eye\eqtl\\combined.csv"


# 定义一个函数来读取单个 CSV 文件
def read_csv_file(args):
    directory, filename = args  # 解包参数
    file_path = os.path.join(directory, filename)
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"读取文件 {filename} 时出现错误: {e}")
        return pd.DataFrame()


def main():
    # 获取目录下所有 CSV 文件的数量
    csv_files = [filename for filename in os.listdir(directory) if filename.endswith('or.csv')]
    total_files = len(csv_files)

    # 将 directory 和文件名打包成元组，作为参数传递给子进程
    file_args = [(directory, filename) for filename in csv_files]

    # 使用多进程并行读取 CSV 文件
    with Pool(cpu_count()) as pool:
        df_list = list(tqdm(pool.imap(read_csv_file, file_args), total=total_files, desc="合并 CSV 文件"))

    # 合并所有 DataFrame
    combined_df = pd.concat(df_list, ignore_index=True)

    combined_df.to_csv(output_file, index=False)

    print(f"合并完成，结果已保存到 {output_file}")

    # 筛选 method 列包含 "Inverse variance weighted" 的行
    ivw_df = combined_df[combined_df['method'].str.contains("Inverse variance weighted", na=False)]

    ivw_df.to_excel(ivw_output_file, index=False)

    print(f"筛选完成，包含 'Inverse variance weighted' 的行已保存到 {ivw_output_file}")


# 百度翻译 API 的相关信息，需要替换为你自己的信息
appid = '20231130001896707'
secretKey = 'MkW4prMH1Nk2XI8dYdOY'
api_url = 'http://api.fanyi.baidu.com/api/trans/vip/translate'

# 缓存机制
translation_cache = {}

# 最大字符限制
MAX_CHAR_LIMIT = 6000

# 请求间隔（秒）
REQUEST_INTERVAL = 0.5

# 最大重试次数
MAX_RETRIES = 3


# 批量翻译函数，考虑字符长度限制
def batch_translate(texts):
    all_translations = []
    current_batch = []
    current_char_count = 0
    for text in texts:
        processed_text = text.replace('_', ' ').replace('-', ' ')
        new_char_count = current_char_count + len(processed_text) + 1  # +1 是为了考虑换行符
        if new_char_count > MAX_CHAR_LIMIT:
            # 达到字符限制，处理当前批次
            translations = _translate_batch(current_batch)
            all_translations.extend(translations)
            # 重置批次
            current_batch = [text]
            current_char_count = len(processed_text)
        else:
            current_batch.append(text)
            current_char_count = new_char_count

    # 处理最后一批
    if current_batch:
        translations = _translate_batch(current_batch)
        all_translations.extend(translations)

    return all_translations


def _translate_batch(batch):
    combined_text = '\n'.join([t.replace('_', ' ').replace('-', ' ') for t in batch])
    salt = '1435660288'
    sign = appid + combined_text + salt + secretKey
    import hashlib
    sign = hashlib.md5(sign.encode()).hexdigest()
    params = {
        'q': combined_text,
        'from': 'auto',
        'to': 'zh',
        'appid': appid,
        'salt': salt,
        'sign': sign
    }
    for retry in range(MAX_RETRIES):
        try:
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            result = json.loads(response.text)
            if 'trans_result' in result:
                translations = [item['dst'] for item in result['trans_result']]
                if len(translations) != len(batch):
                    print(f"翻译结果数量不匹配，请求文本: {batch}，响应结果: {result}")
                return translations
            else:
                print(f"翻译失败，请求结果: {result}，重试 {retry + 1}/{MAX_RETRIES}")
        except requests.RequestException as e:
            print(f"请求 API 时出错: {e}，重试 {retry + 1}/{MAX_RETRIES}")
        except json.JSONDecodeError as e:
            print(f"解析 API 响应时出错: {e}，响应内容: {response.text}，重试 {retry + 1}/{MAX_RETRIES}")
        time.sleep(REQUEST_INTERVAL)
    print(f"达到最大重试次数，放弃翻译: {batch}")
    return [t for t in batch]


def translate_excel_baidu(file_path, column_to_translate):
    # 读取 Excel 文件
    df = pd.read_excel(file_path)
    # 检查需要翻译的列是否存在
    if column_to_translate not in df.columns:
        print(f"Excel 文件中未找到 '{column_to_translate}' 列。")
        return
    texts = df[column_to_translate].dropna().tolist()
    # 分割待翻译文本为批量
    batch_size = 20  # 可根据实际情况调整批量大小
    num_batches = len(texts) // batch_size + (1 if len(texts) % batch_size != 0 else 0)
    progress_bar = tqdm(total=num_batches, desc="翻译进度")
    translated_values = []

    def process_batch(batch):
        cached_translations = []
        uncached_texts = []
        uncached_indices = []
        for i, text in enumerate(batch):
            if text in translation_cache:
                cached_translations.append(translation_cache[text])
            else:
                uncached_texts.append(text)
                uncached_indices.append(i)
        if uncached_texts:
            uncached_translations = batch_translate(uncached_texts)
            for i, index in enumerate(uncached_indices):
                translation = uncached_translations[i]
                batch[index] = translation
                # 修正缓存更新
                translation_cache[uncached_texts[i]] = translation
        return batch

    # 限制并发数
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = list(
            executor.map(process_batch, [texts[i * batch_size:(i + 1) * batch_size] for i in range(num_batches)]))
        for batch in results:
            translated_values.extend(batch)
            progress_bar.update(1)

    progress_bar.close()
    # 新起一列保存翻译结果
    new_column_name = f"{column_to_translate}_translated"
    # 获取需要翻译的列的索引
    col_index = df.columns.get_loc(column_to_translate)
    # 在需要翻译的列后面插入翻译结果列
    df.insert(col_index + 1, new_column_name, df[column_to_translate])
    non_nan_index = df[column_to_translate].dropna().index
    df.loc[non_nan_index, new_column_name] = translated_values
    # 保存翻译后的文件
    output_file = file_path.replace('.xlsx', '_translated.xlsx')
    df.to_excel(output_file, index=False)
    print(f"翻译完成，结果已保存到 {output_file}。")


if __name__ == '__main__':
    freeze_support()  # 在 Windows 上需要调用 freeze_support()
    main()
    #column_to_translate = 'outcome'
    #translate_excel_baidu(ivw_output_file, column_to_translate)
