import concurrent.futures
import requests
import json
import time
import re
# 定义要请求的URL列表和对应的POST数据
urls_and_json = [
    ("http://localhost:8200/core/collector",
     {"ip": "192.168.70.51", "type": 2, "snmpVersion": "v2c", "manufacturer": "Cisco", "snmpCommunity": "public",
      "snmpPort": "161"}),
    ("http://localhost:8200/core/collector",
     {"ip": "192.168.70.51", "type": 2, "snmpVersion": "v2c", "manufacturer": "Cisco", "snmpCommunity": "public",
      "snmpPort": "162"}),
    ("http://localhost:8200/core/collector",
     {"ip": "192.168.70.51", "type": 2, "snmpVersion": "v2c", "manufacturer": "Hillstone", "snmpCommunity": "public",
      "snmpPort": "163"}),
    ("http://localhost:8200/core/collector",
     {"ip": "192.168.70.51", "type": 2, "snmpVersion": "v2c", "manufacturer": "Hillstone", "snmpCommunity": "public",
      "snmpPort": "164"})

]


def extract_between(input_str, start, end):
    # 构建正则表达式模式
    pattern = re.compile(f"{re.escape(start)}(.*?){re.escape(end)}", re.DOTALL)

    # 查找匹配的内容
    match = pattern.search(input_str)

    if match:
        return match.group(1).strip()
    else:
        return None


# 定义POST请求函数
def post_url(url, json_data):
    try:
        response = requests.post(url, json=json_data)
        response.raise_for_status()  # 检查请求是否成功
        return f"成功: {url} - 状态码: {response.status_code} - 返回内容: {response.json()}"
    except requests.exceptions.RequestException as e:
        return f"失败: {url} - 错误: {e}"


# 使用ThreadPoolExecutor并发POST请求，并添加1秒的延时
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    # 提交任务
    futures = []
    for url, json_data in urls_and_json:
        future = executor.submit(post_url, url, json_data)
        futures.append(future)
        #time.sleep(0.05)  # 添加延时

    # 获取结果
    for future in concurrent.futures.as_completed(futures):
        #print(extract_between(future.result(), "description", "deviceName"))
        print(future.result())
print("所有请求完成。")


