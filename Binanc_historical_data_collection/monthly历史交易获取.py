# data/spot/monthly  获取月份格式
# 出现连接错误,可以修改代理重复运行

import os
import requests
from datetime import datetime, timedelta

# 设置币种和时间周期
symbol = "ETHUSDT"
intervals = ["1d", "4h", "1h", "15m", "1m"]


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

proxies = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890", 
    # "socks5": "127.0.0.1:7890",

}

# 日期范围
def get_date_range():
    start_date = datetime(2017, 8, 1)  # 从2017年8月开始
    end_date = datetime(2025, 1, 31)
    return start_date, end_date

# 获取月份列表
def get_month_list(start_date, end_date):
    months = []
    while start_date <= end_date:
        months.append(start_date.strftime("%Y-%m"))
        start_date += timedelta(days=32)  # 向前推进一个月
        start_date = start_date.replace(day=1)  # 确保是下个月的第一天
    return months


def download_data(symbol, interval, months):
    base_url = "https://data.binance.vision/data/spot/monthly/klines/{}/{}/"
    
    if not os.path.exists(interval):
        os.makedirs(interval)

    for month in months:
        url = f"{base_url.format(symbol, interval)}{symbol}-{interval}-{month}.zip"
        file_name = f"{symbol}-{interval}-{month}.zip"
        file_path = os.path.join(interval, file_name)  # 路径将以时间间隔命名的文件夹为前缀
        
        if not os.path.exists(file_path):
            print(f"Downloading {url}...")
            try:
                response = requests.get(url, headers=headers, proxies=proxies, timeout=30)
                if response.status_code == 200:
                    with open(file_path, 'wb') as file:
                        file.write(response.content)
                    print(f"Downloaded {file_path}")
                else:
                    print(f"Failed to download {url}, Status Code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Error downloading {url}: {e}")
        else:
            print(f"{file_path} already exists")

if __name__ == "__main__":
    start_date, end_date = get_date_range()
    months = get_month_list(start_date, end_date)
    for interval in intervals:
        download_data(symbol, interval, months)
