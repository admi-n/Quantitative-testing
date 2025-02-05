import sqlite3
import pandas as pd
import os

def create_table_if_not_exists(conn, table_name, df):
    """创建表结构（如果尚不存在）"""
    cursor = conn.cursor()

    # 创建表的 SQL 语句
    columns = ', '.join([f"{col} TEXT" if df[col].dtype == 'object' else f"{col} REAL" for col in df.columns])

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns},
        PRIMARY KEY (open_time)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

def insert_data_from_csv(db_name, table_name, csv_file_path):
    """从 CSV 文件导入数据到 SQLite"""
    # 读取 CSV 文件
    df = pd.read_csv(csv_file_path)
    
    # 确保 CSV 文件包含预期的列
    expected_columns = [
        'open_time', 'open_price', 'high_price', 'low_price', 'close_price', 
        'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 
        'taker_buy_volume', 'taker_buy_quote_asset_volume'
    ]
    
    # 检查 CSV 列是否匹配
    if not all(col in df.columns for col in expected_columns):
        print(f"CSV 文件列名与预期不匹配，跳过文件: {csv_file_path}")
        return

    # 将 `open_time` 和 `close_time` 转换为字符串类型
    # 假设 `open_time` 和 `close_time` 列格式是 'dd/mm/yyyy hh:mm:ss'
    df['open_time'] = pd.to_datetime(df['open_time'], format='%d/%m/%Y %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')
    df['close_time'] = pd.to_datetime(df['close_time'], format='%d/%m/%Y %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')

    # 连接 SQLite 数据库
    conn = sqlite3.connect(db_name)
    
    # 创建表（如果尚不存在）
    create_table_if_not_exists(conn, table_name, df)

    # 插入数据
    insert_query = f"""
    INSERT OR REPLACE INTO {table_name} (
        open_time, open_price, high_price, low_price, close_price, 
        volume, close_time, quote_asset_volume, number_of_trades, 
        taker_buy_volume, taker_buy_quote_asset_volume
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    # 将 DataFrame 转换为列表，并插入数据库
    records = df[expected_columns].values.tolist()
    cursor = conn.cursor()
    cursor.executemany(insert_query, records)
    
    conn.commit()
    conn.close()
    print(f"成功将数据从 {csv_file_path} 导入到 {table_name} 表中。")

def process_csv_files(input_directory, db_name):
    """遍历目录中的 CSV 文件并导入到 SQLite"""
    for filename in os.listdir(input_directory):
        if filename.endswith('.csv'):
            table_name = filename.split('.')[0]  # 使用文件名作为表名
            csv_file_path = os.path.join(input_directory, filename)
            insert_data_from_csv(db_name, table_name, csv_file_path)

if __name__ == '__main__':
    # 设置 SQLite 数据库文件名
    db_name = 'ethusdt_klines.db'  # 数据库文件名
    input_directory = './csv'  # CSV 文件所在的目录

    # 导入 CSV 文件中的数据
    process_csv_files(input_directory, db_name)
