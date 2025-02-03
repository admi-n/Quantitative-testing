import os
import pandas as pd
import mysql.connector

def create_database_and_tables(mysql_host, mysql_user, mysql_password, database_name):
    connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password
    )
    cursor = connection.cursor()
    
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cursor.execute(f"USE {database_name}")
    
    timeframes = ['1d', '1h', '1m', '4h', '15m']
    
    for tf in timeframes:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS ETHUSDT_{tf} (
            open_time DATETIME,
            open_price DECIMAL(20,10),
            high_price DECIMAL(20,10),
            low_price DECIMAL(20,10),
            close_price DECIMAL(20,10),
            volume DECIMAL(30,10),
            close_time DATETIME,
            quote_asset_volume DECIMAL(30,10),
            number_of_trades INT,
            taker_buy_volume DECIMAL(30,10),
            taker_buy_quote_asset_volume DECIMAL(30,10),
            PRIMARY KEY (open_time)
        )
        """
        cursor.execute(create_table_query)
    
    connection.commit()
    connection.close()

def convert_csv_to_mysql(root_directory, mysql_host, mysql_user, mysql_password, database_name):
    connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=database_name
    )
    cursor = connection.cursor()
    
    timeframes = ['1d', '1h', '1m', '4h', '15m']
    
    for timeframe in timeframes:
        timeframe_path = os.path.join(root_directory, timeframe)
        if not os.path.exists(timeframe_path):
            continue
        
        for dirpath, dirnames, filenames in os.walk(timeframe_path):
            for filename in filenames:
                if filename.endswith('.csv'):
                    full_path = os.path.join(dirpath, filename)
                    
                    try:
                        df = pd.read_csv(full_path, header=None)
                        
                        column_mapping = {
                            10: [
                                'open_time', 'open_price', 'high_price', 'low_price', 
                                'close_price', 'volume', 'close_time', 
                                'quote_asset_volume', 'number_of_trades', 
                                'taker_buy_volume'
                            ],
                            11: [
                                'open_time', 'open_price', 'high_price', 'low_price', 
                                'close_price', 'volume', 'close_time', 
                                'quote_asset_volume', 'number_of_trades', 
                                'taker_buy_volume', 'taker_buy_quote_asset_volume'
                            ],
                            12: [
                                'open_time', 'open_price', 'high_price', 'low_price', 
                                'close_price', 'volume', 'close_time', 
                                'quote_asset_volume', 'number_of_trades', 
                                'taker_buy_volume', 'taker_buy_quote_asset_volume', 
                                'extra_column'
                            ]
                        }
                        
                        if len(df.columns) not in column_mapping:
                            print(f"跳过 {filename}: 不支持的列数 {len(df.columns)}")
                            continue
                        
                        df.columns = column_mapping[len(df.columns)]
                        
                        if len(df.columns) == 12:
                            df = df.iloc[:, :11]
                        
                        if len(df.columns) == 10:
                            df['taker_buy_quote_asset_volume'] = 0
                        
                        table_name = f'ETHUSDT_{timeframe}'
                        
                        insert_query = f"""
                        INSERT INTO {table_name} (
                            open_time, open_price, high_price, low_price, 
                            close_price, volume, close_time, 
                            quote_asset_volume, number_of_trades, 
                            taker_buy_volume, taker_buy_quote_asset_volume
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            open_price = VALUES(open_price),
                            high_price = VALUES(high_price),
                            low_price = VALUES(low_price),
                            close_price = VALUES(close_price),
                            volume = VALUES(volume),
                            close_time = VALUES(close_time),
                            quote_asset_volume = VALUES(quote_asset_volume),
                            number_of_trades = VALUES(number_of_trades),
                            taker_buy_volume = VALUES(taker_buy_volume),
                            taker_buy_quote_asset_volume = VALUES(taker_buy_quote_asset_volume)
                        """
                        
                        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')
                        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')
                        
                        records = df.values.tolist()
                        cursor.executemany(insert_query, records)
                        connection.commit()
                        
                        print(f"成功导入 {filename} 到表 {table_name}")
                    
                    except Exception as e:
                        print(f"导入 {filename} 时出错: {str(e)}")
    
    connection.close()

# 使用示例
if __name__ == '__main__':
    root_directory = os.getcwd()
    mysql_host = 'localhost'
    mysql_user = 'root'
    mysql_password = 'root'
    database_name = 'ETHUSDT'
    
    create_database_and_tables(mysql_host, mysql_user, mysql_password, database_name)
    convert_csv_to_mysql(root_directory, mysql_host, mysql_user, mysql_password, database_name)
