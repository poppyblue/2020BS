import pandas as pd
import time
import os
import datetime
import ccxt

pd.set_option('expand_frame_repr', False)


def crawl_bybit_datas(symbol, start_time, end_time):
    exchange = ccxt.binance()
    print(exchange)
    current_path = os.getcwd()
    file_dir = os.path.join(current_path, symbol.replace('/', ''))
    print(file_dir)
    if not os.path.exists(file_dir):
        # 如果这个文件路径不存在,则创建这个文件夹，来存放数据.
        os.makedirs(file_dir)

    start_time = start_time
    end_time = end_time
    start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d')
    # 我不知道为什么要*1000
    start_time_stamp = int(time.mktime(start_time.timetuple())) * 1000
    end_time_stamp = int(time.mktime(end_time.timetuple())) * 1000

    limit_count = 200

    while True:
        try:
            print(start_time_stamp)
            data = exchange.fetch_ohlcv(symbol, timeframe='1d', since=start_time_stamp, limit=limit_count)
            df = pd.DataFrame(data)
            df.rename(columns={0: 'open_time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'value'}, inplace=True)
            # start_time_stamp = int(df.iloc[-1]['open_time'])  # 获取下个请求的时间
            start_time_stamp = int(df.iloc[-1][0])  # 获取下一个次请求的时间.
            filename = str(start_time_stamp) + '.csv'
            save_file_path = os.path.join(file_dir, filename)
            print("文件保存路径为：%s" % save_file_path)
            df.set_index('open_time', drop=True, inplace=True)
            df.to_csv(save_file_path)
            if start_time_stamp > end_time_stamp:
                print("完成请求")
                break
            time.sleep(0.2)

        except Exception as error:
            print(error)
            time.sleep(10)


def sample_datas(symbol):
    path = os.path.join(os.getcwd(), symbol.replace('/', ''))
    print(path)
    file_paths = []
    for root, dirs, files in os.walk(path):
        if files:
            for file in files:
                if file.endswith('.csv'):
                    file_paths.append(os.path.join(path, file))

    file_paths = sorted(file_paths)
    all_df = pd.DataFrame()

    for file in file_paths:
        df = pd.read_csv(file)
        all_df = all_df.append(df, ignore_index=True)

    all_df = all_df.sort_values('open_time', ascending=True)
    print(all_df)
    return all_df


def clear_datas(symbol):
    df = sample_datas(symbol)
    df['open_time'] = df['open_time'].apply(lambda x: (x // 60) * 60)  
    print(df)
    df.drop_duplicates(subset=['open_time'], inplace=True) #去重
    df.set_index('open_time', inplace=True)
    symbol_path = symbol.replace('/', '')
    df.to_csv(f'{symbol_path}_1_min_data_kaggle_2016_2020.csv')


if __name__ == '__main__':
    crawl_bybit_datas('BTC/USDT', '2016-01-01', '2020-01-01')
    # clear_datas('BTC/USDT')
