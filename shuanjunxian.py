import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from catalyst import run_algorithm
from catalyst.api import record, symbol, order_target_percent
from catalyst.exchange.utils.stats_utils import extract_transactions

# 首先需要加载数据
# catalyst ingest-exchange -x binance -i btc_usdt -f daily

NAMESPACE = 'dual_moving_average'
SIGNAL_BUY = 'buy'
SIGNAL_SELL = 'sell'
SIGNAL_INIT = ''

# 短期均线窗口
SHORT_WIN = 5

# 长期均线窗口
LONG_WIN = 20


def initialize(context):
    context.i = 0
    context.asset = symbol('btc_usdt')
    context.base_price = None
    context.signal = SIGNAL_INIT
    context.set_commission(maker=0.001, taker=0.001)
    context.set_slippage(slippage=0.001)


def handle_data(context, data):
    context.i += 1
    if context.i < LONG_WIN + 2:
        # 交易周期过短，无法计算
        # 当天能否交易，要看前两天的数据
        return

    # 获取历史数据 获取的是收盘价
    history_data = data.history(context.asset,
                                'close',
                                bar_count=LONG_WIN + 2,
                                frequency='1D')
    # 获取当前持仓量
    pos_amount = context.portfolio.positions[context.asset].amount
    # 计算双均线 窗口统计
    short_avgs = history_data.rolling(window=SHORT_WIN).mean()
    long_avgs = history_data.rolling(window=LONG_WIN).mean()
    # 双均线策略
    # 上穿，开全仓买入 target=1
    if (short_avgs[-3] < long_avgs[-3]) and (short_avgs[-2] > long_avgs[-2]) and pos_amount == 0:
        order_target_percent(asset=context.asset, target=1)
        context.signal = SIGNAL_BUY

    # 下穿，卖出 target=0
    if (short_avgs[-3] > long_avgs[-3]) and (short_avgs[-2] < long_avgs[-2]) and pos_amount > 0:
        order_target_percent(asset=context.asset, target=0)
        context.signal = SIGNAL_SELL

    # 获取当前的价格
    price = data.current(context.asset, 'price')
    if context.base_price is None:
        # 如果没有设置初始价格，将第一个周期的价格作为初始价格
        context.base_price = price
    # 计算价格变化百分比
    price_change = (price - context.base_price) / context.base_price
    # 记录每个交易周期的信息
    record(price=price,
           cash=context.portfolio.cash,
           price_change=price_change,
           short_mavg=short_avgs[-1],
           long_mavg=long_avgs[-1],
           signal=context.signal,
           )
    print('日期：{}.价格：{:.4f}.资产：{:.2f}.持仓量：{:.8f}.{}'.format(
        data.current_dt,
        price,
        context.portfolio.portfolio_value,
        pos_amount,
        context.signal))
    # 重置交易信号
    context.signal = SIGNAL_INIT


def analyze(context, perf):
    # 保存交易记录
    perf.to_csv('./shuanjunxian.csv')

    # 获取交易所的计价货币
    exchange = list(context.exchanges.values())[0]
    quote_currency = exchange.quote_currency.upper()

    # 可视化资产值
    ax1 = plt.subplot(411)
    perf['portfolio_value'].plot(ax=ax1)
    ax1.set_ylabel('portfolio value]\n({})'.format(quote_currency))
    start, end = ax1.get_ylim()
    ax1.yaxis.set_ticks(np.arange(start, end, (end - start) / 5))

    # 可视化货币价格，均线，买入卖出点
    ax2 = plt.subplot(412, sharex=ax1)
    perf[['price', 'short_mavg', 'long_mavg']].plot(ax=ax2)
    ax2.set_ylabel('{asset}\n({quote})'.format(asset=context.asset.symbol,quote=quote_currency))
    start, end = ax2.get_ylim()
    ax2.yaxis.set_ticks(np.arange(start, end, (end - start) / 5))
    # 提取交易时间点
    transaction_df = extract_transactions(perf)
    if not transaction_df.empty:
        buy_df = transaction_df[transaction_df['amount'] > 0]
        sell_df = transaction_df[transaction_df['amount'] < 0]
        ax2.scatter(
            buy_df.index.to_pydatetime(),
            perf.loc[buy_df.index, 'price'],
            marker='^',
            s=100,
            c='green',
            label=''
        )
        ax2.scatter(
            sell_df.index.to_pydatetime(),
            perf.loc[sell_df.index, 'price'],
            marker='v',
            s=100,
            c='red',
            label=''
        )

    # 绘制价格变化率和资产变化率
    ax3 = plt.subplot(413, sharex=ax1)
    perf[['algorithm_period_return', 'price_change']].plot(ax=ax3)
    ax3.set_ylabel('percent change')
    start, end = ax3.get_ylim()
    ax3.yaxis.set_ticks(np.arange(0, end, end / 5))

    # 可视化资产数量
    ax4 = plt.subplot(414, sharex=ax1)
    perf['cash'].plot(ax=ax4)
    ax4.set_ylabel('cash\n({})'.format(quote_currency))
    start, end = ax4.get_ylim()
    ax4.yaxis.set_ticks(np.arange(0, end, end / 5))

    # 自动调整子图参数，使之填充整个区域
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    run_algorithm(
        handle_data=handle_data,
        initialize=initialize,
        analyze=analyze,
        data_frequency='daily',
        start=pd.to_datetime('2019-11-01', utc=True),
        end=pd.to_datetime('2020-02-01', utc=True),
        exchange_name='binance',
        quote_currency='usdt',
        capital_base=1000
    )


