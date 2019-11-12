# TechnicalIndicators
技術指標
 謹供面試使用2019/11/02
----
    # python start_indicators.py [stock_number]

example:

    # python start_indicators.py 2330

----
data_saver.py用於爬TWSE之資料, 分為:

1.[每日收盤行情](https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html)
2.[三大法人買賣超日報](https://www.twse.com.tw/zh/page/trading/fund/T86.html)
3.[融資融券餘額](https://www.twse.com.tw/zh/page/trading/exchange/MI_MARGN.html)

----
process_step_1.py則做對DataFrame做運算處理

-----
technical_indicators.py則提供各式指標:
峰點、谷點、MA、KD、MACD、RSI...等
