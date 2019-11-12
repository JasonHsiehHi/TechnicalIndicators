import pandas as pd
import copy
import os


class information_table1:
    def __init__(self, d1_sd, d1_td):

        d1_sd.remain_n_days(90)
        d1_td.remain_n_days(90)

        self.close = d1_sd.timeSeries_for_column('收盤價')
        self.high = d1_sd.timeSeries_for_column('最高價')
        self.low = d1_sd.timeSeries_for_column('最低價')
        self.volume = d1_sd.timeSeries_for_column('成交股數')

        d1_td.add_column_from_other_df(d1_sd.df_data, '成交股數')
        d1_td.add_column_arithmetic(['+', '/'], ['外陸資買進股數(不含外資自營商)', '成交股數'], '外資買進比例')
        d1_td.add_column_arithmetic(['+', '/'], ['外陸資賣出股數(不含外資自營商)', '成交股數'], '外資賣出比例')

        self.foreign_buy = d1_td.timeSeries_for_column('外資買進比例')
        self.foreign_sell = d1_td.timeSeries_for_column('外資賣出比例')

        print('目前有 close high low volume foreign_buy foreign_sell')

    def information_for_specific_stock(self, stock_number):
        point = point_for_Series(self.close.loc[stock_number])
        RSI = RSI_for_Series(self.close.loc[stock_number], 24)
        VR = VR_for_Series(self.close.loc[stock_number], self.volume.loc[stock_number], 24)
        MA5 = MA_for_Series(self.close.loc[stock_number], 5)
        MA10 = MA_for_Series(self.close.loc[stock_number], 10)
        MA20 = MA_for_Series(self.close.loc[stock_number], 20)
        RSV = RSV_for_Series(self.close.loc[stock_number], self.high.loc[stock_number], self.low.loc[stock_number])
        K = K_for_Series(self.close.loc[stock_number], self.high.loc[stock_number], self.low.loc[stock_number])
        D = D_for_Series(self.close.loc[stock_number], self.high.loc[stock_number], self.low.loc[stock_number])
        DIF = DIF_for_Series(self.close.loc[stock_number])
        MACD = MACD_for_Series(self.close.loc[stock_number])
        FB = self.foreign_buy.loc[stock_number]
        FB.name = 'FB'
        FS = self.foreign_sell.loc[stock_number]
        FS.name = 'FS'

        df_concat = pd.concat([point.transpose(), MA5, MA10, MA20, RSV, K, D, DIF, MACD, FB, FS], axis=1).transpose()
        save_dataFrame_or_Series(df_concat, r'./saved/search_for_' + stock_number + '.csv')
        return df_concat


def save_dataFrame_or_Series(data, route=None):
    route_list = route.split('/')
    for x in range(2, len(route_list)):
        if not os.path.isdir("/".join(route_list[:x])):
            os.mkdir("/".join(route_list[:x]))
    data.to_csv(route, encoding='utf_8_sig')


def point_for_Series(s_data: pd.Series):
    s_bottom = copy.deepcopy(s_data)
    s_top = copy.deepcopy(s_data)
    for num in range(1, len(s_data)-1):  # 頭尾無法判斷是否為峰點
        if ~(s_data[num] <= s_data[num-1] and s_data[num] <= s_data[num + 1]):
            s_bottom[num] = None
        if ~(s_data[num] >= s_data[num - 1] and s_data[num] >= s_data[num + 1]):
            s_top[num] = None

    s_bottom.name = 'bottom'
    s_top.name = 'top'
    df_concat = pd.concat([s_data, s_top, s_bottom], axis=1)
    return df_concat.transpose()


def RSI_for_Series(s_data: pd.Series, n_days=12):  # n_days 常用 6 12 24...
    copy_s = copy.deepcopy(s_data)

    for v in range(0, n_days):
        copy_s[v] = None
    for x in range(n_days, len(s_data)):  # 只能從s_data[n_days]開始 資料才會充足
        RSI_rise = 0
        RSI_drop = 0
        for n in range(0, -n_days, -1):
            margin = s_data[x+n] - s_data[x+n-1]
            if margin > 0:
                RSI_rise += margin
            else:
                RSI_drop += abs(margin)
        RSI = (RSI_rise/n_days) / ((RSI_rise/n_days)+(RSI_drop/n_days))
        copy_s[x] = RSI
    copy_s.name = 'RSI'+str(n_days)
    return copy_s


def VR_for_Series(s_price_data: pd.Series, s_vol_data: pd.Series, n_days=12):  # n_days 常用 6 12 24...
    copy_s = copy.deepcopy(s_price_data)
    for v in range(0, n_days):
        copy_s[v] = None
    for x in range(n_days, len(s_price_data)):  # 只能從s_data[n_days]開始 資料才會充足
        VR_rise = 0
        VR_drop = 0
        VR_total = 0
        for n in range(0, -n_days, -1):
            margin = s_price_data[x+n] - s_price_data[x+n-1]
            if margin > 0:
                VR_rise += s_vol_data[x+n]
            if margin < 0:   # 若價格不變 此交易量不能算入任何一方
                VR_drop += s_vol_data[x+n]
            VR_total += s_vol_data[x+n]

        VR = (VR_rise + VR_total/2) / (VR_drop + VR_total/2)
        copy_s[x] = VR
    copy_s.name = 'VR' + str(n_days)
    return copy_s


def MA_for_Series(s_data: pd.Series, n_days=10):  # n_days 常用 5 10 20 60...
    copy_s = copy.deepcopy(s_data)
    for v in range(0, n_days):
        copy_s[v] = None
    for x in range(n_days, len(s_data)):  # 只能從s_data[n_days]開始 資料才會充足
        total = 0
        for n in range(0, -n_days, -1):
            total += s_data[x+n]

        MA = total / n_days
        copy_s[x] = MA
    copy_s.name = 'MA' + str(n_days)
    return copy_s


def EMA_for_Series(s_data: pd.Series, n_days=12):  # n_days 常用 12 26...
    copy_s = copy.deepcopy(s_data)
    alpha = 2 / (n_days+1)
    for v in range(0, n_days):
        copy_s[v] = None
    for x in range(n_days, len(s_data)):  # 只能從s_data[n_days]開始 資料才會充足
        total = 0
        for n in range(0, -n_days, -1):
            total += (alpha * (1-alpha) ** (-n)) * s_data[x+n]  # n為0 -1 -2 -3... 故需翻正
        total += ((1-alpha) ** n_days) * s_data[x-n_days]  # 令EMA(t-n_days) = P(t-n_days)

        EMA = total
        copy_s[x] = EMA

    copy_s.name = 'EMA' + str(n_days)
    return copy_s


def RSV_for_Series(s_close_data, s_high_data, s_low_data, n_days=9):  # n_days 常用 9
    copy_s = copy.deepcopy(s_close_data)

    for v in range(0, n_days):
        copy_s[v] = None
    for x in range(n_days, len(s_close_data)):  # 只能從s_data[n_days]開始 資料才會充足
        high = 0
        low = 10000
        close = s_close_data[x]
        for n in range(0, -n_days, -1):
            if s_high_data[x+n] > high:
                high = s_high_data[x+n]
            if s_low_data[x+n] < low:
                low = s_low_data[x+n]

        RSV = (close-low) / (high-low)
        copy_s[x] = RSV
    copy_s.name = 'RSV' + str(n_days)
    return copy_s


def K_for_Series_from_RSV(s_RSV_data, n_days):
    copy_s = copy.deepcopy(s_RSV_data)
    alpha = 1 / 3
    for v in range(0, n_days):
        copy_s[v] = None
    for x in range(n_days, len(s_RSV_data)):  # 只能從s_data[n_days]開始 資料才會充足
        total = 0
        for n in range(0, -3, -1):
            total += (alpha * (1-alpha) ** (-n)) * s_RSV_data[x+n]  # n為0 -1 -2 -3... 故需翻正
        total += ((1-alpha) ** -(-3)) * s_RSV_data[x+(-3)]  # 令EMA(t-n_days) = P(t-n_days)

        K = total
        copy_s[x] = K
    copy_s.name = 'K' + str(n_days)
    return copy_s


def K_for_Series(s_close_data, s_high_data, s_low_data, n_days=9):  # n_days 常用 9
    # 先求RSV_data 再轉 K_data
    RSV_s = RSV_for_Series(s_close_data, s_high_data, s_low_data, n_days)
    K_s = K_for_Series_from_RSV(RSV_s, n_days)
    return K_s


def D_for_Series(s_close_data, s_high_data, s_low_data, n_days=9):  # n_days 常用 9
    # 先求RSV_data 再轉 K_data 再轉 D_data
    RSV_s = RSV_for_Series(s_close_data, s_high_data, s_low_data, n_days)
    K_s = K_for_Series_from_RSV(RSV_s, n_days)
    D_s = K_for_Series_from_RSV(K_s, n_days)
    D_s.name = 'D' +str(n_days)
    return D_s


def DIF_for_Series(s_data, short_n_days=12, long_n_days=26):
    DIF_s = EMA_for_Series(s_data, short_n_days) - EMA_for_Series(s_data, long_n_days)
    DIF_s.name = 'DIF' + str(short_n_days) + 'to' + str(long_n_days)
    return DIF_s


def MACD_for_Series(s_data, short_n_days=12, long_n_days=26, average_days=9):
    DIF_s = EMA_for_Series(s_data, short_n_days) - EMA_for_Series(s_data, long_n_days)
    MACD_s = EMA_for_Series(DIF_s, average_days)
    MACD_s.name = 'MACD' + str(average_days)
    return MACD_s
