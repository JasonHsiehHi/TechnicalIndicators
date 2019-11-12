import requests
import pandas as pd
from io import StringIO
import datetime
import time
import os
import json
from package.tool import timer  # from.. from ..package 表示上一層 跟 其他層


# 應將saver改成inherited super class
class data_saver:
    def __init__(self):
        self.data = None

    def est_data(self, route):
        """__int__() use the func"""
        try:
            if os.path.exists(route):
                with open(route, 'r') as file_object:
                    contents = json.load(file_object)
                contents_to_df = {}
                for date_from_list in sorted(contents.keys()):
                    contents_to_df[date_from_list] = pd.DataFrame(contents[date_from_list])
                return contents_to_df
            else:
                print("it doesn't exist ")
                return {}
        except:
            return {}

    def remain_of_data(self, n_data):
        df_data_slice = {}
        for key, x in list(self.data.items())[-n_data:]:
            df_data_slice[key] = x
        self.data = df_data_slice
        return df_data_slice

    def crawl_from_time_to_time(self, start_date_str, end_date_str, func):  # 填入日期時段
        """crawl_from_time_to_now() use the func"""
        start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")  # datetime類別 才能用timedelta
        fail_count = 0
        allow_continuous_fail_count = 14  # 防止過年連假 導致中止
        while True:
            print('parsing', start_date_str)
            try:  # 有失敗的可能性都應該用try&except
                if start_date_str in self.data.keys():
                    print("{} 已在資料中!!".format(start_date_str))
                else:
                    result = func(self, start_date_str)
                    if len(result) > 0:
                        self.data[start_date_str] = result  # func此時必須填入完整參數(包含self)
                        print('success!目前存入資料數：{}'.format(len(self.data)))
                    else:
                        print('null! check the date is holiday')
                    time.sleep(5)
                fail_count = 0
            except:  # try中只要有任何一行異常 都會跳到except中
                print('fail! check the date is holiday')
                fail_count += 1
                time.sleep(5)
                if fail_count == allow_continuous_fail_count:
                    raise  # 為了解引發except之原因 並用raise取代break
                # raise 不加任何變數 才能找出引發except的正確行號
            if start_date_str == end_date_str:
                break
            # 最後一個迴圈想要停在cell之中 while True中 將結束迴圈條件寫在if break
            start_date += datetime.timedelta(days=1)
            start_date_str = start_date.strftime("%Y%m%d")

        self.save_data()  # 父輩因為route=None故不會做儲存
        #  method必須能在父輩可供使用 而在子輩時又能合宜

    def crawl_from_time_to_now(self, n_days, func):
        """like crawl_from_time_to_time()"""
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        date_str = (datetime.datetime.now() - datetime.timedelta(days=n_days)).strftime("%Y%m%d")
        self.crawl_from_time_to_time(date_str, today_str, func)

    def save_data(self, route=None):  # raw string為防止/n /t...轉義字元發生作用
        """crawl_from_time_to_time() use the func"""
        data_saved = {}
        for date_from_list in sorted(self.data.keys()):  # 用sorted() 可依日期儲存
            data_saved[date_from_list] = self.data[date_from_list].to_dict()
        route_list = route.split('/')
        for x in range(2, len(route_list)):
            if not os.path.isdir("/".join(route_list[:x])):
                os.mkdir("/".join(route_list[:x]))

        with open(route, 'w') as file_object:
            json.dump(data_saved, file_object)


class stock_data_saver(data_saver):
    def __init__(self):  # 同名一定要inherit
        super(stock_data_saver, self).__init__()
        self.data = self.est_data()
        print("stock_data目前資料到{}".format(list(self.data.keys())[-1]))

    def est_data(self, route=r'./saved/stock_information/stocks_data.json'):
        """__int__() use the func """
        return super(stock_data_saver, self).est_data(route)  # 子輩必須自身有return 以進行output

    def crawl_stock_daily(self, date):  # nan 可能是該日無人交易
        """crawl_from_time_to_time() use the func """
        r = requests.get(
            "http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=" + date + "&type=ALL&_=1555999982351")
        r_list = ([i.translate({ord(c): None for c in ' '})
                   for i in r.text.split('\n') if len(i.split('",')) == 17 and i[0] != '='])
        # translate({}) 為處理空格 當沒有數字時為' '(str)可以轉成None 為使在做df轉換時可以變成None
        # for in split()抓出目標table
        # print(str)會自動處理\r \n... 但print(list) str在list裡面則不會處理\r\n
        ret = pd.read_csv(StringIO("\n".join(r_list)), dtype=object)  # ""\n".join()後 才能.read_csv()轉成dataframe
        ret = ret.drop(['漲跌(+/-)', 'Unnamed: 16'], axis=1)
        ret = ret.set_index('證券代號')
        ret = ret.apply(lambda s: s.str.replace(",", ""))
        return ret

    @timer
    def crawl_from_time_to_time(self, start_date_str, end_date_str, func=crawl_stock_daily):
        # func當作參數 不用加上self. 因為同個class區域
        super(stock_data_saver, self).crawl_from_time_to_time(start_date_str, end_date_str, func)

    @timer
    def crawl_from_time_to_now(self, n_days, func=crawl_stock_daily):
        super(stock_data_saver, self).crawl_from_time_to_now(n_days, func)

    def save_data(self, route=r'./saved/stock_information/stocks_data.json'):  # raw string為防止/n /t...轉義字元發生作用
        """crawl_from_time_to_time() use the func """
        super(stock_data_saver, self).save_data(route)

    def crawl_from_time_to_now_and_save(self, n_days):
        self.crawl_from_time_to_now(n_days)
        self.save_data()


class trading_data_saver(data_saver):
    def __init__(self):  # methods of the same name need to inherit
        super(trading_data_saver, self).__init__()
        self.data = self.est_data()
        print("trading_data目前資料到{}".format(list(self.data.keys())[-1]))

    def est_data(self, route=r'./saved/stock_information/trading_data.json'):
        """__int__() use the """
        return super(trading_data_saver, self).est_data(route)  # 子輩必須自身有return 以進行output

    def crawl_trading_daily(self, date):
        """crawl_from_time_to_time() use the func """
        r = requests.get(
            "https://www.twse.com.tw/fund/T86?response=csv&date=" + date + "&selectType=ALLBUT0999")
        r_list = [i.translate({ord(c): None for c in ' '})
                  for i in r.text.split('\n') if len(i.split('",')) == 20 and i[0] != '=']
        ret = pd.read_csv(StringIO("\n".join(r_list)), dtype=object)  # ""\n".join()後 才能.read_csv()轉成dataframe
        ret = ret.drop(['外資自營商買進股數', '外資自營商賣出股數', '外資自營商買賣超股數', 'Unnamed: 19'], axis=1)
        # TODO 只會顯示當天有進行交易的股票 故總數不會是全部個股
        ret = ret.set_index('證券代號')
        ret = ret.apply(lambda s: s.str.replace(",", ""))
        return ret

    @timer
    def crawl_from_time_to_time(self, start_date_str, end_date_str, func=crawl_trading_daily):
        # func當作參數 不用加上self. 因為同個class區域
        super(trading_data_saver, self).crawl_from_time_to_time(start_date_str, end_date_str, func)

    @timer
    def crawl_from_time_to_now(self, n_days, func=crawl_trading_daily):
        super(trading_data_saver, self).crawl_from_time_to_now(n_days, func)

    def save_data(self, route=r'./saved/stock_information/trading_data.json'):  # raw string為防止/n /t...轉義字元發生作用
        """crawl_from_time_to_time() use the func """
        super(trading_data_saver, self).save_data(route)

    def crawl_from_time_to_now_and_save(self, n_days):
        self.crawl_from_time_to_now(n_days)
        self.save_data()


class financing_data_saver(data_saver):
    def __init__(self):
        super(financing_data_saver, self).__init__()
        self.data = self.est_data()  # 修改變數名稱 應使用refactor才不容易出錯
        print("financing_data目前資料到{}".format(list(self.data.keys())[-1]))

    def crawl_financing_data_daily(self, date):
        r = requests.get("http://www.twse.com.tw/exchangeReport/MI_MARGN?response=csv&date=" + date + "&selectType=ALL")
        r_list = [i.translate({ord(c): None for c in ' '})
                  for i in r.text.split('\n') if len(i.split('",')) == 17 and i[0] != '=']
        # for i in... 不僅可做迴圈 亦可直接用[i for i in...]將其轉為list
        # i.split('","')不能完全用間隔物做區分 因為最後一項為\r項 並非表格裡面的str型態
        ret = pd.read_csv(StringIO("\n".join(r_list)), dtype=object)
        # 用dtype=object 來預防有些series的變數具特定型態 難以轉換

        # column項目為巢狀目錄 最快的方法就是重新命名 比用Extractor快多了
        ret = ret.drop(['資券互抵', '註記', 'Unnamed: 16'], axis=1)
        ret.columns = ['股票代號', '股票名稱', '融資買進', '賣回融資', '現金償還',
                       '融資前日餘額', '融資今日餘額', '融資限額', '買回融券', '融券賣出',
                       '現券償還', '融券前日餘額', '融券今日餘額', '融券限額']
        ret = ret.set_index('股票代號')
        ret = ret.apply(lambda s: s.str.replace(",", ""))
        return ret

    def est_data(self, route=r'./saved/stock_information/financing_data.json'):
        return super(financing_data_saver, self).est_data(r'./saved/stock_information/financing_data.json')

    @timer
    def crawl_from_time_to_time(self, start_date_str, end_date_str, func=crawl_financing_data_daily):
        # func當作參數 不用加上self. 因為同個class區域
        super(financing_data_saver, self).crawl_from_time_to_time(start_date_str, end_date_str, func)

    @timer
    def crawl_from_time_to_now(self, n_days, func=crawl_financing_data_daily):
        super(financing_data_saver, self).crawl_from_time_to_now(n_days, func)

    def save_data(self, route=r'./saved/stock_information/financing_data.json'):
        """crawl_from_time_to_time() use the func """
        super(financing_data_saver, self).save_data(route)

    def crawl_from_time_to_now_and_save(self, n_days):
        self.crawl_from_time_to_now(n_days)
        self.save_data()


class shareholder_data_saver(data_saver):
    def __init__(self):
        super(shareholder_data_saver, self).__init__()

        # self.update_allStocks_list() 變動不大 且 浪費太多時間在抓取基金權證
        self.update_date_list()
        self.date_list = self.est_date_list()
        self.allStocks_list = self.est_allStocks_list()

        self.data = self.est_nestedDict()
        print("shareholder_data目前資料到{}".format(list(self.data.keys())[-1]))
        # self.update_nestedDict(n_terms)  # update after establish() because time-saving for repetitive items

    def est_list(self, route):
        if os.path.exists(route):
            with open(route, 'r') as file_object:
                contents = json.load(file_object)
            return contents
        else:
            return []

    def est_date_list(self, route=r'./saved/stock_concentration/date_list.json'):
        return self.est_list(route)

    def est_allStocks_list(self, route=r'./saved/stock_concentration/allStocks_list.json'):
        return self.est_list(route)

    @timer
    def update_date_list(self):
        payload = {  # 將form_data 以view parsed之形式 it's what the user do after requests.post
            "REQ_OPR": "qrySelScaDates"
        }
        res = requests.post("https://www.tdcc.com.tw/smWeb/QryStockAjax.do", data=payload)
        res_list = [i.translate({ord(c): None for c in '[]"'}) for i in res.text.split(',')]  # 把[ ] "三種符號都丟掉
        res_list.reverse()  # (res_list.reverse()) can just replace (res_list=res_list.reverse())
        print("date_list total:{}".format(len(res_list)))

        self.date_list = res_list
        self.save(self.date_list, "date_list")

    @timer
    def update_allStocks_list(self):
        import re
        df_allStocks = pd.read_html(requests.get("http://isin.twse.com.tw/isin/C_public.jsp?strMode=2").text)[0]
        # strMode=2為上市公司 strMode=4為上櫃公司 # pd.read_html()直接轉成df
        df_allStocks.columns = df_allStocks.iloc[0]
        df_allStocks = df_allStocks.set_index('有價證券代號及名稱')
        # df.index = 則不會改變原始表格 會多設一行index 與 df.set_index() 會改變原始表格 將其中一行column列為index
        df_allStocks = df_allStocks.iloc[1:]  # 用縮小表格 進行行列刪除
        df_allStocks = df_allStocks.loc[:"上市認購(售)權證"].iloc[1:-1]
        # loc[] iloc[] 皆會保留最後一項 range(1,n) 只會到n的前一項
        df_allStocks = df_allStocks.dropna(thresh=3, axis=0).dropna(thresh=3, axis=1)
        # 不同於drop()針對整排整列做刪除 dropna()專門處理Nan資料
        list_allStocks = []
        for i in df_allStocks.index:
            list_allStocks.append(re.sub(r'\D', '', i))
        print("allStocks_list total:{}".format(len(list_allStocks)))

        self.allStocks_list = list_allStocks
        self.save(self.allStocks_list, "allStocks_list")

        # TODO def save 可做資料備份方式

    def save(self, data, data_name):  # it's difficult to save data just as the variable name
        """update_date_list() update_allStocks_list() save_nestedDict() use the func"""
        if not os.path.isdir(r'./saved'):
            os.mkdir(r'./saved')
        if not os.path.isdir(r'./saved/stock_concentration'):
            os.mkdir(r'./saved/stock_concentration')
        with open(r'./saved/stock_concentration/' + data_name + ".json", 'w') as file_object:
            json.dump(data, file_object)

    @timer
    def est_nestedDict(self):
        if os.path.exists(r'./saved/stock_concentration/shareholder_nestedData.json'):
            with open(r'./saved/stock_concentration/shareholder_nestedData.json', 'r') as file_object:
                contents = json.load(file_object)

            contents_to_df = {}
            for date_from_list in sorted(contents.keys()):
                contents_to_df[date_from_list] = {}

            for date_from_list in sorted(contents.keys()):
                for stock_from_list in contents[date_from_list].keys():
                    contents_to_df[date_from_list][stock_from_list] = \
                        pd.DataFrame(contents[date_from_list][stock_from_list])
                    contents_to_df[date_from_list][stock_from_list].index = \
                        contents_to_df[date_from_list][stock_from_list].index.astype(int)  # 必須把index改為int才能做排序
                    contents_to_df[date_from_list][stock_from_list] = \
                        contents_to_df[date_from_list][stock_from_list].sort_index(ascending=True)

            return contents_to_df
        else:
            print("it doesn't exist ")
            nullDict_for_date = {}  # establish the null nested dict
            for date_from_list in self.date_list:
                nullDict_for_date[date_from_list] = {}

            return nullDict_for_date

    @timer
    def update_nestedDict(self, n_terms):  # n_terms為總期數
        """update_nestedDict與est_nestedDict分開 """
        import OpenSSL
        from package.extractor import Extractor  # 必須加上完整路徑 因.extractor不能找到package
        try:
            for date_from_list in self.date_list[-n_terms:]:
                # 應全部搜索 因為date_list也會更新 如果用len()只取後面反而會略過第一項
                if date_from_list in self.data.keys():
                    print("{0},已在資料中".format(date_from_list))
                else:
                    self.data[date_from_list] = {}
                for stock_from_list in self.allStocks_list:
                    if stock_from_list in self.data[date_from_list].keys():
                        # df>1表示沒有錯誤 此時df是搜集成功
                        print("{0} 的 {1},已在資料中".format(date_from_list, stock_from_list))

                    else:
                        payload = {
                            'scaDates': date_from_list,
                            'scaDate': date_from_list,
                            'SqlMethod': 'StockNo',
                            'StockNo': stock_from_list,
                            'radioStockNo': stock_from_list,
                            'StockName': '',
                            'REQ_OPR': 'SELECT',
                            'clkStockNo': stock_from_list,
                            'clkStockName': ''
                        }
                        fail_count = 0
                        while True:
                            res = Extractor(requests.post("https://www.tdcc.com.tw/smWeb/QryStockAjax.do",
                                                          data=payload).text, jquery='table.mt:eq(1)')
                            self.data[date_from_list][stock_from_list] = res.df(header=1)
                            print("success, {0} 的 {1}已處理".format(date_from_list, stock_from_list))

                            print("sleep")
                            time.sleep(2)
                            if len(self.data[date_from_list][stock_from_list]) > 1:
                                # 必須偵測是否爬蟲失敗 會變成row=1 且是 "無此資料"
                                break

                            fail_count += 1
                            if fail_count > 5:  # while True 的fail_count機制 以防程式陷入循環
                                break

                print("success,{0}目前有{1}筆".format(date_from_list, len(self.data[date_from_list])))
        except OpenSSL.SSL.Error:
            print("long sleep")
            time.sleep(30)
            self.update_nestedDict(n_terms)
        self.save_nestedDict()

    def save_nestedDict(self):
        """update_nestedDict() use the func """
        self.unify_column_words()

        nestedDict_saved = {}
        for date_from_list in sorted(self.data.keys()):
            nestedDict_saved[date_from_list] = {}

        for date_from_list in sorted(self.data.keys()):
            for stock_from_list in sorted(self.data[date_from_list].keys()):
                nestedDict_saved[date_from_list][stock_from_list] = \
                    self.data[date_from_list][stock_from_list].to_dict()

        self.save(nestedDict_saved, "shareholder_nestedData")

    def replace_column_name(self, old_column_name, new_column_name):  # to deal with different word of columns
        """save_nestedDict() use the func 占集保庫存數比例 (%) -> 佔集保庫存數比例 (%)"""
        for date_str in self.data.keys():
            for stock_name in self.data[date_str].keys():  # rename({}) 使用dict做文字轉換的元素組
                self.data[date_str][stock_name].rename(columns={old_column_name: new_column_name}, inplace=True)

    def unify_column_words(self):  # to make replace_column_name() better
        """improve replace_column_name"""
        for date_str in self.data.keys():
            for stock_name in self.data[date_str].keys():
                self.data[date_str][stock_name].columns = \
                    ['序', '持股/單位數分級', '人數', '股數/單位數', '佔集保庫存數比例(%)']
