import pandas as pd
import os
import json
from .tool import timer
import copy


# 引入function # 對variable_row做處理 最後回傳一個variable

def sum_abs(s):
    """nestedData_process_step1.find_var_delta() use the func"""
    return round(s.abs().sum(), 3)


def max_abs(s):
    """nestedData_process_step1.find_var_delta() use the func"""
    return round(s[s.abs().idxmax()], 3)


def idxmax_abs(s):
    """nestedData_process_step1.find_var_delta() use the func"""
    return s.abs().idxmax()


class data_process_step1:
    def __init__(self, df_data):
        """df_data from (stock_ trading_ financing_)data_saver.data"""
        self.df_data = df_data
        self.var_timeSeries = pd.DataFrame({})
        print("目前可選的var:{}".format(self.search_columns()))

    def remain_n_days(self, n_days):
        df_data_slice = {}
        for key, x in list(self.df_data.items())[-n_days:]:
            df_data_slice[key] = x
        self.df_data = df_data_slice
        return df_data_slice

    def remain_stock_number(self, group_stock_numbers=['2330', '2317', '3008']):
        df_data_slice = {}
        for key, x in self.df_data.items():
            df_data_slice[key] = x.reindex(group_stock_numbers)  # .reindex 可以直接處理missing value 比loc方便
        self.df_data = df_data_slice
        return df_data_slice

    def remain_columns(self, group_columns):
        df_data_slice = {}
        for key, x in self.df_data.items():
            df_data_slice[key] = x.loc[:, group_columns]
        self.df_data = df_data_slice
        return df_data_slice

    def save_to_csv(self, data, route=None):
        route_list = route.split('/')
        for x in range(2, len(route_list)):
            if not os.path.isdir("/".join(route_list[:x])):
                os.mkdir("/".join(route_list[:x]))
        data.to_csv(route, encoding='utf_8_sig')

    def search_columns(self):
        try:
            return list(list(self.df_data.values())[0].columns)  # 可以直接將df.values()轉list
        except:
            print("can't find the columns! check if the data is df.")

    def add_column_from_other_df(self, other_df_data, column_name):
        """必須檢查兩個df需格式完全相同"""
        copy_df_data = copy.deepcopy(self.df_data)
        if sorted(self.df_data.keys()) == sorted(other_df_data.keys()):
            for key in self.df_data.keys():
                copy_df_data[key][column_name] = other_df_data[key][column_name]
        else:
            print('df_data與other_df_data的key值不相符')
        self.df_data = copy_df_data
        return copy_df_data

    def add_column_ratios(self, column_name1, column_name2, new_column_name):
        """use search_columns() first"""
        copy_df_data = copy.deepcopy(self.df_data)
        for key, x in self.df_data.items():
            copy_df_data[key][new_column_name] = pd.to_numeric(x[column_name1]) / pd.to_numeric(x[column_name2])
        self.df_data = copy_df_data
        return copy_df_data

    def add_multi_column_ratios(self, list_column_names1, list_columns_names2, list_new_column_names):
        """必須照list順序對齊各個分子分母"""
        copy_df_data = copy.deepcopy(self.df_data)
        n = 0
        for key, x in self.df_data.items():
            copy_df_data[key][list_new_column_names[n]] = \
                pd.to_numeric(x[list_column_names1[n]]) / pd.to_numeric(x[list_columns_names2[n]])
            n += 1
        self.df_data = copy_df_data
        return copy_df_data

    def add_column_arithmetic(self, list_operation, list_column_name, new_column_name):  # 比前兩func方便許多
        """use search_columns() first"""
        copy_df_data = copy.deepcopy(self.df_data)
        list_column = []
        for col in list_column_name:
            list_column.append('pd.to_numeric(x["' + col + '"])')
        str_for_eval = ''
        for num in range(len(list_operation)):
            str_for_eval += list_operation[num]
            str_for_eval += list_column[num]
        print("執行字串為：" + str_for_eval)
        for key, x in self.df_data.items():
            copy_df_data[key][new_column_name] = eval(str_for_eval)
        self.df_data = copy_df_data
        return copy_df_data

    def timeSeries_for_column(self, column_name='收盤價', weekly=False):
        """column_name from search_columns()"""
        column_timeSeries = pd.DataFrame({k:d[column_name] for k, d in self.df_data.items()})
        # 最基本的df建立法就是用dict
        # k為"20190423"字串 d為dataFrame但此時只留下特定column
        column_timeSeries = column_timeSeries.apply(lambda s: pd.to_numeric(s, errors='coerce'))  # 全部轉成數值
        # df.apply(f,axis=0) default 表示對不同index(row)的值做處理 故每一條column為一個series
        # 有些股票上市時間不同 或 已經下市
        column_timeSeries = column_timeSeries.dropna(axis=1, how='all')  # 處理empty dataFrame
        if weekly:
            column_timeSeries = self.daily_to_weekly(column_timeSeries)
        self.var_timeSeries = column_timeSeries
        return column_timeSeries

    def timeSeries_for_specific_stock(self, stock_number='2330'):
        """column_name from search_columns()"""
        timeSeries = pd.DataFrame({k: d.loc[stock_number] for k, d in self.df_data.items()})
        timeSeries = timeSeries.apply(lambda s: pd.to_numeric(s, errors='ignore'), axis=1)  # 全部轉成數值
        # df.apply(f,axis=0) default 表示對不同index(row)的值做處理 故每一條column為一個series
        return timeSeries

    @classmethod
    def find_all_return_rates(cls, column_timeSeries):
        """column_timeSeries from timeSeries_for_column()"""
        dict_rate = {}
        count = 1
        while count < len(column_timeSeries.columns):
            for x in range(0, len(column_timeSeries.columns) - count):
                early_date = column_timeSeries.columns[x]
                late_date = column_timeSeries.columns[x + count]
                dict_rate[early_date+"~"+late_date] = \
                    (column_timeSeries[late_date] - column_timeSeries[early_date]) / column_timeSeries[early_date]
            count += 1
        return pd.DataFrame(dict_rate)
    
    def columns_to_all_return_rates(self, column_name, weekly=False):
        """incorporate timeSeries_for_column() and find_all_return_rates()"""
        column_timeSeries = self.timeSeries_for_column(column_name, weekly)
        return self.find_all_return_rates(column_timeSeries)

    @classmethod
    def find_return_rates(cls, column_timeSeries):  # 只會找該項對前一項 不同於find_all_return_rates()
        """like find_all_return_rates()"""
        dict_rate = {}
        for x in range(0, len(column_timeSeries.columns) - 1):
            early_date = column_timeSeries.columns[x]
            late_date = column_timeSeries.columns[x + 1]
            dict_rate[early_date+"~"+late_date] = \
                (column_timeSeries[late_date] - column_timeSeries[early_date]) / column_timeSeries[early_date]
        return pd.DataFrame(dict_rate)
    
    def columns_to_return_rates(self, column_name, weekly=False):
        """like columns_to_all_return_rates()"""
        column_timeSeries = self.timeSeries_for_column(column_name, weekly)
        return self.find_return_rates(column_timeSeries)

    def daily_to_weekly(self, df_data_daily):
        """timeSeries_for_column() use the func"""
        from copy import deepcopy
        import datetime
        df_data_weekly = pd.DataFrame({})
        content = deepcopy(df_data_daily)  # 以防破壞原資料
        count = 0
        end_date = datetime.datetime.strptime(content.columns[len(content.columns)-1], "%Y%m%d")
        next_end_date = end_date + datetime.timedelta(days=7-end_date.weekday())
        content[next_end_date.strftime("%Y%m%d")] = None
        for x in range(0, len(content.columns)-1):
            date = datetime.datetime.strptime(content.columns[x], "%Y%m%d")
            next_date = datetime.datetime.strptime(content.columns[x+1], "%Y%m%d")
            if date.weekday() > next_date.weekday():
                count += 1  # 包含目前要被併入的這一項
                df_data_weekly[content.columns[x]] =\
                    content.iloc[:, x-count+1:x+1].apply(lambda s: s.mean(), axis=1)
                # 不要用late_date~early_date 因為後面還要算return rate 容易搞混
                count = 0
            else:
                count += 1
        return df_data_weekly

    def find_stock_return_rate(self, stock_number, column_name="收盤價", above_standard=0):  # 因範圍太長會拖垮運算
        df_all_return_rates = self.columns_to_all_return_rates(column_name)
        s_result = df_all_return_rates.loc[stock_number].sort_values(ascending=False)
        # 此時為對series做排序 故應不用再指定特定column
        
        s_result["probability"] = (s_result > above_standard).mean()
        # ()可直接用不等式轉成boolean值Series  # Series也可以直接加上key值(columns)
        return s_result

    
class nestedData_process_step1:  # a row in specific stock and specific date
    def __init__(self, df_nestedData):
        """df_data from (shareholder_)data_saver.data"""
        self.df_nestedData = df_nestedData
        self.var_df_data = self.est_var_df_data("percent")  # "percent"  "amount"  "people"
        self.var_deltaData = pd.DataFrame({})
        print("目前可選的var:{}".format(self.search_columns()))
        print("self.var_df_data 為find_var_df_dict()的variable")
        print("self.var_deltaData 為find_var_delta(func)的variable")

    def search_columns(self):  
        return list((list(self.df_nestedData.values())[0]).values())[0].columns  # 選取第一支股票 第一個日期的df

    @timer
    def est_var_df_data(self, var_name="percent"):  # creating the new data wastes lots of time
        if os.path.exists(r'./saved/stock_concentration/'+var_name+'_df_data.json'):
            with open(r'./saved/stock_concentration/'+var_name+'_df_data.json', 'r') as file_object:
                contents = json.load(file_object)
            contents_to_df = {}
            for date_from_list in contents.keys():
                contents_to_df[date_from_list] = pd.DataFrame(contents[date_from_list])
            return contents_to_df
        
        else: 
            dict_for_date = {}
            for date_from_list in self.df_nestedData.keys():
                dict_for_date[date_from_list] = {}
                
            return dict_for_date

    def remain_df_data(self, n_data):
        df_data_slice = pd.DataFrame({})
        for key, x in list(self.df_nestedData.items())[-n_data:]:
            df_data_slice[key] = x
        self.df_nestedData = df_data_slice
        return df_data_slice

    def save_var_df_data(self, data_name):  # data_name = "percent" "amount"  "people"
        data_saved = {}
        for date_from_list in sorted(self.var_df_data.keys()):  # 用sorted() 可依時序做儲存
            data_saved[date_from_list] = self.var_df_data[date_from_list].to_dict()
        
        if not os.path.isdir(r'./saved'):
            os.mkdir(r'./saved')
        if not os.path.isdir(r'./saved/stock_concentration'):
            os.mkdir(r'./saved/stock_concentration')
        with open(r'./saved/stock_concentration/'+data_name+"_df_data.json", 'w') as file_object:
            json.dump(data_saved, file_object)

    def timeSeries_for_column(self, data_dict, column_name):
        """find_var_df_data() use the func"""
        # data=dict ( key=date, value=date's df)
        df = pd.DataFrame({k: d[column_name] for k, d in data_dict.items()})
        df = df.apply(lambda s: pd.to_numeric(s, errors='coerce'))  # 全部轉成數值
        return df

    def transpose_set_seven_grade(self, df):
        """find_var_df_data() use the func"""

        df_new = pd.DataFrame(columns=["1-999", "1000-10000", "10001-20000", "20001-50000",
                                       "50001-400000", "400000-1000000", "1000001-"])
    
        df_new["1-999"] = df.apply(lambda s: s["1-999"], axis=0)
        df_new["1000-10000"] = \
            df.apply(lambda s: s["1000-5000"]+s['5001-10000'], axis=0)
        df_new["10001-20000"] = \
            df.apply(lambda s: s["10001-15000"]+s['15001-20000'], axis=0)
        df_new["20001-50000"] = \
            df.apply(lambda s: s["20001-30000"]+s["30001-40000"]+s["40001-50000"], axis=0)
        df_new["50001-400000"] = \
            df.apply(lambda s: s["50001-100000"]+s["100001-200000"]+s["200001-400000"], axis=0)
        df_new["400000-1000000"] = \
            df.apply(lambda s: s["400001-600000"]+s["600001-800000"]+s["800001-1000000"], axis=0)
        df_new["1000001-"] = df.apply(lambda s: s["1000001-"], axis=0)
        df_new.index.name = "stock"
    
        return df_new

    def transpose_set_eight_grade(self, df):
        """find_var_df_data() use the func"""

        df_new = pd.DataFrame(columns=["1-10000", "10001-20000", "20001-50000", "50001-100000",
                                       "100001-400000", "400000-600000", "600000-1000000", "1000001-"])

        df_new["1-10000"] = \
            df.apply(lambda s: s["1-999"] + s["1000-5000"] + s['5001-10000'], axis=0)
        df_new["10001-20000"] = \
            df.apply(lambda s: s["10001-15000"] + s['15001-20000'], axis=0)
        df_new["20001-50000"] = \
            df.apply(lambda s: s["20001-30000"] + s["30001-40000"] + s["40001-50000"], axis=0)
        df_new["50001-100000"] = \
            df.apply(lambda s: s["50001-100000"], axis=0)
        df_new["100001-400000"] = \
            df.apply(lambda s: s["100001-200000"] + s["200001-400000"], axis=0)

        df_new["400000-600000"] = \
            df.apply(lambda s: s["400001-600000"], axis=0)
        df_new["600000-1000000"] = \
            df.apply(lambda s: s["600001-800000"] + s["800001-1000000"], axis=0)

        df_new["1000001-"] = df.apply(lambda s: s["1000001-"], axis=0)
        df_new.index.name = "stock"

        return df_new

    # 不能完全處理成單一數值 因為其餘數值仍具有用途
    @timer
    def find_var_df_data(self, column_name, data_name, seven_grade=True):
        """use timeSeries_for_column() and transpose_set_seven_grade()"""
        import copy
        for date_from_list in self.df_nestedData.keys():
            self.var_df_data[date_from_list] = \
                self.timeSeries_for_column(self.df_nestedData[date_from_list], column_name)
            self.var_df_data[date_from_list] = self.var_df_data[date_from_list].iloc[:15]  # 刪除最後一行100％

            list_grade = ["1-999", "1000-5000", "5001-10000", "10001-15000", "15001-20000",
                          "20001-30000", "30001-40000", "40001-50000", "50001-100000", "100001-200000",
                          "200001-400000", "400001-600000", "600001-800000", "800001-1000000", "1000001-"]
            self.var_df_data[date_from_list]["分級"] = list_grade
            self.var_df_data[date_from_list] = self.var_df_data[date_from_list].set_index("分級")
            
            if seven_grade:
                self.var_df_data[date_from_list] = self.transpose_set_seven_grade(self.var_df_data[date_from_list])
            else:
                self.var_df_data[date_from_list] = self.var_df_data[date_from_list].transpose()
                self.var_df_data[date_from_list].index.name = "stock"
            
            print("success,{}".format(date_from_list))
        self.save_var_df_data(data_name)  # be careful. probably cover the file
        return copy.deepcopy(self.var_df_data)  # 當不只一個var要做處理時 若只用self.會被蓋掉

    @timer
    def find_var_df_ratio(self, var_df1: dict, var_df2: dict) -> dict:
        new_df = {}
        if len(list(var_df1.values())[0].columns) == len(list(var_df2.values())[0].columns):
            for date_from_list in var_df1.keys():
                new_df[date_from_list] = var_df1[date_from_list] / var_df2[date_from_list]
        else:
            print("the two forms can't match.")
        return new_df

    def find_var_delta(self, func, column_name_simple=False):  # 自由寫入func
        dict_func_delta = {}
        for x in range(0, len(self.var_df_data.keys()) - 1):
            early_date = list(self.var_df_data.keys())[x]
            late_date = list(self.var_df_data.keys())[x + 1]
            if column_name_simple:
                dict_func_delta[late_date] = \
                    self.var_df_data[late_date] - self.var_df_data[early_date]
            else:
                dict_func_delta[early_date+"~"+late_date] = \
                    self.var_df_data[late_date] - self.var_df_data[early_date]

        df_func_delta = \
            pd.DataFrame({k: df.apply(lambda s: func(s), axis=1) for k, df in dict_func_delta.items()}, dtype=float)
        # 此時func應引入完整的delta_df
        self.var_deltaData = df_func_delta
        return df_func_delta
