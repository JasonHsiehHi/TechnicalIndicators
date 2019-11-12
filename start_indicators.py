import technical_indicators as TI
from package import data_saver
from package import process_step1 as p1
import sys


def main():
    # python start_indicators.py [stock_number]
    # python start_indicators.py 2330
    stock_number = str(sys.argv[1])
    sd = data_saver.stock_data_saver()
    sd.crawl_from_time_to_now_and_save(90)
    td = data_saver.trading_data_saver()
    td.crawl_from_time_to_now_and_save(90)
    d1_sd = p1.data_process_step1(sd.data)
    d1_td = p1.data_process_step1(td.data)
    i = TI.information_table1(d1_sd, d1_td)
    i.information_for_specific_stock(stock_number)


if __name__ == '__main__':
    main()
