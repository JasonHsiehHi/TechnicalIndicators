# 裝飾句
import time


def timer(func):
    def wraper(*args, **kwargs):
        before = time.time()
        result = func(*args, **kwargs)  # 若func()最後沒有return 則result None 不會發生錯誤
        print("func:{} finished.".format(func.__name__))
        after = time.time()
        print("elapsed: {}", format(after-before))
        return result
    return wraper
