# 為使package可被執行 python -m package_name

# python package 可直接執行package 會調用裡面的__main__.py
# python -m package 會先import package中的module 再調用裡面的__main__.py

# python -m package_name 與 python package_name之差異
# 前者會先執行__init__.py 後者則直接執行__main__.py
# 但再__main__.py中__package__變數會為None 故直接執行後者會發生問題 無法找到package

# 基於上述2條 應將def main():寫在__int__.py 而後再用__main__.py做執行
# 先引用__int__.py之後 就可以在執行__main__.py時找到package

import os
import sys
import package  # 同層可直接引用package資料夾

# __file__ 為此檔的絕對路徑 "/Users/jason/Desktop/coding_course/package/__main__.py"
# __name__ 為module名稱 若為目前所在位置 則為'__main__'
# __package__ 則為最上層資料夾名稱


if not __package__:  # 用sys.path進行手動添加module
    path = os.path.join(os.path.dirname(__file__), os.pardir)  # os.pardir就是".." 回到上一層
    sys.path.insert(0, path)  # sys.path.append 和 sys.path.insert 兩種方法進行添加module
    # sys.path.insert 的 0 為搜尋順位

# 通常sys.path 用於添加不在同一資料夾的module
package.main()
