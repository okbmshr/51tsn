# -*- coding:utf-8 -*-
"""
機能概要：ログ消去処理
作成日：2017/02/27
作成者：大久保将博
ver:1.0.0
内容：ログ消去
2017/02/27　NST　大久保　初期作成
"""
import os
import time
from operator import itemgetter
import glob

if __name__ == '__main__':
    """
    nst15/logのlogファイルを削除する
    """
    path = os.getcwd()
    filenames = glob.glob(os.path.join(path,'../log/','*.log'))

    # ファイル名、サイズ、日付からなるリストを作る
    file_lst = []
    for myfile in filenames:
        myfile = os.path.join(path,'log',myfile)
        file_lst.append([myfile,os.stat(myfile).st_size,time.ctime(os.stat(myfile).st_mtime),os.path.getmtime(myfile)])

    # 日付の古い順に並び替える
    lst = sorted(file_lst,key=itemgetter(3), reverse = True)
    print(lst)

    # 最大数以上のファイルを古いものから削除
    max_cnt = 3
    for n,myfile in enumerate(lst):
        if n >= max_cnt:
            print (n, myfile[0],myfile[2])
            os.remove(myfile[0])

    print ('ALL CLEAR')
