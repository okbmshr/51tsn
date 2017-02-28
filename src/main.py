# -*- coding:utf-8 -*-
"""
 機能概要：メイン処理
 作成日：2017/02/13
 作成者：大久保将博
 ver:1.0.0
 内容：倉元変換処理を呼ぶ
 2017/02/13　NST　大久保　初期作成
"""

import os
import sys
import datetime
import json
from abstractExecuter import Executor
import common.log
import common.database

def execute(processing_name, todaydetail, tr_tablename, logger_body):
    # JSON設定ファイルの取得
    jsonFile = open('../conf/setting.json', 'r')
    jsonData = json.load(jsonFile)
    # 設定情報ごとの判定
    elementList = (jsonData[processing_name])
    for element in elementList:
        # クラスの取得
        execClassName = element["class"]
        # モジュールの取得
        myClassModule = __import__(element["module"], fromlist=["*"])
        # クラスの取得
        myClass = getattr(myClassModule, execClassName)
        # クラスが基底クラスを継承している場合のみ実行
        if issubclass(myClass, Executor):
            myClassObj = myClass(todaydetail, tr_tablename, logger_body)
            common.database.Database.connectDb(myClassObj)
            myClassObj.convert(logger_body)
    #DB接続終了
    common.database.Database.closeDb(myClassObj)

if __name__ == '__main__':
    """
    Main処理
    """
    #ESBから処理名とトランザクションテーブル名を受け取る
    esb_para = sys.argv
    processing_name = esb_para[1]
    tr_tablename = esb_para[2]

    #日付取得
    todaydetail = (datetime.datetime.today()).strftime("%Y%m%d")

    #ログヘッダー出力
    logger_header = common.log.Log.header_create(todaydetail)
    common.log.Log.header(logger_header, processing_name, tr_tablename)
    #ログボディ部のロガー作成
    logger_body = common.log.Log.body_create(todaydetail)

    #jsonファイルからクラスを呼び出す
    execute(processing_name, todaydetail, tr_tablename, logger_body)

    #ログフッター出力
    logger_footer = common.log.Log.footer_create(todaydetail)
    common.log.Log.footer(logger_footer, processing_name)

    print ('ALL CLEAR')
