# -*- coding:utf-8 -*-
"""
機能概要：DB接続処理
作成日：2017/02/13
作成者：大久保
ver:1.0.0
内容：DB処理
2017/02/13　NST　大久保　初期作成
"""

import psycopg2
import json

class Database:
    """database関連処理クラス"""

    def __init__(self):
        print (__name__)

    def connectDb(self):
        """DB接続"""
        # DB設定情報の取得
        jsonFile = open('../conf/dbconnect.json', 'r')
        jsonData = json.load(jsonFile)
        element = (jsonData["DBconnect"])
        db_host = element["host"]
        db_port = element["port"]
        db_database = element["database"]
        db_user = element["user"]
        db_password = element["password"]

        self.connector = psycopg2.connect(host = db_host, port = db_port, database = db_database, user = db_user, password = db_password)
        self.cursor = self.connector.cursor()

    def selectDb(self, sql):
        """select実行"""
        self.cursor.execute(sql)
        self.result = self.cursor.fetchall()
        return (self.result)

    def updateDb(self, sql, placeholder):
        """update実行"""
        self.cursor.execute(sql, placeholder)
        #self.result = unicode(self.cursor.fetchall(), 'utf-8')
        #return (len(self.result))

    def insertDb(self, sql, placeholder):
        """insert実行"""
        self.cursor.execute(sql, placeholder)

    def deleteDb(self, sql):
        """delete実行"""
        self.cursor.execute(sql)

    def truncateDb(self, sql):
        """truncate実行"""
        self.cursor.execute(sql)

    def vacuumDb(self, sql):
        """truncate実行"""
        self.connector.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.vacuum_cursor = self.connector.cursor()
        self.vacuum_cursor.execute(sql)
        self.connector.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

    def commitDb(self):
        """commit実行"""
        self.connector.commit()

    def closeDb(self):
        """DB切断"""
        #self.connector.commit()
        self.cursor.close()
        self.connector.close()
