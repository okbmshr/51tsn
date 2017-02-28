# -*- coding:utf-8 -*-
"""
 機能概要：ログ機能
 作成日：2017/02/13
 作成者：大久保
 ver:1.0.0
 内容：ログ出力
 2017/02/13　NST　大久保　初期作成
"""

import logging
import logging.config
import codecs

class Log(object):
    """ログ出力用クラス"""

    def header_create(todaydetail):
        logger = logging.getLogger('header_logger')
        logger.setLevel(logging.DEBUG)
        handler = logging.handlers.RotatingFileHandler(filename = '../log/'+ todaydetail + '.log', mode = 'a+', maxBytes=0, backupCount=5, encoding = "UTF-8")
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)-15s :%(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

    def body_create(todaydetail):
        """ログボディ部設定ファイル読み込み"""
        logger = logging.getLogger('body_logger')
        logger.setLevel(logging.DEBUG)
        handler = logging.handlers.RotatingFileHandler(filename = '../log/'+ todaydetail + '.log', mode = 'a+', maxBytes=0, backupCount=5, encoding = "UTF-8")
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)-15s :%(lineno)-4d :%(funcName)-20s :%(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger


    def footer_create(todaydetail):
        """ログフッター部設定ファイル読み込み"""
        logger = logging.getLogger('footer_logger')
        logger.setLevel(logging.DEBUG)
        handler = logging.handlers.RotatingFileHandler(filename = '../log/'+ todaydetail + '.log', mode = 'a+', maxBytes=0, backupCount=5, encoding = "UTF-8")
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)-15s :%(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger


    def header(logger, processing_name, tr_tablename):
        """ログヘッダー部出力"""
        logger.debug(processing_name + " :" + tr_tablename)

    def body_notset(logger, name, tr_tablename):
        """ログボディ部出力(notset)"""
        logger.notset(name + ' - OK')


    def body_debug(logger, name):
        """ログボディ部出力(debug)"""
        logger.debug(name + ' - OK')


    def body_info(logger, name):
        """ログボディ部出力(info)"""
        logger.info(name + ' - OK')


    def body_warning(logger, name):
        """ログボディ部出力(warning)"""
        logger.warning(name + ' - OK')


    def body_error(logger, name):
        """ログボディ部出力(error)"""
        logger.error(name + ' - OK')


    def body_critical(logger, name):
        """ログボディ部出力(critical)"""
        logger.critical(name + ' - OK')


    def footer(logger, processing_name):
        """ログフッター部出力"""
        logger.debug(processing_name + ' :' + '50件処理')
