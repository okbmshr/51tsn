# -*- coding:utf-8 -*-
"""
機能概要：PVコードから倉先各社コードへの変換処理
作成日：2017/02/17
作成者：大久保将博
ver:1.0.0
内容：倉先変換処理
2017/02/17　NST　大久保　初期作成
"""

import common.log
import common.database
from abstractExecuter import Executor

class KrskConversion(Executor):
    """
    PVコードから倉先各社コードに変換
    """
    def __init__(self, todaydetail, tr_tablename, logger_body):
        try:
            self.program_id = __name__
            self.processingdate = todaydetail
            self.tr_tablename = tr_tablename
            print (__name__)
        except Exception as e:
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def convert(self, logger_body):
        """
        倉先変換処理実行
        """
        self.wktable_truncate(logger_body)
        self.krsk_unregistration_wktable_regist(logger_body)
        self.krskhist_unregistration_wktable_regist(logger_body)
        self.krskcode_conversion(logger_body)
        self.cngstatus_setting(logger_body)
        self.hand_selling_preference(logger_body)
        self.krsk_userecord_update(logger_body)
        self.krsk_unregistration_regist(logger_body)
        self.krskhist_unregistration_regist(logger_body)
        self.krsk_unregistration_wktable_delete(logger_body)
        self.krskhist_unregistration_wktable_delete(logger_body)
        self.krskcng_usetable_vacuum(logger_body)

    def wktable_truncate(self, logger_body):
        """
        倉先未登録分ワーク、倉先履歴分ワークテーブルをTruncateする
        """
        truncate_sql = "TRUNCATE TABLE tw_017_krskcngnoadd, tw_018_krskcngrrk;"
        try:
            common.database.Database.truncateDb(self, truncate_sql)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(truncate_sql)
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krsk_unregistration_wktable_regist(self, logger_body):
        """
        倉先未登録分ワークテーブル登録
        """
        placeholder = (self.program_id, self.program_id)
        sql = " ".join(("INSERT INTO tw_017_krskcngnoadd",
                            "(kais_cc,",
                            "kgyuskbt_cc,",
                            "shinsbt_ku,",
                            "krskpv_cc,",
                            "krskpvask_kn,",
                            "tkyukis_nb,",
                            "tkyusryu_nb,",
                            "edp_bn,",
                            "krskpv_kn,",
                            "datsbtuse_ku,",
                            "addjkyo_ku,",
                            "jogi_fl,",
                            "add_nbt,",
                            "addusr_id,",
                            "kusn_nbt,",
                            "kusnusr_id)",
                        "SELECT",
                            "TR.kais_cc,",
                            "TR.kgyuskbt_cc,",
                            "TR.shinsbt_ku,",
                            "TR.krskpv_cc,",
                            "TR.krskpvask_kn,",
                            "'19000101',",
                            "'99991231',",
                            "(SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD') || LPAD(nextval('nst15adm01.edp_bn_seq')::text,5,'0')),",
                            "TR.krskpv_kn,",
                            "TR.datsbt_ku,",
                            "'10',",
                            "'0',",
                            "current_timestamp,",
                            "%s ,",
                            "current_timestamp,",
                            "%s",
                        "FROM ",
                            self.tr_tablename + " TR",
                        "WHERE NOT EXISTS (",
                            "SELECT",
                                "'X'",
                            "FROM",
                                "tm_006_krskcng KRSK",
                            "WHERE",
                                "TR.kais_cc = KRSK.kais_cc",
                                "AND TR.kgyuskbt_cc = KRSK.kgyuskbt_cc",
                                "AND TR.shinsbt_ku = KRSK.shinsbt_ku",
                                "AND TR.krskpv_cc = KRSK.krskpv_cc",
                                "AND TR.krskpvask_kn = KRSK.krskpvask_kn",
                                "AND TR.shincng_st = '0')",
                        "GROUP BY",
                            "TR.kais_cc,",
                            "TR.kgyuskbt_cc,",
                            "TR.shinsbt_ku,",
                            "TR.krskpv_cc,",
                            "TR.krskpvask_kn,",
                            "TR.krskpv_kn,",
                            "TR.datsbt_ku;"))
        try:
            common.database.Database.insertDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))


    def krskhist_unregistration_wktable_regist(self, logger_body):
        """
        倉先履歴分ワークテーブル登録
        """
        placeholder = [self.program_id, self.program_id]
        sql = " ".join(("INSERT INTO tw_018_krskcngrrk",
                            "(kais_cc,"
                            "kgyuskbt_cc,",
                            "shinsbt_ku,",
                            "krskpv_cc,",
                            "krskpvask_kn,",
                            "tkyukis_nb,",
                            "tkyusryu_nb,",
                            "edp_bn,",
                            "krskpv_kn,",
                            "datsbtuse_ku,",
                            "addjkyo_ku,",
                            "jogi_fl,",
                            "add_nbt,",
                            "addusr_id,",
                            "kusn_nbt,",
                            "kusnusr_id)",
                        "SELECT",
                            "TR.kais_cc,",
                            "TR.kgyuskbt_cc,",
                            "TR.shinsbt_ku,",
                            "TR.krskpv_cc,",
                            "TR.krskpvask_kn,",
                            "MAX(KRSK.tkyusryu_nb)+1,",
                            "'99991231',",
                            "KRSK.edp_bn,",
                            "TR.krskpv_kn,",
                            "TR.datsbt_ku,",
                            "'10',",
                            "'0',",
                            "current_timestamp,",
                            "%s,",
                            "current_timestamp,",
                            "%s",
                        "FROM ",
                            self.tr_tablename + " TR, tm_006_krskcng KRSK",
                            "INNER JOIN (",
                                "SELECT",
                                    "kais_cc,",
                                    "kgyuskbt_cc,",
                                    "shinsbt_ku,",
                                    "krskpv_cc,",
                                    "krskpvask_kn,",
                                    "MAX(git_nt) max_git_nt",
                                "FROM",
                                    self.tr_tablename,
                                "GROUP BY",
                                    "kais_cc,",
                                    "kgyuskbt_cc,",
                                    "shinsbt_ku,",
                                    "krskpv_cc,",
                                    "krskpvask_kn) MAXT",
                            "ON (KRSK.kais_cc = MAXT.kais_cc",
                                "AND KRSK.kgyuskbt_cc = MAXT.kgyuskbt_cc",
                                "AND KRSK.shinsbt_ku = MAXT.shinsbt_ku",
                                "AND KRSK.krskpv_cc = MAXT.krskpv_cc",
                                "AND KRSK.krskpvask_kn = MAXT.krskpvask_kn)",
                        "WHERE",
                            "TR.kais_cc = KRSK.kais_cc",
                            "AND TR.kgyuskbt_cc = KRSK.kgyuskbt_cc",
                            "AND TR.shinsbt_ku = KRSK.shinsbt_ku",
                            "AND TR.krskpv_cc = KRSK.krskpv_cc",
                            "AND TR.krskpvask_kn = KRSK.krskpvask_kn",
                            "AND TR.shincng_st = '0'",
                            "AND TO_DATE((max_git_nt || '01'), 'YYYYMMDD') > KRSK.tkyukis_nb",
                            "AND TO_DATE((max_git_nt || '01'), 'YYYYMMDD') > KRSK.tkyusryu_nb",
                        "GROUP BY",
                            "TR.kais_cc,",
                            "TR.kgyuskbt_cc,",
                            "TR.shinsbt_ku,",
                            "TR.krskpv_cc,",
                            "TR.krskpvask_kn,",
                            "TR.krskpv_kn,",
                            "TR.datsbt_ku,",
                            "KRSK.edp_bn;"))
        try:
            common.database.Database.insertDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krskcode_conversion(self, logger_body):
        """
        倉先コード変換
        """
        placeholder = [self.program_id]
        sql = " ".join(("UPDATE " + self.tr_tablename + " TR",
                        "SET",
                            "krsktrhks_cc = KRSK.krsktrhks_cc,",
                            "krskknr_ku = KRSK.krskknr_ku,",
                            "krskcng_st = '0',",
                            "kusn_nbt = current_timestamp,",
                            "kusnusr_id = %s",
                        "FROM",
                            "tm_006_krskcng KRSK",
                        "WHERE",
                            "TR.kais_cc = KRSK.kais_cc",
                            "AND TR.kgyuskbt_cc = KRSK.kgyuskbt_cc",
                            "AND TR.shinsbt_ku = KRSK.shinsbt_ku",
                            "AND TR.krskpv_cc = KRSK.krskpv_cc",
                            "AND TR.krskpvask_kn = KRSK.krskpvask_kn",
                            "AND TO_DATE((git_nt || '01'), 'YYYYMMDD') >= KRSK.tkyukis_nb",
                            "AND TO_DATE((git_nt || '01'), 'YYYYMMDD') <= KRSK.tkyusryu_nb",
                            "AND TR.shincng_st = '0'",
                            "AND KRSK.jogi_fl = '0'",
                            "AND KRSK.addjkyo_ku = '60';"))
        try:
            common.database.Database.updateDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def cngstatus_setting(self, logger_body):
        """
        変換ステータス付与
        """
        placeholder = [self.program_id]
        sql = " ".join(("UPDATE " + self.tr_tablename + " TR",
                        "SET",
                            "krskcng_st = '1',",
                            "kusn_nbt = current_timestamp,",
                            "kusnusr_id = %s",
                        "FROM",
                            "tm_006_krskcng KRSK",
                        "WHERE",
                            "TR.shincng_st = '0'",
                            "AND TR.krskcng_st != '0';"))
        try:
            common.database.Database.updateDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def hand_selling_preference(self, logger_body):
        """
        手売変換
        """
        placeholder = [self.program_id]
        sql = " ".join(("UPDATE " + self.tr_tablename + " TR",
                        "SET",
                            "krskknr_ku = '0',",
                            "kusn_nbt = current_timestamp,",
                            "kusnusr_id = %s",
                        "FROM",
                            "(tm_010_kais KAIS",
                            "INNER JOIN",
                                "tm_003_kkstrhks TRHKSK",
                            "ON KAIS.kaisgrp_cc = TRHKSK.kaisgrp_cc)",
                            "INNER JOIN",
                                "tm_002_turtisu TUR",
                            "ON TRHKSK.kgut_cc = TUR.kgut_cc",
                        "WHERE",
                            "TR.kais_cc = KAIS.kais_cc",
                            "AND KAIS.kaisgrp_cc = TRHKSK.kaisgrp_cc",
                            "AND TRHKSK.kgut_cc = TUR.kgut_cc",
                            "AND TR.krsktrhks_cc = TRHKSK.trhks_cc",
                            "AND TR.kksskbt_cc = TUR.kksskbt_cc",
                            "AND TO_DATE((TR.git_nt || '01'), 'YYYYMMDD') >= TUR.tkyukis_nb",
                            "AND TO_DATE((TR.git_nt || '01'), 'YYYYMMDD') <= TUR.tkyusryu_nb",
                            "AND TR.krskcng_st = '0'",
                            "AND TR.krskknr_ku = '9';"))
        try:
            common.database.Database.updateDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krsk_userecord_update(self, logger_body):
        """
        倉先変換マスタ使用レコード更新
        """
        placeholder = [self.processingdate[0:6], self.program_id]
        sql = " ".join(("UPDATE tm_006_krskcng KRSK",
                        "SET",
                            "sisnuse_nt = %s,",
                            "datsbtuse_ku = CASE WHEN KRSK.datsbtuse_ku = TR.datsbt_ku THEN TR.datsbt_ku ELSE '3' END,",
                            "kusn_nbt = current_timestamp, kusnusr_id = %s",
                        "FROM",
                            self.tr_tablename + " TR",
                        "WHERE",
                            "TR.kais_cc = KRSK.kais_cc",
                            "AND TR.kgyuskbt_cc = KRSK.kgyuskbt_cc",
                            "AND TR.shinsbt_ku = KRSK.shinsbt_ku",
                            "AND TR.krskpv_cc = KRSK.krskpv_cc",
                            "AND TR.krskpvask_kn = KRSK.krskpvask_kn",
                            "AND TR.shincng_st = '0'",
                            "AND KRSK.jogi_fl = '0'",
                            "AND KRSK.addjkyo_ku = '60';"))
        try:
            common.database.Database.updateDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krsk_unregistration_regist(self, logger_body):
        """
        倉先未登録分登録
        """
        placeholder = [self.program_id, self.program_id]
        sql = " ".join(("INSERT INTO tm_006_krskcng",
                            "(kais_cc,",
                            "kgyuskbt_cc,",
                            "shinsbt_ku,",
                            "krskpv_cc,",
                            "krskpvask_kn,",
                            "tkyukis_nb,",
                            "tkyusryu_nb,",
                            "edp_bn,",
                            "krskpv_kn,",
                            "krsktrhks_cc,",
                            "krskknr_ku,",
                            "sisnuse_nt,",
                            "datsbtuse_ku,",
                            "addjkyo_ku,",
                            "jogi_fl,",
                            "sgyu_nbt,",
                            "sgyuusr_id,",
                            "kknn_nbt,",
                            "kknnusr_id,",
                            "add_nbt,",
                            "addusr_id,",
                            "kusn_nbt,",
                            "kusnusr_id)",
                        "SELECT",
                            "WK.kais_cc,",
                            "WK.kgyuskbt_cc,",
                            "WK.shinsbt_ku,",
                            "WK.krskpv_cc,",
                            "WK.krskpvask_kn,",
                            "WK.tkyukis_nb,",
                            "WK.tkyusryu_nb,",
                            "WK.edp_bn,",
                            "WK.krskpv_kn,",
                            "WK.krsktrhks_cc,",
                            "WK.krskknr_ku,",
                            "WK.sisnuse_nt,",
                            "WK.datsbtuse_ku,",
                            "WK.addjkyo_ku,",
                            "WK.jogi_fl,",
                            "WK.sgyu_nbt,",
                            "WK.sgyuusr_id,",
                            "WK.kknn_nbt,",
                            "WK.kknnusr_id,",
                            "CURRENT_TIMESTAMP,",
                            "%s,",
                            "CURRENT_TIMESTAMP,",
                            "%s",
                        "FROM",
                            "tw_017_krskcngnoadd WK;"))
        try:
            common.database.Database.insertDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krskhist_unregistration_regist(self, logger_body):
        """
        倉先履歴未登録登録
        """
        placeholder = [self.program_id, self.program_id]
        sql = " ".join(("INSERT INTO tm_006_krskcng",
                            "(kais_cc,",
                            "kgyuskbt_cc,",
                            "shinsbt_ku,",
                            "krskpv_cc,",
                            "krskpvask_kn,",
                            "tkyukis_nb,",
                            "tkyusryu_nb,",
                            "edp_bn,",
                            "krskpv_kn,",
                            "krsktrhks_cc,",
                            "krskknr_ku,",
                            "sisnuse_nt,",
                            "datsbtuse_ku,",
                            "addjkyo_ku,",
                            "jogi_fl,",
                            "sgyu_nbt,",
                            "sgyuusr_id,",
                            "kknn_nbt,",
                            "kknnusr_id,",
                            "add_nbt,",
                            "addusr_id,",
                            "kusn_nbt,",
                            "kusnusr_id)",
                        "SELECT",
                            "WK.kais_cc,",
                            "WK.kgyuskbt_cc,",
                            "WK.shinsbt_ku,",
                            "WK.krskpv_cc,",
                            "WK.krskpvask_kn,",
                            "WK.tkyukis_nb,",
                            "WK.tkyusryu_nb,",
                            "WK.edp_bn,",
                            "WK.krskpv_kn,",
                            "WK.krsktrhks_cc,",
                            "WK.krskknr_ku,",
                            "WK.sisnuse_nt,",
                            "WK.datsbtuse_ku,",
                            "WK.addjkyo_ku,",
                            "WK.jogi_fl,",
                            "WK.sgyu_nbt,",
                            "WK.sgyuusr_id,",
                            "WK.kknn_nbt,",
                            "WK.kknnusr_id,",
                            "CURRENT_TIMESTAMP,",
                            "%s,",
                            "CURRENT_TIMESTAMP,",
                            "%s",
                        "FROM",
                            "tw_018_krskcngrrk WK;"))
        try:
            common.database.Database.insertDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krsk_unregistration_wktable_delete(self, logger_body):
        """
        倉先未登録分ワークテーブル削除
        """
        sql = "DELETE FROM tw_017_krskcngnoadd"
        try:
            common.database.Database.deleteDb(self, sql)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql)
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krskhist_unregistration_wktable_delete(self, logger_body):
        """
        倉先履歴分ワークテーブル削除
        """
        sql = "DELETE FROM tw_018_krskcngrrk"
        try:
            common.database.Database.deleteDb(self, sql)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql)
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krskcng_usetable_vacuum(self, logger_body):
        """
        トランザクションテーブルと倉先変換マスタテーブルをVACUUMする
        """
        vacuum_tr_table = "VACUUM ANALYZE " + self.tr_tablename
        vacuum_krskcng = "VACUUM ANALYZE tm_006_krskcng"
        try:
            common.database.Database.vacuumDb(self, vacuum_tr_table)
            common.database.Database.vacuumDb(self, vacuum_krskcng)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(vacuum_tr_table)
            logger_body.info(vacuum_krskcng)
            logger_body.info(str(type(e)))
            logger_body.info(str(e))
