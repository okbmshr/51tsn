# -*- coding:utf-8 -*-
"""
機能概要：PVコードから倉元各社コードへの変換処理
作成日：2017/02/13
作成者：大久保将博
ver:1.0.0
内容：倉元変換処理
2017/02/13　NST　大久保　初期作成
"""

import common.log
import common.database
from abstractExecuter import Executor

class KrmtConversion(Executor):
    """
    PVコードから倉元各社コードに変換
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
        倉元変換処理実行
        """
        self.wktable_truncate(logger_body)
        self.krmt_unregistration_wktable_regist(logger_body)
        self.krmthist_unregistration_wktable_regist(logger_body)
        self.krmtcode_conversion(logger_body)
        self.cngstatus_setting(logger_body)
        self.krmt_userecord_update(logger_body)
        self.krmt_unregistration_regist(logger_body)
        self.krmthist_unregistration_regist(logger_body)
        self.krmt_unregistration_wktable_delete(logger_body)
        self.krmthist_unregistration_wktable_delete(logger_body)
        self.krmtcng_usetable_vacuum(logger_body)

    def wktable_truncate(self, logger_body):
        """
        倉先未登録分ワーク、倉先履歴分ワークテーブルをTruncateする
        """
        truncate_sql = "TRUNCATE TABLE tw_015_krmtcngnoadd, tw_016_krmtcngrrk"
        try:
            common.database.Database.truncateDb(self, truncate_sql)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(truncate_sql)
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krmt_unregistration_wktable_regist(self, logger_body):
        """
        倉元未登録分ワークテーブル登録
        """
        placeholder = [self.program_id, self.program_id]
        sql = " ".join(("INSERT INTO tw_015_krmtcngnoadd",
                            "(kais_cc,",
                            "kgyuskbt_cc,",
                            "shinsbt_ku,",
                            "krmtpv_cc,",
                            "krmtpvask_kn,",
                            "tkyukis_nb,",
                            "tkyusryu_nb,",
                            "edp_bn,",
                            "krmtpv_kn,",
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
                            "TR.krmtpv_cc,",
                            "TR.krmtpvask_kn,",
                            "'19000101',",
                            "'99991231',",
                            "(SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD') || LPAD(nextval('nst15adm01.edp_bn_seq')::text,5,'0')),",
                            "TR.krmtpv_kn,",
                            "TR.datsbt_ku,",
                            "'10',",
                            "'0',",
                            "current_timestamp,",
                            "%s,",
                            "current_timestamp,",
                            "%s",
                        "FROM",
                            self.tr_tablename + " TR",
                        "WHERE NOT EXISTS (",
                            "SELECT",
                                "'X'",
                            "FROM",
                                "tm_005_krmtcng KRMT",
                            "WHERE",
                                "TR.kais_cc = KRMT.kais_cc",
                                "AND TR.kgyuskbt_cc = KRMT.kgyuskbt_cc",
                                "AND TR.shinsbt_ku = KRMT.shinsbt_ku",
                                "AND TR.krmtpv_cc = KRMT.krmtpv_cc",
                                "AND TR.krmtpvask_kn = KRMT.krmtpvask_kn",
                                "AND TR.shincng_st = '0')",
                        "GROUP BY",
                            "TR.kais_cc,",
                            "TR.kgyuskbt_cc,",
                            "TR.shinsbt_ku,",
                            "TR.krmtpv_cc,",
                            "TR.krmtpvask_kn,",
                            "TR.krmtpv_kn,",
                            "TR.datsbt_ku;"))
        try:
            common.database.Database.insertDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krmthist_unregistration_wktable_regist(self, logger_body):
        """
        倉元履歴分ワークテーブル登録
        """
        placeholder = [self.program_id, self.program_id]
        sql = " ".join(("INSERT INTO tw_016_krmtcngrrk",
                            "(kais_cc,",
                            "kgyuskbt_cc,",
                            "shinsbt_ku,",
                            "krmtpv_cc,",
                            "krmtpvask_kn,",
                            "tkyukis_nb,",
                            "tkyusryu_nb,",
                            "edp_bn,",
                            "krmtpv_kn,",
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
                            "TR.krmtpv_cc,",
                            "TR.krmtpvask_kn,",
                            "MAX(KRMT.tkyusryu_nb)+1,",
                            "'99991231',",
                            "KRMT.edp_bn,",
                            "TR.krmtpv_kn,",
                            "TR.datsbt_ku,",
                            "'10',",
                            "'0',",
                            "current_timestamp,",
                            "%s,",
                            "current_timestamp,",
                            "%s",
                        "FROM",
                            self.tr_tablename + " TR, tm_005_krmtcng KRMT",
                            "INNER JOIN (",
                                "SELECT",
                                    "kais_cc,",
                                    "kgyuskbt_cc,",
                                    "shinsbt_ku,",
                                    "krmtpv_cc,",
                                    "krmtpvask_kn,",
                                    "MAX(git_nt) max_git_nt",
                                "FROM",
                                    self.tr_tablename,
                                "GROUP BY",
                                    "kais_cc,",
                                    "kgyuskbt_cc,",
                                    "shinsbt_ku,",
                                    "krmtpv_cc,",
                                    "krmtpvask_kn) MAXT",
                            "ON (KRMT.kais_cc = MAXT.kais_cc",
                                "AND KRMT.kgyuskbt_cc = MAXT.kgyuskbt_cc",
                                "AND KRMT.shinsbt_ku = MAXT.shinsbt_ku",
                                "AND KRMT.krmtpv_cc = MAXT.krmtpv_cc",
                                "AND KRMT.krmtpvask_kn = MAXT.krmtpvask_kn)",
                        "WHERE",
                            "TR.kais_cc = KRMT.kais_cc",
                            "AND TR.kgyuskbt_cc = KRMT.kgyuskbt_cc",
                            "AND TR.shinsbt_ku = KRMT.shinsbt_ku",
                            "AND TR.krmtpv_cc = KRMT.krmtpv_cc",
                            "AND TR.krmtpvask_kn = KRMT.krmtpvask_kn",
                            "AND TR.shincng_st = '0'",
                            "AND TO_DATE((max_git_nt || '01'), 'YYYYMMDD') > KRMT.tkyukis_nb",
                            "AND TO_DATE((max_git_nt || '01'), 'YYYYMMDD') > KRMT.tkyusryu_nb",
                        "GROUP BY",
                            "TR.kais_cc,",
                            "TR.kgyuskbt_cc,",
                            "TR.shinsbt_ku,",
                            "TR.krmtpv_cc,",
                            "TR.krmtpvask_kn,",
                            "TR.krmtpv_kn,",
                            "TR.datsbt_ku,",
                            "KRMT.edp_bn;"))
        try:
            common.database.Database.insertDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krmtcode_conversion(self, logger_body):
        """
        倉元コード変換
        """
        placeholder = [self.program_id]
        sql = " ".join(("UPDATE " + self.tr_tablename + " TR ",
                        "SET",
                            "krmttrhks_cc = KRMT.krmttrhks_cc,",
                            "krmtknr_ku = KRMT.krmtknr_ku,",
                            "krmtcng_st = '0',",
                            "kusn_nbt = current_timestamp,",
                            "kusnusr_id = %s",
                        "FROM",
                            "tm_005_krmtcng KRMT",
                        "WHERE",
                            "TR.kais_cc = KRMT.kais_cc",
                            "AND TR.kgyuskbt_cc = KRMT.kgyuskbt_cc",
                            "AND TR.shinsbt_ku = KRMT.shinsbt_ku",
                            "AND TR.krmtpv_cc = KRMT.krmtpv_cc",
                            "AND TR.krmtpvask_kn = KRMT.krmtpvask_kn",
                            "AND TO_DATE((git_nt || '01'), 'YYYYMMDD') >= KRMT.tkyukis_nb",
                            "AND TO_DATE((git_nt || '01'), 'YYYYMMDD') <= KRMT.tkyusryu_nb",
                            "AND TR.shincng_st = '0'",
                            "AND KRMT.jogi_fl = '0'",
                            "AND KRMT.addjkyo_ku = '60';"))
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
                            "krmtcng_st = '1',",
                            "kusn_nbt = current_timestamp,",
                            "kusnusr_id = %s",
                        "FROM",
                            "tm_005_krmtcng KRMT",
                        "WHERE",
                            "TR.shincng_st = '0'",
                            "AND TR.krmtcng_st != '0';"))
        try:
            common.database.Database.updateDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krmt_userecord_update(self, logger_body):
        """
        倉元変換マスタ使用レコード更新
        """
        placeholder = [self.processingdate[0:6], self.program_id]
        sql = " ".join(("UPDATE tm_005_krmtcng KRMT",
                        "SET",
                            "sisnuse_nt = %s,",
                            "datsbtuse_ku = CASE WHEN KRMT.datsbtuse_ku = TR.datsbt_ku THEN TR.datsbt_ku ELSE '3' END,",
                            "kusn_nbt = current_timestamp,",
                            "kusnusr_id = %s",
                        "FROM ",
                            self.tr_tablename + " TR",
                        "WHERE",
                            "TR.kais_cc = KRMT.kais_cc",
                            "AND TR.kgyuskbt_cc = KRMT.kgyuskbt_cc",
                            "AND TR.shinsbt_ku = KRMT.shinsbt_ku",
                            "AND TR.krmtpv_cc = KRMT.krmtpv_cc",
                            "AND TR.krmtpvask_kn = KRMT.krmtpvask_kn",
                            "AND TR.shincng_st = '0'",
                            "AND KRMT.jogi_fl = '0'",
                            "AND KRMT.addjkyo_ku = '60';"))
        try:
            common.database.Database.updateDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krmt_unregistration_regist(self, logger_body):
        """
        倉元未登録分登録
        """
        placeholder = [self.program_id, self.program_id]
        sql = " ".join(("INSERT INTO tm_005_krmtcng",
                            "(kais_cc,",
                            "kgyuskbt_cc,",
                            "shinsbt_ku,",
                            "krmtpv_cc,",
                            "krmtpvask_kn,",
                            "tkyukis_nb,",
                            "tkyusryu_nb,",
                            "edp_bn,",
                            "krmtpv_kn,",
                            "krmttrhks_cc,",
                            "krmtknr_ku,",
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
                            "WK.krmtpv_cc,",
                            "WK.krmtpvask_kn,",
                            "WK.tkyukis_nb,",
                            "WK.tkyusryu_nb,",
                            "WK.edp_bn,",
                            "WK.krmtpv_kn,",
                            "WK.krmttrhks_cc,",
                            "WK.krmtknr_ku,",
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
                            "tw_015_krmtcngnoadd WK;"))
        try:
            common.database.Database.insertDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krmthist_unregistration_regist(self, logger_body):
        """
        倉元履歴未登録登録
        """
        placeholder = [self.program_id, self.program_id]
        sql = " ".join(("INSERT INTO tm_005_krmtcng",
                            "(kais_cc,",
                            "kgyuskbt_cc,",
                            "shinsbt_ku,",
                            "krmtpv_cc,",
                            "krmtpvask_kn,",
                            "tkyukis_nb,",
                            "tkyusryu_nb,",
                            "edp_bn,",
                            "krmtpv_kn,",
                            "krmttrhks_cc,",
                            "krmtknr_ku,",
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
                            "WK.krmtpv_cc,",
                            "WK.krmtpvask_kn,",
                            "WK.tkyukis_nb,",
                            "WK.tkyusryu_nb,",
                            "WK.edp_bn,",
                            "WK.krmtpv_kn,",
                            "WK.krmttrhks_cc,",
                            "WK.krmtknr_ku,",
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
                            "tw_016_krmtcngrrk WK;"))
        try:
            common.database.Database.insertDb(self, sql, placeholder)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql % tuple(placeholder))
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krmt_unregistration_wktable_delete(self, logger_body):
        """
        倉元未登録分ワークテーブル削除
        """
        sql = "DELETE FROM tw_015_krmtcngnoadd"
        try:
            common.database.Database.deleteDb(self, sql)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql)
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krmthist_unregistration_wktable_delete(self, logger_body):
        """
        倉元履歴分ワークテーブル削除
        """
        sql = "DELETE FROM tw_016_krmtcngrrk"
        try:
            common.database.Database.deleteDb(self, sql)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(sql)
            logger_body.info(str(type(e)))
            logger_body.info(str(e))

    def krmtcng_usetable_vacuum(self, logger_body):
        """
        トランザクションテーブルと倉先変換マスタテーブルをVACUUMする
        """
        vacuum_tr_table = "VACUUM ANALYZE " + self.tr_tablename
        vacuum_krmtcng = "VACUUM ANALYZE tm_005_krmtcng"
        try:
            common.database.Database.vacuumDb(self, vacuum_tr_table)
            common.database.Database.vacuumDb(self, vacuum_krmtcng)
            common.database.Database.commitDb(self)
        except Exception as e:
            logger_body.info(vacuum_tr_table)
            logger_body.info(vacuum_krskcng)
            logger_body.info(str(type(e)))
            logger_body.info(str(e))
