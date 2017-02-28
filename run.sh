#!/bin/sh

#ESBから受け取ったパラメータを代入
processing_name="Krskcng"
tr_tablename="tt_001_slejisk_ajimisi_finet"

cd src/

#実行時にはトランザクション名、実行処理名を渡す
python main.py processing_name tr_tablename &
