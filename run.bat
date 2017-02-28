@echo off
REM main.pyのパスを記述する
cd C:\Users\masahiro_okubo\AppData\Local\Programs\Python\pythontest\nst15\src

SET processing_name="Krskcng"
SET tr_tablename="tt_001_slejisk_ajimisi_finet"

REM 実行時にはトランザクション名、実行処理名を渡す
python main.py %* %processing_name% %tr_tablename%

pause
