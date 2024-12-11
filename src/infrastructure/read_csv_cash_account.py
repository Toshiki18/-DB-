# Libraries
import polars as pl
from logging import getLogger

class ReadCsvCashAcount():
    """
        ゆうちょ銀行からダウンロードしたcsvを加工し、必要な列を抽出する。
    """
    def __init__(self) -> None:
        self.logger = getLogger(self.__class__.__name__)

    def read_csv(self, file_path: str, target_ym: str):
        """
            ゆうちょ銀行のcsvはヘッダーが8行目にあるため、skip_rows=7を設定する
            Args:
                file_path: 処理するデータのパス
                target_ym: 処理するデータの対象年月
            
            Returns:
                Polars_DataFrame: 処理するデータのDataFrame    
        """
        csv_path = file_path

        try:
            _df_csv = pl.read_csv(file_path + f"/{target_ym}" + "/cash_record.csv", skip_rows=7, encoding="utf-16")
            self.logger.info("処理するデータの読み込み")
        except Exception as e:
            self.logger.error(f"読み込みエラーが発生しました: {e}")
            raise e
        
        return _df_csv