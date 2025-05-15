from separated_scripts.binance_history_transfers_downloader.data_downloaders.usd_price_downloader import USDpricer
from app.managers.sql_manager import SqlManager
import time
import pandas as pd


from pyodbc import DatabaseError
import logging
from support.slacker.kancl_slacker import KanclSlacker


class DatabaseUsdPriceFiller:
	def __init__(self):
		self.SqlManager = SqlManager()

	def get_missing_ids(self):
		q = """SELECT [TransferId]
               FROM [Fantoci].[dbo].[ExternalTransferHistory]
               WHERE [UsdAmount] IS NULL
               ORDER BY [Datetime]"""
		df1 = pd.read_sql(q, self.SqlManager.db_zdar.conn)
		return df1['TransferId'].tolist()

	def download_row(self, transfer_id, attemps = 3):
		query = f"""SELECT [TransferId], [Datetime], [Identita], [Amount], [Asset]
                FROM [Fantoci].[dbo].[ExternalTransferHistory]
                WHERE [TransferId] = {transfer_id}"""

		for attempt in range(attemps):
			try:
				df = pd.read_sql(query, self.SqlManager.db_zdar.conn)
				database_time = df['Datetime'].iloc[0]
				binance_time = int(database_time.timestamp() * 1000)
				print(df)
				return df, binance_time

			except DatabaseError:
				self.SqlManager.close_connection()
				time.sleep(20)
				if attempt == attemps -1:
					raise DatabaseError

	def get_usd_amount(self, df, binance_time):
		row = df.iloc[0]
		usd_price = USDpricer().add_usd_amount(binance_time, row['Asset'])
		usd_amount = round(float(row['Amount']) * usd_price, 4) if usd_price is not None else None
		return df, usd_amount

	def insert_to_db(self, df, usd_amount):
		if usd_amount is None:
			print("USD amount is None, skipping update.")
			return
		row = df.iloc[0]
		query2 = f"""UPDATE [Fantoci].[dbo].[ExternalTransferHistory]
					SET [UsdAmount] = {usd_amount}
					WHERE [TransferId] = {row['TransferId']}
					AND [Datetime] = '{row['Datetime']}'
					AND [Identita] = '{row['Identita']}'"""
		x = self.SqlManager.db_zdar.execute(query2, commit=True)
		print(x)


if __name__ == '__main__':
	x = DatabaseUsdPriceFiller()
	missing_ids = x.get_missing_ids()
	for transfer_id in missing_ids:
		df, db_time = x.download_row(transfer_id)
		df, usd_amount = x.get_usd_amount(df, db_time)
		x.insert_to_db(df, usd_amount)
		time.sleep(5)


