from app.managers.sql_manager import SqlManager
import time
import pandas as pd
import datetime
from pyodbc import DatabaseError
import logging
from support.slacker.kancl_slacker import KanclSlacker
from separated_scripts.binance_history_transfers_downloader.csv_procesor import Csv
logger = logging.getLogger(__name__)
SQL = SqlManager()


class Database:
	def __init__(self, identity):
		self.identity = identity

	def get_last_sql_saving(self, attemps=5):
		print('get_last_sql_saving function')
		return datetime.datetime.utcnow()-datetime.timedelta(days=30)

		q = f"""SELECT TOP (1) [Datetime]
				FROM [Fantoci].[dbo].[ExternalTransferHistory]
				WHERE [Identita] = '{self.identity}'
				ORDER BY [TransferId] DESC"""
		# for attempt in range(attemps):
		# 	try:
		# 		logger.info(f'attempt number: {attempt} to get last saved date from SQL')
		# 		df = pd.read_sql(q, SQL.db_zdar.conn)
		# 		database_time = df.iloc[0][0].value * 1e-9
		# 		modified_time = datetime.datetime.utcfromtimestamp(database_time)
		# 		x = datetime.datetime.min if df.empty else modified_time
		# 		logger.info(f'get_last_sql_saving return {x}')
		# 		return x
		#
		# 	except DatabaseError:
		# 		logger.info(f'attempt to load data was not successful')
		# 		SQL.close_connection()
		# 		time.sleep(20)
		# 		if attempt == attemps -1:
		# 			raise DatabaseError

	def insert_into_sql(self, models, downloaded_time):
		attemps = 5
		merge_query = """
			MERGE INTO [Fantoci].[dbo].[ExternalTransferHistory] AS target 
			USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (Datetime, Exchange, Identita, Amount, Asset, 
				Fee, Direction, Type, ExchangeTransferId, Status, RawData, UsdAmount) 
			ON ABS(DATEDIFF_BIG(millisecond, target.Datetime, source.Datetime)) <= 5000 AND 
				target.Identita = source.Identita AND 
				target.Amount = source.Amount AND 
				target.ExchangeTransferId = source.ExchangeTransferId
			WHEN MATCHED THEN
				UPDATE SET
            target.Exchange = source.Exchange,
            target.Fee = source.Fee,
            target.Direction = source.Direction,
            target.Type = source.Type,
            target.Status = source.Status,
            target.RawData = source.RawData,
            target.UsdAmount = source.UsdAmount
            WHEN NOT MATCHED THEN
            INSERT (Datetime, Exchange, Identita, Amount, Asset, Fee, Direction, Type, ExchangeTransferId, Status, RawData, UsdAmount)
            VALUES (source.Datetime, source.Exchange, source.Identita, source.Amount, source.Asset, source.Fee, source.Direction, source.Type, source.ExchangeTransferId, source.Status, source.RawData, source.usdAmount);
    """
		records = []
		for model in models:
			record = (model.time, model.exchange, model.identita, model.amount, model.asset, model.totalFee, model.direction, model.transferType, model.address, model.status, model.raw, model.usdAmount)
			records.append(record)
		for attempt in range(attemps):
			try:
				SQL.db_zdar.executemany(merge_query, records)

				SQL.db_zdar.conn.commit()
				logger.info(f'data for identity {self.identity} saved in to SQL, last saved date is: {downloaded_time}')

				print(f'data for identity {self.identity} saved in to SQL, last saved date is: {downloaded_time}')

				Csv(last_saved_date=downloaded_time, identity=self.identity)
				break

			except DatabaseError as e:
				logger.error(f'Attempt to save data was not successful. Error: {e}. Records: {records}')
				KanclSlacker().send_error(f"[Binance_history_transfer_downloader.Database] Attempt to save data was not "
											f"successful. Error: {e}. Records: {records}'")
				print(f'Attempt to save data was not successful. Error: {e}. Records: {records}')
				SQL.close_connection()
				time.sleep(20)
				if attempt == attemps -1:
					raise DatabaseError