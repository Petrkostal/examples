import logging
from csv_procesor import Csv
from data_downloaders import *
from separated_scripts.binance_history_transfers_downloader.data_downloaders.card_withdrawal import CardWithdrawal
from separated_scripts.binance_history_transfers_downloader.db.database import Database
from transfer_case import Case
import datetime

logger = logging.getLogger(__name__)


class Factory:

	def __init__(self, identity: str):
		self.df = None
		self.df_name = ""
		self.case = Case(identity)
		self.identity = self.case.identity

		self.target_secret = self.case.target_secret

	def get_all(self):
		transaction_type_classes = [
					PayHistory,
					FiatDeposit,
					FiatWithdrawal,
					CoinWithdrawal,
					CoinDeposit,
					CardWithdrawal,
					CardDeposit
					]
		last_saved_datetime = datetime.datetime.min
		transaction_data = []

		for transaction_type_class in transaction_type_classes:
			transaction_type = transaction_type_class(self.case)
			transaction_type.load_data()
			transaction_data += transaction_type.data
			if transaction_type.last_saved_datetime > last_saved_datetime:
				last_saved_datetime = transaction_type.last_saved_datetime
		if transaction_data is None or not transaction_data:
			logger.info(f"no data, there is no insert to sql")
			print("no data, there is no insert to sql")
			Csv(last_saved_date=last_saved_datetime, identity=self.identity)
			return
		datab_obj = Database(self.identity)
		datab_obj.insert_into_sql(transaction_data, last_saved_datetime)
		logger.info(f"saved to database")
		#logger.exception()




