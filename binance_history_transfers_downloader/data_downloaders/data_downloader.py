import logging
import datetime
import time
from config import EXCHANGES
from exchange_clients import BinanceClient as Client
from abc import ABC, abstractmethod
from separated_scripts.binance_history_transfers_downloader.transfer_case import Case, UnifiedData
from separated_scripts.binance_history_transfers_downloader.data_downloaders.usd_price_downloader import USDpricer
logger = logging.getLogger(__name__)


class DataDownloader(ABC):
	API_CODE = ""
	SLEEP = 63
	EXCHANGE = EXCHANGES.BINANCE
	LIMIT = 500

	def __init__(self, case: Case):

		self.raw_data = []
		self.data = None
		self.page = 1
		self.from_dt = case.from_dt
		self.to_dt = datetime.datetime.utcnow()
		self.time_distance = datetime.timedelta(days=90)
		self.client = Client(*case.target_secret.credentials)
		self.identity = case.identity
		self.filename = ""
		self.last_saved_datetime = None

	def get_data(self):
		a_dt = self.from_dt
		self.page = 1
		while a_dt < self.to_dt:
			b_dt = min(self.to_dt, a_dt + self.time_distance)
			print(f'pocatecni cas: {a_dt} konecny cas:{b_dt}')
			resp = self.client._request('get', self.API_CODE, True, data=self._get_params(a_dt, b_dt))
			print(self._get_params(a_dt, b_dt))
			if "FIAT" in self.filename or "PAY" in self.filename:
				self.raw_data.extend(resp['data'])
				if 'total' in resp:
					if resp['total'] == self.LIMIT:
						self.page += 1
				a_dt += self.time_distance
				time.sleep(self.SLEEP)
			elif "COIN" in self.filename:
				self.raw_data.extend(resp)
				a_dt += self.time_distance
				time.sleep(self.SLEEP)
			elif "CARD" in self.filename:
				if resp['total'] != 0:
					print(resp['rows'])
					self.raw_data.extend(resp['rows'])
				a_dt += self.time_distance
				time.sleep(self.SLEEP)

	def load_data(self):
		logger.info(f'call for get_data')
		self.get_data()
		logger.info(f'make data model from downloaded data')
		self.data = self._make_data_model()
		self.last_saved_datetime = self.get_highest_date(self.data)

	def _post_process_data(self, df):
		return df

	def _get_params(self, a_dt, b_dt):
		pass

	def _make_data_model(self):
		data_model = []
		if len(self.raw_data) > 0:
			for row in self.raw_data:
				data_model.append(self.unify_row(row))
		return data_model

	def get_highest_date(self, data_model):
		if not data_model:
			logger.info(f'there were no data model in get_highest_date, return to_dt: {self.to_dt}')
			start_download_date = self.to_dt
			start_download_date.strftime('%Y-%m-%d %H:%M:%S')
			return start_download_date
		return max(data_model, key=lambda x: x.time).time

	@staticmethod
	def unify_row(row: dict) -> UnifiedData:
		pass
