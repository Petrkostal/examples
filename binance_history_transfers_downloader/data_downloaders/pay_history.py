import logging
import datetime
import json
import pandas as pd

from .data_downloader import DataDownloader
from separated_scripts.binance_history_transfers_downloader.transfer_case import Case, UnifiedData
from separated_scripts.binance_history_transfers_downloader.data_downloaders.usd_price_downloader import USDpricer

logger = logging.getLogger(__name__)


class PayHistory(DataDownloader):
	API_CODE = "https://api.binance.com/sapi/v1/pay/transactions"

	def __init__(self, case: Case):
		super().__init__(case)
		self.filename = "PAY-History"
		self.time_distance = datetime.timedelta(days=80)
		logger.info(f'PayHistory init start')

	def unify_row(self, row: dict) -> UnifiedData:
		raw_amount = row['amount']
		amount = float (raw_amount)
		usd_price = USDpricer().add_usd_amount(row['transactionTime'], row['currency'])
		usd_amount = round(float(row['amount']) * usd_price, 4) if usd_price is not None else None
		direction = "deposit" if amount > 0 else "withdrawal"
		return UnifiedData(
			time=pd.to_datetime(row['transactionTime'], unit='ms').replace(microsecond=0),
			amount=row['amount'],
			usdAmount=usd_amount,
			asset=row['currency'],
			transferType=row['orderType'],
			address=row['transactionId'],
			direction=direction,
			totalFee=0,
			identita=self.identity,
			exchange=self.EXCHANGE,
			status=None,
			raw=json.dumps(row))

	def _get_params(self, a_dt, b_dt):
		params = {}
		params['startTime'] = int(a_dt.timestamp() * 1000)
		params['endTime'] = int(b_dt.timestamp() * 1000)
		params['limit'] = 100
		logger.info(f'parameters for PayHistory are: {params}')
		return params