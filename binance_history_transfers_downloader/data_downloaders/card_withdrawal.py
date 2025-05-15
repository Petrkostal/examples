import pandas as pd
import datetime
import json
import logging
from separated_scripts.binance_history_transfers_downloader.transfer_case import Case, UnifiedData
from separated_scripts.binance_history_transfers_downloader.data_downloaders.usd_price_downloader import USDpricer
from .data_downloader import DataDownloader

logger = logging.getLogger(__name__)


class CardWithdrawal(DataDownloader):
	API_CODE = "https://api.binance.com/sapi/v1/asset/transfer"
	SLEEP = 20

	def __init__(self, case: Case):
		super().__init__(case)
		self.filename = "CARD_Withdrawal"
		self.time_distance = datetime.timedelta(days=30)
		logger.info(f'CardWithdrawal init start')

	def unify_row(self, row: dict) -> UnifiedData:

		usd_price = USDpricer().add_usd_amount(row['timestamp'], row['asset'])
		usd_amount = round(float(row['amount']) * usd_price, 4) if usd_price is not None else None

		return UnifiedData(
			time=pd.to_datetime(row['timestamp'], unit='ms').replace(microsecond=0),
			amount=row['amount'],
			usdAmount=usd_amount,
			asset=row['asset'],
			totalFee=0,
			direction="withdrawal",
			transferType="ToCardTransfer",
			identita=self.identity,
			address=row['tranId'],
			exchange=self.EXCHANGE,
			status=row['status'],
			raw=json.dumps(row)
		)

	def _get_params(self, a_dt, b_dt):
		params = {}
		params['startTime'] = int(a_dt.timestamp() * 1000)
		params['endTime'] = int(b_dt.timestamp() * 1000)
		params['type'] = "MAIN_FUNDING"
		logger.info(f'parameters for CardWithdrawal are: {params}')
		return params


class Import:
	pass