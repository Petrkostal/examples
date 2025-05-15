import pandas as pd
import json
import logging
from separated_scripts.binance_history_transfers_downloader.transfer_case import Case, UnifiedData
from separated_scripts.binance_history_transfers_downloader.data_downloaders.usd_price_downloader import USDpricer
from .data_downloader import DataDownloader

logger = logging.getLogger(__name__)


class CoinDeposit(DataDownloader):
	API_CODE = "https://api.binance.com/sapi/v1/capital/deposit/hisrec"
	SLEEP = 20

	def __init__(self, case: Case):
		super().__init__(case)
		self.filename = "COIN_Deposit"
		logger.info(f'CoinDeposit init start')

	def unify_row(self, row: dict) -> UnifiedData:
		status_codes = {
			0: 'pending',
			1: 'success',
			6: 'credited but cannot withdraw',
			7: 'Wrong Deposit',
			8: 'Waiting User confirm'
		}
		usd_price = USDpricer().add_usd_amount(row['insertTime'], row['coin'])
		usd_amount = round(float(row['amount']) * usd_price, 4) if usd_price is not None else None

		return UnifiedData(
			time=pd.to_datetime(row['insertTime'], unit='ms').replace(microsecond=0),
			amount=row['amount'],
			usdAmount=usd_amount,
			asset=row['coin'],
			transferType="coinTransfer",
			address=row['address'],
			direction="deposit",
			totalFee=0,
			exchange=self.EXCHANGE,
			identita=self.identity,
			status=status_codes.get(row['status'], 'Unknown'),
			raw=json.dumps(row)
		)

	def _get_params(self, a_dt, b_dt):
		params = {}
		params['startTime'] = int(a_dt.timestamp() * 1000)
		params['endTime'] = int(b_dt.timestamp() * 1000)
		params['limit'] = 1000
		logger.info(f'parameters for CoinDeposit are: {params}')
		return params

