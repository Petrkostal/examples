import pandas as pd
import json
import logging
from separated_scripts.binance_history_transfers_downloader.transfer_case import Case, UnifiedData
from separated_scripts.binance_history_transfers_downloader.data_downloaders.usd_price_downloader import USDpricer
from .data_downloader import DataDownloader

logger = logging.getLogger(__name__)


class CoinWithdrawal(DataDownloader):
	API_CODE = "https://api.binance.com/sapi/v1/capital/withdraw/history"
	SLEEP = 20

	def __init__(self, case: Case):
		super().__init__(case)
		self.filename = "COIN-Withdrawal"
		logger.info(f'CoinWithdrawal init start')

	def unify_row(self, row: dict) -> UnifiedData:
		status_codes = {
			0: 'Email Sent',
			1: 'Cancelled',
			2: 'Awaiting Approval',
			3: 'Rejected',
			4: 'Processing',
			5: 'Failure',
			6: 'Completed'
		}
		time_datetime = pd.to_datetime(row['applyTime'])
		usd_price = USDpricer().add_usd_amount(int(time_datetime.timestamp() * 1000), row['coin'])
		usd_amount = round(float(row['amount']) * usd_price, 4) if usd_price is not None else None

		return UnifiedData(
			time=pd.to_datetime(row['applyTime']),
			amount=row['amount'],
			usdAmount=usd_amount,
			asset=row['coin'],
			transferType="coinTransfer",
			address=row['address'],
			direction="withdrawal",
			totalFee=row['transactionFee'],
			identita=self.identity,
			exchange=self.EXCHANGE,
			status=status_codes.get(row['status'], 'Unknown'),
			raw=json.dumps(row))

	def _get_params(self, a_dt, b_dt):
		params = {}
		params['startTime'] = int(a_dt.timestamp() * 1000)
		params['endTime'] = int(b_dt.timestamp() * 1000)
		params['limit'] = 1000
		logger.info(f'parameters for CoinWithdrawal are: {params}')
		return params
