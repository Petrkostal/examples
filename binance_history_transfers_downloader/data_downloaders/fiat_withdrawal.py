import datetime
import json
import logging

from separated_scripts.binance_history_transfers_downloader.transfer_case import Case, UnifiedData
from separated_scripts.binance_history_transfers_downloader.data_downloaders.usd_price_downloader import USDpricer
from .data_downloader import DataDownloader

logger = logging.getLogger(__name__)


class FiatWithdrawal(DataDownloader):
	API_CODE = "https://api.binance.com/sapi/v1/fiat/orders"

	def __init__(self, case: Case):
		super().__init__(case)
		self.filename = "FIAT-Withdrawals"
		logger.info(f'FiatWithdrawal init start')

	def unify_row(self, row: dict) -> UnifiedData:
		usd_price = USDpricer().add_usd_amount(row['createTime'], row['fiatCurrency'])
		usd_amount = round(float(row['amount']) * usd_price, 4) if usd_price is not None else None
		return UnifiedData(
			time=datetime.datetime.fromtimestamp(row['createTime'] / 1000).replace(microsecond=0),
			amount=row['amount'],
			usdAmount=usd_amount,
			asset=row['fiatCurrency'],
			transferType="bankTransfer",
			address=row['orderNo'],
			direction="withdrawal",
			totalFee=row['totalFee'],
			identita=self.identity,
			exchange=self.EXCHANGE,
			status=row['status'],
			raw=json.dumps(row))

	def _get_params(self, a_dt, b_dt):
		params = {}
		params['beginTime'] = int(a_dt.timestamp() * 1000)
		params['endTime'] = int(b_dt.timestamp() * 1000)
		params['transactionType'] = 1
		params['page'] = self.page
		params['rows'] = self.LIMIT
		logger.info(f'parameters for FiatWithdrawal are: {params}')
		return params