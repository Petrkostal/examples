import logging
from datetime import datetime, timedelta
import pandas as pd
from separated_scripts.atm_scoring import atm_sql


logger = logging.getLogger(__name__)


class Instrument:
	def __init__(self, symbolid, symbol, market, style, status, time_start_test=None, time_end_test=None):
		self.id = symbolid
		self.name = symbol
		self.market = market
		self.style = style
		self.score = None
		self.is_tested = False
		self.last_test_time = None
		self.details = {}
		self.data_history = pd.DataFrame()
		if status == 'Completed':
			self.last_test_time = time_end_test
			self.is_tested = True
		elif status == 'Not Tested':
			self.last_test_time = (datetime.utcnow() - timedelta(days=14))
		else:
			logger.info(f' neocekavany Test_Status v databazi :{status}')

	def get_historic_data(self):
		logger.info(f'stahuji data pro symbol: {self.name} ({self.id}) a styl: {self.style}')
		self.data_history = atm_sql.AtmSqlManager.get_atm_market_history(self.id, self.last_test_time, self.style)
		if self.data_history.empty:
			print("Warning: No data returned for instrument with ID:", self.id)


if __name__ == '__main__':
	Instrument(1, 1, 1, 1, 1).get_historic_data()
	print()
