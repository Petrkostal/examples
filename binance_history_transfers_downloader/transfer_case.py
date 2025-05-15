import logging
import datetime
from api_secrets import Secrets
from dataclasses import dataclass
from separated_scripts.binance_history_transfers_downloader.db.database import Database
from separated_scripts.binance_history_transfers_downloader.csv_procesor import Csv

from typing import Optional
from decimal import Decimal

logger = logging.getLogger(__name__)


class Case:

	def __init__(self, identity):
		self.identity = identity
		self.from_dt = self.get_last_time(self.identity)-datetime.timedelta(days=30)
		self.target_secret = Secrets().find_master(self.identity)
		logger.info('secrets downloaded')

	def get_last_time(self, identity):
		database = Database(identity)
		last_db_time = database.get_last_sql_saving()
		last_csv_time = Csv(identity=identity).last_saved_date
		x = max(last_csv_time, last_db_time)
		logger.info(f'get_last_time return: {x}')
		return x


@dataclass
class UnifiedData:
	time: datetime.datetime
	amount: Decimal
	usdAmount:Decimal
	asset: str
	totalFee: Decimal
	direction: str
	transferType: str
	identita: str
	address: str
	exchange: str
	status: Optional[str]
	raw: str

