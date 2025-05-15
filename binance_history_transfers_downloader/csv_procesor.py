import csv
import os
import datetime
import logging
logger = logging.getLogger(__name__)


class Csv:
	NAME = 'External_transfers_downloader_notes.csv'
	FIELD_NAMES = ['IDENTITY', 'DATETIME']

	def __init__(self, identity, last_saved_date=None):
		self.identity = identity
		self.last_saved_date = last_saved_date
		if self.last_saved_date is not None:
			self._update_date()
		else:
			self.last_saved_date = self._find_date()

	def _update_date(self):
		row = {'IDENTITY': self.identity, 'DATETIME': self.last_saved_date}
		with open(self.NAME, 'a') as file_object:
			dictwriter_object = csv.DictWriter(file_object, fieldnames=self.FIELD_NAMES)
			dictwriter_object.writerow(row)
			file_object.close()
			logger.info(f'Last saved date vas updated in CSV')

# TODO rozdelit funkci find_date na hledani a zvlaste na vytvareni noveho csv

	def _find_date(self) -> datetime.datetime:
		if not os.path.exists(self.NAME):
			with open(self.NAME, 'w', newline='') as csvfile:
				writer = csv.DictWriter(csvfile, fieldnames=self.FIELD_NAMES)
				writer.writeheader()
				return datetime.datetime.min
		else:
			latest_date = datetime.datetime.min
			with open(self.NAME, 'r', newline='') as csvfile:
				reader = csv.DictReader(csvfile)
				for row in reader:
					print(row)
					if row['IDENTITY'] == self.identity:
						try:
							row_datetime = datetime.datetime.strptime(row['DATETIME'], '%Y-%m-%d %H:%M:%S.%f')
						except ValueError:
							row_datetime = datetime.datetime.strptime(row['DATETIME'], '%Y-%m-%d %H:%M:%S')
						if row_datetime > latest_date:
							latest_date = row_datetime
			logger.info(f'last saved date got from CSV is {latest_date}')
			return latest_date









