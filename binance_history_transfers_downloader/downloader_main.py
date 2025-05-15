import sys; sys.path.append('..\\..')
import logging

from api_secrets import Secrets
from config import IDENTITY, EXCHANGES
from factory import Factory


if __name__ == '__main__':

	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	logger_handler = logging.FileHandler('external_transfer_history.log')
	logger_handler.setLevel(logging.INFO)
	logger_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s || %(message)s")
	logger_handler.setFormatter(logger_formatter)
	logger.addHandler(logger_handler)

	for identity in Secrets().get_identities(exchange=EXCHANGES.BINANCE):

		logger.info(f'call Factory - get_all for indentity: {identity}')
		print(identity)
		Factory(identity).get_all()

