import sys; sys.path.append('..\\..')
import time
import math
from atm_sql import AtmSqlManager
from _datetime import datetime

from helpers.logging_helpers.basic_logging import basic_logger
from instrument import Instrument
from score_calculator import ScoreCalculator

if __name__ == '__main__':
	logger = basic_logger('ATM_scoring2.log')

	allowed_traiding_styles = ["Triangle",
								"BySelf",
								#"Chlup",
								"ByFutures"
								]

	scoring_instruments = AtmSqlManager.get_atm_vatas_info(allowed_traiding_styles)
	score_models = []
	current_time_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
	total_symbols = len(scoring_instruments)
	for index, row in scoring_instruments.iterrows():
		print(f'prcesing symbol number {index + 1} from total symbols {total_symbols}')
		logger.info(f'prcesing symbol number {index + 1} from total symbols {total_symbols}')
		instrument = Instrument(symbolid=row['SymbolId'], symbol=row['Symbol'], market=row['Market'], style=row['Style'],
								status=row['TestStatus'], time_start_test=row['Time_start'], time_end_test=row['Time_end'])
		instrument.get_historic_data()

		# vypocet score
		calculator = ScoreCalculator(instrument)
		calculator.calculate_score()

		# Kontrola a ošetření None a NaN hodnot
		score = instrument.score if instrument.score is not None and not math.isnan(instrument.score) else 0
		details = instrument.details if instrument.details is not None else {}

		# vytvoreni data modelu
		score_models.append((current_time_str, instrument.id, instrument.name, instrument.style, instrument.score,
		                    instrument.details))
		time.sleep(2)
	score_models = [
		(
			score[0],  # current_time_str
			score[1],  # instrument.id
			score[2],  # instrument.name
			score[3],  # instrument.style
			0 if (score[4] is None or math.isnan(score[4])) else score[4],  # instrument.score
			score[5] if score[5] is not None else {}  # instrument.details
		)
		for score in score_models]
	logger.info(f"mam score_model s {len(score_models)} zaznamy a jdu je vlozit do SQL")
	AtmSqlManager.insert_score(score_models)
