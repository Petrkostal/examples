import logging
from pydantic.dataclasses import dataclass
from instrument import Instrument
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import math  # Import pro nové funkce

logger = logging.getLogger(__name__)

class ScoreConfig:
    TIME_SINCE_LAST_TEST = 1
    VOLUME_CHANGE_MEAN = 1
    VOLUME_CHANGE_STD = 1
    SPREAD_PERCENT_MEAN = 2
    SPREAD_PERCENT_STD = 1
    PRICE_CHANGE_MEAN = 0.5
    PRICE_CHANGE_STD = 1
    PROFIT_CHANGE_MEAN = 0.01
    PROFIT_CHANGE_STD = 0.01
    TRADING_ACTIVITY_CHANGE = 1
    LAST_TEST_RESULT = 1
    PROFIT = 1
    PROFITABILITY = 1
    TICK_SIZE = 40
    NORMALIZATION_CONSTANTS = {
        'BinanceUSDVolume': {'mean': 5, 'std': 4},
        'SpreadPercent': {'mean': 20, 'std': 7},
        'AvgPrice': {'mean': 50, 'std': 6},
        'NetProfit10s': {'mean': 0.01, 'std': 0.01}
    }
    PROFITABILITY_CONSTANTS = {
        'Treshold_volume': 2000,
        'Treshold_trades': 123,
        'Max_value': 15,
        'Profitability_normalized': 100
    }


class ScoreCalculator:
    def __init__(self, instrument: Instrument):
        self.instrument = instrument
        self.df = self.instrument.data_history
        if self.df.empty:
            logger.warning(f'DataFrame for instrument {self.instrument.name} is empty.')
            print(f"Warning: DataFrame for instrument {self.instrument.name} is empty.")
            self.instrument.score = None
            self.instrument.details = json.dumps({})
            return

        if not pd.to_datetime(self.df['HourlyInterval'], errors='coerce').notnull().all():
            logger.info('Invalid datetime format in HourlyInterval')
            print("Error: Invalid datetime format in 'HourlyInterval")
        self.df.index = self.df['HourlyInterval']
        logger.info(f'DataFrame time index range from: {self.df.index.min()} to: {self.df.index.max()}')
        print(f"DataFrame time index range from: {self.df.index.min()} to: {self.df.index.max()}")

        self.set_window_time()

    def set_window_time(self):
        try:
            last_test_time_rounded = self.instrument.last_test_time.replace(minute=0, second=0, microsecond=0)
            self.first_window_start_time = last_test_time_rounded - timedelta(days=1)
            self.first_window_end_time = last_test_time_rounded

            self.adjust_windows(self.first_window_start_time, self.first_window_end_time, 'first')

            time_minus_2_minutes = datetime.utcnow() - timedelta(minutes=2)
            self.second_window_end_time = time_minus_2_minutes.replace(minute=0, second=0, microsecond=0)
            self.second_window_start_time = self.second_window_end_time - timedelta(days=1)

            self.adjust_windows(self.second_window_start_time, self.second_window_end_time, 'second')
        except Exception as e:
            logger.error(f"Error in setting time windows: {e}")
            print(f"Error in setting time windows: {e}")

    def adjust_windows(self, start_time, end_time, window_name):
        print(f"funkce adjust_window dostava start time {start_time} a end time {end_time}")
        logger.info(f"funkce adjust_window dostava start time {start_time} a end time {end_time}")
        # Funkce pro úpravu oken tak, aby měly vždy 24 hodin
        window = self.df.loc[start_time:end_time].iloc[:-1]
        print(f"window obsahuje: {window}")

        # Kontrola, zda má okno 24 hodin
        while len(window) < 24 and start_time > self.df.index.min():
            start_time -= timedelta(hours=1)
            print(f"window nemelo 24 hodin")
            window = self.df.loc[start_time:end_time].iloc[:-1]

        if len(window) < 24:
            print(f'posun casoveho okna do leva nemel uspech')
            # Fallback na pevný počet řádků
            window = self.df.iloc[:24]

        # Uložení upraveného času pro dané okno
        if window_name == 'first':
            self.first_window_start_time = window.index[0]
            print(f'first_window_start_time: {self.first_window_start_time}')
            self.first_window_end_time = window.index[-1]
            print(f'first_window_end_time: {self.first_window_end_time}')
        else:
            self.second_window_start_time = window.index[0]
            print(f'second_window_start_time: {self.second_window_start_time}')
            self.second_window_end_time = window.index[-1]
            print(f'second_window_end_time: {self.second_window_end_time}')

    def scale_sym_diff(self, x, constant, max_value=15):
        print(f'funkce scale_sym_diff got these values > x: {x}, constant: {constant}, max_value: {max_value}')
        logger.info(f'funkce scale_sym_diff got these values > x: {x}, constant: {constant}, max_value: {max_value}')
        return min(max_value, (constant / (2.001 - x)) - (constant / 2.001))

    def normalize_profitability(self, abs_profitability_diff, min_trades, min_volume):
        threshold_volume = ScoreConfig.PROFITABILITY_CONSTANTS['Treshold_volume']
        threshold_trades = ScoreConfig.PROFITABILITY_CONSTANTS['Treshold_trades']
        max_value = ScoreConfig.PROFITABILITY_CONSTANTS['Max_value']
        profitability_normalized_to_10 = ScoreConfig.PROFITABILITY_CONSTANTS['Profitability_normalized']

        # Pre-penalizace z hodnoty profitability
        pre_penalization_value = min(abs_profitability_diff * (10 / profitability_normalized_to_10), max_value)
        # Penalizace podle objemu obchodů
        penalization_trades = min(math.sqrt(min_trades) / math.sqrt(threshold_trades), 1)
        penalization_volumes = min(math.sqrt(min_volume) / math.sqrt(threshold_volume), 1)

        # Výsledná penalizovaná hodnota profitability
        return pre_penalization_value * penalization_trades * penalization_volumes

    def normalize_time_passed(self, value):
        time_giving_score_10 = 504  # 504 hours = 3 weeks
        return round(value * (10 / time_giving_score_10), 8)

    def calculate_parameter(self, parameter, normalize=False):
        print(f"nyni pocitam parametr s parametrem: {parameter}")

        def calculate_metrics(window, parameter):
            if self.df.empty:
                logger.warning("Skipping parameter calculation due to empty DataFrame.")
                return np.nan, np.nan
            if window.empty:
                return np.nan, np.nan
            mean = window[parameter].mean()
            std = window[parameter].std()
            return mean, std

        def calculate_sym_difference(value1, value2):
            if np.isclose(value1, 0, atol=1e-10) or np.isnan(value1):
                return 0
            sym_difference_result = (value2 - value1) / ((value1 + value2)/2)
            print(f"symetric difference result is: {sym_difference_result}")
            return sym_difference_result

        window_1 = self.df.loc[self.first_window_start_time:self.first_window_end_time]
        window_2 = self.df.loc[self.second_window_start_time:self.second_window_end_time]

        mean_1, std_1 = calculate_metrics(window_1, parameter)
        mean_2, std_2 = calculate_metrics(window_2, parameter)

        sym_diff_mean = calculate_sym_difference(mean_1, mean_2)
        abs_sym_diff_mean = abs(sym_diff_mean)
        sym_diff_std = calculate_sym_difference(std_1, std_2)
        abs_sym_diff_std = abs(sym_diff_std)
        print(f'abs_symetric_difference MEAN before normalization {abs_sym_diff_mean}')
        print(f'abs_symetric_difference STD before normalization {abs_sym_diff_std}')

        if normalize:
            params = ScoreConfig.NORMALIZATION_CONSTANTS.get(parameter, {'mean': 1, 'std': 1})
            abs_sym_diff_mean = self.scale_sym_diff(abs_sym_diff_mean, constant=params['mean'])
            print(f'abs_symetric_difference MEAN after normalization {abs_sym_diff_mean}')
            abs_sym_diff_std = self.scale_sym_diff(abs_sym_diff_std, constant=params['std'])
            print(f'abs_symetric_difference STD after normalization {abs_sym_diff_std}')

        return round(abs_sym_diff_mean, 8), round(abs_sym_diff_std, 8), round(sym_diff_mean, 8), round(sym_diff_std, 8)

    def calculate_profitability(self):
        window_1 = self.df.loc[self.first_window_start_time:self.first_window_end_time]
        window_2 = self.df.loc[self.second_window_start_time:self.second_window_end_time]

        total_profit_window_1 = window_1['NetProfit10s'].sum()
        total_volume_window_1 = window_1['VolumeMAKE'].sum()
        profitability_reference = (total_profit_window_1 / total_volume_window_1) * 100000 if total_volume_window_1 > 0 else 0

        total_profit_window_2 = window_2['NetProfit10s'].sum()
        total_volume_window_2 = window_2['VolumeMAKE'].sum()
        profitability_eval = (total_profit_window_2 / total_volume_window_2) * 100000 if total_volume_window_2 > 0 else 0

        abs_profitability_diff = abs(profitability_eval - profitability_reference)

        # Získání minimálního objemu a počtu obchodů pro penalizaci
        min_volume = min(total_volume_window_1, total_volume_window_2)
        min_trades = min(window_1['Trades'].sum(), window_2['Trades'].sum())

        # Normalizace profitability
        normalize_profitability = self.normalize_profitability(abs_profitability_diff, min_trades, min_volume)
        return round(normalize_profitability, 8), (min_volume, min_trades)

    def time_since_last_test(self):
        # funkce vraci pocet hodin od posledniho testu
        delta_hours = datetime.utcnow() - self.instrument.last_test_time
        total_hours = delta_hours.total_seconds() / 3600
        return int(total_hours)

    def count_ticksize_change(self):
        # funkce kontroluje rozdil mezi Tick Size mezi prvnim a druhym oknem
        first_hour_mintick = self.df['TickSize'].iloc[0]
        last_hour_mintick = self.df['TickSize'].iloc[-1]
        mintick_change = 1 if first_hour_mintick != last_hour_mintick else 0
        return mintick_change, (first_hour_mintick, last_hour_mintick)

    def calculate_score(self):
        if self.df.empty:
            logger.warning("Skipping score calculation due to empty DataFrame. Score set to zero")
            self.instrument.score = 0
            self.instrument.details = json.dumps({"reason": "empty dataframe"})
            return
        logger.info(f"zacinam pocitat score")
        score = 0
        details = {}

        # Získání dat pro obě časová okna
        window_1 = self.df.loc[self.first_window_start_time:self.first_window_end_time]
        window_2 = self.df.loc[self.second_window_start_time:self.second_window_end_time]
        print(f"calculate score function has this windo1 {window_1}")
        print(f"calculate score function has this windo2 {window_2}")
        logger.info(f"calculate score function has this windo1 {window_1}")
        logger.info(f"calculate score function has this windo2 {window_2}")

        count_window_1 = len(window_1)
        count_window_2 = len(window_2)

        # check if both window has same size
        if count_window_1 != count_window_2:
            logger.warning("Skipping score calculation due to unequal window sizes. Score set to zero")
            print(f"Unequal window sizes: Window 1 has {count_window_1} rows, Window 2 has {count_window_2} rows")
            self.instrument.score = 0
            self.instrument.details = json.dumps({
                "reason": "Unequal window sizes",
                "window_1_size": count_window_1,
                "window_2_size": count_window_2
            })
            return

        logger.info(f"Both windows have equal size: {count_window_1} rows")

        profitability_score, min_volume_and_trades = self.calculate_profitability()
        details['profitability'] = {
            'profitability_score': float(profitability_score),
            'coefficient': ScoreConfig.PROFITABILITY,
            'penalization_volume': float(min_volume_and_trades[0]),
            'penalization_trades': float(min_volume_and_trades[1])
        }
        print(f" profitability score is: {profitability_score}")
        logger.info(f" profitability score is: {profitability_score}")

        time_since_last_test = self.time_since_last_test()
        normalized_time_since_last_test = self.normalize_time_passed(time_since_last_test)
        details['time_since_last_test'] = {
            'hours since last test': int(time_since_last_test),
            'normalized hours': float(normalized_time_since_last_test),
            'coefficient': ScoreConfig.TIME_SINCE_LAST_TEST
        }
        print(f" normalized hours since last test is: {normalized_time_since_last_test}")
        logger.info(f" normalized hours since last test is: {normalized_time_since_last_test}")

        normalize = True
        abs_volume_change_mean, abs_volume_change_std, \
        real_volume_change_mean, real_volume_change_std = self.calculate_parameter('BinanceUSDVolume', normalize=normalize)
        details['volume_change'] = {
            'normalization': "on" if normalize else "off",
            'abs volume change mean': float(abs_volume_change_mean),
            'abs std_dev': float(abs_volume_change_std),
            'real volume change mean': float(real_volume_change_mean),
            'real volume change std': float(real_volume_change_std),
            'mean_coefficient': ScoreConfig.VOLUME_CHANGE_MEAN,
            'std_dev_coefficient': ScoreConfig.VOLUME_CHANGE_STD
        }
        print(f"volume_change_mean: {abs_volume_change_mean}, volume_change_std: {abs_volume_change_std}")
        logger.info(f"volume_change_mean: {abs_volume_change_mean}, volume_change_std: {abs_volume_change_std}")

        normalize = True
        abs_spread_mean, abs_spread_std, real_spread_mean, real_spread_std_dev = self.calculate_parameter('SpreadPercent', normalize=normalize)
        details['spread_percent'] = {
            'normalization': "on" if normalize else "off",
            'abs spread mean': abs_spread_mean,
            'abs spraed std_dev': abs_spread_std,
            'real spread mean': real_spread_mean,
            'real spread std_dev': real_spread_std_dev,
            'mean_coefficient': ScoreConfig.SPREAD_PERCENT_MEAN,
            'std_dev_coefficient': ScoreConfig.SPREAD_PERCENT_STD
        }
        print(f"spread_percent_mean: {abs_spread_mean}, spread_percent_std: {abs_spread_std}")
        logger.info(f"spread_percent_mean: {abs_spread_mean}, spread_percent_std: {abs_spread_std}")

        normalize = True
        abs_price_change_mean, abs_price_change_std, real_price_change_mean, real_price_change_std = self.calculate_parameter('AvgPrice', normalize=normalize)
        details['price_change'] = {
            'normalization': "on" if normalize else "off",
            'mean': abs_price_change_mean,
            'std_dev': abs_price_change_std,
            'real change mean': real_price_change_mean,
            'real change std': real_price_change_std,
            'mean_coefficient': ScoreConfig.PRICE_CHANGE_MEAN,
            'std_dev_coefficient': ScoreConfig.PRICE_CHANGE_STD
        }
        print(f"price_change_mean: {abs_price_change_mean}, price_change_std: {abs_price_change_std}")
        logger.info(f"price_change_mean: {abs_price_change_mean}, price_change_std: {abs_price_change_std}")

        ticksize_change, mintick_values = self.count_ticksize_change()
        details['mintick_change'] = {
            'first hour mintick': float(mintick_values[0]),
            'last hour mintick': float(mintick_values[1]),
            'tick size change coeficient': ScoreConfig.TICK_SIZE
        }
        print(f"tick size change: {ticksize_change}")
        logger.info(f"tick size change: {ticksize_change}")

        score += profitability_score
        score += normalized_time_since_last_test
        score += abs_volume_change_mean
        score += abs_volume_change_std
        score += abs_spread_mean
        score += abs_spread_std
        score += abs_price_change_mean
        score += abs_price_change_std
        score += ticksize_change * ScoreConfig.TICK_SIZE
        self.instrument.score = score
        logger.info(f"vysledne score pro instrument {self.instrument.name} je: {self.instrument.score}")
        self.instrument.details = json.dumps(details)

