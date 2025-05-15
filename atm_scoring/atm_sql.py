from app.managers.sql_manager import SqlManager
#from instrument import Instrument
import time
import pandas as pd
import datetime
from pyodbc import DatabaseError
import logging
from support.slacker.kancl_slacker import KanclSlacker
from separated_scripts.binance_history_transfers_downloader.csv_procesor import Csv
SQL = SqlManager
logger = logging.getLogger(__name__)
from support.slacker.kancl_slacker import KanclSlacker


class AtmSqlManager(SqlManager):

	@classmethod
	def get_atm_vatas_info(cls, allowed_trading_styles):
		logger.info(f'stahuji seznam aktivnich vat se stitkem ATM a s povolenymi traiding styly:{allowed_trading_styles}')
		trading_styles_placeholder = ', '.join(['?'] * len(allowed_trading_styles))
		q = f'''
						WITH 
				ActiveVatas AS (
				    SELECT
				        m.SymbolID,
				        m.Market,
				        LEFT(m.Style, PATINDEX('%[0-9]%', m.Style + '0') - 1) AS Style, -- Odstranění číselné části
				        ROW_NUMBER() OVER(
				            PARTITION BY m.SymbolID, m.Market, LEFT(m.Style, PATINDEX('%[0-9]%', m.Style + '0') - 1)
				            ORDER BY m.TimeEnd DESC
				        ) AS rn
				    FROM [VataDatabase].[dbo].View_Statistic5 m
				    WHERE m.TimeEnd > DATEADD(HOUR, -1, GETDATE())
				      AND LEFT(m.Style, PATINDEX('%[0-9]%', m.Style + '0') - 1) IN ({trading_styles_placeholder})
				),
				LatestCompletedTests AS (
				    SELECT
				        ah.SymbolID,
				        ah.Style,
				        ah.Time_start,
				        ah.Time_end,
				        ah.Status,
				        ROW_NUMBER() OVER (PARTITION BY ah.SymbolID, ah.Style ORDER BY ah.Time_start DESC) AS rn
				    FROM [VataDatabase].[dbo].[AtmHistory] ah
				    WHERE ah.Status IN ('Completed', 'Unverified')
				)
				SELECT
				    sa.SymbolId,
				    s.Symbol,
				    av.Market,
				    av.Style,
				    COALESCE(
				        CASE
				            WHEN lt.SymbolID IS NULL THEN 'Not Tested'
				            ELSE 'Completed'
				        END,
				        'Not Tested'
				    ) AS TestStatus,
				    lt.Time_start,
				    lt.Time_end
				FROM dbo.SymbolAttributes sa
				LEFT JOIN ActiveVatas av ON sa.SymbolId = av.SymbolId
				LEFT JOIN LatestCompletedTests lt ON av.SymbolID = lt.SymbolID AND av.Style = lt.Style AND lt.rn = 1
				JOIN dbo.Symbols s on sa.SymbolId = s.SymbolId
				WHERE av.rn = 1 and ATM in (1,2)
				        '''
		conn = cls.get_new_conn()
		df = pd.read_sql(q, conn, params=allowed_trading_styles)
		conn.close()
		df['Time_start'] = pd.to_datetime(df['Time_start'], format='%Y-%m-%d %H:%M:%S')
		df['Time_end'] = pd.to_datetime(df['Time_end'], format='%Y-%m-%d %H:%M:%S')
		logger.info(f'stazeno {df.shape[0]} zaznamu')
		return df

	@classmethod
	def get_atm_market_history(cls, instrument_id, last_test_time, trading_style):
		max_retries = 3
		retry_delay = 10
		retry_count = 0

		last_test_time_datetime = last_test_time - datetime.timedelta(hours=29)
		start_time = last_test_time_datetime.strftime('%Y-%m-%d %H:%M:%S')
		end_time_datetime = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
		end_time = end_time_datetime.strftime('%Y-%m-%d %H:%M:%S')
		logger.info(f'stahuji data s pocatecnim casem: {start_time} a konecnym casem:{end_time}')
		print(f' start time is: {start_time}, end time is {end_time}')
		q = f'''
		SELECT PeriodEnd as HourlyInterval
	   ,SUM(StatisticCount) as StatisticCount
	   ,AVG(VolumeUsd) as BinanceUSDVolume
	   ,AVG(Price) as AvgPrice -- neni avg, ale Close
	   ,MAX(TickSize) as TickSize
	   ,SUM([SumTradedVolumeMakeUSD]) as VolumeMAKE
	   ,SUM([SumProfit10sUSD] + RebateComputed + dbo.calc_ProfitMisc8(StatisticCount, SumTradedVolumeMakeUSD, sym.Market)) as NetProfit10s
	   ,SUM([SumProfitHyperCW] + RebateComputed + dbo.calc_ProfitMisc8(StatisticCount, SumTradedVolumeMakeUSD, sym.Market)) as NetProfitHyper
	   ,AVG([AvgSpreadMpc] / 1000.0) as SpreadPercent
	   ,SUM(SumOrders) as TodayOrders -- tohle je neco jinyho
	   ,SUM(SumTrades) as Trades
		FROM View_VataMarket vm
		INNER JOIN (SELECT DISTINCT Wid FROM Meta2 WHERE SymbolId = {instrument_id} AND Style = '{trading_style}') m ON vm.Wid = m.Wid
		INNER JOIN Symbols sym ON vm.SymbolId = sym.SymbolId
		WHERE PeriodEnd >= '{start_time}' AND PeriodEnd <'{end_time}'
		GROUP BY PeriodEnd
		ORDER BY HourlyInterval
		'''

		while retry_count < max_retries:
			try:
				conn = cls.get_new_conn()
				if retry_count == max_retries - 1:
					conn.timeout = 450
					logger.info(f'posledni pokus s timeout= 450')
					print(f'posledni pokus s timeout= 450')
				df = pd.read_sql(q, conn)
				conn.close()
				df['HourlyInterval'] = pd.to_datetime(df['HourlyInterval'])
				if df.empty:
					logger.info(f'data pro symbol {instrument_id} se nepodarilo stahnout')
				else:
					logger.info(f'Data se podarilo stahnout. Pocet radku: {len(df)}')
					print(df)
				return df
			except Exception as e:
				logger.error(f'Chyba pri stahovani dat: {e}. Pokus {retry_count + 1} z {max_retries}')
				print(f'Chyba pri stahovani dat: {e}. Pokus {retry_count + 1} z {max_retries}')
				retry_count += 1
				if retry_count < max_retries:
					logger.info(f'cekám {retry_delay} sekund pred dalsim pokusem.')
					print(f'cekám {retry_delay} sekund pred dalsim pokusem.')
					time.sleep(retry_delay)
		logger.error(f'Nepodarilo se ziskat data pro symbol {instrument_id} po {max_retries} pokusech.')
		print(f'Nepodarilo se ziskat data pro symbol {instrument_id} po {max_retries} pokusech.')
		return pd.DataFrame()  # Return an empty DataFrame if all attempts fail

	@classmethod
	def insert_score(cls, score_models):
		q = "INSERT INTO [VataDatabase].[dbo].[AtmScore] (EvaluationTime,SymbolID, Symbol, " \
		    "Style, Score, Details) " \
			"VALUES (?,?,?,?,?,?)"
		conn = cls.get_new_conn()
		cursor = conn.cursor()
		cursor.executemany(q, score_models)
		conn.commit()

