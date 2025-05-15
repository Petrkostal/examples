
from binance.client import Client
import logging
from support.slacker.kancl_slacker import KanclSlacker

logger = logging.getLogger(__name__)


class USDpricer:
    USD_SYMBOLS = {'USD': 1, 'USDT': 1, 'BUSD': 1, 'TUSD': 1, 'FUSD': 1, 'USDC': 1, 'USDS': 1, 'BOBA': 0.22, 'BTG': 18,
                   'OGV': 0.006, 'MITH': 0.0017, 'GRS': 0.45, 'CND': 0.00093, 'NEBL': 0.016, 'REP': 0.8, 'AION': 0.0013,
                   'TORN': 1.7, 'AUTO': 26, 'PERL': 0.0018, 'WTC': 0.012, 'QSP': 0.005, 'BRD': 0.008, 'GO': 0.0048,
                   'NXS': 0.037, 'SPARTA': 0.06, 'MDA': 0.025, 'EZ': 0.034, 'NAV': 0.048, 'NAS': 0.012, 'BEAM': 0.018,
                   'DNT': 0.028, 'QLC': 0.011, 'TCT': 0.000536, 'HEGIC': 0.025535, 'MTH': 0.0045, 'ANT': 8.00,
                   'XMR': 138.84, 'MOB': 0.097, 'DREP': 0.0246, 'MIR': 0.09, 'MULTI': 0.77, 'CAT': 0.051, 'NFT':0.000001,
                   'DAR': 0.216}
    LIST = {'EURPS': 'EUR',
            'ETHW': 'ETH'}
    QUOTE_ASSETS = ['USDT', 'USDC']

    def __init__(self):
        self.client = Client()

    def _fetch_kline_open(self, pair: str, time: int) -> float | None:
        """Zkusí stáhnout open-price první minutové svíčky; pokud selže, vrátí None."""
        try:
            klines = self.client.get_klines(symbol=pair,
                                            interval='1m',
                                            startTime=time,
                                            limit=1)
            if not klines:
                return None
            return float(klines[0][1])
        except Exception as e:
            logger.debug(f"Binance kline failed for {pair}: {e}")
            return None

    def add_usd_amount(self, time: int, symbol: str) -> float | None:
        s = symbol.upper()
        # 1) aliasy (EURPS→EUR, ETHW→ETH…)
        s = self.LIST.get(s, s)

        # 2) hotové kurzy
        if s in self.USD_SYMBOLS:
            return self.USD_SYMBOLS[s]

        # 3) zkoušíme všechny quote měny
        for quote in self.QUOTE_ASSETS:
            # a) přímý pár: SYMBOL+QUOTE
            pair = f"{s}{quote}"
            price = self._fetch_kline_open(pair, time)
            if price is not None:
                return price

            # b) invertovaný: QUOTE+SYMBOL
            pair_rev = f"{quote}{s}"
            rev_price = self._fetch_kline_open(pair_rev, time)
            if rev_price not in (None, 0):
                return 1.0 / rev_price

        # 4) pokud pořád nic, varování a Slack
        logger.warning(f"[USDpricer] nelze zjistit USD kurz pro '{symbol}' "
                       f"ani proti {self.QUOTE_ASSETS}")
        KanclSlacker().send_error(
            f"[USDpricer] nenalezen USD kurz pro instrument '{symbol}'"
        )
        return None

