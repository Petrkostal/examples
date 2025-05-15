import os
import json
import time
from dataclasses import dataclass
import pandas as pd
import requests
import datetime
from separated_scripts.binance_history_transfers_downloader.db.database import Database
from typing import Optional

""" Toto je pouze provizorni kod pro web scrapingove stazeni historickych dat o prevodu z master uctu na bibnance 
platebeni kartu a zpet. stahujij to pomoci webu https://curlconverter.com/ z binance 
https://www.binance.com/cs/cards/transaction """
@dataclass
class UnifiedData:
	time: datetime.datetime
	amount: float
	asset: str
	totalFee: int
	direction: str
	transferType: str
	identita: str
	address: str
	exchange: str
	status: Optional[str]
	raw: str

class Scraper:
	EXCHANGE = "Binance"
	def __init__(self, identity):
		self.identity = identity
		self.years = (2021, 2022, 2023)
		self.base_path = "C:\\Users\\PetrK\\PycharmProjects\\watch_dev\\separated_scripts\\taxes\\Deposits_Withdrawals_Reports"

	def _make_data_model(self, data):
		data_model = []
		if len(data) > 0:
			for row in data:
				data_model.append(self.unify_row(row))
		return data_model

	def unify_row(self, row: dict) -> UnifiedData:
		if row['fromAccount'] == 'MAIN':
			return UnifiedData(
				time=pd.to_datetime(row['createTime'], unit='ms').replace(microsecond=0),
				amount=row['amount'],
				asset=row['asset'],
				totalFee=0,
				direction="withdrawal",
				transferType="FromMainToCard",
				identita=self.identity,
				exchange=self.EXCHANGE,
				address="",
				status=None,
				raw=json.dumps(row)
			)
		else:
			return UnifiedData(
				time=pd.to_datetime(row['createTime'], unit='ms').replace(microsecond=0),
				amount=row['amount'],
				asset=row['asset'],
				totalFee=0,
				direction="deposit",
				transferType="FromCardToMain",
				identita=self.identity,
				exchange=self.EXCHANGE,
				address="",
				status=None,
				raw=json.dumps(row)
			)

	def get_highest_date(self, data_model):
		if not data_model:
			return datetime.datetime.min
		return max(data_model, key=lambda x: x.time).time

	def run(self):
		for year in self.years:
			data = []
			od = datetime.datetime(year, 1, 1)
			do = datetime.datetime(year + 1, 1, 1)
			json_data1 = {
				'page': 1,
				'rows': 10,
				'asset': '',
				'fromAccount': 'MAIN',
				'toAccount': 'CARD',
				'startTime': int(od.timestamp() * 1000),
				'endTime': int(do.timestamp() * 1000),
			}
			json_data2 = {
				'page': 1,
				'rows': 10,
				'asset': '',
				'fromAccount': 'CARD',
				'toAccount': 'MAIN',
				'startTime': int(od.timestamp() * 1000),
				'endTime': int(do.timestamp() * 1000),
			}
			json_data = (json_data1, json_data2)
			for direction in json_data:
				cookies = {
					 'theme': 'dark',
				    '_gid': 'GA1.2.1446254950.1701693558',
				    'bnc-uuid': 'e8b1e01d-21bf-4c91-b6c2-2a378bd6bbd2',
				    'se_sd': 'goVFQBV0IHYAg0VYWWgcgZZAgFRVQEXWlBRRdVE9lBSWgF1NWU9W1',
				    'se_gd': 'QxRGxXRgTGWUl0WMIUBFgZZAABQAQBXWlZcRdVE9lBSWgVVNWUcP1',
				    'userPreferredCurrency': 'USD_USD',
				    'BNC_FV_KEY': '33c2d794b558a28aa64eb3e44dc4968a68a729d2',
				    'se_gsd': 'fys2AUJgIzU0MwU3JRw7Mxs3VQ0XDwFSVFRCWldXU1BUJ1NT1',
				    'BNC_FV_KEY_T': '101-cGL1XXvDLImn5dduyJCM1ZuM%2B348vIo9nAL4fTY5j%2F4D5I5pRrye1Xjz%2FgEFbksNElRIErrrV4P6Z25yzbPkOw%3D%3D-%2BJLZsE8UncxyeMZyjd3tyg%3D%3D-da',
				    'BNC_FV_KEY_EXPIRE': '1701799452373',
				    'g_state': '{"i_p":1701785059162,"i_l":1}',
				    'lang': 'cs',
				    'cr00': '6CC434DEFAEF1C62DC347AD626E34E97',
				    'd1og': 'web.130836296.C632E44604668DA110B76EA07B83F115',
				    'r2o1': 'web.130836296.345203CEDD51F80840C132897E3ED88D',
				    'f30l': 'web.130836296.E9F9090416063408C395895CCBE489C9',
				    'logined': 'y',
				    'BNC-Location': 'BINANCE',
				    'p20t': 'web.130836296.32F469B6C12C426BCA21C068896775DD',
				    'source': 'referral',
				    'campaign': 'accounts.binance.com',
				    '_gat_UA-162512367-1': '1',
				    'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%22130836296%22%2C%22first_id%22%3A%2218c34d712151da-02a4b2646ef7ec6-26031051-2304000-18c34d712161fc%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMThjMzRkNzEyMTUxZGEtMDJhNGIyNjQ2ZWY3ZWM2LTI2MDMxMDUxLTIzMDQwMDAtMThjMzRkNzEyMTYxZmMiLCIkaWRlbnRpdHlfbG9naW5faWQiOiIxMzA4MzYyOTYifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%22130836296%22%7D%2C%22%24device_id%22%3A%2218c34d712151da-02a4b2646ef7ec6-26031051-2304000-18c34d712161fc%22%7D',
				    '_ga_3WP50LGEEC': 'GS1.1.1701777848.2.1.1701777937.60.0.0',
				    '_ga': 'GA1.1.1271860326.1701693558',
				    'OptanonConsent': 'isGpcEnabled=0&datestamp=Tue+Dec+05+2023+13%3A05%3A41+GMT%2B0100+(st%C5%99edoevropsk%C3%BD+standardn%C3%AD+%C4%8Das)&version=202303.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=1f48d493-f0fa-48d7-8f22-3cb2db0a2354&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0004%3A1%2CC0002%3A1&AwaitingReconsent=false',
				}

				headers = {
					'authority': 'www.binance.com',
				    'accept': '*/*',
				    'accept-language': 'cs-CZ,cs;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6',
				    'bnc-uuid': 'e8b1e01d-21bf-4c91-b6c2-2a378bd6bbd2',
				    'clienttype': 'web',
				    'content-type': 'application/json',
				    # 'cookie': 'theme=dark; _gid=GA1.2.1446254950.1701693558; bnc-uuid=e8b1e01d-21bf-4c91-b6c2-2a378bd6bbd2; se_sd=goVFQBV0IHYAg0VYWWgcgZZAgFRVQEXWlBRRdVE9lBSWgF1NWU9W1; se_gd=QxRGxXRgTGWUl0WMIUBFgZZAABQAQBXWlZcRdVE9lBSWgVVNWUcP1; userPreferredCurrency=USD_USD; BNC_FV_KEY=33c2d794b558a28aa64eb3e44dc4968a68a729d2; se_gsd=fys2AUJgIzU0MwU3JRw7Mxs3VQ0XDwFSVFRCWldXU1BUJ1NT1; BNC_FV_KEY_T=101-cGL1XXvDLImn5dduyJCM1ZuM%2B348vIo9nAL4fTY5j%2F4D5I5pRrye1Xjz%2FgEFbksNElRIErrrV4P6Z25yzbPkOw%3D%3D-%2BJLZsE8UncxyeMZyjd3tyg%3D%3D-da; BNC_FV_KEY_EXPIRE=1701799452373; g_state={"i_p":1701785059162,"i_l":1}; lang=cs; cr00=6CC434DEFAEF1C62DC347AD626E34E97; d1og=web.130836296.C632E44604668DA110B76EA07B83F115; r2o1=web.130836296.345203CEDD51F80840C132897E3ED88D; f30l=web.130836296.E9F9090416063408C395895CCBE489C9; logined=y; BNC-Location=BINANCE; p20t=web.130836296.32F469B6C12C426BCA21C068896775DD; source=referral; campaign=accounts.binance.com; _gat_UA-162512367-1=1; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22130836296%22%2C%22first_id%22%3A%2218c34d712151da-02a4b2646ef7ec6-26031051-2304000-18c34d712161fc%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMThjMzRkNzEyMTUxZGEtMDJhNGIyNjQ2ZWY3ZWM2LTI2MDMxMDUxLTIzMDQwMDAtMThjMzRkNzEyMTYxZmMiLCIkaWRlbnRpdHlfbG9naW5faWQiOiIxMzA4MzYyOTYifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%22130836296%22%7D%2C%22%24device_id%22%3A%2218c34d712151da-02a4b2646ef7ec6-26031051-2304000-18c34d712161fc%22%7D; _ga_3WP50LGEEC=GS1.1.1701777848.2.1.1701777937.60.0.0; _ga=GA1.1.1271860326.1701693558; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Dec+05+2023+13%3A05%3A41+GMT%2B0100+(st%C5%99edoevropsk%C3%BD+standardn%C3%AD+%C4%8Das)&version=202303.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=1f48d493-f0fa-48d7-8f22-3cb2db0a2354&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0004%3A1%2CC0002%3A1&AwaitingReconsent=false',
				    'csrftoken': '68caa723c717a4186455e970ab103b1e',
				    'device-info': 'eyJzY3JlZW5fcmVzb2x1dGlvbiI6IjE5MjAsMTIwMCIsImF2YWlsYWJsZV9zY3JlZW5fcmVzb2x1dGlvbiI6IjE5MjAsMTE2MCIsInN5c3RlbV92ZXJzaW9uIjoiV2luZG93cyAxMCIsImJyYW5kX21vZGVsIjoidW5rbm93biIsInN5c3RlbV9sYW5nIjoiY3MtQ1oiLCJ0aW1lem9uZSI6IkdNVCswMTowMCIsInRpbWV6b25lT2Zmc2V0IjotNjAsInVzZXJfYWdlbnQiOiJNb3ppbGxhLzUuMCAoV2luZG93cyBOVCAxMC4wOyBXaW42NDsgeDY0KSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvMTE5LjAuMC4wIFNhZmFyaS81MzcuMzYiLCJsaXN0X3BsdWdpbiI6IlBERiBWaWV3ZXIsQ2hyb21lIFBERiBWaWV3ZXIsQ2hyb21pdW0gUERGIFZpZXdlcixNaWNyb3NvZnQgRWRnZSBQREYgVmlld2VyLFdlYktpdCBidWlsdC1pbiBQREYiLCJjYW52YXNfY29kZSI6ImRmMjZlMGU0Iiwid2ViZ2xfdmVuZG9yIjoiR29vZ2xlIEluYy4gKEludGVsKSIsIndlYmdsX3JlbmRlcmVyIjoiQU5HTEUgKEludGVsLCBJbnRlbChSKSBIRCBHcmFwaGljcyA1NTAwICgweDAwMDAxNjE2KSBEaXJlY3QzRDExIHZzXzVfMCBwc181XzAsIEQzRDExKSIsImF1ZGlvIjoiMTI0LjA0MzQ3NTI3NTE2MDc0IiwicGxhdGZvcm0iOiJXaW4zMiIsIndlYl90aW1lem9uZSI6IkV1cm9wZS9QcmFndWUiLCJkZXZpY2VfbmFtZSI6IkNocm9tZSBWMTE5LjAuMC4wIChXaW5kb3dzKSIsImZpbmdlcnByaW50IjoiOTI5NjI3ZTMzZjlmZWM1MGZiMzFmYzliZTdmYjAyNTIiLCJkZXZpY2VfaWQiOiIiLCJyZWxhdGVkX2RldmljZV9pZHMiOiIifQ==',
				    'fvideo-id': '33c2d794b558a28aa64eb3e44dc4968a68a729d2',
				    'fvideo-token': '4oveh3eAE5E20ryifOS2nZLFAcJx5scDYekj4ADbjwMyk910/RWcvVRQYoskvGOl9Tvfc/vsHMMIpytbfEHaUuxZYGEUE+Rxh+QPb1WUTWla+1vk0AdM0JFu4uwLXjEC/q/UqwtGnmaJAY7ChkFXa/tZ/1EKCUosbo0oYcAQwfjKlBwwOUkHdkoTLxNasg2tA=66',
				    'lang': 'cs',
				    'origin': 'https://www.binance.com',
				    'referer': 'https://www.binance.com/cs/cards/transaction',
				    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
				    'sec-ch-ua-mobile': '?0',
				    'sec-ch-ua-platform': '"Windows"',
				    'sec-fetch-dest': 'empty',
				    'sec-fetch-mode': 'cors',
				    'sec-fetch-site': 'same-origin',
				    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
				    'x-passthrough-token': '',
				    'x-trace-id': '51003939-d6ef-472d-8933-faf8839bbd26',
				    'x-ui-request-trace': '51003939-d6ef-472d-8933-faf8839bbd26',
				}

				response = requests.post(
				    'https://www.binance.com/bapi/asset/v1/private/asset-service/wallet/transfer-history',
				    cookies=cookies,
				    headers=headers,
				    json=direction,
				)

				# Note: json_data will not be serialized by requests
				# exactly as it was in the original request.
				#data = '{"page":1,"rows":10,"asset":"","fromAccount":"MAIN","toAccount":"CARD","startTime":1668466800000,"endTime":1684187999999}'
				#response = requests.post(
				#    'https://www.binance.com/bapi/asset/v1/private/asset-service/wallet/transfer-history',
				#    cookies=cookies,
				#    headers=headers,
				#    data=data,
				#)
				answer = response.json()

				print(f'toto jsou stazena data:{json.dumps(answer, indent=4)}')
				if answer['total'] != 0:
					data.extend(answer['data'])
				else:
					print('no data')
				time.sleep(10)

			data_model = self._make_data_model(data)
			highest_date = self.get_highest_date(data_model)
			df = pd.DataFrame(data_model)
			#df['createTime'] = df['createTime'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1000))
			#df['cas'] = df['createTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
			datab_obj = Database(self.identity)
			datab_obj.insert_into_sql(data_model, highest_date)
			file_path = os.path.join(self.base_path, f"{self.identity}_{year}_Card_Deposits_Withdrawals.csv")
			df.to_csv(file_path)
			print(data)



Scraper("Honza").run()