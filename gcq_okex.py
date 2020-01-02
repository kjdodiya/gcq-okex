# coding: utf-8

from __future__ import absolute_import
import aiohttp
import asyncio
import okex.account_api as account
import okex.ett_api as ett
import okex.futures_api as future
import okex.lever_api as lever
import okex.spot_api as spot
import okex.swap_api as swap






# HTTP request timeout in seconds
TIMEOUT = 20.0


class OkexClientError(Exception):
	pass


class GCQOkex(object):
	def __init__(self, instru_type, api_key, api_secret, passphrase, loop=None, logger = None):
		self.instru_type = instru_type		
		self.KEY = api_key
		self.SECRET = api_secret
		self.PASSPHRASE = passphrase
		#self.session = aiohttp.ClientSession(loop=loop)
		self.logger = logger
#		self.okex = Exchange.objects.get(code='OKX')
		self.timeout_retry = 0
		if self.instru_type == 'spot':
			self.client = spot.SpotAPI(self.KEY, self.SECRET, self.PASSPHRASE, True)
		if self.instru_type == 'future':	
			self.client = future.FutureAPI(self.KEY, self.SECRET, self.PASSPHRASE, True)
		if self.instru_type == 'swap':
			self.client = swap.SwapAPI(self.KEY, self.SECRET, self.PASSPHRASE, True)

	#def close(self):
	#	""" Close this session.
	#	:returns: None
	#	"""
	#	await self.session.close()
	#	return

	async def ticker(self, symbol):
		resp = await self.client.get_specific_ticker(symbol)
		return resp

async def okex_trade():
	api_key = "87e34868-d3c1-4b3b-9752-62a154349569"
	api_secret = "EE8534F26BD53DE380D51604BD67D957"
	passphrase = "kamalftw"
	instru_type = 'spot'
	gcq_okex = GCQOkex(instru_type, api_key, api_secret, passphrase)

	symbol = 'ETH-USDT'

	ticker_resp = await gcq_okex.ticker(symbol)
	print (ticker_resp)
	# trades_resp = await okex_spot.trades(symbol)
	# print (trades_resp)
	# currencies_resp = await okex_spot.get_currencies()
	# print (currencies_resp)
	# orders_resp = await okex_spot.get_orders()
	# print (orders_resp)



if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	loop.run_until_complete(okex_trade())
	loop.close()