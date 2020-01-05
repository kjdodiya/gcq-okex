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
from datetime import datetime




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
		self.account_client = account.AccountAPI(self.KEY, self.SECRET, self.PASSPHRASE, True)
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

	async def depth(self, symbol, size=200, future=False, type = None):
		resp = await self.client.get_depth(symbol, size)
		return resp

	async def get_order_history(self, symbol, since=0):
		result = await self.client.get_orders_list('all', symbol, limit=100)
		return result

	async def balances(self, account='FT'):
		if account == 'WA':
			result = await self.account_client.get_wallet()
		return result

	async def place_order(self, amount, price, ord_type, symbol='btcusd', future=None):

		if future is not None:
			print ("Future Not supported yet")
		else:
			#                          take_order(self, otype, side, instrument_id, size, margin_trading=1, client_oid='', price='', funds='',order_type = '')
			coid = "kdwokexv3%d" % (datetime.utcnow().timestamp()*1000)
			# print (coid)
			result = await self.client.take_order('limit', 'sell', 'ETH-USDT', amount, price=price, client_oid=coid, order_type='0')
			print (result)			
			return result

	async def cancel_order(self, oid, symbol):
		result = await self.client.revoke_order(oid, symbol)
		print (result)
		return result

async def okex_trade():
	api_key = "87e34868-d3c1-4b3b-9752-62a154349569"
	api_secret = "EE8534F26BD53DE380D51604BD67D957"
	passphrase = "kamalftw"
	instru_type = 'spot'
	gcq_okex = GCQOkex(instru_type, api_key, api_secret, passphrase)

	symbol = 'ETH-USDT'

	#ticker_resp = await gcq_okex.ticker(symbol)
	#print (ticker_resp)
	# ticker_resp = await gcq_okex.depth(symbol)
	# print (ticker_resp)
	# order_hs = await gcq_okex.get_order_history(symbol)
	# print (order_hs[0][0])
	# wallet_info = await gcq_okex.balances(account='WA')
	# print (wallet_info)
	# new_order = await gcq_okex.place_order(0.001,'136.01','sell')
	# print (new_order)
	order_hs = await gcq_okex.get_order_history(symbol)
	print (order_hs[0][0])
	# cancel_order = await gcq_okex.cancel_order('4158334007773184', symbol)
	# print (cancel_order)
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