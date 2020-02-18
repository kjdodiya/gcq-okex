# coding: utf-8

from __future__ import absolute_import
import aiohttp
import asyncio
import decimal
import hashlib
import time
import logging
from datetime import date, datetime
from urllib.parse import urlencode
from basis.models import FUTURE_TYPES, FuturePosition

from django.db.models import Q
from basis.models import Asset, Exchange, CurrentBalance

import OrderManager.exchanges.okexv3.account_api as account
import OrderManager.exchanges.okexv3.ett_api as ett
import OrderManager.exchanges.okexv3.futures_api as future
import OrderManager.exchanges.okexv3.lever_api as lever
import OrderManager.exchanges.okexv3.spot_api as spot
import OrderManager.exchanges.okexv3.swap_api as swap




# HTTP request timeout in seconds
TIMEOUT = 20.0


class OkexClientError(Exception):
	pass


class Okex(object):
	def __init__(self, api_key, api_secret, loop=None, logger = None, passphrase= None,instru_type='spot'):
		self.instru_type = instru_type		
		self.KEY = api_key
		self.SECRET = api_secret
		self.PASSPHRASE = passphrase
		self.account_client = account.AccountAPI(self.KEY, self.SECRET, self.PASSPHRASE, True)
		#self.session = aiohttp.ClientSession(loop=loop)
		self.logger = logger
		# self.okex = Exchange.objects.get(code='OKX')
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


	async def place_order(self, amount, price, ord_type, symbol='btcusd', future=None):
		"""
		# Request
		POST https://www.okex.com/api/v1/trade.do
		# Response
		{"result":true,"order_id":123456}
		"""
		assert (isinstance(amount, str) and isinstance(price, str))

		if ord_type not in ('buy', 'sell', 'buy_market', 'sell_market'):
			raise OkexClientError("Invaild order type")

		if future is not None:
			print ("Future Not supported yet")
		else:
			#                          take_order(self, otype, side, instrument_id, size, margin_trading=1, client_oid='', price='', funds='',order_type = '')
			coid = "kdwokexv3%d" % (datetime.utcnow().timestamp()*1000)
			# print (coid)
			result = await self.client.take_order('limit', ord_type, symbol, amount, price=price, client_oid=coid, order_type='0')			
			#{'type': 'limit', 'side': 'sell', 'instrument_id': 'ETH-USDT', 'size': 0.001, 'client_oid': 'kdwokexv31581816572734', 'price': '136.01', 'funds': '', 'margin_trading': 1, 'order_type': '0'}
			#{'client_oid': 'kdwokexv31581816572734', 'error_code': '', 'error_message': '', 'order_id': '4396193107946496', 'result': True}
			#{'client_oid': 'kdwokexv31581816572734', 'error_code': '', 'error_message': '', 'order_id': '4396193107946496', 'result': True}

			return result

	# This function should be replaced when gets ingegated with jabba
	#async def order_status(self, symbol, oid, cid):
	async def order_status(self, order):
	
		"""
		Standard Jabba API for status of both cash and future orders
		Open Order response 
		 {'client_oid': 'kdwokexv31581880997065', 'created_at': '2020-02-16T19:23:27.737Z', 'filled_notional': '0', 'filled_size': '0', 'funds': '', 'instrument_id': 'ETH-USDT', 'notional': '', 'order_id': '4400415226468352', 'order_type': '0', 'price': '252', 'price_avg': '0', 'product_id': 'ETH-USDT', 'side': 'sell', 'size': '0.001', 'state': '0', 'status': 'open', 'timestamp': '2020-02-16T19:23:27.737Z', 'type': 'limit'}
		Filled Order response
		 {'client_oid': 'kdwokexv31581880997065', 'created_at': '2020-02-16T19:23:27.738Z', 'filled_notional': '0.252', 'filled_size': '0.001', 'funds': '', 'instrument_id': 'ETH-USDT', 'notional': '', 'order_id': '4400415226468352', 'order_type': '0', 'price': '252', 'price_avg': '252', 'product_id': 'ETH-USDT', 'side': 'sell', 'size': '0.001', 'state': '2', 'status': 'filled', 'timestamp': '2020-02-16T19:23:27.738Z', 'type': 'limit'}
		"""
		payload = {
			"symbol": order.listing.symbol, "order_id": order.exchange_id
		#	"symbol": symbol, "order_id": oid
		}
		try:
			if self.instru_type == 'future':
				pass
			#if order.listing.asset.type == 'FT':
				#payload['contract_type'] = dict(FUTURE_TYPES).get(order.listing.asset.future.future_type)
				#result = await self._post(self.url_for(PATH_FUTURE_ORDER_INFO), params=payload)
			else:
				#result = await self._post(self.url_for(PATH_ORDER_INFO), params=payload)
				result = await self.client.get_order_info(instrument_id=payload['symbol'],order_id=payload['order_id'],client_oid=cid)
				#print (result)
			if result:
				return result
				raise OkexClientError('Failed to get order status:' + str(result))
		except asyncio.TimeoutError:
			# if we get a timeout, let's sleep a couple and retry
			await asyncio.sleep(1)
			await self.order_status(order)

	#async def cancel_order(self, oid, symbol):
	async def cancel_order(self, order):
		oid = order.exchange_id
		symbol = order.listing.symbol
		result = await self.client.revoke_order(oid, symbol)
		return result

	async def cancel_orders(self, symbol, order_ids):
		params = [{"instrument_id":symbol,"order_ids":order_ids}]
		result = await self.client.revoke_orders(params)
		return result

	async def active_orders(self, symbol):
		# Check with derek what is meant by pending orders
		# Does it mean order placed but not yet filled ?
		act_ords = await self.client.get_orders_pending(None, None, 100, symbol)
		return act_ords

	async def history(self, symbol, status, limit=500):
		ordrs_history = await self.client.get_orders_list(status, symbol)
		return ordrs_history	

	async def get_order_history(self, symbol, since=0):
		result = await self.client.get_orders_list('all', symbol, limit=100)
		return result

	async def balances(self, account='FT'):
		if account == 'WA':
			result = await self.account_client.get_wallet()
		return result

	async def get_balances(self):
		#todo : retrieve balances from the wallet as well
		pass

	async def position(self, listing):
		# implement when future is active - no way to test 
		pass

	async def ticker(self, symbol):
		resp = await self.client.get_specific_ticker(symbol)
		return resp

	async def trades(self, symbol, since_tid=None):
		trades_since = await self.client.get_deal(symbol, since_tid, to, limit)
		return trades_since

	async def depth(self, symbol, size=200, future=False, type = None):
		resp = await self.client.get_depth(symbol, size)
		return resp


	def parse_response(self, response, order):
		"""
		{'client_oid': 'kdwokexv31581881593466', 'created_at': '2020-02-16T19:33:23.945Z', 'filled_notional': '0', 
		'filled_size': '0', 'funds': '', 'instrument_id': 'ETH-USDT', 'notional': '', 'order_id': '4400454299490304',
		'order_type': '0', 'price': '254', 'price_avg': '0', 'product_id': 'ETH-USDT', 'side': 'sell', 
		'size': '0.001', 'state': '0', 'status': 'open', 'timestamp': '2020-02-16T19:33:23.945Z', 'type': 'limit'}

		{'client_oid': 'kdwokexv31581880997065', 'created_at': '2020-02-16T19:23:27.000Z', 'filled_notional': '0.252',
		 'filled_size': '0.001', 'funds': '', 'instrument_id': 'ETH-USDT', 'notional': '', 'order_id': '4400415226468352', 
		 'order_type': '0', 'price': '252', 'price_avg': '252', 'product_id': 'ETH-USDT', 'side': 'sell', 'size': '0.001', 
		 'state': '2', 'status': 'filled', 'timestamp': '2020-02-16T19:23:27.000Z', 'type': 'limit'}
		"""
		try:
			if response['orders']:
				order.risk += decimal.Decimal(response['filled_size']) - order.filled_size
				if order.risk > 0:
					order.status = 'PF'
					avg_price = order.filled_price = decimal.Decimal(str(response['price_avg']))
					order.risk_price = (avg_price * decimal.Decimal(response['filled_size'])
										- order.filled_price * order.filled_size) / order.risk
					order.filled_size = decimal.Decimal(response['filled_size'])
					order.filled_price = avg_price
				if order.filled_size == order.asked_size:
					order.filled = True
					order.status = 'FI'
				if response['status'] == 'cancelled':
					order.cancelled = True
					if order.filled_size == 0:
						order.status = 'CL'
					else:
						order.status = 'PC'
				if 'fee' in response:
					order.fee = response['fee']
					if abs(order.fee / (order.listing.taker_fee / 10000 * order.filled_size) - 1) > abs(order.fee / (order.listing.maker_fee / 10000 * order.filled_size) - 1):
						order.maker = True
		except Exception as e:
			self.logger.error("Okex Order Failed : %s" % e)
			order.in_error = True
		return order
	def roll_contract(self, future):
		d1 = future.expiry
		d2 = date.today()
		if (d1 - d2).days > 13:
			return dict(FUTURE_TYPES).get(future.future_type)
		elif 6 < (d1 - d2).days <= 13:
			return dict(FUTURE_TYPES).get('NW')
		else:
			return dict(FUTURE_TYPES).get('WK')


async def okex_trade():
	api_key = "87e34868-d3c1-4b3b-9752-62a154349569"
	api_secret = "EE8534F26BD53DE380D51604BD67D957"
	passphrase = "kamalftw"
	instru_type = 'spot'
	gcq_okex = Okex(api_key, api_secret, passphrase=passphrase)
	#gcq_okex_future = GCQOkex('future', api_key, api_secret, passphrase)
	symbol = 'ETH-USDT'

	#ticker_resp = await gcq_okex.ticker(symbol)
	#print (ticker_resp)
	# ticker_resp = await gcq_okex.depth(symbol)
	# print (ticker_resp)
	# order_hs = await gcq_okex.get_order_history(symbol)
	# print (order_hs[0][0])
	# wallet_info = await gcq_okex.balances(account='WA')
	# print (wallet_info)
	##new_order = await gcq_okex.place_order('0.001','257.00','sell', symbol)
	##print (">>>> ORDER PLACED\n", new_order)
	# order_hs = await gcq_okex.get_order_history(symbol)
	# print (order_hs[0][0])
	#new_order =  {'client_oid': 'kdwokexv31581893409984', 'error_code': '', 'error_message': '', 'order_id': '4401228705246208', 'result': True}
	##print (">>>> REQUESTING ORDER STATUS\n", new_order['order_id'])
	##order_status = await gcq_okex.order_status(symbol, new_order['order_id'], new_order['client_oid'])
	##print (">>>> ORDER STATUS\n", order_status)
	##print (">>>> CANCELING ORDER\n", new_order['order_id'])
	##act_orders = await gcq_okex.active_orders(symbol)
	##print (">>>> ACTIVE ORDERS", act_orders)
	open_orders = await gcq_okex.history(symbol,'open')
	print (">>>> ACTIVE ORDERS", open_orders)
	filled_orders = await gcq_okex.history(symbol,'filled')
	print (">>>> FILLED ORDERS", filled_orders)
	#cancel_order = await gcq_okex.cancel_order(new_order['order_id'], symbol)
	#print (">>>> CANCELED ORDER", cancel_order)
	all_orders = await gcq_okex.history(symbol,'all')
	print (">>>> ALL ORDERS", all_orders)
	# trades_resp = await okex_spot.trades(symbol)
	# print (trades_resp)
	# currencies_resp = await okex_spot.get_currencies()
	# print (currencies_resp)
	# orders_resp = await okex_spot.get_orders()
	# print (orders_resp)


	"""
	SPOT
	Place Order -> Request Order Status - Cancel Order
	{'type': 'limit', 'side': 'sell', 'instrument_id': 'ETH-USDT', 'size': '0.001', 'client_oid': 'kdwokexv31581881593466', 'price': '254.00', 'funds': '', 'margin_trading': 1, 'order_type': '0'}
	{'client_oid': 'kdwokexv31581881593466', 'error_code': '', 'error_message': '', 'order_id': '4400454299490304', 'result': True}
	>>>> ORDER PLACED
	 {'client_oid': 'kdwokexv31581881593466', 'error_code': '', 'error_message': '', 'order_id': '4400454299490304', 'result': True}
	>>>> REQUESTING ORDER STATUS
	 4400454299490304
	>>>> ORDER STATUS
	 {'client_oid': 'kdwokexv31581881593466', 'created_at': '2020-02-16T19:33:23.945Z', 'filled_notional': '0', 'filled_size': '0', 'funds': '', 'instrument_id': 'ETH-USDT', 'notional': '', 'order_id': '4400454299490304', 'order_type': '0', 'price': '254', 'price_avg': '0', 'product_id': 'ETH-USDT', 'side': 'sell', 'size': '0.001', 'state': '0', 'status': 'open', 'timestamp': '2020-02-16T19:33:23.945Z', 'type': 'limit'}
	>>>> CANCELING ORDER
	 4400454299490304
	>>>> CANCELED ORDER {'client_oid': '', 'error_code': '', 'error_message': '', 'order_id': '4400454299490304', 'result': True}
	"""


if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	loop.run_until_complete(okex_trade())
	loop.close()