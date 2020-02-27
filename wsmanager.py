import asyncio
from decimal import Decimal
import json
import websockets
import logging
import django

from OrderManager.exchanges.okex import Okex

# A WebSocket connection goes through the following four states, in order:
CONNECTING, OPEN, CLOSING, CLOSED = range(4)

from OrderManager.exchanges.bitmex import BitMEX
from basis.models import FuturePosition, Order, Listing






class WSManager:
    def __init__(self, exchange, keyring=None, logger=None):
        self.exchange = exchange
        self.url = exchange.websocket
        self.keyring = keyring
        self.logger = logger
        self.initialized = False
        self.breather = 1
        self.logger = logger

    async def subscribe_bitmex(self, websocket, topics=['order'], symbols=None):
        #print(await websocket.recv())  # get the welcome message
        nonce = BitMEX.generate_nonce(None)
        credentials = self.keyring.get_credentials(self.exchange)
        signature = BitMEX.generate_signature(None, credentials['api_secret'], 'GET', '/realtime', nonce, "")
        await websocket.send(json.dumps({"op": "authKey", "args": [credentials['api_key'], nonce, signature]}))
        #print(await websocket.recv())  # get auth message
        await websocket.send(json.dumps({"op": "subscribe", "args": topics}))

    async def listen_bitmex(self, topics=['order'], live_orders=None):
        while True:
            try:
                async with websockets.connect("wss://testnet.bitmex.com/realtime") as websocket:  #todo this is retarded change to live/testnet
                    print("Subscribing to Bitmex order updates")
                    await self.subscribe_bitmex(websocket, topics)
                    while True:
                        try:
                            msg = await asyncio.wait_for(websocket.recv(), timeout=5)
                            # print(msg)
                            self.process(msg, live_orders)
                            self.breather = max(self.breather - 1, 1)
                        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                            if websocket.state == CLOSING or websocket.state == CLOSED:
                                raise ConnectionError("Connection has been closed")
                            try:
                                pong = await websocket.ping(data='ping')
                                await asyncio.wait_for(pong, timeout=5)
                                continue
                            except Exception as e:
                                self.logger.exception("Error while sending keepalive to Bitmex server: %s" % e)
                                await websocket.close()
                                break  # inner loop
            except Exception as e:
                self.logger.exception("Exception on bitmex order websocket : %s, rerying in %s s" % (e, self.breather * 20))
                self.breather += 1
                await asyncio.sleep(self.breather * 20)
                continue

    def process(self, message, live_orders):
        msg = json.loads(message)  # was uncommented
        print("Live orders before: \n{}".format(live_orders))
        if 'data' in msg:
            data = msg['data']
            print(data)
            for response in data:
                self.logger.debug("indiv order from ws: {}".format(response))
                if not self.initialized:
                    self.logger.info("Initializing the Bitmex live orders websocket")
                    # self.initialized = True
                    self.logger.warning("Found prior order {} on Bitmex, adding it to the Order Manager:".format(response))
                    # let's see if the order is in the database:
                    try:
                        bitmex_order = self.response_to_order(response)
                        order = Order.objects.get(exchange_id=bitmex_order.exchange_id)
                        live_orders['BTM'][order.exchange_id] = order
                        # GC addition TODO
                        live_orders['BTM'][response['orderID']].save()
                    except Exception as e:
                        self.logger.exception("Order %s is not in the database" % bitmex_order.exchange_id)
                        live_orders['BTM'][bitmex_order.exchange_id] = bitmex_order
                else:
                    if response['orderID'] in live_orders:
                        # update the status of the live_order:
                        BitMEX.parse_response(None, response, live_orders['BTM'][response['orderID']])
                        live_orders['BTM'][response['orderID']].save()
                    # there was no logic to reput live orders after the first
                    elif response['orderID'] not in live_orders:
                        try:
                            bitmex_order = self.response_to_order(response)
                            order = Order.objects.get(exchange_id=bitmex_order.exchange_id)
                            live_orders['BTM'][order.exchange_id] = order
                            # GC addition TODO
                            live_orders['BTM'][response['orderID']].save()
                        except Exception as e:
                            self.logger.exception("Order %s is not in the database" % bitmex_order.exchange_id)
                            live_orders['BTM'][bitmex_order.exchange_id] = bitmex_order
                    else:
                        if 'orderQty' in response:
                            size = str(response["orderQty"])
                        else:
                            size = 'NaN'
                        self.logger.warning("Warning : {0} order for size {1} not followed by the order manager : {2}".format(response["symbol"], size, response['orderID']))
                self.initialized = True #TODO this is a GC bug fix
            print("Live orders after: \n{}".format(live_orders))

    def response_to_order(self, response):
        try:
            listing = Listing.objects.filter(asset__code=response['symbol']).first()
            order = Order(type='PO', listing=listing)
            order.exchange_id = response['orderID']
            if response['side'] == 'Sell':
                order.side = 'O'
            else:
                order.side = 'B'
            order.asked_size = response['orderQty']
            order.filled_size = response['cumQty']
            order.limit_price = Decimal(str(response['price']))
            order.open_timestamp = response['timestamp']
            return order
        except Exception as e:
            # django connections can fail from bad network, need to cull them
            print("WS_manager Django error in adding new feeds, {} \n".format(e))
            django.db.close_old_connections()
            django.db.connection.close()
            print('wsmanager - closing django connection to db, should auto reconnect')

