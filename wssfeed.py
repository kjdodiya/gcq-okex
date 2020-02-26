# Trial for usage of the websockets package instead of websocket wihout an s
import asyncio
import datetime
import platform
import pytz
from decimal import Decimal
import json
import logging
from arctic.date import DateRange
import pandas as pd
import websockets
import time
#from WhiteHat.DataHack.API.data_retrieve import compute_up
import copy

from Listener.abstraction.feed import Feed
from Listener.classes import (Dispatch, DispatchAction, DispatchNature,
                              OrderBook)

from Listener.abstraction.tools import decompress_okex_message
from Listener.abstraction import ws_subscriptions
from OrderManager.exchanges.cex import CEX

from basis.tools.logger import get_logger

# A WebSocket connection goes through the following four states, in order:
CONNECTING, OPEN, CLOSING, CLOSED = range(4)


class WSSFeed(Feed):

    def __init__(self, listing, keyring=None, loop=None, capture_data=False, logger=None): #, lib_ob_flat=None):
        self.keyring = keyring
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self.listing = listing
        self.order_book = OrderBook(self.listing, 0, 0)
        #prepare the WS URL:
        self.url = self.listing.exchange.websocket
        if self.listing.exchange.code == 'OKX' :
            # and self.listing.asset.type == 'CN':
            # yet another Okex wart....
            # self.url = self.url.replace('10440', '10441')
            # self.url = self.url.replace('/okexapi', '')
            pass
        elif self.listing.exchange.code == 'BNC' and 'ip-172' in platform.node():
            # remove the depth and tick from binance to lighten the load on AWS
            self.url = self.url.replace('/{0}@trade', '')
            self.url = self.url.replace('/{0}@depth20', '')
            self.logger.info("Modified Binance URL for lighter load to: %s" % self.url)
        elif self.listing.exchange.code == 'BTM' and 'ip-172' in platform.node():
            # remove the depth and tick from binance to lighten the load on AWS
            self.url = self.url.replace(',trade:{0}', '')
        self.url = self.url.format(self.listing.symbol)
        self.ws = None
        self.startup_completed = False
        self.capture_data = capture_data
        self.last_timestamp = 0
        self.last_tick_id = 0
        self.loop = loop
        self.name = listing.code
        self.breather = 1
        if listing.exchange.code == 'CEX':
            self.cex = CEX(username=listing.exchange.account, **self.keyring.get_credentials(listing.exchange), loop=loop)
        # self.wslogger = get_logger('websockets', filepath='C:/Users/charles/Documents/listener.log')
        # if lib_ob_flat:
        #       self.lib_ob_flat = lib_ob_flat
        #       self.flat_ob = []

    async def listen(self):
        while True:
            self.logger.info("Listening on websocket %s" % self.listing.code)
            if self.listing.exchange.websocket_subscription_required:
                # this calls ws_subscrptions.py name of exchange
                call = 'subscribe_' + self.listing.exchange.code
                func = getattr(ws_subscriptions, call)
            try:
                async with websockets.connect(self.url) as websocket:
                    if self.listing.exchange.websocket_subscription_required:
                        websocket = await func(self, websocket)
                    print('wssfeed pre bool')
                    self.startup_completed = True
                    print('wssfeed startup completed {}'.format(self.startup_completed()))
                    self.ws = websocket
                    #code that will eventually replace the while true
                    # async for message in websocket:
                    #       self.process(message) # only works with python 3.6c
                    while True:
                        print('while true')
                        try:
                            msg = await asyncio.wait_for(websocket.recv(), timeout=5)
                            self.order_book.time = time.time()
                            self.breather = max(self.breather - 1, 1)
                            self.process(msg)
                            # if self.lib_ob_flat:
                            #       self.update_flat_ob()
                        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                            if websocket.state == CLOSING or websocket.state == CLOSED:
                                raise ConnectionError("Connection has been closed")
                            try:
                                pong = await websocket.ping(data='ping')
                                await asyncio.wait_for(pong, timeout=6)
                                # self.logger.info('Ping OK, keeping connection %s alive...' % self.listing.code)
                                continue
                            except Exception as e:
                                await websocket.close()
                                self.logger.exception("Error while sending keepalive to %s server: %s" % (self.listing.exchange.name, e))
                                break  # inner loop
            except Exception as e:
                self.logger.exception("Exception on websocket %s : %s, reconnecting in %s s" % (self.listing.code, e, self.breather * 20))
                self.breather += 1
                await asyncio.sleep(self.breather * 20)
                continue

    def process(self, message):
        if self.listing.exchange.code == 'OKX':
            message = decompress_okex_message(message)
        msg = json.loads(message)
        dispatches = self.__parse_message(msg)
        if dispatches is not None:
            for dispatch in dispatches:
                self.__update_order_book(dispatch)
        if not self.startup_completed:
            self.startup_completed = True
        if self.order_book.best_bid_size < 0 or self.order_book.best_offer_size < 0 or (self.order_book.best_bid > self.order_book.best_offer and self.order_book.best_offer >0):
            self.logger.warning('Houston, we have a problem with listing {0}. Best bid: {1} in {2} lots, Best offer: {3} in {4} lots'.format(
                self.listing.code, self.order_book.best_bid, self.order_book.best_bid_size, self.order_book.best_offer, self.order_book.best_offer_size))
            self.order_book.empty()

    def __update_order_book(self, dispatch):
        """ (dispatch) -> None
            Consumes a single dispatch object and uses it to update the order book
        """
        try:
            if dispatch.nature == DispatchNature.trade:
                dico = {}
                dico['buy/sell'] = ('buy' if dispatch.action == DispatchAction.lift else 'sell')
                dico['price'] = float(dispatch.price)  # need to cast to float because arctic does not accept decimals
                dico['volume'] = float(dispatch.size)
                dico['timestamp'] = dispatch.timestamp
                dico['buyer'] = dispatch.buyer
                dico['seller'] = dispatch.seller
                self.order_book.trades.append(dico)
            elif dispatch.nature == DispatchNature.quote:
                if dispatch.price != 0 and dispatch.size != 0:
                    if dispatch.action == DispatchAction.bid:
                        self.order_book.best_bid = dispatch.price
                        self.order_book.best_bid_size = dispatch.size
                    elif dispatch.action == DispatchAction.ask:
                        self.order_book.best_offer = dispatch.price
                        self.order_book.best_offer_size = dispatch.size
                else:
                    return
            elif dispatch.nature == DispatchNature.add or dispatch.nature == DispatchNature.refresh:
                if dispatch.nature == DispatchNature.add:
                    add = Decimal('1.0')
                else:
                    add = Decimal('0.0')  # refresh only needs the new size
                if dispatch.action == DispatchAction.bid:
                    if self.order_book.best_bid < dispatch.price:
                        self.order_book.best_bid = dispatch.price
                        self.order_book.best_bid_size = dispatch.size
                        self.order_book.bids[dispatch.price] = dispatch.size
                    else:
                        if dispatch.price in self.order_book.bids:
                            self.order_book.bids[dispatch.price] = self.order_book.bids[dispatch.price] * add + dispatch.size
                            if self.order_book.best_bid == dispatch.price:
                                self.order_book.best_bid_size = self.order_book.bids[dispatch.price]
                        else:
                            self.order_book.bids[dispatch.price] = dispatch.size
                elif dispatch.action == DispatchAction.ask:
                    if self.order_book.best_offer > dispatch.price or self.order_book.best_offer == 0:
                        self.order_book.best_offer = dispatch.price
                        self.order_book.best_offer_size = dispatch.size
                        self.order_book.offers[dispatch.price] = dispatch.size
                    else:
                        if dispatch.price in self.order_book.offers:
                            self.order_book.offers[dispatch.price] = self.order_book.offers[dispatch.price] * add + dispatch.size
                            if self.order_book.best_offer == dispatch.price:
                                self.order_book.best_offer_size = self.order_book.offers[dispatch.price]
                        else:
                            self.order_book.offers[dispatch.price] = dispatch.size
            elif dispatch.nature == DispatchNature.cancel or dispatch.nature == DispatchNature.reduce:
                if dispatch.action == DispatchAction.bid:
                    if dispatch.price in self.order_book.bids:
                        if dispatch.nature == DispatchNature.cancel or self.order_book.bids[dispatch.price] == dispatch.size:
                            del self.order_book.bids[dispatch.price]
                            if self.order_book.best_bid == dispatch.price:
                                if self.order_book.bids:
                                    self.order_book.best_bid = max(self.order_book.bids.keys())
                                    self.order_book.best_bid_size = self.order_book.bids[self.order_book.best_bid]
                                else:
                                    self.order_book.best_bid = 0
                                    self.order_book.best_bid_size = 0
                        else:
                            self.order_book.bids[dispatch.price] = self.order_book.bids[dispatch.price] - dispatch.size
                            if self.order_book.best_bid == dispatch.price:
                                self.order_book.best_bid_size -= dispatch.size
                    else:
                        print("%s not in bids of %s" % (dispatch.price, self.listing.code))
                elif dispatch.action == DispatchAction.ask:
                    if dispatch.price in self.order_book.offers:
                        if dispatch.nature == DispatchNature.cancel:
                            del self.order_book.offers[dispatch.price]
                            if self.order_book.best_offer == dispatch.price:
                                if self.order_book.offers:
                                    self.order_book.best_offer = min(self.order_book.offers.keys())
                                    self.order_book.best_offer_size = self.order_book.offers[self.order_book.best_offer]
                                else:
                                    self.order_book.best_offer = 0
                                    self.order_book.best_offer_size = 0
                        else:
                            self.order_book.offers[dispatch.price] = self.order_book.offers[dispatch.price] - dispatch.size
                            if self.order_book.best_offer == dispatch.price:
                                self.order_book.best_offer_size -= dispatch.size
                    else:
                        print("%s not in offers of %s" % (dispatch.price, self.listing.code))
        except Exception as e:
            self.logger.exception("Error %s while updating order book for % s, dispatch nature is %s" % (e, self.listing.code, dispatch.nature))

    async def subscribe(self, subs=None):
        if subs:
            if self.listing.exchange.code == 'BTM':
                request = {"op": "subscribe", "args": ["orderBookL2" + self.listing.symbol]}
            await self.ws.send(json.dumps(request))
        return

    def __parse_message(self, msg):
        #two underscores mean private, no inheritance, one means private by convention
        #todo : move parsing code to the exchange specific files
        """
        :param msg: JSON object to parse - format depends on the exchange
        :return: a list of standardized dispatch objects that will be used to update the Datafeed's internal order book
        """
        dispatches = []
        if self.listing.exchange.code == 'BTM':
            action = msg['table'] if 'table' in msg else None
            if action == 'quote':
                b = Dispatch(self.listing.code, DispatchNature.quote, DispatchAction.bid, Decimal(str(msg['data'][0]['bidPrice'])), Decimal(str(msg['data'][0]['bidSize'])))
                o = Dispatch(self.listing.code, DispatchNature.quote, DispatchAction.ask, Decimal(str(msg['data'][0]['askPrice'])), Decimal(str(msg['data'][0]['askSize'])))
                return [b, o]
            elif action == 'trade':
                for trade in msg['data']:
                    if trade['side'] == 'Sell':
                        action = DispatchAction.give
                    else:
                        action = DispatchAction.lift
                    timestamp = datetime.datetime.strptime(trade['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
                    dispatch = Dispatch(self.listing.code, DispatchNature.trade, action, trade['price'], trade['size'], timestamp)
                    dispatches.append(dispatch)
        elif self.listing.exchange.code == 'BNC':
            if 'stream' in msg:
                if '{0}@trade'.format(self.listing.symbol) in msg['stream']:
                    if msg['data']['m']:
                        action = DispatchAction.give
                    else:
                        action = DispatchAction.lift
                    time = datetime.datetime.utcfromtimestamp(msg['data']['T'] / 1000).replace(tzinfo=pytz.UTC)  # binance posix in ms
                    dispatch = Dispatch(self.listing.code, DispatchNature.trade, action, Decimal(str(msg['data']['p'])), Decimal(str(msg['data']['q'])), time, msg['data']['b'], msg['data']['a'])
                    return [dispatch]
                elif '{0}@depth20'.format(self.listing.symbol) in msg['stream']:
                    self.order_book.empty()
                    for level in msg['data']['asks']:
                        dispatch = Dispatch(self.listing.code, DispatchNature.refresh, DispatchAction.ask, price=Decimal(str(level[0])), size=Decimal(str(level[1])))
                        dispatches.append(dispatch)
                    for level in msg['data']['bids']:
                        dispatch = Dispatch(self.listing.code, DispatchNature.refresh, DispatchAction.bid, price=Decimal(str(level[0])), size=Decimal(str(level[1])))
                        dispatches.append(dispatch)
                elif '{0}@ticker'.format(self.listing.symbol) in msg['stream']:
                    # self.logger.info("Time of ticker  is %s vs real time %s" %(datetime.datetime.utcfromtimestamp(int(msg['data']['O'])/1000), datetime.datetime.utcnow()))
                    b = Dispatch(self.listing.code, DispatchNature.quote, DispatchAction.bid, Decimal(str(msg['data']['b'])), Decimal(str(msg['data']['B'])))
                    o = Dispatch(self.listing.code, DispatchNature.quote, DispatchAction.ask, Decimal(str(msg['data']['a'])), Decimal(str(msg['data']['A'])))
                    return [b, o]
        elif self.listing.exchange.code == 'OKX':
            # OKEX message parser v3
            if 'data' in msg.keys():
                data = msg['data'][0] #msg [data] is a list with a dict inside
                if 'asks' in data.keys():
                    self.order_book.best_offer = Decimal(str(data['asks'][0][0]))
                    self.order_book.best_offer_size = Decimal(str(data['asks'][0][1]))
                    self.order_book.offers = {}
                    for level in data['asks']:
                        dispatch = Dispatch(self.listing.code,
                                            DispatchNature.refresh,
                                            DispatchAction.ask,
                                            Decimal(str(level[0])),
                                            Decimal(str(level[1])))
                        dispatches.append(dispatch)
                if 'bids' in data.keys():
                    self.order_book.best_bid = Decimal(str(data['bids'][0][0]))
                    self.order_book.best_bid_size = Decimal(str(data['bids'][0][1]))
                    self.order_book.bids = {}
                    for level in data['bids']:
                        dispatch = Dispatch(self.listing.code,
                                            DispatchNature.refresh,
                                            DispatchAction.bid,
                                            Decimal(str(level[0])),
                                            Decimal(str(level[1])))
                        dispatches.append(dispatch)
                else:
                    if 'result' in data:
                        # this is an acknowledgement, nothing to do
                        pass
                    else:
                        for trade in data:
                            if trade[4] == 'ask':
                                action = DispatchAction.give
                            else:
                                action = DispatchAction.lift
                            timestamp = datetime.datetime.combine(datetime.date.today(),
                                                                  datetime.datetime.strptime(trade[3], '%H:%M:%S').time()).replace(tzinfo=pytz.UTC) - datetime.timedelta(hours=8, minutes=0)

                            dispatch = Dispatch(self.listing.code,
                                                DispatchNature.trade,
                                                action,
                                                trade[1],
                                                trade[2],
                                                timestamp)
                            dispatches.append(dispatch)
        elif self.listing.exchange.code == 'GEM':
            if 'events' in msg:
                for level in msg['events']:
                    if level['type'] == 'trade':
                        size = Decimal(str(level['amount']))
                        trade_price = Decimal(str(level['price']))
                        if level['makerSide'] == 'bid':
                            action = DispatchAction.give
                        else:
                            action = DispatchAction.lift
                        dispatch = Dispatch(self.listing.code, DispatchNature.trade, action, trade_price, abs(size), msg['timestampms'] / 1000)
                        dispatches.append(dispatch)  # returns a list of messages cause on init there are more than one order in the message
                    elif level['type'] == 'change':
                        if level['side'] == 'bid':
                            action = DispatchAction.bid
                        else:
                            action = DispatchAction.ask
                        if level['remaining'] == 0:
                            nature = DispatchNature.cancel
                        else:
                            nature = DispatchNature.reduce
                        if level['reason'] == 'initial' or level['reason'] == 'place':  # first msg with whole order book in it, only parse it if our own book is empty
                            dispatch = Dispatch(self.listing.code, DispatchNature.add, action, Decimal(str(level['price'])), abs(Decimal(str(level['delta']))))
                        elif level['reason'] == 'cancel':
                            dispatch = Dispatch(self.listing.code, nature, action, Decimal(str(level['price'])), abs(Decimal(str(level['delta']))))
                        elif level['reason'] == 'trade':
                            dispatch = Dispatch(self.listing.code, nature, action, Decimal(str(level['price'])), abs(Decimal(str(level['delta']))))
                        else:
                            dispatch = None
                        dispatches.append(dispatch)
        elif self.listing.exchange.code == 'CEX':
            #self.logger.warning(msg)
            if 'e' in msg:
                if msg['e'] == 'history-update':
                    for tick in msg['data'][::-1]:
                        if tick[0] == 'buy':
                            action = DispatchAction.lift
                        else:
                            action = DispatchAction.give
                        time = datetime.datetime.utcfromtimestamp(int(tick[1]) / 1000).replace(tzinfo=pytz.UTC)  # cex posix in ms
                        dispatch = Dispatch(self.listing.code, DispatchNature.trade, action, Decimal(tick[3]), abs(int(tick[2]) / 1e8), time, txid=tick[4])
                        dispatches.append(dispatch)
                elif msg['e'] == 'md':
                    b = Dispatch(self.listing.code, DispatchNature.quote, DispatchAction.bid, Decimal(str(msg['data']['buy'][0][0])), Decimal(str(msg['data']['buy'][0][1] / 1e8)))
                    o = Dispatch(self.listing.code, DispatchNature.quote, DispatchAction.ask, Decimal(str(msg['data']['sell'][0][0])), Decimal(str(msg['data']['sell'][0][1] / 1e8)))
                    return [b, o]
                elif msg['e'] == 'ping':
                    self.ws.send(json.dumps({"e": "pong"}))
                    return None
        elif self.listing.exchange.code == 'BFX':  #BFX returns lists with no codes to identify. Duck typing approach, let's hope they don't change their output
            if isinstance(msg, list):
                if 'hb' in msg:
                    pass
                elif len(msg) == 11:  #ticker information
                    b = Dispatch(self.listing.code, DispatchNature.quote, DispatchAction.bid, Decimal(str(msg[1])), Decimal(str(msg[2])))
                    o = Dispatch(self.listing.code, DispatchNature.quote, DispatchAction.ask, Decimal(str(msg[3])), Decimal(str(msg[4])))
                    return [b, o]
                elif len(msg) == 2 and isinstance(msg[1], list):  # snapshot of an order book
                    self.order_book.empty()
                    for level in msg[1]:
                        if level[2] > 0:  # bid
                            dispatch = Dispatch(self.listing.code, DispatchNature.refresh, DispatchAction.bid, Decimal(str(level[0])), Decimal(str(level[2])))
                            dispatches.append(dispatch)
                        elif level[2] < 0:  # ask
                            dispatch = Dispatch(self.listing.code, DispatchNature.refresh, DispatchAction.ask, Decimal(str(level[0])), -Decimal(str(level[2])))
                            dispatches.append(dispatch)
                        else:
                            print("Bitfinex returning a snapshot of orderbook with an empty level")
                elif len(msg) == 4:  # update of an orderbook
                    if msg[2] == 0:
                        if msg[3] == 1:
                            dispatch = Dispatch(self.listing.code, DispatchNature.cancel, DispatchAction.bid, Decimal(str(msg[1])), Decimal('0.0'))
                            dispatches.append(dispatch)
                        elif msg[3] == -1:
                            dispatch = Dispatch(self.listing.code, DispatchNature.cancel, DispatchAction.ask, Decimal(str(msg[1])), Decimal('0.0'))
                            dispatches.append(dispatch)
                    elif msg[2] > 0:
                        if msg[3] > 0:
                            dispatch = Dispatch(self.listing.code, DispatchNature.refresh, DispatchAction.bid, Decimal(str(msg[1])), Decimal(str(msg[3])))
                            dispatches.append(dispatch)
                        elif msg[3] < 0:
                            dispatch = Dispatch(self.listing.code, DispatchNature.refresh, DispatchAction.ask, Decimal(str(msg[1])), -Decimal(str(msg[3])))
                            dispatches.append(dispatch)
        return dispatches

    def is_alive(self):
        """
        Get the status of the datafeed object
        :return: Tre if connected, but this will be enriched to improve monitoring.py of the system
        """
        if self.startup_completed == False:
            status = False
        else:
            if self.ws.state == CLOSING or self.ws.state == CLOSED:
                    self.logger.warning("Connection lost, disconnected from %s" % self.listing.code)
                    status = False
            else:
                status = True
        return status

    def update_flat_ob(self, dump_size=95000):
        t = datetime.datetime.utcfromtimestamp(self.order_book.time).replace(tzinfo=pytz.UTC)
        bids = [[k, v] for k, v in self.order_book.bids.items()]
        for i in range(len(bids)):
            b = bids[i]
            self.flat_ob += [{'price': float(b[0]), 'size': float(b[1]), 'timestamp': t, 'side': 'bid'}]
        offers = [[k, v] for k, v in self.order_book.offers.items()]
        for i in range(len(offers)):
            b = offers[i]
            self.flat_ob += [{'price': float(b[0]), 'size': float(b[1]), 'timestamp': t, 'side': 'offer'}]
        if len(self.flat_ob) >= dump_size:
            df = pd.DataFrame(self.flat_ob)
            df.set_index('timestamp', inplace=True)
            self.flat_ob = []
            self.lib_ob_flat.write(self.listing.code, df)
            print('Flat order book of listing ' + self.listing.code + ' was loaded in MongoDB')
