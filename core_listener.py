import asyncio
import datetime
import platform
from decimal import Decimal
import django

import zmq
import zmq.asyncio

from basis.models import Asset, Listing, Exchange
from basis.tools.access import Access
from basis.tools.logger import get_logger
# insert hormes logger
from core.logger import set_logger
# from exec_bridge.logger import set_logger
from Listener.classes import BestPrice, Quote, CombinedOrderBook
from CryptoTrader.tools.functions import round_down

from Listener.abstraction.restfeed import RESTFeed
from Listener.abstraction.wssfeed import WSSFeed

# import sock config
from Messaging.socket_config import socket_config


class Market(object):
    datafeeds = {}

    def __init__(self, loop=None, mode=None, exclude_bitmex=False):
        # Create all the websockets we need via Datafeeds
        self.loop = loop
        self.log_file_path = '../logs/listener.log'
        # self.logger = get_logger('Listener')#, self.log_file_path)
        # intercept and replace logger
        # hormes logger
        self.logger = set_logger("Exec Listener")
        self.listings = Listing.objects.filter(listen=True).select_related(
            'asset', 'numeraire', 'exchange', 'asset__future')
        # prefetch
        list(self.listings)
        self.assets = Asset.objects.all()
        list(self.assets)
        self.logger.info("Found a total of %s listeners" % len(self.listings))
        self.reference_numeraire = Asset.objects.get(code='USD')
        self.keyring = Access(False)
        self.mode = mode
        self.exclude_bitmex = exclude_bitmex
        if mode == 'capture' or mode == 'captureonly':
            self.capture_data = True
        else:
            self.capture_data = False
        for l in self.listings:
            self.launch_feed(l)

    async def run(self, port):
        for key, datafeed in self.datafeeds.items():
            asyncio.ensure_future(datafeed.listen())
        await asyncio.sleep(10)  # give time for all the feeds to open
        print(self.datafeeds)

        # Then launch the datacapture process
        asyncio.ensure_future(self.purge_order_books(60))

        if self.mode == 'test':
            asyncio.ensure_future(
                self.serve_requests(1111))  # for test purposes only
        elif self.mode == 'serve' or self.mode == 'capture':
            # Finally listen to the ZMQ broker_socket for requests from the other application
            for i in range(30):
                # port +0 for frontend, port + 1 generic trader, +2 controller, +3 eod process, above for single strat traders
                asyncio.ensure_future(self.serve_requests(
                    str(int(port) + i)))  # listen on n sockets
            asyncio.ensure_future(self.serve_requests('1954'))  # for BobaFett
            asyncio.ensure_future(self.serve_requests('9032'))  # for Greedo
        elif self.mode == "lux":
            # this is a lux mode with less ports spawned
            # GHETTO AS F*CK
            asyncio.ensure_future(self.serve_requests(
                socket_config['MarketLink_Listener']['REP'], mode=self.mode))
            asyncio.ensure_future(
                self.serve_requests(socket_config['MarketLink_Trader']['REP'],
                                    mode=self.mode))
        # finally launch the function that adds new feeds
        asyncio.ensure_future(self.add_new_feeds())

    def launch_feed(self, l):
        feed = None
        if l.asset.type == 'FT':
            if l.asset.future.expiry < datetime.datetime.now().date():
                self.logger.info(
                    "Future expired, not launching feed: %s" % l.code)
                return
        if l.exchange.websocket is None or (l.exchange.code in ['OKX',
                                                                'BFX'] and 'ip-172' in platform.node()):
            self.logger.info('Launching REST datafeed on %s' % l.code)
            feed = RESTFeed(l, self.keyring, self.loop, self.capture_data,
                            logger=self.logger)
            self.datafeeds[l.id] = feed
        else:
            if not self.exclude_bitmex or l.exchange.code != 'BTM':
                self.logger.info('Launching WSS datafeed on %s' % l.code)
                feed = WSSFeed(l, self.keyring, self.loop, self.capture_data,
                               logger=self.logger)
                self.datafeeds[l.id] = feed
        return feed

    async def add_new_feeds(self):
        while True:
            await asyncio.sleep(120) # was 300
            try:
                new_listings = Listing.objects.filter(listen=True).exclude(
                    id__in=self.datafeeds.keys())
                if new_listings:
                    for listing in new_listings:
                        datafeed = self.launch_feed(listing)
                        if datafeed:
                            asyncio.ensure_future(datafeed.listen(),
                                                  loop=self.loop)
                    self.listings = Listing.objects.filter(
                        listen=True).select_related('asset', 'numeraire',
                                                    'exchange', 'asset__future')
                    list(self.listings)
            except Exception as e:
                # django connections can fail from bad network, need to cull them
                print("core_listener Django error in adding new feeds, {} \n".format(e))
                django.db.close_old_connections()
                django.db.connection.close()
                print('core listener - closing django connection to db, should auto reconnect')


    async def serve_requests(self, port, mode):
        """
        Handles more complex request from the strategies)
        :return: sends the requested quote over the ZMQ broker_socket
        """

        context = zmq.asyncio.Context()
        socket = context.socket(zmq.REP)
        # we need a selecto for proxied/ux vs "normal" jabba
        if mode == "lux":
            socket.connect("tcp://localhost:" + str(port))
        else:
            socket.bind("tcp://*:" + str(port))  # bind for the server
        while True:
            #  Wait for next request from client
            try:
                # if await broker_socket.poll():
                msg = await socket.recv_json()
                output = []
                if msg['call'] == 'get_quote':
                    if msg['listing_id'] not in self.datafeeds:
                        answer = [Quote(error=-1)]
                    else:
                        if not self.datafeeds[msg['listing_id']].is_alive():
                            self.logger.info(
                                "Dead datafeed : %s" % self.datafeeds[
                                    msg['listing_id']].listing.code)
                            answer = [Quote(error=-1)]
                        else:
                            answer = [self.datafeeds[
                                          msg['listing_id']].get_last_quote()]
                elif msg['call'] in ['get_best_bid', 'get_best_offer']:
                    func = getattr(self, msg['call'])
                    needed_size = Decimal(msg['needed_size'])
                    if needed_size > 0 and not msg['single_exchange']:
                        test = 1
                    answer = func(msg['asset_id'], msg['numeraire_id'],
                                  needed_size, msg['excluded'],
                                  msg['force_exchange'],
                                  msg['single_exchange'])
                elif msg['call'] == 'get_combined_orderbook':
                    func = getattr(self, msg['call'])
                    answer = func(msg['asset_id'], msg['numeraire_id'],
                                  msg['depth'], msg['excluded'],
                                  msg['force_exchange'],
                                  msg['single_exchange'])
                #  Send reply back to client
                for item in answer:
                    output.append(item.toJSON())
                await socket.send_json(output)

            except zmq.error.ZMQError as e:
                message = "ZMQ Exception %s in " \
                          "core_listener.serve_requests {}".format(e)
                self.logger.critical(message)
                self.trader.logger.exception(message)
                print(message)
                context = zmq.asyncio.Context()
                socket = context.socket(zmq.REP)
                # we need a selecto for proxied/ux vs "normal" jabba
                print("core_listener - resetting ZMQ REP socket")
                if mode == "lux":
                    socket.connect("tcp://localhost:" + str(port))
                else:
                    socket.bind("tcp://*:" + str(port))  # bind for the server
                continue
            except Exception as e:
                message = "ZMQ Exception %s in serve request" % e
                if 'msg' in locals():
                    message += " for request : %s" % msg
                self.logger.exception(message)
                await socket.send_json('{error:-1}')
                continue

    def get_best_bid(self, asset_id, numeraire_id, size, exclude=None,
                     force_exchange=None, single_exchange=False):
        if single_exchange or size == 0:
            return self.get_best_bid_mono_exchange(asset_id, numeraire_id,
                                                   size, exclude,
                                                   force_exchange)
        else:
            return self.get_best_bid_multi_exchange(asset_id, numeraire_id,
                                                    size, exclude,
                                                    force_exchange)

    def get_best_bid_mono_exchange(self, asset_id, numeraire_id, size,
                                   exclude=None, force=None):
        # find all the listings that have this pair
        # This function doesn't support numeraire = 0
        listings = self.__get_listings(asset_id, numeraire_id, exclude, force)
        if not listings:
            return [BestPrice(
                error=-1)]  # returns an empty object if we did not find the correct listeners
        pos = 0
        best_bid = 0
        best_bid_with_brokerage = 0
        worst_price = 0
        best_available_size = 0
        for l in listings:
            brokerage = l.taker_fee
            try:
                if not self.datafeeds[l.id].is_alive():
                    self.logger.error(
                        "Dead datafeed : %s" % self.datafeeds[l.id].name)
                else:
                    quote = self.datafeeds[l.id].get_last_quote()
                    order_book = self.datafeeds[l.id].get_last_order_book()
                    if order_book is None or size == 0:
                        ob = False
                    else:
                        bids = sorted(order_book.bids.items(),
                                      key=lambda x: x[0], reverse=True)
                        n = len(bids)
                        if n == 0:
                            ob = False
                        else:
                            ob = True
                    if ob:
                        achieved_size = Decimal('0.0')
                        last_price = Decimal('0.0')
                        cost = Decimal('0.0')
                        available_size = Decimal(
                            '0.0')  # available at prices not worse than worst price
                        i = 0
                        while achieved_size < size and i < n:
                            if achieved_size + bids[i][1] < size:
                                cost += bids[i][1] * bids[i][0]
                                last_price = bids[i][0]
                                achieved_size += bids[i][1]
                                available_size += bids[i][1]
                            else:
                                cost += (size - achieved_size) * bids[i][0]
                                last_price = bids[i][0]
                                achieved_size = Decimal(str(size))
                                available_size += bids[i][1]
                            i += 1
                        bid = cost / achieved_size
                        bid_with_brokerage = bid * (1 - brokerage / 10000)
                        if achieved_size < size:
                            self.logger.info(
                                "Bid on %s: Can only have size %s out ot %s at avg price %s, worst price %s" % (
                                l.exchange.name, achieved_size, size, bid,
                                last_price))
                        else:
                            self.logger.info(
                                "Bid on %s: Managed to have size %s at avg price %s, can have %s at worst price %s" % (
                                l.exchange.name, size, bid, available_size,
                                last_price))
                            if bid_with_brokerage > best_bid_with_brokerage:
                                best_available_size = available_size
                                best_bid_with_brokerage = bid_with_brokerage
                                best_bid = bid
                                worst_price = last_price
                                pos = l.id
                                self.logger.info(
                                    "Current best bid for size %s is avg = %s, worst = %s  on %s" % (
                                    available_size, best_bid, worst_price,
                                    l.exchange.name))
                    else:
                        bid = quote.bid
                        bid_with_brokerage = bid * (1 - brokerage / 10000)
                        available_size = quote.bid_size
                        if size > quote.bid_size:
                            self.logger.info(
                                "Bid on %s: Can only have size %s out ot %s at price %s" % (
                                l.exchange.name, available_size, size, bid))
                        else:
                            self.logger.info(
                                "Bid on %s: Managed to have size %s at price %s" % (
                                l.exchange.name, available_size, bid))
                            if bid_with_brokerage > best_bid_with_brokerage:
                                best_available_size = available_size
                                best_bid_with_brokerage = bid_with_brokerage
                                best_bid = bid
                                worst_price = bid
                                pos = l.id
                                self.logger.info(
                                    "Current best bid for size %s is %s on %s" % (
                                    best_available_size, bid, l.exchange.name))
            except Exception as e:
                self.logger.exception(
                    "Exception while getting quote in get_best_bid for %s: %s" % (
                    l.code, e))
                continue
        l = next(lis for lis in listings if lis.id == pos)
        if best_bid == 0 or best_available_size == 0:
            return [BestPrice(error=-1)]
        best_bid_object = BestPrice(asset_id, 'B', worst_price,
                                    round_down(best_available_size,
                                               l.size_decimal_places),
                                    l.exchange_id, numeraire_id, error=0,
                                    listing_id=l.id)
        return [best_bid_object]

    def get_best_bid_multi_exchange(self, asset_id, numeraire_id, size,
                                    exclude=None, force=None):
        # find all the listings that have this pair
        # This function doesn't support numeraire = 0
        listings = self.__get_listings(asset_id, numeraire_id, exclude, force)
        if not listings:
            return [BestPrice(
                error=-1)]  # returns an empty object if we did not find the correct listeners
        try:
            asset = next(
                asset for asset in self.assets if asset.id == asset_id)
            numeraire = next(
                asset for asset in self.assets if asset.id == numeraire_id)
            multi_order_book, min_size_decimal_places = self.create_combined_bids(
                listings)
            n = len(multi_order_book)
            filled_size = Decimal('0.0')
            cost = Decimal('0.0')
            i = 0
            orders = {}
            while (filled_size < size and i < n) or i == 0:
                bid = multi_order_book[i]
                if filled_size + bid[2] < size:
                    last_size = round_down(bid[2], min_size_decimal_places)
                else:
                    last_size = round_down(size - filled_size,
                                           min_size_decimal_places)
                    if size == 0:
                        avg_price = bid[1]
                filled_size += last_size
                cost += last_size * bid[1]
                if bid[3] not in orders:
                    orders[bid[3]] = (last_size, bid[1])
                else:
                    old = orders[bid[3]]
                    orders[bid[3]] = (last_size + old[0], bid[1])
                i += 1
            if filled_size > 0:
                avg_price = cost / filled_size
            if filled_size == size or size == 0:
                self.logger.info(
                    "Best Bid for %s in %s: Could get size %s at avg price %s with breakdown:" % (
                    asset.code, numeraire.code, filled_size, avg_price))
            else:
                self.logger.info(
                    "Best Bid for %s in %s: Could only get size %s out of %s at avg price %s with breakdown:" % (
                    asset.code, numeraire.code, filled_size, size, avg_price))
            if size == 0:
                listing = next(u for u in orders.keys())
                size, price = orders[listing]
                self.logger.info("      %s: %s available at price %s" % (
                listing.exchange.name, size, price))
                if price == 0:
                    output = [BestPrice(error=-1)]
                else:
                    output = [BestPrice(asset_id, 'B', price, size,
                                        listing.exchange.name, numeraire_id,
                                        listing_id=listing.id)]
            else:
                best_object_list = []
                for listing, value in orders.items():
                    bid_size, bid_price = value
                    self.logger.info("      %s: %s available at price %s" % (
                    listing.exchange.name, bid_size, bid_price))
                    if bid_size * bid_price != 0:
                        best = BestPrice(asset.id, 'B', bid_price, bid_size,
                                         listing.exchange.id, numeraire_id,
                                         listing_id=listing.id, error=0)
                        best_object_list.append(best)
                output = best_object_list
            return output
        except Exception as e:
            self.logger.exception("Error in get best bid for %s in %s: %s" % (
            asset.code, numeraire.code, e))

    def get_best_offer(self, asset_id, numeraire_id, size, exclude=None,
                       force_exchange=None, single_exchange=False):
        if single_exchange or size == 0:
            return self.get_best_offer_mono_exchange(asset_id, numeraire_id,
                                                     size, exclude,
                                                     force_exchange)
        else:
            return self.get_best_offer_multi_exchange(asset_id, numeraire_id,
                                                      size, exclude,
                                                      force_exchange)

    def get_best_offer_mono_exchange(self, asset_id, numeraire_id, size,
                                     exclude=None, force=None):
        # find all the listings that have this pair
        # This function doesn't support numeraire = 0
        listings = self.__get_listings(asset_id, numeraire_id, exclude, force)
        if not listings:
            return [BestPrice(
                error=-1)]  # returns an empty object if we did not find the correct listeners
        pos = 0
        best_offer = 10 ** 6
        best_offer_with_brokerage = 10 ** 6
        worst_price = 0
        best_available_size = 0
        for l in listings:
            brokerage = l.taker_fee
            try:
                if not self.datafeeds[l.id].is_alive():
                    self.logger.error(
                        "Dead datafeed : %s" % self.datafeeds[l.id].name)
                else:
                    quote = self.datafeeds[l.id].get_last_quote()
                    order_book = self.datafeeds[l.id].get_last_order_book()
                    if order_book is None or size == 0:
                        ob = False
                    else:
                        offers = sorted(order_book.offers.items(),
                                        key=lambda x: x[0], reverse=False)
                        n = len(offers)
                        if n == 0:
                            ob = False
                        else:
                            ob = True
                    if ob:
                        achieved_size = Decimal('0.0')
                        last_price = Decimal('0.0')
                        cost = Decimal('0.0')
                        available_size = Decimal(
                            '0.0')  # available at prices not worse than worst price
                        i = 0
                        while achieved_size < size and i < n:
                            if achieved_size + offers[i][1] < size:
                                cost += offers[i][1] * offers[i][0]
                                last_price = offers[i][0]
                                achieved_size += offers[i][1]
                                available_size += offers[i][1]
                            else:
                                cost += (size - achieved_size) * offers[i][0]
                                last_price = offers[i][0]
                                achieved_size = size
                                available_size += offers[i][1]
                            i += 1
                        offer = cost / achieved_size
                        offer_with_brokerage = offer * (1 + brokerage / 10000)
                        if achieved_size < size:
                            self.logger.info(
                                "Offer on %s: Can only have size %s out ot %s at avg price %s, worst price %s" % (
                                l.exchange.name, achieved_size, size, offer,
                                last_price))
                        else:
                            self.logger.info(
                                "Offer on %s: Managed to have size %s at avg price %s, can have %s at worst price %s" % (
                                l.exchange.name, size, offer, available_size,
                                last_price))
                            if 0 < offer_with_brokerage < best_offer_with_brokerage:
                                best_available_size = available_size
                                best_offer_with_brokerage = offer_with_brokerage
                                best_offer = offer
                                worst_price = last_price
                                pos = l.id
                                self.logger.info(
                                    "Current best offer for size %s is avg = %s, last = %s  on %s" % (
                                    available_size, best_offer, worst_price,
                                    l.exchange.name))
                    else:
                        offer = quote.offer
                        offer_with_brokerage = offer * (1 + brokerage / 10000)
                        available_size = quote.offer_size
                        if size > quote.offer_size:
                            self.logger.info(
                                "Offer on %s: Can only have size %s out of %s at price %s" % (
                                l.exchange.name, available_size, size, offer))
                        else:
                            self.logger.info(
                                "offer on %s: Managed to have size %s at price %s" % (
                                l.exchange.name, available_size, offer))
                            if 0 < offer_with_brokerage < best_offer_with_brokerage:
                                best_available_size = available_size
                                best_offer_with_brokerage = offer_with_brokerage
                                best_offer = offer
                                worst_price = offer
                                pos = l.id
                                self.logger.info(
                                    "Current best offer for size %s is %s on %s" % (
                                    best_available_size, offer,
                                    l.exchange.name))
            except Exception as e:
                self.logger.exception(
                    "Exception while getting quote in get_best_offer for %s: %s" % (
                    l.code, e))
                continue
        l = next(lis for lis in listings if lis.id == pos)
        if best_offer == 0 or best_available_size == 0:
            return [BestPrice(error=-1)]
        best_offer_object = BestPrice(asset_id, 'O', worst_price,
                                      round_down(best_available_size,
                                                 l.size_decimal_places),
                                      l.exchange_id, numeraire_id,
                                      listing_id=l.id)
        return [best_offer_object]

    def get_best_offer_multi_exchange(self, asset_id, numeraire_id, size,
                                      exclude=None, force=None):
        # find all the listings that have this pair
        # This function doesn't support numeraire = 0
        listings = self.__get_listings(asset_id, numeraire_id, exclude, force)
        if not listings:
            return [BestPrice(
                error=-1)]  # returns an empty object if we did not find the correct listeners
        try:
            asset = next(
                asset for asset in self.assets if asset.id == asset_id)
            numeraire = next(
                asset for asset in self.assets if asset.id == numeraire_id)
            multi_order_book, min_size_decimal_places = self.create_combined_offers(
                listings)
            n = len(multi_order_book)
            filled_size = Decimal('0.0')
            cost = Decimal('0.0')
            i = 0
            orders = {}
            while (filled_size < size and i < n) or i == 0:
                offer = multi_order_book[i]
                if filled_size + offer[2] < size:
                    last_size = round_down(offer[2], min_size_decimal_places)
                else:
                    last_size = round_down(size - filled_size,
                                           min_size_decimal_places)
                    if size == 0:
                        avg_price = offer[1]
                filled_size += last_size
                cost += last_size * offer[1]
                if offer[3] not in orders:
                    orders[offer[3]] = (last_size, offer[1])
                else:
                    old = orders[offer[3]]
                    orders[offer[3]] = (last_size + old[0], offer[1])
                i += 1
            if filled_size > 0:
                avg_price = cost / filled_size
            if filled_size == size or size == 0:
                self.logger.info(
                    "Best Offer for %s in %s: Could get size %s at avg price %s with breakdown:" % (
                    asset.code, numeraire.code, filled_size, avg_price))
            else:
                self.logger.info(
                    "Best Offer for %s in %s: Could only get size %s out of %s at avg price %s with breakdown:" % (
                    asset.code, numeraire.code, filled_size, size, avg_price))
            if size == 0:
                listing = next(u for u in orders.keys())
                size, price = orders[listing]
                self.logger.info("      %s: %s available at price %s" % (
                listing.exchange.name, size, price))
                if price == 0:
                    output = [BestPrice(error=-1)]
                else:
                    output = [BestPrice(asset_id, 'O', price, size,
                                        listing.exchange.name, numeraire_id,
                                        listing_id=listing.id)]
            else:
                best_object_list = []
                for listing, value in orders.items():
                    offer_size, offer_price = value
                    self.logger.info("      %s: %s available at price %s" % (
                    listing.exchange.name, offer_size, offer_price))
                    if offer_size * offer_price != 0:
                        best = BestPrice(asset.id, 'O', offer_price,
                                         offer_size, listing.exchange.id,
                                         numeraire_id, listing_id=listing.id)
                        best_object_list.append(best)
                output = best_object_list
            return output
        except Exception as e:
            self.logger.exception(
                "Error in get best offer for %s in %s: %s" % (
                asset.code, numeraire.code, e))

    def get_composite(self, asset_id):
        # todo : build composite
        listings = self.listings.filter(composite__asset__id=asset_id)
        if listings:
            composite = listings[0].composite
            if composite.mode == 'Q':
                quotes = []
                for l in listings:
                    quote = self.datafeeds[l.id].get_last_quote
                    quotes.append(
                        quote)  # to be completed, there may be easy way to average lists etc
        return 1

    def change_numeraire(self, listing, new_numeraire):
        """
        :param listing: the listing to normalize
        :param numeraire_id: to numeraire to normalize to
        :return: a quote object with the price in the new numeraire
        If there is no rate betwen the 2 numeraires we might have an issue
        """
        quote = self.datafeeds[listing.id].get_last_quote()
        if listing.numeraire == new_numeraire:
            return quote
        if listing.numeraire.type == 'FT' and new_numeraire == 'FT':
            #   we need to get the FX rate
            rates = [rate for rate in self.fx_rates if
                     rate.base == new_numeraire and rate.quoted_id == listing.numeraire_id]
            if not rates:
                #  try the other way around
                rates = [rate for rate in self.fx_rates if
                         rate.base_id == listing.numeraire_id and rate.quoted == new_numeraire]
                if not rates:
                    return quote
                fx = rates[0].rate
            else:
                fx = 1 / rates[0].rate
            quote.bid *= fx
            quote.offer *= fx
        else:
            # we have a mix and match of fiat and crypto, need to find the corresponding listing
            listings = [lis for lis in self.listings if
                        lis.asset_id == listing.numeraire_id and lis.numeraire == new_numeraire]
            if not listings:
                listings = [lis for lis in self.listings if
                            lis.asset == new_numeraire and lis.numeraire_id == listing.numeraire_id]
                if not listings:
                    return quote
            # now find a feed on one of those listings
            feed = None
            for l in listings:
                if l.id in self.datafeeds.keys():
                    feed = self.datafeeds[l.id]
                    break
            if feed is not None:
                x_quote = feed.get_last_quote()
                # now do the conversion
                mid = (x_quote.bid + x_quote.offer) / 2
                if feed.listing.asset == listing.numeraire:
                    quote.bid *= mid
                    quote.offer *= mid
                else:
                    quote.bid /= mid
                    quote.offer /= mid
            else:
                return quote
        return quote

    def __get_listings(self, asset_id, numeraire_id, exclude=None, force=None):
        if numeraire_id == 0:
            listings = [listing for listing in self.listings if
                        listing.asset__id == asset_id]
        else:
            listings = [listing for listing in self.listings if
                        listing.asset_id == asset_id and listing.numeraire_id == numeraire_id]
        if exclude:
            listings = [listing for listing in listings if
                        listing.exchange_id not in exclude]
        elif force:
            listings = [listing for listing in listings if
                        listing.exchange_id == force]
        return listings

    async def purge_order_books(self, frequency):
        """
        This function purges the internal dataframes regularly to avoid consumming too much resources
        """
        while True:
            for key, feed in self.datafeeds.items():
                feed.order_book.trades.clear()
            await asyncio.sleep(frequency)

    def get_combined_orderbook(self, asset_id, numeraire_id, depth=-1,
                               exclude=None, force=None,
                               single_exchange=False):
        listings = self.__get_listings(asset_id, numeraire_id, exclude, force)
        bids = self.create_combined_bids(listings)[0][:depth]
        offers = self.create_combined_offers(listings)[0][:depth]
        return [CombinedOrderBook(asset_id, numeraire_id, depth, bids, offers)]

    def create_combined_bids(self, listings):
        multi_order_book = []
        min_size_decimal_places = 100
        for l in listings:
            brokerage = l.taker_fee
            min_size_decimal_places = min(min_size_decimal_places,
                                          l.size_decimal_places)
            try:
                if not self.datafeeds[l.id].is_alive():
                    self.logger.error(
                        "Dead datafeed : %s" % self.datafeeds[l.id].name)
                else:
                    quote = self.datafeeds[l.id].get_last_quote()
                    order_book = self.datafeeds[l.id].get_last_order_book()
                    if order_book is None or order_book.bids == {}:
                        ob = False
                    else:
                        ob = True
                    if ob:
                        for price, size_bid in order_book.bids.items():
                            bid_bro = price * (1 - brokerage / 10000)
                            multi_order_book.append(
                                (bid_bro, price, size_bid, l))
                    else:
                        if quote.error != -1 and quote.bid != 0:
                            bid_bro = quote.bid * (1 - brokerage / 10000)
                            multi_order_book.append(
                                (bid_bro, quote.bid, quote.bid_size, l))
                        else:
                            self.logger.error(
                                "Couldn't get quote nor order book for %s" % l.code)
            except Exception as e:
                self.logger.exception(
                    "Error while getting orderbook and quote for listing %s: %s" % (
                    l, e))
                continue
        multi_order_book.sort(key=lambda x: x[0], reverse=True)
        return multi_order_book, min_size_decimal_places

    def create_combined_offers(self, listings):
        multi_order_book = []
        min_size_decimal_places = 100
        for l in listings:
            brokerage = l.taker_fee
            min_size_decimal_places = min(min_size_decimal_places,
                                          l.size_decimal_places)
            try:
                if not self.datafeeds[l.id].is_alive():
                    self.logger.error(
                        "Dead datafeed : %s" % self.datafeeds[l.id].name)
                else:
                    quote = self.datafeeds[l.id].get_last_quote()
                    order_book = self.datafeeds[l.id].get_last_order_book()
                    if order_book is None or order_book.offers == {}:
                        ob = False
                    else:
                        ob = True
                    if ob:
                        for price, size_offer in order_book.offers.items():
                            if price != 0:
                                offer_bro = price * (1 + brokerage / 10000)
                                multi_order_book.append(
                                    (offer_bro, price, size_offer, l))
                    else:
                        if quote.error != -1 and quote.offer != 0:
                            offer_bro = quote.offer * (1 + brokerage / 10000)
                            multi_order_book.append(
                                (offer_bro, quote.offer, quote.offer_size, l))
                        else:
                            self.logger.error(
                                "Couldn't get quote nor order book for %s" % l.code)
            except Exception as e:
                self.logger.exception(
                    "Error while getting orderbook and quote for listing %s: %s" % (
                    l, e))
                continue
        multi_order_book.sort(key=lambda x: x[0], reverse=False)
        return multi_order_book, min_size_decimal_places
