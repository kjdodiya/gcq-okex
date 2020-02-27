__author__ = 'Nicolas'

import sys
import traceback
from decimal import Decimal

import django
from CryptoTrader.tools.functions import side_to_sign, reverse_side
from Listener.classes import Quote, BestPrice
from Messaging.robust_zmq import RobustZmqReqSync
from basis.models import Listing, Exchange
# replacement logger
from core.logger import set_logger


# todo : add a function that tests if a listener is reachable and does not block

class MarketLink(object):

    def __init__(self, port=7777, logger=None):
        self.port = port
        # zmq_context = zmq.Context()
        # self.broker_socket = zmq_context.broker_socket(zmq.REQ)
        # self.broker_socket.connect("tcp://localhost:" + str(self.port))  # connect for the client
        self.agent = RobustZmqReqSync(server_endpoint=
                                      "tcp://localhost:" + str(self.port))
        if logger is None:
            self.logger = set_logger(
                "Market Link")  # get_logger('MarketLink', '../logs/marketlink.log')
        else:
            self.logger = logger
        # self.broker_socket.connect("tcp://10.193.219.50:9032")  # jabba
        # self.broker_socket.connect("tcp://10.193.219.87:9032")  # bobba fett

    def get_quote(self, listing):
        """
        :param Listing id: The listing ID we want the quote of
        :return:
        """
        quote = Quote()
        if isinstance(listing, int):
            id = listing
        elif isinstance(listing, Listing):
            id = listing.id
        else:
            return Quote(error=-2)
        try:
            req = {'call': 'get_quote', 'listing_id': id}

            message = self.agent.send_json(json_msg=req)

            if message != -1:
                print('MarketLink msg - {}'.format(message))
                quote.fromJSON(message[0])

            else:
                quote = Quote(error=-1)
        except Exception as err:
            traceback.print_exc(file=sys.stdout)
            self.logger.exception("Exception in MarketLink {}".format(err))
            message = "ZMQ Exception in get_quote : {}".format(err)
            django.db.close_old_connections()
            django.db.connection.close()
            print('marketlink - closing django connection to db,\
                 should auto reconnect')
            print(message)
            return None
        return quote

    def get_best(self, asset, numeraire, side, needed_size=0, excluded=None,
                 force_exchange=None, single_exchange=False):
        best_list = []
        if isinstance(asset, int):
            asset_id = asset
        else:
            asset_id = asset.id
        if isinstance(numeraire, int):
            numeraire_id = numeraire
        else:
            numeraire_id = numeraire.id
        try:
            if side == 'B':
                command = 'get_best_bid'
            else:
                command = 'get_best_offer'
            req = {'call': command, 'asset_id': asset_id,
                   'numeraire_id': numeraire_id,
                   'needed_size': str(needed_size),
                   'single_exchange': single_exchange, 'force_exchange': None,
                   'excluded': None}
            if excluded is not None:
                req['excluded'] = excluded
            if force_exchange:
                if isinstance(force_exchange, Exchange):
                    req['force_exchange'] = force_exchange.id
                elif isinstance(force_exchange, int):
                    req['force_exchange'] = force_exchange

            # Get the reply.
            message = self.agent.send_json(json_msg=req)

            if not isinstance(message, str):
                for price in message:
                    best = BestPrice()
                    best.fromJSON(price)
                    best_list.append(best)
            else:
                best = BestPrice(error=-1)
        except Exception as e:
            self.logger.exception("Exception in get_best")
            best.error = -1
            print("ZMQ Exception in get_best() : %s" % e)
            django.db.close_old_connections()
            django.db.connection.close()
            print(
                'marketlink - closing django connection to db, should auto reconnect')
            pass
        if needed_size == 0 or single_exchange:
            return [best]
        else:
            return best_list

    def get_spread(self, listing1, listing2, mode='abs'):
        """Compute the spread between a two listeners and returns it as a dictionary
        Modes are abs for abslute value of the spread or bps for percentage of the spread in bps
        Takes an id as argument rather than an object because the frontend only stores ids in the session
        """
        spread = {}
        spread['error'] = False
        spread['bid'] = 0
        spread['offer'] = 0
        quote1 = self.get_quote(listing1)
        quote2 = self.get_quote(listing2)
        try:
            if quote1.error != 0 or quote2.error != 0 or quote1.bid == 0 or quote2.bid == 0:
                spread['error'] = True
            spread['bid'] = quote1.bid - quote2.offer
            spread['offer'] = quote1.offer - quote2.bid
            if mode == 'bps' and quote2.offer != 0 and quote2.bid != 0:
                spread['bid'] = Decimal(spread['bid'] / quote2.offer * 10000)
                spread['offer'] = Decimal(spread['offer'] / quote2.bid * 10000)
        except Exception as e:
            self.logger.exception("Exception in get_spread")
            spread['error'] = True
            django.db.close_old_connections()
            django.db.connection.close()
            print(
                'marketlink - closing django connection to db, should auto reconnect')
        return spread

    def get_future_basis(self, listing, mode, fees=False):
        """Compute the spread between a two listeners and returns it as a dictionary
        Modes are abs for abslute value of the spread or bps for percentage of the spread in bps
        """
        spread = {}
        spread['error'] = False
        spread['bid'] = 0
        spread['offer'] = 0
        quote = self.get_quote(listing)
        if quote.error == -1:
            spread['error'] = True
            return spread
        best_bid = self.get_best(listing.asset.future.underlying,
                                 listing.asset.future.numeraire, 'B')[0]
        if best_bid.error == 0 and best_bid.price > 0:
            best_bid_listing = Listing.objects.get(asset__id=best_bid.asset_id,
                                                   exchange__id=best_bid.exchange_id,
                                                   numeraire__id=best_bid.numeraire_id)
            exchange_name = best_bid_listing.exchange.name
            print("Best bid for %s: %s on %s" % (
            best_bid_listing.asset.code, best_bid.price, exchange_name))
        else:
            exchange_name = 'ERROR'
        best_offer = self.get_best(listing.asset.future.underlying,
                                   listing.asset.future.numeraire, 'O')[0]
        if best_offer.error == 0 and best_offer.price > 0:
            best_offer_listing = Listing.objects.get(
                asset__id=best_offer.asset_id,
                exchange__id=best_offer.exchange_id,
                numeraire__id=best_offer.numeraire_id)
            exchange_name = best_offer_listing.exchange.name
            print("Best offer for %s: %s on %s" % (
            best_offer_listing.asset.code, best_offer.price, exchange_name))
        else:
            exchange_name = 'ERROR'
        try:
            if quote.error != 0 or best_bid.error != 0 or best_offer.error != 0 or quote.bid == 0 or quote.offer == 0 or best_offer.price == 0 or best_bid.price == 0:
                spread['error'] = True
            spread['bid'] = quote.bid - best_offer.price
            spread['offer'] = quote.offer - best_bid.price
            if mode == 'bps' and best_offer.price != 0 and best_bid.price != 0:
                spread['bid'] = Decimal(
                    spread['bid'] / best_offer.price * 10000)
                spread['offer'] = Decimal(
                    spread['offer'] / best_bid.price * 10000)
                if fees:
                    spread[
                        'bid'] -= listing.maker_fee  # + best_offer_listing.taker_fee)
                    spread[
                        'offer'] += listing.maker_fee  # + best_bid_listing.taker_fee)
        except Exception as e:
            self.logger.exception("Exception in get_basis")
            spread['error'] = True
            django.db.close_old_connections()
            django.db.connection.close()
            print(
                'marketlink - closing django connection to db, should auto reconnect')
        return spread

    def get_triangle(self, triangle):
        """Compute the spread between a two listeners and returns it as a dictionary
        Modes are abs for abslute value of the spread or bps for percentage of the spread in bps
        Takes an id as argument rather than an object because the frontend only stores ids in the session
        """
        tri_price = {}
        tri_price['error'] = False
        tri_price['bid'] = 0
        tri_price['offer'] = 0
        quote1 = self.get_quote(triangle.listing1)
        quote2 = self.get_quote(triangle.listing2)
        quote3 = self.get_quote(triangle.listing3)
        try:
            if quote1.error != 0 or quote2.error != 0 or quote1.bid == 0 or quote2.bid == 0 or quote3.bid == 0 or quote3.bid == 0:
                tri_price['error'] = True
            tri_price['offer'] = quote1.get_side('O')
            tri_price['offer'] *= quote2.get_side(
                reverse_side(triangle.listing2_way)) ** side_to_sign(
                triangle.listing2_way)
            tri_price['offer'] *= quote3.get_side(
                reverse_side(triangle.listing3_way)) ** side_to_sign(
                triangle.listing3_way)
            tri_price['bid'] = quote1.get_side('B') ** -1
            tri_price['bid'] *= quote2.get_side(
                triangle.listing2_way) ** side_to_sign(
                reverse_side(triangle.listing2_way))
            tri_price['bid'] *= quote3.get_side(
                triangle.listing3_way) ** side_to_sign(
                reverse_side(triangle.listing3_way))
            tri_price['bid'] = (tri_price['bid'] - 1) * 10000
            tri_price['offer'] = (tri_price['offer'] - 1) * 10000
        except Exception as e:
            self.logger.exception("Exception in get_triangle")
            tri_price['error'] = True
            tri_price['err_msg'] = e
            django.db.close_old_connections()
            django.db.connection.close()
            print(
                'marketlink - closing django connection to db, should auto reconnect')
        return tri_price

    def get_combined_orderbook(self, asset, numeraire, depth=-1, excluded=None,
                               force_exchange=None, single_exchange=False):
        if isinstance(asset, int):
            asset_id = asset
        else:
            asset_id = asset.id
        if isinstance(numeraire, int):
            numeraire_id = numeraire
        else:
            numeraire_id = numeraire.id
        try:
            req = {'call': 'get_combined_orderbook', 'asset_id': asset_id,
                   'numeraire_id': numeraire_id, 'depth': depth,
                   'single_exchange': single_exchange, 'force_exchange': None,
                   'excluded': None}
            if excluded is not None:
                req['excluded'] = excluded
            if force_exchange:
                if isinstance(force_exchange, Exchange):
                    req['force_exchange'] = force_exchange.id
                elif isinstance(force_exchange, int):
                    req['force_exchange'] = force_exchange
            # self.broker_socket.send_json(req)
            # Get the reply.
            # message = self.broker_socket.recv_json()
            message = self.agent.send_json(json_msg=req)
        except Exception as e:
            self.logger.exception("Exception in get_combined_orderbook")
            print("ZMQ Exception in get_best_combined_orderbook() : %s" % e)
            django.db.close_old_connections()
            django.db.connection.close()
            print(
                'marketlink - closing django connection to db, should auto reconnect')
            pass
        return message[0]
