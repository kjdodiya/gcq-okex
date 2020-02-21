import asyncio
import json
import websockets
from OrderManager.exchanges.okex3 import Okex

from OrderManager.exchanges.cex import CEX


async def subscribe_OKX(feed, websocket):
    try:
        if feed.listing.asset.type == 'FT':
            future_type = Okex.roll_contract(None, feed.listing.asset.future)
            request = "{'event':'addChannel','channel':'ok_sub_futureusd_%s_depth_%s_%s'}" % (feed.listing.symbol[:3], future_type, feed.listing.exchange.order_book_depth)
            await websocket.send(request)
            msg = await asyncio.wait_for(websocket.recv(), timeout=20)  # ack message
            # second request for the trade data
            request = "{'event':'addChannel','channel':'ok_sub_futureusd_%s_trade_%s'}" % (feed.listing.symbol[:3], future_type)
            await websocket.send(request)
            msg = await asyncio.wait_for(websocket.recv(), timeout=20)  # ack message
        elif feed.listing.asset.type == 'CN':
            # we request depth and trades as one
            request = {"op": 1,
                       "args":
                       ["spot/depth5:{symbol}".format(symbol=feed.listing.symbol),
                        "spot/trades:{symbol}".format(symbol=feed.listing.symbol)]}
            request = json.dumps(request)
            print('okx req {}'.format(request))
            # request = "{'event':'addChannel','channel':'ok_sub_spot_%s_depth_%s'}" % (feed.listing.symbol, feed.listing.exchange.order_book_depth)
            await websocket.send(request)
            msg = await asyncio.wait_for(websocket.recv(), timeout=20)  # ack message
            print(msg)
           
           #  # second request for the trade data
           #  request = "{'event':'addChannel','channel':'ok_sub_spot_%s_deals'}" % (feed.listing.symbol)
           #  await websocket.send(request)
           #  msg = await asyncio.wait_for(websocket.recv(), timeout=20)  # ack message
    except Exception as e:
            print(e)
    finally:
            return websocket


async def subscribe_CEX(feed, websocket):
        nonce, signature = feed.cex.sign_ws()
        request = "{'e': 'auth','auth': {'key': %s, 'signature': %s, 'timestamp': %s}, 'oid': 'auth',}" % (
                feed.cex.api_key, signature, nonce)
        try:
                #await websocket.send(request)  # auth request
                #msg = await asyncio.wait_for(websocket.recv(), timeout=20)
                #print(msg)
                request = json.dumps({"e": "subscribe", "rooms": ["pair-" + feed.listing.symbol]})
                await websocket.send(request)  # request data
                msg = await asyncio.wait_for(websocket.recv(), timeout=20)
                #print(msg)
        except Exception as e:
                print(e)
        finally:
                return websocket


async def subscribe_BFX(feed, websocket):
        try:
                # another request for the order book
                request = json.dumps({"event": "subscribe", "channel": "book", "symbol": feed.listing.symbol, "prec": "P0", "freq": "F0", "len": 25})
                await websocket.send(request)
                try:
                        msg = await asyncio.wait_for(websocket.recv(), timeout=20)
                        print(msg)
                except Exception as e:
                        print("Websocket connection error\n{}".format(e))
                        print('Reconnecting')
                        websocket = await websockets.connect(request)
        except Exception as e:
                print(e)
        finally:
                return websocket
