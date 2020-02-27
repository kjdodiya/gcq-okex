import asyncio
import json

import websockets
from Listener.abstraction.tools import decompress_okex_message


async def subscribe_OKX(feed, websocket):
    try:
        if feed.listing.asset.type == 'FT':
            # we request depth and trades
            request_list = [
                "futures/depth5:{symbol}".format(symbol=feed.listing.symbol),
                "futures/trade:{symbol}".format(symbol=feed.listing.symbol)]
            for string_request in request_list:
                request = {"op": "subscribe",
                           "args": string_request}
                request = json.dumps(request)
                # print('okx req {}'.format(request))
                await websocket.send(request)
                msg = await asyncio.wait_for(websocket.recv(),
                                             timeout=30)  # ack message
                # print('okx resp {}'.format(decompress_okex_message(msg)))
        elif feed.listing.asset.type == 'CN':
            # we request depth and trades 
            request_list = [
                "spot/depth5:{symbol}".format(symbol=feed.listing.symbol),
                "spot/trade:{symbol}".format(symbol=feed.listing.symbol)]
            for string_request in request_list:
                request = {"op": "subscribe",
                           "args": string_request}
                request = json.dumps(request)
                # print('okx req {}'.format(request))
                await websocket.send(request)
                msg = await asyncio.wait_for(websocket.recv(),
                                             timeout=30)  # ack message
                print('okx resp {}'.format(decompress_okex_message(msg)))
        elif feed.listing.asset.type == 'SW':
            # we request depth and trades 
            request_list = [
                "swap/depth5:{symbol}".format(symbol=feed.listing.symbol),
                "swap/trade:{symbol}".format(symbol=feed.listing.symbol)]
            for string_request in request_list:
                request = {"op": "subscribe",
                           "args": string_request}
                request = json.dumps(request)
                # print('okx req {}'.format(request))
                await websocket.send(request)
                msg = await asyncio.wait_for(websocket.recv(),
                                             timeout=30)  # ack message
                # print('okx resp {}'.format(decompress_okex_message(msg)))

    except Exception as e:
        print('subscribe OKEX {}'.format(e))
    finally:
        print('ws_subscriptions websocket returned')
        return websocket


async def subscribe_CEX(feed, websocket):
    nonce, signature = feed.cex.sign_ws()
    request = "{'e': 'auth','auth': {'key': %s, 'signature': %s, 'timestamp': %s}, 'oid': 'auth',}" % (
        feed.cex.api_key, signature, nonce)
    try:
        # await websocket.send(request)  # auth request
        # msg = await asyncio.wait_for(websocket.recv(), timeout=20)
        # print(msg)
        request = json.dumps(
            {"e": "subscribe", "rooms": ["pair-" + feed.listing.symbol]})
        await websocket.send(request)  # request data
        msg = await asyncio.wait_for(websocket.recv(), timeout=20)
    # print(msg)
    except Exception as e:
        print(e)
    finally:
        return websocket


async def subscribe_BFX(feed, websocket):
    try:
        # another request for the order book
        request = json.dumps({"event": "subscribe", "channel": "book",
                              "symbol": feed.listing.symbol, "prec": "P0",
                              "freq": "F0", "len": 25})
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
