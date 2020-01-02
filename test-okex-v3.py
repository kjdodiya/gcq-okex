
import okex.account_api as account
import okex.ett_api as ett
import okex.futures_api as future
import okex.lever_api as lever
import okex.spot_api as spot
import okex.swap_api as swap
import json

from requests_futures.sessions import FuturesSession

# ['API_KEY', 'API_SECRET_KEY', 'PASSPHRASE', '__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__', '__init_subclass__', '__le__', '__lt__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_get_timestamp', '_request', '_request_with_params', '_request_without_params', 'get_account_info', 'get_coin_account_info', 'get_coin_info', 'get_deal', 'get_depth', 'get_fills', 'get_kline', 'get_ledger_record', 'get_order_info', 'get_orders_list', 'get_orders_pending', 'get_specific_ticker', 'get_ticker', 'revoke_order', 'revoke_orders', 'take_order', 'take_orders', 'use_server_time']


if __name__ == '__main__':

    api_key = "87e34868-d3c1-4b3b-9752-62a154349569"
    seceret_key = "EE8534F26BD53DE380D51604BD67D957"
    passphrase = "kamalftw"



    spotAPI = spot.SpotAPI(api_key, seceret_key, passphrase, True)
    # result = spotAPI.get_account_info()
    # print (result)
    # result = spotAPI.get_coin_account_info('BTC')
    # print (result)
    # result = spotAPI.get_ledger_record('BTC', limit=1)
    # print (result)
    # result = spotAPI.get_coin_info()
    # print (result)
    # result = spotAPI.get_depth('LTC-USDT')
    # print (result)
    # result = spotAPI.get_ticker()
    # print (result)
    # result = spotAPI.get_specific_ticker('LTC-USDT')
    # print (result)
    # print (dir(spotAPI))
    # result = spotAPI.get_ticker()
    # print (result)
    # result = spotAPI.get_specific_ticker('LTC-USDT')
    # print (result)