import requests
import json
import sys
import os
import random
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from plugins.config import Config

class Coingecko:
    def __init__(self):
        self._config = Config().get_config()

    def get_token_price_by_address(self, symbol):
        token_price_endpoint = (self._config['token']['coingecko']['endpoints']['base']
                                + self._config['token']['coingecko']['endpoints']['token_price']
                                + f'''?ids={symbol}&vs_currencies=usd'''.format(symbol))

        list_token = {s: random.uniform(100.0, 200.0) for s in symbol.split(',')}
        response = requests.get(token_price_endpoint)
        data = json.loads(response.text)
        for key, value in data.items():
            list_token[key.upper()] = value
        return list_token

    def get_config(self):
        return self._config

