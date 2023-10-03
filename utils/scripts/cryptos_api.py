import sys

sys.path.append(".")

import requests as req
import pandas as pd
import datetime as dt
from binance import Client
from settings import api_key, api_secret


# Función para obtener el precio de una criptomoneda en un momento dado
def precio_historico(
    symbol,
    api_key,
    api_secret,
    after="2020-01-01 00:00:00",
):
    if api_key is None or api_secret is None:
        raise Exception("API Key and Secret required")

    else:
        client = Client(api_key, api_secret)
        # get history price
        today = dt.date.today()

        klines = client.get_historical_klines(
            symbol,
            Client.KLINE_INTERVAL_1DAY,
            after,
            today.strftime("%d %b %Y %H:%M:%S"),
        )
        data = pd.DataFrame(klines)
        if data.empty:
            return data
        else:
            data = data.iloc[:, 0:6]
            print(data)
            data.columns = [
                "OpenTime",
                "OpenPrice",
                "HighPrice",
                "LowPrice",
                "ClosePrice",
                "Volume",
            ]
            data["OpenTime"] = pd.to_datetime(data["OpenTime"], unit="ms")
            data["OpenPrice"] = data["OpenPrice"].astype(float)
            data["HighPrice"] = data["HighPrice"].astype(float)
            data["LowPrice"] = data["LowPrice"].astype(float)
            data["ClosePrice"] = data["ClosePrice"].astype(float)
            data["OpenTime"] = pd.to_datetime(data["OpenTime"], unit="s")
            data.reset_index(drop=True, inplace=True)

            return data


# print(precio_historico("BTCUSDT", api_key, api_secret, after="02 Oct 2023 00:00:00"))
