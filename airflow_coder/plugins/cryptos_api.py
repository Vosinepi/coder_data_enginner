import requests as req
import pandas as pd


# Función para obtener el precio de una criptomoneda en un momento dado
def precio_historico(
    symbol,
    exchange="binance",
    after="2020-01-01",
    api_key=None,
    api_secret=None,
):
    """
    La función `precio_historico` recupera datos históricos de precios para un símbolo de criptomoneda
    determinado de un intercambio específico a partir de una fecha específica.

    :param symbol: El parámetro de símbolo representa el símbolo o teletipo de la criptomoneda. Por
    ejemplo, si desea obtener los datos históricos del precio de Bitcoin, deberá pasar 'btc' como
    parámetro de símbolo
    :param exchange: El parámetro de intercambio especifica el intercambio de criptomonedas del que
    desea recuperar los datos históricos de precios. El valor predeterminado es 'binance', pero puedes
    especificar cualquier otro intercambio admitido, defaults to binance (optional)
    :param after: El parámetro 'después' se utiliza para especificar la fecha de inicio de los datos de
    precios históricos. Tiene el formato 'AAAA-MM-DD' y el valor predeterminado es '2020-01-01' si no se
    proporciona, defaults to 2020-01-01 (optional)
    :return: datos históricos de precios para un símbolo de criptomoneda determinado en un intercambio
    específico.
    """
    if api_key is None or api_secret is None:
        raise Exception("API Key and Secret required")

    else:
        url = "https://api.cryptowat.ch/markets/{exchage}/{symbol}usdc/ohlc".format(
            exchage=exchange, symbol=symbol
        )
        # genero el response con la api_key y api_secret
        header = {"X-CW-API-Key": api_key, "X-CW-API-Secret": api_secret}

        response = req.get(
            url,
            params={
                "periods": "86400",
                "after": str(int(pd.Timestamp(after).timestamp())),
            },
            headers=header,
        )

        # response = req.get(
        #     url,
        #     params={"periods": "86400", "after": str(int(pd.Timestamp(after).timestamp()))},
        # )
        response.raise_for_status()
        data = response.json()

        return data
