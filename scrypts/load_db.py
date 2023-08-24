import sys
import pandas as pd
import datetime as dt

sys.path.append(".")

from util.db import engine
from cryptos_api import precio_historico


# defino el periodo de tiempo a descargar
ultimos_365_dias = dt.datetime.now() - pd.offsets.Day(365)
periodo = ultimos_365_dias


# defino las criptomonedas a descargar
coins = [
    "btc",
    "eth",
    "op",
    "bnb",
    "ada",
    "doge",
    "dot",
    "xrp",
    "ltc",
    "link",
    "bch",
]


# descargo datos de cryptos
def crypto_activo(coins):
    after_date = periodo.strftime("%Y-%m-%d")
    cryptos = {}
    for coin in coins:
        print(f"Descargando datos de {coin}")
        cryptos[coin] = precio_historico(coin, "kucoin", after=after_date)
        print(f"Datos de {coin} descargados")
    print(cryptos.keys())
    return cryptos


# creo la tabla ismaelpiovani_coderhouse.cryptos
tabla = (
    "Coin",
    "CloseTime",
    "OpenPrice",
    "HighPrice",
    "LowPrice",
    "ClosePrice",
    "Volume",
    "NA",
)


def make_table():
    """
    La función `make_table` crea una tabla en una base de datos si aún no existe.
    """
    conn = engine.connect()
    query = f"""
    CREATE TABLE IF NOT EXISTS ismaelpiovani_coderhouse.cryptos (
        {tabla[0]} VARCHAR(255) NOT NULL,
        {tabla[1]} DATE,
        {tabla[2]} FLOAT,
        {tabla[3]} FLOAT,
        {tabla[4]} FLOAT,
        {tabla[5]} FLOAT,
        {tabla[6]} FLOAT,
        {tabla[7]} FLOAT,
        PRIMARY KEY ({tabla[0]})
    );
    """

    conn.execute(query)
    conn.close()


# cargo en la tabla cryptos los datos de las criptomonedas


def load_db(list_of_coins):
    """
    La función `load_db` inserta datos de un diccionario de monedas en una tabla de base de datos
    llamada `cryptos`.

    :param coins: El parámetro `coins` es un diccionario que contiene información sobre diferentes
    criptomonedas. Cada clave del diccionario representa una criptomoneda y el valor correspondiente es
    otro diccionario que contiene datos para esa criptomoneda
    """
    conn = engine.connect()

    for crypto in list_of_coins:
        print(f"insertando datos de {crypto} en la tabla cryptos")

        for i in range(len(list_of_coins[crypto]["result"]["86400"])):
            timestamp = dt.datetime.fromtimestamp(
                list_of_coins[crypto]["result"]["86400"][i][0]
            )
            open_price = list_of_coins[crypto]["result"]["86400"][i][1]
            high_price = list_of_coins[crypto]["result"]["86400"][i][2]
            low_price = list_of_coins[crypto]["result"]["86400"][i][3]
            close_price = list_of_coins[crypto]["result"]["86400"][i][4]
            volume = list_of_coins[crypto]["result"]["86400"][i][5]
            na = list_of_coins[crypto]["result"]["86400"][i][6]

            # Verificar si el registro ya existe
            query_check = f"""
            SELECT 1
            FROM ismaelpiovani_coderhouse.cryptos
            WHERE coin = '{crypto}' AND closetime = '{timestamp}';
            """

            result = conn.execute(query_check)

            if (
                not result.fetchone()
            ):  # Si no se encuentra el registro, entonces insertarlo
                query_insert = f"""
                INSERT INTO ismaelpiovani_coderhouse.cryptos
                (coin, closetime, openprice, highprice, lowprice, closeprice, volume, na)
                VALUES (
                    '{crypto}',
                    '{timestamp}',
                    {open_price},
                    {high_price},
                    {low_price},
                    {close_price},
                    {volume},
                    {na}
                );
                """

                try:
                    conn.execute(query_insert)
                except Exception as e:
                    print(f"Error al insertar datos de {crypto}: {str(e)}")

        print(f"datos de {crypto} insertados en la tabla cryptos")

    conn.close()


make_table()
load_db(crypto_activo(coins))
