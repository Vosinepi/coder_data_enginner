import sys
import pandas as pd
import datetime as dt

sys.path.append(".")

from util.db import engine
from cryptos_api import precio_historico

# descargo datos de cryptos
ultimos_365_dias = dt.datetime.now() - pd.offsets.Day(365)
periodo = ultimos_365_dias


after_date = periodo.strftime("%Y-%m-%d")

btc = precio_historico("btc", "kucoin", after=after_date)
eth = precio_historico("eth", "kucoin", after=after_date)
op = precio_historico("op", "kucoin", after=after_date)
bnb = precio_historico("bnb", "kucoin", after=after_date)
ada = precio_historico("ada", "kucoin", after=after_date)
doge = precio_historico("doge", "kucoin", after=after_date)
dot = precio_historico("dot", "kucoin", after=after_date)
xrp = precio_historico("xrp", "kucoin", after=after_date)
ltc = precio_historico("ltc", "kucoin", after=after_date)
link = precio_historico("link", "kucoin", after=after_date)
bch = precio_historico("bch", "kucoin", after=after_date)

# creo un diccionario con los datos de las criptomonedas
coins = {
    "btc": btc,
    "eth": eth,
    "op": op,
    "bnb": bnb,
    "ada": ada,
    "doge": doge,
    "dot": dot,
    "xrp": xrp,
    "ltc": ltc,
    "link": link,
    "bch": bch,
}

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
        {tabla[2]} INT,
        {tabla[3]} INT,
        {tabla[4]} INT,
        {tabla[5]} INT,
        {tabla[6]} INT,
        {tabla[7]} INT,
        PRIMARY KEY ({tabla[0]})
    );
    """

    conn.execute(query)
    conn.close()


# cargo en la tabla cryptos los datos de las criptomonedas


def load_db(coins):
    """
    La función `load_db` inserta datos de un diccionario de monedas en una tabla de base de datos
    llamada `cryptos`.

    :param coins: El parámetro `coins` es un diccionario que contiene información sobre diferentes
    criptomonedas. Cada clave del diccionario representa una criptomoneda y el valor correspondiente es
    otro diccionario que contiene datos para esa criptomoneda
    """
    conn = engine.connect()

    for crypto in coins:
        print(f"insertando datos de {crypto} en la tabla cryptos")

        for i in range(len(coins[crypto]["result"]["86400"])):
            timestamp = dt.datetime.fromtimestamp(
                coins[crypto]["result"]["86400"][i][0]
            )
            open_price = coins[crypto]["result"]["86400"][i][1]
            high_price = coins[crypto]["result"]["86400"][i][2]
            low_price = coins[crypto]["result"]["86400"][i][3]
            close_price = coins[crypto]["result"]["86400"][i][4]
            volume = coins[crypto]["result"]["86400"][i][5]
            na = coins[crypto]["result"]["86400"][i][6]

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
load_db(coins)
