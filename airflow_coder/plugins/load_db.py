import pandas as pd
import datetime as dt


from db import ddbb_conection
from cryptos_api import precio_historico


# defino el periodo de tiempo a descargar
dias_de_descarga = 365 * 2
periodo = (dt.datetime.now() - pd.offsets.Day(dias_de_descarga)).strftime("%d %b %Y ")


# defino las criptomonedas a descargar
coins = [
    "BTCUSDT",
    "ETHUSDT",
    "OPUSDT",
    "BNBUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "DOTUSDT",
    "XRPUSDT",
    "LTCUSDT",
    "LINKUSDT",
    "BCHUSDT",
]

# creo la tabla ismaelpiovani_coderhouse.cryptos
tabla = (
    "coin",
    "opentime",
    "openprice",
    "highprice",
    "lowprice",
    "closeprice",
    "volume",
)


# descargo datos de cryptos
def crypto_activo(coins, api_key, api_secret, conn):
    """
    La función `crypto_activo` recupera la última fecha de datos de una tabla y luego descarga datos
    históricos de precios para una lista de criptomonedas.

    :param coins: El parámetro "monedas" es una lista de símbolos de criptomonedas. Representa las
    criptomonedas para las que desea descargar datos históricos de precios
    :return: La función `crypto_activo` devuelve un diccionario `cryptos` que contiene los datos
    históricos del precio de cada moneda en la lista `coins`.
    """
    # busco en la tabla la ultima fecha de datos

    cur = conn.cursor()

    query = f"""
    SELECT opentime
    FROM ismaelpiovani_coderhouse.cryptos
    ORDER BY opentime DESC
    LIMIT 1;
    """

    cur.execute(query)
    try:
        last_date = cur.fetchone()[0]
    except TypeError:
        last_date = None

    print(f"La ultima fecha de datos es {last_date}")
    print(dt.date.today().strftime("%d %b %Y %H:%M:%S"))

    cryptos = {}

    if last_date is None:
        after_date = periodo
    else:
        after_date = (last_date).strftime("%d %b %Y %H:%M:%S")

        print(f"La fecha de datos de after date es {after_date}")

    for coin in coins:
        print(f"Descargando datos de {coin}")
        cryptos[coin] = precio_historico(coin, api_key, api_secret, after_date)
        print(f"Datos de {coin} descargados")

    return cryptos


# creo la tabla cryptos en la base de datos
def make_table(db_name, db_user, db_password, db_host, db_port):
    """
    La función `make_table` crea una tabla en una base de datos si aún no existe.
    """
    conn = ddbb_conection(db_name, db_user, db_password, db_host, db_port)

    cur = conn.cursor()

    query = f"""
    CREATE TABLE IF NOT EXISTS ismaelpiovani_coderhouse.cryptos (
        {tabla[0]} VARCHAR(255) NOT NULL,
        {tabla[1]} DATE,
        {tabla[2]} FLOAT,
        {tabla[3]} FLOAT,
        {tabla[4]} FLOAT,
        {tabla[5]} FLOAT,
        {tabla[6]} FLOAT,
        PRIMARY KEY ({tabla[0]})
    );
    """

    cur.execute(query)
    conn.commit()
    conn.close()


# cargo en la tabla cryptos los datos de las criptomonedas usando COPY
def load_datab(
    coins, api_key, api_secret, db_name, db_user, db_password, db_host, db_port
):
    """
    La función `load_db` inserta datos de un diccionario de monedas en una tabla de base de datos
    llamada `cryptos`.

    :param coins: El parámetro `coins` es un diccionario que contiene información sobre diferentes
    criptomonedas. Cada clave del diccionario representa una criptomoneda y el valor correspondiente es
    otro diccionario que contiene datos para esa criptomoneda
    """
    # Conecto a la base de datos
    conn = ddbb_conection(db_name, db_user, db_password, db_host, db_port)

    # descargo los datos de las criptomonedas
    list_of_coins = crypto_activo(coins, api_key, api_secret, conn)

    cur = conn.cursor()
    if list_of_coins == {}:
        print("No hay datos nuevos para descargar")
    else:
        for crypto in list_of_coins:
            print(f"insertando datos de {crypto} en la tabla cryptos")

            # paso los values a un dataframe
            dataframe = (
                "OpenTime",
                "OpenPrice",
                "HighPrice",
                "LowPrice",
                "ClosePrice",
                "Volume",
            )

            df = pd.DataFrame(list_of_coins[crypto], columns=dataframe)
            df["Coin"] = crypto
            df["OpenTime"] = pd.to_datetime(df["OpenTime"], unit="s")
            df["OpenTime"] = df["OpenTime"].dt.date

            df = df[
                [
                    "Coin",
                    "OpenTime",
                    "OpenPrice",
                    "HighPrice",
                    "LowPrice",
                    "ClosePrice",
                    "Volume",
                ]
            ]

            print(f"datos de {crypto} convertidos a dataframe")
            print(df)
            # si el df esta vacio no realizo la query
            if df.empty:
                print(f"no hay datos nuevos de {crypto}")
                continue

            else:
                # compruebo que los datos a insertar con esa fecha no existan en la tabla
                query = f"""
                SELECT opentime
                FROM ismaelpiovani_coderhouse.cryptos
                WHERE opentime = '{df.iloc[0,1]}' AND coin = '{crypto}'
                """
                cur.execute(query)
                result = cur.fetchone()
                if result is not None:
                    print(f"los datos de {crypto} ya estan en la tabla cryptos")
                    continue
                else:
                    # inserto los datos en la tabla cryptos
                    print(f"insertando datos de {crypto} en la tabla cryptos")
                    query = "INSERT INTO ismaelpiovani_coderhouse.cryptos (coin, opentime, openprice, highprice, lowprice, closeprice, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"

                    a_tabla = df.to_records(index=False).tolist()
                    cur.executemany(query, a_tabla)

                    conn.commit()
                    print(f"datos de {crypto} insertados en la tabla cryptos")

        cur.close()
        print("Todos los datos ya estan cargados en la tabla cryptos")
        print("Base de datos cerrada")
