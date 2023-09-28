import sys

sys.path.append(".")

from psycopg2 import connect

from .settings import db_name, db_user, db_password, db_host, db_port

# Conecto a la base de datos
conn = connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port,
)

# Creo cursor
cur = conn.cursor()
