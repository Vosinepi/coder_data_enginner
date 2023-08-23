import sys

sys.path.append(".")

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import URL

from util.settings import db_name, db_user, db_password, db_host, db_port

SQL_DATABASE_URL = URL.create(
    drivername="redshift+redshift_connector",
    username=db_user,
    password=db_password,
    host=db_host,
    port=db_port,
    database=db_name,
)


engine = create_engine(SQL_DATABASE_URL)

inspector = inspect(engine)
