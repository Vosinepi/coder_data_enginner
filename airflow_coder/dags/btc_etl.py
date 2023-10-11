from datetime import datetime, timedelta
import os
import sys

from airflow import DAG  # type: ignore
from airflow.decorators import dag  # type: ignore
from airflow.models import Variable  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.email import EmailOperator  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore

# Agregar la carpeta 'plugins' al PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "plugins")))

from load_db import make_table, load_datab, coins, ultimos_datos_cargados  # type: ignore

# variables

api_key = Variable.get("SECRET_API_KEY")
api_secret = Variable.get("SECRET_SECRET_KEY")

db_name = Variable.get("DB_NAME")
db_user = Variable.get("DB_USER")
db_password = Variable.get("SECRET_DB_PASS")
db_host = Variable.get("SECRET_DB_HOST")
db_schema = Variable.get("DB_SCHEMA")
db_port = Variable.get("DB_PORT")

# direccion de correo para alertas modificar de ser necesario

if Variable.get("destinatario", default_var=None) is None:
    Variable.set("destinatario", "ismaelpiovani@gmail.com")
else:
    pass

destinatario = Variable.get("destinatario")

# variable para email con reporte de datos cargados

if Variable.get("cantidad_dias") is None:
    Variable.set("cantidad_dias", 1)
else:
    pass

periodo = Variable.get("cantidad_dias")

# Creo instancia de DAG se ejecuta a diario a las 11:30am UTC
dag = DAG(
    dag_id="crypto_etl",
    schedule_interval="30 11 * * *",  # todos los dias a las 11:30am UTC
    catchup=False,
    default_args={
        "owner": "airflow",
        "start_date": datetime(2023, 9, 27),
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "email": ["ismaelpiovani@gmail.com"],
        "email_on_failure": True,
        "email_on_retry": True,
    },
)


# Tarea 0: Crear tabla cryptos
t0 = PythonOperator(
    task_id="create_table",
    python_callable=make_table,
    op_args=[db_name, db_user, db_password, db_host, db_port],
    dag=dag,
)
# Tarea 1: Cargar datos en la tabla cryptos
t1 = PythonOperator(
    task_id="load_db",
    python_callable=load_datab,
    op_args=[
        coins,
        api_key,
        api_secret,
        db_name,
        db_user,
        db_password,
        db_host,
        db_port,
    ],
    dag=dag,
)

t2 = PythonOperator(
    task_id="ultimo_datos_cargados",
    python_callable=ultimos_datos_cargados,
    op_args=[
        destinatario,
        db_name,
        db_user,
        db_password,
        db_host,
        db_port,
        periodo,
    ],
    dag=dag,
)

t3 = EmailOperator(
    task_id="send_email",
    to=destinatario,
    subject="Crypto ETL",
    html_content="<h3>Tareas concluidas</h3>",
    dag=dag,
)

# Tarea 2: echo de confirmacion de carga de datos
t4 = BashOperator(
    task_id="echo",
    bash_command="echo 'Tareas concluidas'",
    dag=dag,
)

t0 >> t1 >> t2 >> t3 >> t4
