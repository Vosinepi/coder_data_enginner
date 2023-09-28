from datetime import datetime, timedelta

from airflow import DAG  # type: ignore
from airflow.decorators import dag  # type: ignore

from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.email import EmailOperator  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore


from scripts.load_db import make_table, load_datab, coins


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
    dag=dag,
)
# Tarea 1: Cargar datos en la tabla cryptos
t1 = PythonOperator(
    task_id="load_db",
    python_callable=load_datab,
    op_args=[coins],
    dag=dag,
)

""""No esta configurado el SMTP por cuestion de seguridad"""

# t2 = EmailOperator(
#     task_id="send_email",  # No esta configurado el SMTP por cuestion de seguridad
#     to="ismaelpiovani@gmail.com",
#     subject="Crypto ETL",
#     html_content="<h3>Los datos fueron cargados correctamente</h3>",
#     dag=dag,
# )

# Tarea 2: echo de confirmacion de carga de datos
t2 = BashOperator(
    task_id="send_email",
    bash_command="echo 'Los datos fueron cargados correctamente'",
    dag=dag,
)

t0 >> t1 >> t2