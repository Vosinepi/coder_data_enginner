## Iber Ismael Piovani

Carga de datos desde una API a un datawarehouse Redshift

## Objetivo

Tomar los datos de paises de una API externa e inyectarlos en una base de datos Redshift.

## Requerimientos

- Python 3.8 o superior

## Dependencias

- [pandas](https://pandas.pydata.org/)
- [SQLAlchemy](https://www.sqlalchemy.org/)
- [sqlalchemy-redshift]
- [redshift_connector]
- [requests](https://requests.readthedocs.io/en/master/)
- [psycopg2](https://www.psycopg.org/docs/)
- [python-dotenv]

## Uso

- Clonar el repositorio

```
git clone
```

- Crear un entorno virtual

```
python -m venv venv
```

- Activar el entorno virtual
- Instalar las dependencias
- Modificar las variables de entorno necesarias en el .yaml
- Crear contenedor de airflow para automatizar el proceso, correr el docker-compose-airflow.yaml

```
docker-compose -f docker-compose-airflow.yaml up -d
```

- localhost:8081

```
Correos y alertas
```

- Se crea un variable con un "destinatario" destinatario definido. Si es necesario se puede modificar desde el DAG o luego desde el GUI de airflow.

- El correo de alerta de ultimos datos cargados por defecto muestra el ultimo dia cargado pero se puede modificar la variable "cantidad_dias" para definir el intervalo de datos.

```

```

## Contacto

- [Linkedin](https://www.linkedin.com/in/iber-ismael-piovani-8b35bbba/)
- [Twitter](https://twitter.com/laimas)
- [Github](https://github.com/Vosinepi)

```

```
