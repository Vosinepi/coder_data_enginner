import os
from dotenv import load_dotenv

load_dotenv()

# El código utiliza la función `os.getenv()` para recuperar variables de entorno.
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASS")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_schema = "ismaelpiovani_coderhouse"
api_key = os.getenv("API_KEY")
api_secret = os.getenv("SECRET_KEY")
