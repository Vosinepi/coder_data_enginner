import pandas as pd
import datetime as dt
import smtplib
from email.message import EmailMessage

from airflow.models import Variable  # type: ignore

# credenciales smtp
user = Variable.get("SECRET_MAIL_USER")
password = Variable.get("SECRET_MAIL_PASS")


def enviar_mails(asunto, mensaje, destinatario):
    # format the email message with color a text size

    contenido_html = """
                    <html>
                    <body>
                    <p><span style="font-size: 24px;">AVISO: </p></span>
                    <p><span style="font-size: 24px;"><strong>{}</strong></span>.</p>
                    </body>
                    </html>
                    """.format(
        mensaje
    )

    print(mensaje)

    # creacion del mail
    mensaje = EmailMessage()
    mensaje["Subject"] = asunto
    mensaje["From"] = "etl_coder@gmail.com"
    mensaje["To"] = destinatario
    mensaje.set_content(contenido_html, subtype="html")

    # conexion smtp

    server = smtplib.SMTP_SSL("smtp.gmail.com")

    server.login(user, password)

    # envio de mail

    server.send_message(mensaje)

    server.quit()

    # Actualizar la variable de Airflow para indicar que la tarea se ejecutó hoy

    print("Se envio el mail")
