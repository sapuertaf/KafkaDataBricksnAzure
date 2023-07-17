from email_creds import creds
import smtplib
import ssl
from typeguard import typechecked
from validate_email import validate_email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


PORT:int = 465 #puerto a usar para el envio del correo
MAIL_SERVER = "smtp.gmail.com" #servidor a usar para enviar el correo (debe de ser de acuerdo al correo del emisor)



class EmailSenderBot:
    @typechecked
    def __init__(
        self, 
        email_sender:str = creds["sender"], 
        email_sender_password:str = creds["password"]
    ):
        #verificar que el emisor tenga un correo valido
        assert validate_email(email_sender, verify=True), "La direccion de correo para el emisor dada es invalida"
        self.sender = email_sender 
        self.sender_password = email_sender_password



    @typechecked
    def set_mail_receptor(self, email_receptor:str):
        assert validate_email(email_receptor, verify=True), "La direccion de correo para el receptor dada es invalida"
        self.receptor = email_receptor



    @typechecked
    def struct_message(self, email_subject:str, body:str=""):
        self.email = MIMEMultipart() #se deben de crear tantos contextos de email como emails se deseen enviar
        self.email["To"] = self.receptor
        self.email["From"] = self.sender
        self.email["Subject"] = email_subject
        self.email.attach(MIMEText(body, "plain"))



    def send(self):
        #uso de SSM para tener una conexion segura al enviar el correo
        context_ = ssl.create_default_context()
        with smtplib.SMTP_SSL(MAIL_SERVER, PORT, context=context_) as smtp:
            if isinstance(self.sender, tuple):
                smtp.login(*self.sender, self.sender_password) #logearse desempaquetando la tupla
            smtp.login(self.sender, self.sender_password) #logearse
            smtp.sendmail(self.sender, self.receptor, self.email.as_string())



if __name__ == "__main__":
    sender = EmailSenderBot()
    sender.struct_message("santiago.puerta@quind.io","prueba","prueba")
    sender.send()