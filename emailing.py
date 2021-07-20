import os
import smtplib
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase


def send_emails(TO, FROM="emailing.py", SUBJECT="", BODY="", SMTPserver='localhost', USER=None, PASS=None, attachements=()):

    if FROM=="emailing.py" and USER:
        FROM = USER

    # MIME INIT
    msg = MIMEMultipart()
    msg['From'] = FROM
    msg['To'] = ','.join(TO)
    msg['Subject'] = SUBJECT
    msg.attach(MIMEText(BODY, 'plain'))

    for file_path in attachements:
        with open(file_path, "rb") as f:
            part = MIMEBase('application', "octet-stream")
            part.set_payload(f.read())
            # Encoding payload is necessary if encoded (compressed) file has to be attached.
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(file_path)}")
            msg.attach(part)

    if SMTPserver == 'localhost':   # send mail from local server
        # Start local SMTP server
        server = smtplib.SMTP(SMTPserver)
        text = msg.as_string()
        server.send_message(msg)
    else:
        # Start SMTP ssl server
        server = smtplib.SMTP_SSL(SMTPserver)
        # Enter login credentials for the email you want to sent mail from
        server.login(USER, PASS)
        text = msg.as_string()
        # Send mail
        server.sendmail(FROM, TO, text)

    server.quit()
