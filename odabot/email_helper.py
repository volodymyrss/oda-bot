from __future__ import annotations

from odabot.cli import logger
import requests
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

class MissingEnvironmentVariable(Exception): ...

def send_email(
    to: str | list[str], 
    subject: str, 
    text: str, 
    attachments: list[str] | None = None,
    cc: str | list[str] = [], 
    bcc: str | list[str] = [], 
    method: str = 'smtp'
    ):

    try:
        if method == 'smtp':
            for name in ('EMAIL_SMTP_SERVER', 
                         'EMAIL_SMTP_USER',
                         'EMAIL_SMTP_PASSWORD',
                         ):
                if os.getenv(name) is None:
                    raise MissingEnvironmentVariable(f"'{name}' environment variable is not set")

            msg = MIMEMultipart()
            part1 = MIMEText(text, "plain")
            msg.attach(part1)

            if attachments is not None:
                for attachment in attachments:
                    with open(attachment, 'rb') as fd:
                        part = MIMEApplication(fd.read())
                    part.add_header("Content-Disposition",
                                    f"attachment; filename= {attachment.split('/')[-1]}")
                    msg.attach(part)

            msg['Subject'] = subject
            email_from = os.getenv('EMAIL_FROM', os.getenv('EMAIL_SMTP_USER'))
            msg['From'] = email_from  # type: ignore

            if to: msg['To'] = ', '.join(to) if isinstance(to, list) else to
            if cc: msg['Cc'] = ', '.join(cc) if isinstance(cc, list) else cc
            if bcc: msg['Bcc'] = ', '.join(bcc) if isinstance(bcc, list) else bcc

            if os.getenv('EMAIL_SMTP_ENCRYPTION') == 'SSL':
                with smtplib.SMTP_SSL(os.environ['EMAIL_SMTP_SERVER'],
                                      port=int(os.getenv('EMAIL_SMTP_PORT', 465))) as smtp:
                    smtp.login(os.environ['EMAIL_SMTP_USER'], os.environ['EMAIL_SMTP_PASSWORD'])
                    smtp.send_message(msg)
            elif os.getenv('EMAIL_SMTP_ENCRYPTION') == 'TLS':
                with smtplib.SMTP(os.environ['EMAIL_SMTP_SERVER'],
                                  port=int(os.getenv('EMAIL_SMTP_PORT', 587))) as smtp:
                    smtp.starttls()
                    smtp.login(os.environ['EMAIL_SMTP_USER'], os.environ['EMAIL_SMTP_PASSWORD'])
                    smtp.send_message(msg)
            else: 
                # assume no encryption
                with smtplib.SMTP(os.environ['EMAIL_SMTP_SERVER'],
                                  port=int(os.getenv('EMAIL_SMTP_PORT', 25))) as smtp:
                    smtp.login(os.environ['EMAIL_SMTP_USER'], os.environ['EMAIL_SMTP_PASSWORD'])
                    smtp.send_message(msg)
        
        else:
            raise NotImplementedError(f'E-mail sending method {method} not implemented.')

    except Exception as e:
        logger.exception('Exception while sending email: %s', e)