import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class EmailConfig:
    SMTP_SERVER = 'smtp.gmail.com'
    SMTP_PORT = 465
    SENDER_EMAIL = 'vishwashah1104@gmail.com'
    SENDER_PASSWORD = 'vypuwcsdtxxpwsgx'  # Use App Password if 2FA is enabled

    @staticmethod
    def send_email(receiver_email, subject, html_content):
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = EmailConfig.SENDER_EMAIL
        msg['To'] = receiver_email

        msg.attach(MIMEText(html_content, 'html'))

        try:
            # Use SMTP_SSL for a direct SSL connection
            with smtplib.SMTP_SSL(EmailConfig.SMTP_SERVER, EmailConfig.SMTP_PORT) as server:
                server.login(EmailConfig.SENDER_EMAIL, EmailConfig.SENDER_PASSWORD)
                server.sendmail(EmailConfig.SENDER_EMAIL, receiver_email, msg.as_string())
                print(f"Email sent to {receiver_email} successfully.")
        except smtplib.SMTPException as e:
            print(f"Failed to send email: {e}")
        except Exception as e:
            print(f"General error: {e}")

# Testing sending an email
