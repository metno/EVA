import smtplib
import email.mime.text

import eva.mail.text


class Mailer(object):
    """!
    @brief A class that simplifies sending e-mail from EVA.
    """
    def __init__(self, group_id, smtp_host, mail_from, recipients):
        """!
        @param group_id str The EVA instance ID, also known as group_id
        @param smtp_host str Which SMTP server to use for sending e-mails
        @param mail_from str The source e-mail address from which mail originates
        @param recipients list A list strings with mail recipients
        """
        self.group_id = group_id
        self.smtp_host = smtp_host
        self.mail_from = mail_from
        self.recipients = recipients

    def send_email(self, subject, text):
        """!
        @brief Send an e-mail to the pre-configured recipients.
        """
        text = eva.mail.text.MASTER_TEXT % text
        message = email.mime.text.MIMEText(text)
        message['Subject'] = eva.mail.text.MASTER_SUBJECT % (self.group_id, subject)
        try:
            mailer = smtplib.SMTP(self.smtp_host)
            mailer.send_message(message, from_addr=self.mail_from, to_addrs=self.recipients)
            mailer.quit()
        except smtplib.SMTPException:
            # silently ignore errors
            pass


class NullMailer(object):
    """!
    @brief The NullMailer class acts as the Mailer class, but is a no-op.
    """
    def send_email(self, *args, **kwargs):
        pass
