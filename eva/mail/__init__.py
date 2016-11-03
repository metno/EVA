import smtplib
import email.policy
import email.mime.text

import eva.mail.text


class Mailer(object):
    """!
    @brief A class that simplifies sending e-mail from EVA.
    """
    def __init__(self, group_id, smtp_host, mail_from, recipients):
        self.mailer = smtplib.SMTP(smtp_host)
        self.group_id = group_id
        self.mail_from = mail_from
        self.policy = email.policy.EmailPolicy()
        self.recipients = recipients

    def send_email(self, recipients, subject, text):
        """!
        @brief Send an e-mail to selected recipients. If the class is
        instantiated with a override recipient list, it is used instead.
        """
        if self.recipients:
            recipients = self.recipients
        text = eva.mail.text.MASTER_TEXT % text
        message = email.mime.text.MIMEText(text)
        message['Subject'] = eva.mail.text.MASTER_SUBJECT % (self.group_id, subject)
        self.mailer.connect()
        self.mailer.send_message(message, from_addr=self.mail_from, to_addrs=recipients)
        self.mailer.quit()


class NullMailer(object):
    """!
    @brief The NullMailer class acts as the Mailer class, but is a no-op.
    """
    def send_email(self, *args, **kwargs):
        pass
