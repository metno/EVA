import smtplib
import email.mime.text

import eva.config
import eva.globe
import eva.mail.text


class Mailer(eva.config.ConfigurableObject, eva.globe.GlobalMixin):
    """!
    @brief A class that simplifies sending e-mail from EVA.
    """

    CONFIG = {
        'enabled': {
            'type': 'bool',
            'help': 'Send e-mails to product owner when something unexpected happens',
            'default': 'NO',
        },
        'from': {
            'type': 'string',
            'help': 'EVA sender e-mail address',
            'default': 'eva@localhost',
        },
        'recipients': {
            'type': 'list_string',
            'help': 'List of recipients of e-mails from EVA',
            'default': '',
        },
        'smtp_host': {
            'type': 'string',
            'help': 'Which SMTP server to use when sending e-mails',
            'default': '127.0.0.1',
        },
    }

    OPTIONAL_CONFIG = [
        'enabled',
        'from',
        'recipients',
        'smtp_host',
    ]

    def _factory(self):
        """!
        @brief Instantiate a mailer class, if configured.
        """
        if not self.env['enabled']:
            return self
        if not self.env['recipients']:
            raise eva.exceptions.InvalidConfigurationException(
                "The 'recipients' option must be configured when e-mails are enabled."
            )
        return self

    def send_email(self, subject, text):
        """!
        @brief Send an e-mail to the pre-configured recipients.
        """
        if not self.env['enabled']:
            return

        text = eva.mail.text.MASTER_TEXT % text
        message = email.mime.text.MIMEText(text)
        message['Subject'] = eva.mail.text.MASTER_SUBJECT % (self.group_id, subject)
        try:
            mailer = smtplib.SMTP(self.env['smtp_host'])
            mailer.send_message(message, from_addr=self.env['from'], to_addrs=self.env['recipients'])
            mailer.quit()
        except smtplib.SMTPException:
            # silently ignore errors
            pass
