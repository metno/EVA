"""!
@brief A collection of strings that are used to compose e-mail messages.
"""


# The top-level template for all e-mails sent by EVA
MASTER_SUBJECT = 'EVA %s: %s'
MASTER_TEXT = """Hi,

%s

Best regards,
The EVent Adapter automatic mailer"""

# Message sent when a job fails
JOB_FAIL_SUBJECT = 'Job failed'
JOB_FAIL_TEXT = """I'm sorry to inform you that your job has failed. Your job will be retried, and
you will get an e-mail as soon as the job succeeds.

Note that you will not receive e-mails about further failures of this event.

Job ID:        %(job_id)s
Adapter:       %(adapter)s
Failure count: %(failures)d
Job status:    %(status)s
"""

# Message sent when a job recovers
JOB_RECOVER_SUBJECT = 'Job succeeded after %(failures)d failures'
JOB_RECOVER_TEXT = """Your previously failing job has finally succeeded.

Job ID:        %(job_id)s
Adapter:       %(adapter)s
Failure count: %(failures)d
Job status:    %(status)s
"""

# Message sent upon a critical error
CRITICAL_ERROR_SUBJECT = "%(error_message)s"
CRITICAL_ERROR_TEXT = """I'm terribly sorry, but EVA has encountered a critical error which caused the program to crash.

%(error_message)s

%(backtrace)s
"""
