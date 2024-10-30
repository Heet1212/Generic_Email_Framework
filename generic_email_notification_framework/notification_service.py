from email_sender import EmailSender
import logging

class NotificationService:
    def __init__(self, receiver_email):
        self.receiver_email = receiver_email
        logging.basicConfig(filename='job_log.log', level=logging.INFO)

    def monitor_job(self, job_name, job_callable, execution_time):
        try:
            logging.info(f"Running job {job_name}...")
            job_callable()
            EmailSender.send_success_notification(job_name, self.receiver_email, execution_time)
            logging.info(f"Job {job_name} completed successfully.")
        except Exception as e:
            logging.error(f"Job {job_name} failed: {e}")
            EmailSender.send_failure_notification(job_name, self.receiver_email, str(e), execution_time)
