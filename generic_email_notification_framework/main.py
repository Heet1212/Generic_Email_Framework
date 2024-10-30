from email_sender import EmailSender

def main():
    # Path to the job info CSV file and email template
    job_info_csv = "job_info.csv"
    template_file = "email_template.html"
    # Send email notifications based on job data in CSV
    EmailSender.send_email_from_csv(job_info_csv)

if __name__ == "__main__":
    main()
