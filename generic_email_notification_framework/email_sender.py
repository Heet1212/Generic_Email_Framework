import pandas as pd
from email_config import EmailConfig
from jinja2 import Environment, FileSystemLoader

class EmailSender:
    @staticmethod
    def send_email_from_csv(job_info_csv):
        # Load job info CSV data into a DataFrame
        job_info_df = pd.read_csv(job_info_csv)

        # Initialize Jinja2 environment to load templates
        env = Environment(loader=FileSystemLoader('templates'))

        # Iterate through each job entry
        for _, row in job_info_df.iterrows():
            # Extract job information
            recipient_email = row['Recipient Email']
            job_name = row['Job Name']
            job_id = row['Job Id']
            job_status = row['Job Status']
            execution_time = row['Execution Time']
            error_message = row['Error Message'] if job_status == 'Failed' else ""
            date = row['Date']
            start_time = row['Start Time']
            end_time = row['End Time']
            row_count = row['Row Count'] if job_status == 'Success' else "N/A"
            source_table = row["Source Table"]  # Replace with actual table name if applicable
            target_table = row["Target Table"]
            # Define the email context
            base_context = {
                'recipient_name': recipient_email.split("@")[0],
                'job_name': job_name,
                'job_id': job_id,
                'job_status': job_status,
                'execution_time': execution_time,
                'date': date,
                'start_time': start_time,
                'end_time': end_time
            }

            # Select the correct template and context based on job status
            if job_status == 'Success':
                # Add row count and table name to context for success emails
                context = {**base_context, 'row_count': row_count, 'source_table': source_table,"target_table":target_table}
                template_file = 'success_template.html'
            else:
                # Add error message to context for failure emails
                context = {**base_context, 'error_message': error_message,'source_table': source_table}
                template_file = 'failure_template.html'

            template = env.get_template(template_file)
            html_content = template.render(context)

            # Set email subject
            subject = f"Job {job_name} {job_status}"

            # Send the email
            try:
                EmailConfig.send_email(recipient_email, subject, html_content)
                print(f"Email sent successfully to {recipient_email} for job {job_name}.")
            except Exception as e:
                print(f"Failed to send email to {recipient_email} for job {job_name}. Error: {e}")

# Example usage
if __name__ == "__main__":
    EmailSender.send_email_from_csv('job_info.csv')
