# Generic Email Notification Framework

## Overview
This project demonstrates a generic framework for sending email notifications for job monitoring, providing reusable functions to avoid code redundancy.

## Structure
- `email_config.py`: Handles email sending logic with SMTP server configuration.
- `email_sender.py`: Sends success and failure email notifications using HTML templates.
- `notification_service.py`: Monitors job execution and sends notifications.
- `job_monitor.py`: Contains example jobs for success and failure scenarios.
- `main.py`: Entry point to run the project.

## Setup
1. Create a virtual environment: `python3 -m venv venv`
2. Activate the environment:
   - On Linux/macOS: `source venv/bin/activate`
   - On Windows: `venv\Scripts\activate`
3. Install the required packages: `pip install -r requirements.txt`

## Run the project
- Execute `main.py` to monitor jobs and trigger email notifications.
