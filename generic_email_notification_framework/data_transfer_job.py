import pandas as pd
from datetime import datetime
import uuid
import os

# Define the CSV file paths
source_csv_path = 'source_data.csv'
target_csv_path = 'target_data.csv'
job_info_csv_path = 'job_info.csv'


def load_job_data():
    """Load existing job data from job_info.csv or create an empty DataFrame if the file doesn't exist."""
    try:
        job_df = pd.read_csv(job_info_csv_path)
    except FileNotFoundError:
        job_df = pd.DataFrame(
            columns=["Job Id", "Job Name", "Job Status", "Execution Time", "Start Time", "End Time", "Row Count",
                     "Error Message"])
    return job_df

"""
def transfer_data():
    
    try:
        # Read data from source CSV
        source_df = pd.read_csv(source_csv_path)

        # Write data to target CSV (overwrite if exists)
        source_df.to_csv(target_csv_path, index=False)

        # Return success with row count and no error message
        return True, source_df.shape[0], ""
    except Exception as e:
        # If an error occurs, return failure, None for row count, and the error message
        return False, None, str(e)

"""


def transfer_data():
    """Simulate data transfer from source_data.csv to target_data.csv, returning job status, row count, and error message."""
    try:
        # Uncomment the line below to simulate a failure
        raise ValueError("Simulated data transfer error")

        # Normal data transfer (comment out to simulate a failure)
        source_df = pd.read_csv(source_csv_path)
        source_df.to_csv(target_csv_path, index=False)
        return False, source_df.shape[0], ""
    except Exception as e:
        # Return failure, None for row count, and the error message
        return False, None, str(e)


def add_job_entry():
    """Generate and add a new job entry to job_info.csv based on the transfer operation's success or failure."""
    # Load existing job data
    job_df = load_job_data()

    # Define job details
    job_id = str(uuid.uuid4())[:8]
    print(job_id)
    job_name = "Data Transfer Job"
    start_time = datetime.now()

    # Attempt data transfer and capture status, row count, and any error
    success, row_count, error_message = transfer_data()
    job_status = "Success" if success else "Failed"

    # Calculate end time and execution time
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()

    # Format times for CSV compatibility
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    date_str = start_time.strftime("%Y-%m-%d")
    # Create a new job dictionary
    new_job = pd.DataFrame([{
        "Job Id": job_id,
        "Job Name": job_name,
        "Recipient Email":"heetshah381997@gmail.com",
        "Job Status": job_status,
        "Date": date_str,
        "Execution Time": execution_time,
        "Start Time": start_time_str,
        "End Time": end_time_str,
        "Row Count": row_count if row_count is not None else "N/A",
        "Error Message": error_message,
        "Source Table" : source_csv_path,
        "Target Table" : target_csv_path
    }])

    # Append the new job to the DataFrame using pd.concat
    job_df = pd.concat([job_df, new_job], ignore_index=True)

    # Save updated DataFrame to job_info.csv
    job_df.to_csv(job_info_csv_path, index=False)
    print(f"Job entry added successfully: {new_job.to_dict(orient='records')}")


# Run the function to automatically add a job entry
if __name__ == "__main__":
    add_job_entry()
