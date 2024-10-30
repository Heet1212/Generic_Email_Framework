import time

def success_job():
    time.sleep(2)  # Simulate job execution
    print("Success job completed.")

def failure_job():
    time.sleep(2)  # Simulate job execution
    raise Exception("Failure job encountered an error.")
