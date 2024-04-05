import psutil
import subprocess
import time

def is_process_running(process_name):
    for proc in psutil.process_iter(['pid', 'name']):
        if process_name.lower() in proc.info['name'].lower():
            return True
    return False

def start_process(process_path):
    subprocess.Popen(['python', process_path])

def main():
    process_name = "google_scraping.py"
    process_path = "./google_scraping.py"  # Change this to the actual path of your checup.py script

    while True:
        if not is_process_running(process_name):
            print("Process is not running, starting...")
            start_process(process_path)
        else:
            print("Process is running.")

        time.sleep(5)  # Check every 5 seconds

if __name__ == "__main__":
    main()
