import subprocess
import time


def is_process_running(process_name):
    try:
        output = subprocess.check_output(['pgrep', '-f', process_name])
        return True
    except subprocess.CalledProcessError:
        return False


def start_process(process_name):
    subprocess.Popen(['python3', process_name])


def main():
    process_name = "google_scraping.py"

    while True:
        if not is_process_running(process_name):
            print("Process is not running, starting...")
            start_process(process_name)
        else:
            print("Process is running.")

        time.sleep(5)  # Check every 5 seconds


if __name__ == "__main__":
    main()
