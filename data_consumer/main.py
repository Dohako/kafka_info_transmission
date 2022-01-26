from multiprocessing import Process, Event
from consumer import start_consumer
from loguru import logger
from os import name as os_name
from time import time, sleep
import subprocess

def make_tests():
    testing_status = run_tests()
    if testing_status != 0:
        logger.error("Tests not cleared, need update")
        raise Exception("Tests not cleared, need update")
    logger.info("tests cleared")

def start_consumer_process():
    logger.info("Starting consumer")
    event = Event()
    consumer_process = Process(target=start_consumer, kwargs={'event' : event})
    consumer_process.start()
    return consumer_process, event

def check_git() -> str:
    """
    check if not windows(not developer machine presumably)
    make git status
    if git status says that is not up to date -> git pull
    :return: "Updated" / "Not update"
    """
    if os_name == 'nt':
        return "Not updated"
    result = subprocess.run("git status",
                            stdout=subprocess.PIPE, 
                            stderr=subprocess.PIPE, 
                            check=True, text=True)
    if "is up to date" not in result.stdout:
        pull_result = subprocess.run("git pull",
                                        stdout=subprocess.PIPE, 
                                        stderr=subprocess.PIPE, 
                                        check=True, text=True)
        # I assume that PULL is always a success, but I know it is not
        #TBD Checking
        return "Updated"
    return "Not updated"

def run_tests():
    if os_name == 'nt':
        retcode = subprocess.call("../venv/Scripts/python.exe ./test/test_consumer.py")
    else:
        retcode = subprocess.call("python ./test/test_consumer.py")
    return retcode

def process_handler():
    make_tests()
    checking_time = int(time())
    consumer_process, consumer_event = start_consumer_process()

    while True:
        # every 5 seconds checking scripts and restarting them if needed
        current_time = int(time())
        if current_time > checking_time + 1:
            checking_time = int(time())

            # if repo is updated - reload script gentle
            if check_git() == "Updated":
                make_tests()
                if consumer_process.is_alive() is True:
                    consumer_event.set()
                    consumer_process.join()
                consumer_process, consumer_event = start_consumer_process()

            # checking Consumer
            if consumer_process.is_alive() is False:
                consumer_process, consumer_event = start_consumer_process()

if __name__ == "__main__":
    logger.add('./logs/log.log')
    while True:
        try:
            process_handler()
        except KeyboardInterrupt:
            quit()
        except Exception as ex:
            logger.error(ex)
            sleep(15)
