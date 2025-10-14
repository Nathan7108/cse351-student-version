"""
Course    : CSE 351
Assignment: 04
Student   : <your name here>

Instructions:
    - review instructions in the course

In order to retrieve a weather record from the server, Use the URL:

f'{TOP_API_URL}/record/{name}/{recno}

where:

name: name of the city
recno: record number starting from 0

"""

import time
import threading
import queue
import requests
from common import *

from cse351 import *

THREADS = 120
WORKERS = 10
RECORDS_TO_RETRIEVE = 5000
COMMAND_BATCH = 1000


# ---------------------------------------------------------------------------
def retrieve_weather_data(cmd_q, data_q, noaa):
    session = requests.Session()

    while True:
        commands = []
        try:
            for _ in range(COMMAND_BATCH):
                cmd = cmd_q.get_nowait()
                if cmd is None:
                    return
                commands.append(cmd)
        except:
            pass

        if not commands:
            try:
                cmd = cmd_q.get(timeout=1)
                if cmd is None:
                    return
                commands.append(cmd)
            except:
                continue

        for city, recno in commands:
            url = f"{TOP_API_URL}/record/{city}/{recno}"
            try:
                resp = session.get(url, timeout=10)
                resp.raise_for_status()
                item = resp.json()
                if item and 'temp' in item:
                    with noaa.city_locks[city]:
                        noaa.temps[city].append(item['temp'])
            except:
                response = get_data_from_server(url)
                if response and 'temp' in response:
                    with noaa.city_locks[city]:
                        noaa.temps[city].append(response['temp'])


# ---------------------------------------------------------------------------
class Worker(threading.Thread):

    def __init__(self, noaa):
        threading.Thread.__init__(self)
        self.noaa = noaa

    def run(self):
        pass


# ---------------------------------------------------------------------------
class NOAA:

    def __init__(self):
        self.temps = {city: [] for city in CITIES}
        self.city_locks = {city: threading.Lock() for city in CITIES}
        self.read_lock = threading.Lock()

    def get_temp_details(self, city):
        with self.read_lock:
            values = self.temps[city]
            if values:
                return sum(values) / len(values)
        return 0.0


# ---------------------------------------------------------------------------
def verify_noaa_results(noaa):

    answers = {
        'sandiego': 14.5004,
        'philadelphia': 14.865,
        'san_antonio': 14.638,
        'san_jose': 14.5756,
        'new_york': 14.6472,
        'houston': 14.591,
        'dallas': 14.835,
        'chicago': 14.6584,
        'los_angeles': 15.2346,
        'phoenix': 12.4404,
    }

    print()
    print('NOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        answer = answers[name]
        avg = noaa.get_temp_details(name)

        if abs(avg - answer) > 0.00001:
            msg = f'FAILED  Expected {answer}'
        else:
            msg = f'PASSED'
        print(f'{name:>15}: {avg:<10} {msg}')
    print('===================================')


# ---------------------------------------------------------------------------
def main():

    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    noaa = NOAA()

    data = get_data_from_server(f'{TOP_API_URL}/start')

    print('Retrieving city details')
    city_details = {}
    name = 'City'
    print(f'{name:>15}: Records')
    print('===================================')
    for name in CITIES:
        city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
        print(f'{name:>15}: Records = {city_details[name]['records']:,}')
    print('===================================')

    records = RECORDS_TO_RETRIEVE

    cmd_q = queue.Queue(maxsize=10)

    retrievers = []
    for _ in range(THREADS):
        t = threading.Thread(target=retrieve_weather_data, args=(cmd_q, None, noaa))
        t.start()
        retrievers.append(t)

    workers = []
    for _ in range(WORKERS):
        w = Worker(noaa)
        w.start()
        workers.append(w)

    for city in CITIES:
        for rec in range(records):
            cmd_q.put((city, rec))

    for _ in range(THREADS):
        cmd_q.put(None)

    for t in retrievers:
        t.join()

    for w in workers:
        w.join()

    data = get_data_from_server(f'{TOP_API_URL}/end')
    print(data)

    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')


if __name__ == '__main__':
    main()
