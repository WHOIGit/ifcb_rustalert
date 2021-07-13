import pytz
import argparse
import requests
import pandas as pd
import datetime as dt
from time import sleep

CREDS = ('admin','password')
OUTLET_HOST = 'http://127.0.0.1'
OUTLET_ID = 0

def get_outlet(url_prefix, credentials, outlet):
    r = requests.get(
        f'{url_prefix}/restapi/relay/outlets/{outlet}/physical_state/',
        auth=credentials
    )
    return r.json()

def set_outlet(url_prefix, credentials, outlet, state):
    requests.put(
        f'{url_prefix}/restapi/relay/outlets/{outlet}/state/',
        data={'value': 'true' if state else 'false'},
        headers={'X-CSRF': 'asdf'},
        auth=credentials
    )


def rust_demo(bins, threshold, pump_timer, simulation_secs_per_bin=1):
    # Calculating pump-off cycles
    pump_off = None
    for idx, row in bins.iterrows():
        sleep(simulation_secs_per_bin)
        print(idx, row['sample_time'].isoformat()[:-6], round(row['Margalefidinium_perL']), 'perL')

        # if value above threshold, note start time
        if row[series] >= threshold:
            if pump_off is None:
                print('  Pump turned OFF')
                pump_off = row['sample_time']
                set_outlet(OUTLET_HOST,CREDS,OUTLET_ID,False)
                sleep(1)
                assert get_outlet(OUTLET_HOST,CREDS,OUTLET_ID)==False, 'Outlet state not False'

        # if perL below threshold and pump timer has elapsed, note time to turn pump on again
        elif pump_off and row['sample_time'] - pump_off > dt.timedelta(hours=pump_timer):
            print('  Pump back ON')
            pump_off = None
            set_outlet(OUTLET_HOST, CREDS, OUTLET_ID, True)
            sleep(1)
            assert get_outlet(OUTLET_HOST,CREDS,OUTLET_ID)==True, 'Outlet state not True'

    print('SIMULATION FINISHED')



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A CLI tool for monitoring and responding to algal blooms.')
    parser.add_argument('--threshold', required=True, type=int,
        help='Threshold ppm beyond which valves and emails are triggered')
    parser.add_argument('--pump-timer', default=1.5, type=float,
        help='Minimum amount of time to turn power off for in hours. Default is "1.5" hours')
    parser.add_argument('--simulation-secs-per-bin', default=1, type=float,
        help='How many seconds to wait between bins in seconds. Default is "1"')

    args = parser.parse_args()

    series = 'Margalefidinium_perL'
    bins = pd.read_csv(f'{series}.csv', index_col='pid', parse_dates=['sample_time'])

    rust_demo(bins, args.threshold, args.pump_timer, simulation_secs_per_bin=args.simulation_secs_per_bin)
