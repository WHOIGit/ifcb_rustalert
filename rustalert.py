import argparse
import datetime as dt
import os.path
from time import sleep
from smtplib import SMTP_SSL as SMTP
from urllib.error import HTTPError
from email.mime.text import MIMEText
import shlex

import pandas as pd
import requests
import pytz

from emailing import send_emails

def list_bins(url_prefix, dataset, instrument, start_date, end_date=None):
    url = f'{url_prefix}/api/list_bins'

    if isinstance(start_date,dt.datetime):
        start_date = start_date.isoformat(timespec='seconds')
    if isinstance(end_date, dt.datetime):
        end_date = end_date.isoformat(timespec='seconds')

    params = {'dataset': dataset,
              'instrument': instrument,
              'start_date': start_date,
              'skip_filter': 'exclude' }
    if end_date:
        params['end_date'] = end_date

    r = requests.get(url, params=params)
    df = pd.DataFrame(r.json()['data'])
    if df.empty: return df

    df = df.set_index('pid')
    df['sample_time'] = pd.to_datetime(df['sample_time'])
    del df['skip']
    return df


def get_bin_meta(url_prefix, bin_id):
    url = f'{url_prefix}/api/bin/{bin_id}'
    r = requests.get(url, params={'include_coordinates': 'false'})
    return r.json()


def get_class_scores(url_prefix, dataset, bin_id):
    url = f'{url_prefix}/{dataset}/{bin_id}_class_scores.csv'
    return pd.read_csv(url, index_col='pid')


def get_outlet(url_prefix, credentials, outlet):
    url = f'{url_prefix}/restapi/relay/outlets/{outlet}/physical_state/'
    r = requests.get(url, auth=credentials, timeout=7)
    return r.json()


def set_outlet(url_prefix, credentials, outlet, state):
    url = f'{url_prefix}/restapi/relay/outlets/{outlet}/state/'
    requests.put(url, auth=credentials,
        data={'value': 'true' if state else 'false'},
        headers={'X-CSRF': 'asdf'} )


def set_pumpOff_aeratorOn(pump_args,aerator_args):
    set_outlet(*pump_args,False)
    sleep(1)
    assert get_outlet(*pump_args) is False
    set_outlet(*aerator_args,True)
    sleep(1)
    assert get_outlet(*aerator_args) is True


def set_pumpOn_aeratorOff(pump_args,aerator_args):
    set_outlet(*pump_args,True)
    sleep(1)
    assert get_outlet(*pump_args) is True
    set_outlet(*aerator_args,False)
    sleep(1)
    assert get_outlet(*aerator_args) is False

def get_pump_timer(fname):
    if os.path.isfile(fname) and os.path.getsize(fname) > 0:
        with open(fname) as f:
            return pd.to_datetime(f.read())
    return None

def set_pump_timer(fname,timestamp):
    if timestamp is None:
        timestamp = ''
    elif isinstance(timestamp,pd.Timestamp):
        timestamp = timestamp.isoformat()
    elif isinstance(timestamp,dt.datetime):
        timestamp = timestamp.isoformat(timespec='seconds')
    with open(fname, 'w') as f:
        f.write(timestamp)


def update_datafile(args):
    # 1) collect new bin list
    now = dt.datetime.now(pytz.UTC)
    now = now-dt.timedelta(microseconds=now.microsecond) # dump microseconds
    poll_date = now-dt.timedelta(hours=3)
    poll_date = poll_date.isoformat(timespec='hours')
    if args.v>=2: print(f'Fetching Bins since {poll_date} from {args.dashboard.replace("https://","")} {args.dataset} {args.ifcb}')
    bin_df = list_bins(args.dashboard, args.dataset, args.ifcb, start_date=poll_date)
    if args.v>=3: print('  '+'\n  '.join(list(bin_df.index)))
    if args.v>2: print(f'  {len(bin_df)} bins fetched')

    # 2) load datafile, append new bins to df, else create new df
    if args.v>=2: print(f'Loading Datafile: {args.datafile}')
    try:
        df = pd.read_csv(args.datafile, index_col='pid', parse_dates=['sample_time'])
        df = df.combine_first(bin_df)
    except FileNotFoundError:
        if args.v: print('  FileNotFound: one will be created')
        df = bin_df
        df['bin_ml'] = float('nan')
        df['bin_added'] = float('nan')
        df['taxon_count'] = float('nan')
        df['taxon_perL'] = float('nan')
        df['taxon_added'] = float('nan')

    # 3) collect new ml for any new bins
    if args.v: print('Collecting bin_ml values')
    def do_bin_ml(row):
        if pd.isna(row['bin_ml']):
            if args.v: print(f'  {row.name}: ', end='')
            try:
                d = get_bin_meta(args.dashboard, row.name)
                row['bin_ml'] = float(d['ml_analyzed'].rstrip(' ml'))
                row['bin_added'] = now
                if args.v: print(row['bin_ml'],'ml')
            except Exception as e:
                if args.v: print(f'NaN ({e})')
        return row
    df = df.apply(do_bin_ml, axis='columns')

    # 4) collect class scores for any missing
    if args.v: print('Collecting class scores and calculating counts')
    def taxon_per_liter(row):
        if pd.isna(row['taxon_count']):
            if args.v: print(f'  {row.name}: ', end='')
            try:
                score_df = get_class_scores(args.dashboard, args.dataset, row.name)
                counts_series = score_df.idxmax(axis='columns').value_counts()
                row['taxon_count'] = counts_series[args.taxon] if args.taxon in counts_series else 0
                row['taxon_perL'] = 1000*row['taxon_count']/row['bin_ml']
                row['taxon_added'] = now
                if args.v: print(f"{row['taxon_count']} ({round(row['taxon_perL'])} perL)")
            except HTTPError as e:
                if args.v: print(f'NaN ({e})')
        return row
    df = df.apply(taxon_per_liter, axis='columns')

    # 5) limit size of saved df
    df = df[df['sample_time']>now-dt.timedelta(days=args.buffer)]

    # 6) save datafile
    if args.v>=2: print(f'Saving file: {args.datafile}')
    df.to_csv(args.datafile)
    return df


def check_datafile(args, df_bins=None):
    now = dt.datetime.now(pytz.UTC)
    now = now-dt.timedelta(microseconds=now.microsecond) # dump microseconds

    # 1) load files
    # Datafile
    if df_bins is None:
        if args.v>=2: print(f'Reading Datafile: {args.datafile}')
        df_bins = pd.read_csv(args.datafile, index_col='pid', parse_dates=['sample_time'])
    latest_valid_row = df_bins[~df_bins['taxon_perL'].isna()].iloc[-1]
    bin_id = latest_valid_row.name
    taxon_perL = latest_valid_row['taxon_perL']
    sample_time = latest_valid_row['sample_time']

    # Logfile
    try:
        if args.v>=2: print(f'Reading Logfile: {args.logfile}')
        df_log = pd.read_csv(args.logfile, index_col='triggering_bin')
        df_log['pump_turned_off'] = pd.to_datetime(df_log['pump_turned_off'], errors='coerce')
        df_log['pump_back_on'] = pd.to_datetime(df_log['pump_back_on'], errors='coerce')
    except FileNotFoundError:
        if args.v>=2: print('  FileNotFound: one may be created')
        cols = dict(triggering_bin=[], pump_turned_off=[], pump_back_on=[])
        df_log = pd.DataFrame(cols).set_index('triggering_bin')

    # Pump Timer
    if args.v>=2: print(f'Reading Timerfile: {args.timerfile}')
    pump_timer = get_pump_timer(args.timerfile)
    if args.v>=2: print(f'  Pump timer: {str(now-pump_timer).split(".")[0] if pump_timer else None}')

    # 2) get pump and aerator state
    if args.powerstrip:
        if args.v: print('Checking Powerstrip States')
        pump_args = [args.powerstrip,args.powerstrip_auth,args.pump_outlet]
        aerator_args = [args.powerstrip,args.powerstrip_auth,args.aerator_outlet]
        try:
            pump_on = get_outlet(*pump_args)
            aerator_on = get_outlet(*aerator_args)
            if args.v: print(f'  Pump Outlet:    {"ON" if pump_on else "OFF"}\n  Aerator Outlet: {"ON" if aerator_on else "OFF"}')
        except requests.exceptions.RequestException as e:
            print(f'  ERROR: get_outlet({args.powerstrip}) - Connection Failed')
            pump_on,aerator_on = None,None
            if args.v: print('  Pump Outlet:    ???\n  Aerator Outlet: ???')

    if args.email_config:
        email_args = dict(TO=args.emails, SMTPserver=args.email_config[0], USER=args.email_config[1], PASS=args.email_config[2])

    # 3) check for any triggering behaviors
    ago = now - sample_time
    if args.v:
        print(f'Checking Counts Against Threshold ({args.threshold} perL)')
        print(f'  Latest Counts: {round(taxon_perL)} perL (sample_time: {str(ago).replace("0 days ","")} ago, from {bin_id})')

    if taxon_perL > args.threshold:
        set_pump_timer(args.timerfile, sample_time)
        if pump_timer is None:
            msg = f'Counts Above Threshold\n  \
                    Threshold: {args.threshold}/L\n  \
                    Counts: {taxon_perL}/L\n  \
                    SampleTime: {sample_time.astimezone(pytz.timezone("US/Eastern"))}\n  \
                    Bin: {bin_id}\n\n\
                    Setting pump timer + Turning Pump OFF and Aerator ON'
            if args.v: print(msg.replace('\n','; '))
            if args.powerstrip:
                try: set_pumpOff_aeratorOn(pump_args,aerator_args)
                except requests.exceptions.RequestException as e:
                    print(f'  ERROR: set_pumpOff_aeratorOn({args.powerstrip}) - Connection Failed')
            if args.email_config:
                subject = f"[{args.ifcb}] ALERT: Rust Above Threshold"
                send_emails(SUBJECT=subject, BODY=msg, **email_args)
            # writing to pump logfile
            if args.v>=2: print(f'Saving new entry to logfile: {args.logfile}')
            df_log = df_log.append(pd.Series(name=bin_id, data={'pump_turned_off':now}))
            df_log.to_csv(args.logfile)
        elif pump_timer==sample_time:
            msg = 'Counts Still Above Threshold: No New Classification Data'
            if args.v: print(msg)
        else:
            msg = 'Counts Still Above Threshold: Re-Setting pump timer'
            if args.v: print(msg)

    elif pump_timer and sample_time - pump_timer > dt.timedelta(hours=args.timer):
        msg = 'Counts Below Threshold and Pump Timer has run out\n\nTurning Pump back ON and Aerator OFF'
        if args.v: print(' ',msg.replace('\n',';'))
        if args.powerstrip:
            try: set_pumpOn_aeratorOff(pump_args,aerator_args)
            except requests.exceptions.RequestException as e:
                print(f'  ERROR: set_pumpOn_aeratorOff({args.powerstrip}) - Connection Failed')
        set_pump_timer(args.timerfile,None)
        if args.email_config:
            subject = f"[{args.ifcb}]Rust Back Below Threshold"
            send_emails(SUBJECT=subject, BODY=msg, **email_args)
        # writing to pump logfile
        if args.v>=2: print(f'Saving new entry to logfile: {args.logfile}')
        df_log.iat[-1,df_log.columns.get_loc('pump_back_on')] = now
        df_log.to_csv(args.logfile)

    elif pump_timer:
        if args.v:
            remaining = (now-pump_timer)-dt.timedelta(hours=args.timer)
            msg = f'Counts Below Threshold, but {str(remaining).replace("0 days ","")} remains on Pump Timer'
            print(' ',msg)

    else:
        msg = "Counts Below Threshold: all is well"
        if args.v: print(' ',msg)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A CLI tool for monitoring and responding to algal blooms.')
    parser.add_argument('-v', '--verbose', dest='v', action='count', default=0)

    conn = parser.add_argument_group(title='Connection', description=None)
    conn.add_argument('--dashboard', metavar='URL', help='The target ifcb dashboard url.')
    conn.add_argument('--dataset', help='An ifcb dataset.')
    conn.add_argument('--ifcb', metavar='ID', help='Instrument to pull data from.')

    data = parser.add_argument_group(title='Data', description=None)
    data.add_argument('--taxon', default='Margalefidinium',
        help='Taxon to trigger threshold off of. Default is "Margalefidinium"')
    data.add_argument('--threshold', metavar='INT', type=int,
        help='Threshold perL-counts beyond which pump, aerator, and emails are triggered')
    data.add_argument('--datafile', default='data/{TAXON}.csv',
        help='File to record bin data to. Default is "data/{TAXON}.csv"')
    data.add_argument('--buffer', metavar='DAYS', default=14, type=int,
        help='How many days of recent bins to include. Default is "14" days, ie 2 weeks')

    power = parser.add_argument_group(title='Powerstrip', description=None)
    power.add_argument('--timer', metavar='HOURS', default=1.5, type=float,
        help='Minimum amount of time to toggle off for in hours. Default is "1.5" hours')
    power.add_argument('--powerstrip', metavar='URL', help='The url of the network switch.')
    power.add_argument('--powerstrip-auth', nargs=2, metavar=('USER','PASS'),
        help='The login user and password of the network switch.')
    power.add_argument('--pump-outlet', metavar='ID', type=int)
    power.add_argument('--aerator-outlet', metavar='ID', type=int)
    power.add_argument('--logfile', default='data/{TAXON}.pumplog.csv',
        help='Notes when outlets are toggled. Default is "data/{TAXON}.pumplog.csv"')
    power.add_argument('--timerfile', default='data/.{TAXON}.pumptimer.txt',
        help='Pump timer reset file. Default is "data/.{TAXON}.pumptimer.txt"')

    alert = parser.add_argument_group(title='Alerts', description=None)
    alert.add_argument('--emails', metavar='EMAIL', nargs='+')
    alert.add_argument('--email-config', metavar=('SMTP','USER','PASS'), nargs=3)

    # alternate method of providing arguments
    class LoadFromFile(argparse.Action):
        # parse arguments in the file and store them in the target namespace
        def __call__(self, parser, namespace, values, option_string=None):
            with values as f:
                parser.parse_args(shlex.split(f.read()), namespace)
    parser.add_argument('--file', type=open, action=LoadFromFile)

    args = parser.parse_args()
    print('TAXON:',args.taxon)

    if not args.dashboard.startswith(('https://','http://')):
        args.dashboard = 'https://'+args.dashboard

    def filecheck(fn):
        fn = fn.format(TAXON=args.taxon)
        os.makedirs(os.path.dirname(fn), exist_ok=True)
        return fn
    args.logfile = filecheck(args.logfile)
    args.datafile = filecheck(args.datafile)
    args.timerfile = filecheck(args.timerfile)

    if args.powerstrip.lower() in ['none','0']: args.powerstrip = None
    if args.powerstrip_auth: args.powerstrip_auth = tuple(args.powerstrip_auth)

    if args.pump_outlet: args.pump_outlet -=1
    if args.aerator_outlet: args.aerator_outlet -=1

    df = update_datafile(args)
    if args.threshold:
        check_datafile(args, df)
    else:
        if args.v: print('No Threshold set. PROGRAM END')
