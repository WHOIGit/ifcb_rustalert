import datetime as dt
import pytz
import matplotlib.pyplot as plt
import pandas as pd

def plot4email(df_counts, df_pump=None, threshold=None, ago_limit=None, title=None, output='plot4email.png'):

    if ago_limit:
        start = dt.datetime.now(pytz.UTC) - dt.timedelta(days=ago_limit)
        #end = dt.datetime(2020, 9, 16, tzinfo=pytz.UTC)
        #mask = (df['sample_time'] > start) & (df['sample_time'] <= end)
        mask_counts = df_counts['sample_time'] > start
        df_counts = df_counts[mask_counts]
        if df_pump is not None:
            mask_pump = df_pump['pump_turned_off'] > start
            df_pump = df_pump[mask_pump]

    fig, ax = plt.subplots(figsize=(10, 4))

    # plot cells per liter timeseries
    ax.plot(df_counts['sample_time'], df_counts['taxon_perL'])

    if df_pump is not None:
        # plot a vertical span for each pump off-on cycle
        for idx, row in df_pump.iterrows():
            pump_on = row['pump_back_on'] if not pd.isna(row['pump_back_on']) else dt.datetime.now(pytz.UTC)
            ax.axvspan(xmin=row['pump_turned_off'], xmax=pump_on,
                       alpha=0.25, color='orange')

    if threshold:
        # plot horizontal threshold line
        ax.hlines(y=threshold, xmin=df_counts.sample_time[0], xmax=df_counts.sample_time[-1], color='black', linewidth=1)

    # annotations
    if title: ax.set_title(title)
    ax.set_ylabel("Cells per Liter")
    plt.xticks(rotation=40, ha='right')

    if output:
        fig.savefig(output, bbox_inches='tight')
    else:
        fig.show()

