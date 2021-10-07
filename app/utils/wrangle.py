import datetime
import pandas as pd

def gen_query_recent_messages(start, stop) -> str:
    """
    Generic non-patient level details for HL7 messages in a specified period

    :params start: start message datetime
    :params stop: stop message datetime
    :returns: formatted query string
    """
    return f"""
    SELECT
     unid
    ,messagedatetime
    ,messagetype
    ,messageformat
    ,messageversion
    ,senderapplication
    FROM tbl_ids_master 
    WHERE messagedatetime >  '{start}' AND messagedatetime <= '{stop}'
    ORDER BY unid DESC
    """


def round_time(dt=None, date_delta=datetime.timedelta(minutes=1), to='average'):
    """
    Round a datetime object to a multiple of a timedelta
    dt : datetime.datetime object, default now.
    dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
    from:  http://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object-python
    """
    round_to = date_delta.total_seconds()
    if dt is None:
        dt = datetime.datetime.now()
    seconds = (dt - dt.min).seconds

    if seconds % round_to == 0 and dt.microsecond == 0:
        rounding = (seconds + round_to / 2) // round_to * round_to
    else:
        if to == 'up':
            # // is a floor division, not a comment on following line (like in javascript):
            rounding = (seconds + dt.microsecond/1000000 + round_to) // round_to * round_to
        elif to == 'down':
            rounding = seconds // round_to * round_to
        else:
            rounding = (seconds + round_to / 2) // round_to * round_to

    return dt + datetime.timedelta(0, rounding - seconds, - dt.microsecond)


def unpack_msg_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Separate message type into type and trigger
    e.g. ADT^5 -> type: ADT trigger: 5

    :params df: a dataframe containing a column of HL7 message types
    :returns: a dataframe with two columns (type and trigger)
    """
    res = df['messagetype'].str.split('^', expand=True)
    res.rename(columns={0: 'type', 1: 'trigger'}, inplace=True)
    return res


def count_msgs_per_unit_time(df, unit_time='1T'):
    """

    :params df: dataframe returned from query to IDS
    :params unit_time: collapse data over what time unit
    :returns: new dataframe (dn) ready to plot
    """
    # three col data frame
    res = df.loc[:, ['messagedatetime', 'messagetype']]
    res.set_index('messagedatetime', inplace=True)
    res = unpack_msg_type(res)
    # group and sum
    res['n'] = 1
    res = res.groupby([pd.Grouper(freq=unit_time), 'type'])['n'].sum().reset_index()
    return res


def fill_missing_with_zero(df, start, stop, unit_time='1T'):
    """
    Now expand the data set over the start/stop range with zeros
    where there are no messages and at the desired cadence
    """

    # pivot
    df = df.pivot_table(
        values='n',
        index='messagedatetime',
        columns='type',
        aggfunc='sum'
    )

    # create skeleton time range
    ts = pd.Series(pd.date_range(start, stop, freq=unit_time).round(unit_time),
                   name='messagedatetime')

    # left join onto skeleton
    df = pd.merge(
        ts,
        df,
        how='left',
        on='messagedatetime')

    # melt back
    df = df.melt(id_vars='messagedatetime')
    df.rename(columns={'variable': 'type', 'value': 'n'}, inplace=True)

    # fill with zeros
    df = df.fillna(0)

    return df