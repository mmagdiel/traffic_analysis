import os
import pandas as pd

# Loads functions
def load_user_data():
    USERS_PATH = os.path.join("users.csv")
    return pd.read_csv(filepath_or_buffer=USERS_PATH, sep=",")

def load_events_data():
    EVENTS_PATH = os.path.join("events.csv")
    return pd.read_csv(filepath_or_buffer=EVENTS_PATH, sep=",")

def load_description_data():
    DESCRIPTION_PATH = os.path.join("variables_description_users.csv")
    return pd.read_csv(filepath_or_buffer=DESCRIPTION_PATH, sep=",")

# Basic transformation functions
def basic_transformation_for_events(df):
    df.rename(columns={'event_name': 'external_event'}, inplace=True)
    df.external_event = df.external_event.astype('category')
    return df

def basic_transformation_for_users(df):
    df.columns = df.columns.map(lambda column: column.lower())
    del df['keyword']
    df.rename(columns={'event_name': 'internal_event'}, inplace=True)
    df.country_code = df.country_code.astype('category')
    df.sourcemedium = df.sourcemedium.astype('category')
    df.internal_event = df.internal_event.astype('category')
    df.device_category = df.device_category.astype('category')
    df.channel = df.channel.astype('category')
    df.carrier = df.carrier.astype('category')
    df.wifi = df.wifi.astype('category')
    df.operator = df.operator.astype('category')
    df.clientid = df.clientid.astype('category')
    return df

# Expands
def expand_datatime_events(df, col_src_start, col_src_end, col_dist, freq):
    df[col_dist] = df.apply(diff_time, axis=1, start_col=col_src_start, end_col=col_src_end, freq=freq)
    df = delete_columns(df, col_src_start, col_src_end)
    df = df.explode(col_dist)
    df = df.set_index(col_dist)
    return df

def clean_event_column(df, col_src, col_dist):
    df[col_dist] = df.apply(extract_datetime, axis=1, str=col_src)
    del df[col_src]
    return df


def clean_user_column(df, col_src, col_dist):
    temp = df.apply(extract_datetime, axis=1, str=col_src)
    df[col_dist] = pd.to_datetime(temp)
    del df[col_src]
    df = df.set_index(col_dist)
    return df

# General helpers
def delete_columns(df, *columns):
    for col in columns:
        del df[col]
    return df



# For apply functions
def extract_datetime(row, str):
    return row[str][:-4]

def diff_time(row, start_col, end_col, freq):
    return pd.date_range(start=row[start_col], end=row[end_col], freq=freq).tolist()

