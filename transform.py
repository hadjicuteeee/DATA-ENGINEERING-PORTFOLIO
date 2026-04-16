import pandas as pd


def create_dataframe(data):
    return pd.DataFrame(data)


# CLEANING
def clean_runtime(df):
    df['runtime'] = pd.to_numeric(df['runtime'], errors='coerce').fillna(0)
    return df


# ANALYSIS 1
def get_type_distribution(df):
    result = df['type'].value_counts(dropna=False)

    if result.sum() != len(df):
        raise ValueError("Data mismatch detected")

    return result


# ANALYSIS 2
def get_status_distribution(df):
    if df['status'].isna().all():
        raise ValueError("No data detected")

    result = (
        df['status']
        .value_counts(dropna=False)
        .reset_index()
    )

    result.columns = ['status', 'count']
    return result


# ANALYSIS 3
def get_genres_distribution(df):
    if df['genres'].isna().all():
        raise ValueError("No data detected")

    result = (
        df['genres']
        .str.split(',')
        .explode()
        .str.strip()
        .value_counts()
        .reset_index()
    )

    result.columns = ['genre', 'count']
    return result


# ANALYSIS 4
def get_avg_runtime_by_type(df):
    result = (
        df.groupby('type', as_index=False)['runtime']
        .mean()
        .rename(columns={'runtime': 'avg_runtime'})
    )

    return result