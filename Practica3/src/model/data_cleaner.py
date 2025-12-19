def clean_data(df):
    df = df.drop_duplicates()
    df = df.dropna()
    return df
