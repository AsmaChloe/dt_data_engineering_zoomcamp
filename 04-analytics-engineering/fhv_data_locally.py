import pandas as pd
def download_and_concatenate_fhv_data():

    months = ['0' + str(i) if i <= 9 else str(i) for i in range(1, 13)]
    year = 2019

    datasets = []
    for month in months :
        print(f"Downloading {year}-{month}")
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month}.parquet"

        datasets.append(pd.read_parquet(path=url))
    concatenated_df = pd.concat(datasets)

    # Save concatenated dataframe to a Parquet file

    filename = f"fhv_data_{year}.parquet"
    concatenated_df.to_parquet(filename, index=False)
    print(f"Concatenated data saved to {filename}")

download_and_concatenate_fhv_data()