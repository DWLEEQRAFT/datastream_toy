import datetime

from datastream_soap_api import SoapDataStreamAPI

import glob
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# key file directory
key_path = glob.glob("./config/*.json")[0]
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials,
                         project=credentials.project_id)

datastream = SoapDataStreamAPI()

# upload ticker info
schema_datastream_ticker = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("field", "STRING"),
    bigquery.SchemaField("last_updated", "DATE")
]

schema_datastream_series = [
    bigquery.SchemaField("calendar_date", "DATE"),
    bigquery.SchemaField("value", "NUMERIC"),
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("field", "STRING")
]

table_id_ticker = f"innate-plexus-345505.datastream.ticker"
table_id_series = f"innate-plexus-345505.datastream.series"

# market ticker for etfs
temp_df = pd.read_csv("./temp/market_info.csv")

df_datastream_ticker = pd.DataFrame()

df_datastream_ticker["ticker"] = temp_df["TICKER"]
df_datastream_ticker["field"] = temp_df["FIELD"]
df_datastream_ticker["last_updated"] = datetime.datetime.today()

df_datastream_ticker = df_datastream_ticker.set_index("ticker", drop=True)

job_config_upload_ticker = bigquery.LoadJobConfig(
    schema=schema_datastream_ticker,
    autodetect=False,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
)

#client.load_table_from_dataframe(df_datastream_ticker, table_id_ticker, job_config=job_config_upload_ticker).result()

### call soapDatastream
# df_temp_series = datastream.get_time_series_data('MSEMKF$',
#                                                  'MSPI',
#                                                  date_from='1986-12-31',
#                                                  date_to='9999-12-31',
#                                                  frequency='D',
#                                                  daily_to_monthly=False
#                                                  )
#
# print(df_temp_series);


### initial Data Upload
df_datastream_ticker = df_datastream_ticker.reset_index()

print(df_datastream_ticker)

for row in df_datastream_ticker.iterrows():
    raw_data = datastream.get_time_series_data(row["ticker"],
                                               row["field"],
                                               date_from='1986-12-31',
                                               date_to='9999-12-31',
                                               frequency='D',
                                               daily_to_monthly=False)

    df_raw_data = pd.DataFrame(raw_data).set_index("date", drop=True)

    # df_new = df_raw_data.loc[df_raw_data["date"]> df_datastream_ticker["last_updated"]]

    job_config_upload_series = bigquery.LoadJobConfig(
        schema=schema_datastream_series,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    #client.load_table_from_dataframe(df_raw_data, table_id_series, job_config=job_config_upload_series).result()
    print(df_raw_data["ticker"] + " series uploaded")
