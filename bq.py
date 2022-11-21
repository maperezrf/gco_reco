from google.cloud import bigquery
import google.auth 

credentials, project = google.auth.default()

bqclient = bigquery.Client(credentials=credentials)

# Download query results.
query_string = """
SELECT * 
FROM `txd-ops-control-faco-dtlk.f4_gco.f4tipo` 
"""

dataframe = bqclient.query(query_string).result().to_dataframe()

print(dataframe)