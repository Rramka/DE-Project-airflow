import myFramework.utils.utils as utils

import pandas as pd
from sqlalchemy import create_engine 

# read data into Df
data = pd.read_csv("/Users//ramazkapanadze/Downloads/sales_data_sample.csv",  encoding='Latin-1')
df = pd.DataFrame(data)

utils.fillstaging(df,'DBStaging','sales','sales_data')

# engine = create_engine('postgresql://ramazkapanadze:1604@localhost:5432/DBStaging')

# df.to_sql('sales_data', engine,schema='sales', if_exists='append', index=False)



# try:

#   conn = psycopg2.connect(
#     host = hostname,
#     dbname = database,
#     user = username,
#     password = pwd,
#     port = port_id    
#   )

#   cur = conn.cursor()


# except Exception as error:
#   print(error)

# finally:
#   if cur is not None:
#     cur.close()
#   if conn is not None:
#     conn.close(