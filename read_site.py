import pandas as pd
dbengine = "sqlite:///{}".format('CO_snotel.db')
obs_cmd = "SELECT * FROM '737'"                 # this is the station that we are interested in 
                                                # it is the table named 737 

# read the sql database 
df = pd.read_sql(obs_cmd, con=dbengine)

# convert to datetime 
df = df.set_index(pd.to_datetime(df.Date))