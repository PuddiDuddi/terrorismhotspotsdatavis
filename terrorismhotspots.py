import pandas as pd
df = pd.read_csv("terrorismcsv.csv")
pd.set_option('display.max_columns', None)
df[['imonth', 'iday']] = df[['imonth', 'iday']].replace(0,1)
df['date'] = pd.to_datetime(df[['iyear', 'imonth', 'iday']].rename(columns={'iyear': 'year', 'imonth': 'month', 'iday':
    'day'}))
df['date'] = df['date'].dt.strftime('%Y-%m-%d')
usefulldata = df[["eventid","date","country_txt","region_txt","provstate","city","latitude","longitude","success",
                  "suicide","attacktype1_txt","targtype1_txt","gname","nperps","weapsubtype1_txt","nkill","nkillter", 
                  "nwound"]]
usefulldata.fillna(value="null")
usefulldata.to_csv('filteredterroristdata.csv')