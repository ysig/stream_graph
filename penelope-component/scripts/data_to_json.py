import pandas as pd
import json

tls_df = pd.read_csv('../../tutorials/ODYCCEUS/migrant-day-hashtag-hashtag-simple.zip', names=['date', 'u', 'v'], sep=' ', keep_default_na=False)
remove_words = {'migrant', 'migrants', 'immigrant', 'immigrants', 'emigrant', 'emigrants'}
tls_df = tls_df[~tls_df.u.isin(remove_words) & ~tls_df.v.isin(remove_words)]
date_to_t = {date: i for i, date in enumerate(sorted(list(set(tls_df['date']))))}
t_to_date = {i: date  for date, i in date_to_t.items()}
def to_date(t):
    return pd.to_datetime(t_to_date[t], format='%Y-%m-%d')
tls_df['ts'] = tls_df['date'].map(date_to_t)
tls_df.drop(columns=['date'], inplace=True)

with open('date.json', 'w') as f:
    json.dump(t_to_date, f)


data = {'temporal-link-set':
           {'discrete': True,
            'instantaneous': True,
            'u': list(tls_df['u']),
            'v': list(tls_df['v']),
            'ts': list(tls_df['ts'])}}

with open('data.json', 'w') as f:
    json.dump(data, f)
