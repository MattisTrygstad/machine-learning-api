import pandas as pd
from decouple import config
import requests
import numpy as np
import sys


class Fetcher():

    __client_id = config('CLIENT_ID')
    __client_secret = config('CLIENT_SECRET')
    __base_url = 'https://frost.met.no/observations/v0.jsonld'

    api_features = ['mean(surface_air_pressure P1D)', 'mean(water_vapor_partial_pressure_in_air P1D)', 'mean(relative_humidity P1D)', 'specific_humidity', 'mean(cloud_area_fraction P1D)',
                    'mean(air_temperature P1D)', 'max(max(wind_speed PT1H) P1D)', 'sum(precipitation_amount P1D)', 'boolean_overcast_weather(cloud_area_fraction P1D)']

    __column_names = (
        'air_pressure', 'water_vapor_pressure', 'relative_air_humidity', 'specific_air_humidity', 'average_cloud_cover', 'temperature', 'wind_speed', 'downfall', 'cloudy_weather'
    )

    def fetch_data(self, date_start: str, date_stop: str) -> pd.DataFrame:

        parameters = {
            'sources': 'SN68860',  # Trondheim, Voll
            'elements': ','.join(Fetcher.api_features),
            # 'elements': 'mean(surface_air_pressure P1D),mean(water_vapor_partial_pressure_in_air P1D),mean(relative_humidity P1D),mean(specific_humidity P1D),mean(cloud_area_fraction P1D),mean(air_temperature P1D),max(mean(wind_speed PT1H) P1D),sum(precipitation_amount P1D),boolean_overcast_weather(cloud_area_fraction P1D)',
            'referencetime': '{}/{}'.format(date_start, date_stop), 'timeoffsets': 'default'
        }
        column_names = Fetcher.__column_names

        # TODO: add Session to increase performance
        r = requests.get(Fetcher.__base_url, parameters,
                         auth=(Fetcher.__client_id, ''))
        print(r.url)

        json = r.json()

        if r.status_code == 200:
            data = json['data']
            print('Data retrieved from frost.met.no!')
        else:
            print('Error! Returned status code %s' % r.status_code)
            print('Message: %s' % json['error']['message'])
            print('Reason: %s' % json['error']['reason'])
            sys.exit()

        df = pd.DataFrame()

        date_list = []
        features_list = [[] for _ in range(len(column_names))]

        # Iterate through every day in the date range
        for i in range(len(data)):

            # Create new dataframe containing the observations for the given day
            obs = pd.DataFrame.from_dict(
                pd.json_normalize(data[i]['observations']), orient='columns')

            # New dataframe which will also contain missing features
            obs_complete = pd.DataFrame()

            for index in range(len(Fetcher.api_features)):

                # If feature is in the original obs, append the row to obs_complete
                if Fetcher.api_features[index] in obs.values:
                    obs_index = np.where(
                        obs['elementId'] == Fetcher.api_features[index])[0]
                    obs_complete = obs_complete.append(obs.iloc[obs_index])
                # If not, append empty row with feature name
                else:
                    obs_complete = obs_complete.append(
                        {'elementId': Fetcher.api_features[index]}, ignore_index=True)

            date_list.append(data[i]['referenceTime'])
            for x in range(len(column_names)):
                features_list[x].append(obs_complete.iloc[x]['value'])

        df['date'] = date_list

        for x in range(len(column_names)):
            df[column_names[x]] = features_list[x]

        df.set_index('date')
        df.dropna(how='all', axis='columns', inplace=True)
        return df
