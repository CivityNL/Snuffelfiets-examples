# Because of the amount of Snuffelfiets data which has been collected by now, downloading the entire dataset in one
# go is no longer feasible. The CKAN data store API though allows you to download chunks of Snuffelfiets data. This
# code snippet provides an example how this could be achieved. All Snuffelfiets measurements contain a
# recording_timestamp column which can be used for this purpose. This particular example will download Snuffelfiets data
# per hour and calculates summary statistics per hour using a pandas data frame. But other processes, for instance
# to save the data to local files, can be implemented in a similar fashion. What a suitable interval is to download
# data for depends on the amount of sensors collecting data using a particular resource. If the interval is too big,
# time outs may occur.
#
# The Snuffelfiets data are available in different versions: unprocessed data, unprocessed calibrated data and
# processed data. This approach will work for all of them. Not all versions are publicly available though. If a version
# is not publicly available, an API key is required to be able to access the data. If you have an API key, it can be
# found in CKAN. Log in and click on your username in the top right corner of the screen. You can then find your API
# key on the left of the screen. To select the proper version, the identifier of the resource must be provided. This
# identifier can be found in CKAN as well, for instance in the address bar. Not all variants are publicly available.

import requests
import json
import pandas as pd
from datetime import datetime, timedelta


def calculate_statistics(resource_id, api_key, no_hours):
    # Determine starting timestamp
    to_date_time = datetime.now()

    for i in range(no_hours):
        # Calculate interval
        from_date_time = to_date_time + timedelta(hours=-1)

        # Date/time to string
        from_date_time_string = from_date_time.strftime('%Y-%m-%d %H:%M:%S')
        to_date_time_string = to_date_time.strftime('%Y-%m-%d %H:%M:%S')

        # Create SQL for data store API call
        sql = "SELECT * " \
              "FROM {} " \
              "WHERE recording_timestamp > '{}' " \
              "AND recording_timestamp < '{}'".format(
            resource_id,
            from_date_time_string,
            to_date_time_string
        )

        # Send request to server
        url = 'https://ckan-dataplatform-nl.dataplatform.nl/api/3/action/datastore_search_sql?sql=' + sql
        payload = {}
        headers = {
            'Authorization': api_key
        }
        response = json.loads(requests.request("GET", url, headers=headers, data=payload).text)

        # Check if the request has been handled successfully
        if response['success']:
            records = response['result']['records']
            if len(records) > 0:
                # Do something useful with the data, in this example calculate statistics
                df = pd.DataFrame(records)
                statistics_df = df.describe()
                print('Statistics for Snuffelfiets data from {} to {}'.format(from_date_time_string, to_date_time_string))
                print(statistics_df)
            else:
                print('Downloaded Snuffelfiets data contains no records')
        else:
            print('Error downloading Snuffelfiets data: {}'.format(response['error']['message']))

        # Prepare for the next loop
        to_date_time = to_date_time + timedelta(hours=-1)


if __name__ == '__main__':
    calculate_statistics(
        'provincie_utrecht_snuffelfiets_measurement_rydruofi',
        '1dab0923-5aed-4ffc-878f-3da9dbe9019a',
        5
    )
