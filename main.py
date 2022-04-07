# Copyright (c) 2022, Civity BV Zeist
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# * Neither the name of the copyright holder nor the names of its contributors
#   may be used to endorse or promote products derived from this software without
#   specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

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
        sql = "SELECT # " \
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
