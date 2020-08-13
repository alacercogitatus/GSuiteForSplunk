[ga://default]
*This is how the Google Apps For Splunk is configured

domain = <value>
*This is the domain name for the Google Apps Domain

servicename = <value>
*This is the service name to pull from the Google APIs. Refer to the README for specifics.

proxy_name = <value>
* Setups the settings for a proxy connection

extraconfig = <value>
*Allows for extra configuration for the API Calls.

[ga_ss://default]
*This is how the Google Apps For Splunk is configured

domain = <value>
*This is the domain name for the Google Apps Domain

ss_id = <value>
*This is the SpreadSheet ID to sync

ss_sheet = <value>
* This is the sheet id to sync

destination = <value>
* Where should we put this? Defaults to index. Values: index, kvstore, transform

proxy_name = <value>
* Setups the settings for a proxy connection

[ga_bigquery://default]
* Query BigQuery Tables
domain = <value>
* This is the domain name for the Google Apps Domain

project = <value>
* This is the project ID for the big query table

table = <value>
* This is the table name to query in the form <dataset>.<table>

proxy_name = <value>
* Setups the settings for a proxy connection

dataset = <value>
* The dataset to query with.

[ga_classroom://default]
* Consume Classroom Data
domain = <value>
*This is the domain name for the Google Apps Domain

class_servicename = <value>
*This is the service name to pull from the Google APIs. Refer to the README for specifics.

proxy_name = <value>
* Setups the settings for a proxy connection

historical = <value>
* Allows for historical data pull