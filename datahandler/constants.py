class Constants:
  '''
  A class that holds all the constant property values that are shared throughout the application.
  All the properties are initialized to default values. In case the property file in not available,
  the default values are taken into account.
  '''

  #url to fetch data from GDELT2.0 master files list
  url="http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"

  #working directory to work on the downloaded files
  working_dir="/tmp"

  #Number of files to download at a time
  download_limit=100

  #Chronologically sort the files according to date before downloading
  download_sort=True

  #directory to store the log files
  log_dir="app.log"

  #path to the property file
  prop_file="app.properties"

  #mongodb host
  mongo_host = "localhost"

  #mongodb port
  mongo_port = 27017

  #mongodb pool size
  mongo_pool_size = 200

  #database name
  db_name = "db_sense"

  # file urls cache file
  cache_file = "file_urls.cache"

  #mongo events import command
  event_import_command = "mongoimport -d DBNAME -c cameo_events --type tsv --file \"FILEPATH\" --fieldFile \"events_headers.txt\" -h localhost:3000"

  #mongo mentions import command
  mentions_import_command = "mongoimport -d DBNAME -c cameo_mentions --type tsv --file \"FILEPATH\" --fieldFile \"mentions_headers.txt\" -h localhost:3000"

  def _init_(self):
    return
