class Constants:
  '''
  A class that holds all the constant property values that are shared throughout the application.
  All the properties are initialized to default values. In case the property file in not available,
  the default values are taken into account.
  '''

  #url to fetch data from GDELT2.0 master files list
  url="http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
  url2="http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

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
  event_import_command = "mongoimport -d DBNAME -c cameo_events --type tsv --file \"FILEPATH\" --fieldFile \"events_headers.txt\""

  #mongo mentions import command
  mentions_import_command = "mongoimport -d DBNAME -c cameo_mentions --type tsv --file \"FILEPATH\" --fieldFile \"mentions_headers.txt\""

  #image svg for linked locations
  svg = "M9,0C4.029,0,0,4.029,0,9s4.029,9,9,9s9-4.029,9-9S13.971,0,9,0z M9,15.93 c-3.83,0-6.93-3.1-6.93-6.93S5.17,2.07,9,2.07s6.93,3.1,6.93,6.93S12.83,15.93,9,15.93 M12.5,9c0,1.933-1.567,3.5-3.5,3.5S5.5,10.933,5.5,9S7.067,5.5,9,5.5 S12.5,7.067,12.5,9z"

  def _init_(self):
    return
