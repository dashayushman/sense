from pymongo import MongoClient
import pymongo
import logging
import os
import sys, traceback
from datetime import datetime
import copy

class Dal:
  '''
  Data access layer class for all db related operations in the application
  '''

  #the mongodb client
  client = None
  util = None
  logger = None

  def __init__(self,host,port,pool_size,util,log_dir):
    '''
    Constructor for creating a data access layer object.
    :param host: mongodb host
    :param port: mongodb port number
    :param pool_size: pol size for mongodb client
    :param logger: logger object for logging
    '''
    self.client = MongoClient(host, port, maxPoolSize=pool_size)
    self.util = util
    #self.initialize_logger(log_dir)
    return

  def initialize_logger(self,log_path):
    '''
    Method to initialize logger for dal
    :param log_path: path for storing the log file
    :return:
    '''
    try:
      #logging.basicConfig(filename=log_path,format='%(asctime)s %(message)s %(levelname)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)
      self.logger = logging.getLogger("utils")
      self.logger.setLevel(logging.DEBUG)

      # create a file handler
      log_path = os.path.join(log_path,"app.dal.log")
      handler = logging.handlers.RotatingFileHandler(log_path, maxBytes=5000, backupCount=5)

      # create a logging format
      formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
      handler.setFormatter(formatter)
      # add the handlers to the logger
      self.logger.addHandler(handler)
    except Exception as e:
      print("error({0}): {1}".format(e.errno, e.strerror))
      print("unable to set specified logpath. Setting logpath to default which is app.log in the application folder itself.")
      #logging.basicConfig(filename="app.log", format='%(asctime)s %(message)s %(levelname)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)
      self.logger = logging.getLogger("dal")
      self.logger.setLevel(logging.DEBUG)

      # create a file handler
      handler = logging.handlers.RotatingFileHandler("app.dal.log", maxBytes=5000, backupCount=5)
      # create a logging format
      formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
      handler.setFormatter(formatter)
      # add the handlers to the logger
      self.logger.addHandler(handler)

  def insert_new_files(self,current_file_url_list,db_name,logger):
    '''
    dal method to insert newly added files into db.
    :param current_file_url_list: new list of files
    :param db_name: name of the database that we will query
    :param logger: logger object
    :return: status of insert
    '''
    self.log_info("inserting new file urls to db.")
    try:

      db = self.client[db_name]
      col_file_url = db["coll_file_urls"]
      inserted = 0
      for n,fileobj in enumerate(current_file_url_list):

        if n%500 == 0:
          self.log_info("Checking file " + str(n+1) + " of " + str(len(current_file_url_list)) + " file urls")
        '''
        del fileobj["mentions"]["status"]
        del fileobj["event"]["status"]
        del fileobj["kgraph"]["status"]
        '''
        query = {"mentions.url" : fileobj["mentions"]["url"], "event.url" : fileobj["event"]["url"], "timestamp":fileobj["timestamp"]}
        exists = col_file_url.find_one(query)

        if exists is None:

          fileobj["mentions"]["status"] = 0
          fileobj["event"]["status"] = 0
          fileobj["kgraph"]["status"] = 0

          col_file_url.insert_one(fileobj)
          inserted = inserted + 1

      self.log_info("Number of files inserted: " + str(inserted) +
                    "\nNumber of new files skipped because they already exist:" + str(len(current_file_url_list) - inserted))
      return True

    except Exception as e:

      self.log_error("DB init Error {0}".format(e.message))
      self.log_error("Error occured while inserting newly added file urls into db")

      return False

  def get_file_urls_by_status(self,db_name,working_dir,status=0,sort=True):
    '''
    dal method for retrieving all file urls with a given status
    :param db_name: name of the database that we will query
    :param logger: logger object
    :param status: status of the file, 0 means not downloaded nor inserted and 1 means downloaded and inserted
    :return:
    '''
    self.log_info("Getting pending files from db.")
    pending_files = None
    query = {"$or":[{"mentions.status":0},{"event.status":0}]}

    try:
      db = self.client[db_name]
      col_file_url = db["coll_file_urls"]
      if sort:
        pending_files = col_file_url.find(query).sort('timestamp', pymongo.DESCENDING)
      else:
        pending_files = col_file_url.find(query)
      print("number of results "+str(pending_files.count()))
      return pending_files

    except Exception as e:
      self.log_error("DB Error ({0}): {1}".format(e.errno, e.strerror))
      self.log_error("Error occured while getting pending file urls from db")
      return None

  def update_file_status(self,db_name,fileobj,category,status):
    '''
    Method to update the status of a file in db
    :param db_name: name of the database
    :param fileobj: The file object that needs to be updated
    :param category: category of file (events, mentions, knowledge graph)
    :param status: new status of the file
    :return: update status (True or False)
    '''
    self.log_info("Updating the status of file in database to inserted (status 1)")
    try:
      fileobj[category]["status"] = status
      db = self.client[db_name]
      col_file_url = db["coll_file_urls"]
      update_status = col_file_url.update({
        '_id': fileobj['_id']
      },{
        '$set': fileobj
      }, upsert=False)
      if update_status["nModified"] == 1:
        #self.log_info("Successfully updated status of file in database")
        return True
      else:
        #self.log_info("Unable to update status of file in database")
        return False
    except Exception as e:
      self.log_error("DB update Error {0}".format(e.message))
      self.log_error("Exception occured while updating status of file in db")
      return False

  def update_impact_map(self,db_name,start_date,end_date):
    '''
    Method to update global impact map data
    :param db_name: name of the db
    :param start_date: start date for considering goldstein's impact
    :param end_date: start date for considering goldstein's impact
    :return: Status of update
    '''
    latlong = {}
    mapData = []
    self.log_info("Updating global impact map dat into db")
    try:
      db = self.client[db_name]
      col_cameo_events = db["cameo_events"]
      col_fips_country = db["fips_country"]

      #queries
      get_events_in_range_query = {"$and":[{"DATEADDED": {"$gt": start_date}},{"DATEADDED": {"$lt": end_date}}]}
      get_country_name_from_code = {"CountryCode" : "code"}

      last_day_events = col_cameo_events.find(get_events_in_range_query)
      if last_day_events.count() == 0:
        self.log_info("there have been no events in the last day of last update")
        return 1 # there have been no events in the last day of last update

      last_day_distinct_country_codes = last_day_events.distinct("ActionGeo_CountryCode")

      if len(last_day_distinct_country_codes) == 0:
        self.log_info("there are no distinct countries in the last one day of last update")
        return 2 # there are no distinct countries in the last one day of last update

      for n_c,country in enumerate(last_day_distinct_country_codes):
        if n_c%50:
          self.log_info("Calculating impact for " + str(n_c) + " out of " + str(len(last_day_distinct_country_codes)) + " countries")
        new_query = copy.deepcopy(get_events_in_range_query)
        new_query["$and"].append({"ActionGeo_CountryCode":country})
        country_events = col_cameo_events.find(new_query)
        #country_events = last_day_events.where("this.ActionGeo_CountryCode == \""+ country + "\"")
        # FIPS Country Name
        get_country_name_from_code["CountryCode"] = country
        country_name = col_fips_country.find_one(get_country_name_from_code)
        if country_name is None:
          self.log_error("Invalid Country Code : " + country)
          continue
        country_name = country_name["CountryName"]
        # Lat Long
        lat, long = country_events.__getitem__(0)["ActionGeo_Lat"],country_events.__getitem__(0)["ActionGeo_Long"]

        #Avg Goldstein's score
        score = 0
        n_c_events = country_events.count()
        for i,events in enumerate(country_events):
          #print events["GoldsteinScale"]
          if i%1000 == 0:
            self.log_info(str(i) + " of " + str(n_c_events) + " events for " + country_name)
            self.log_info("Total Score : " + str(score) + "\nG Score: " + str(events["GoldsteinScale"]))
          score = score + events["GoldsteinScale"]
        avg_score = score/n_c_events

        #hex colour
        hex_color = self.util.convert_goldstein_to_hex_color(avg_score)

        #circle_value
        value = abs(avg_score)
        if n_c%50:
          self.log_info("Country : " + country_name +
                        "\nCountry Code: " + str(len(last_day_distinct_country_codes)) +
                        "\nlatitude: " + str(lat)+
                        "\nlongitude: "+ str(long)+
                      "\naverage goldstein's score: " + str(avg_score)+
                      "\nhex color code: " + hex_color)

        latlong[country] = {
          "latitude":lat,
          "longitude":long
        }

        map_data_obj = {
          "code":country,
          "name":country_name,
          "value":value,
          "color":hex_color,
          "avg":avg_score
        }

        mapData.append(map_data_obj)

      result = db["coll_impact_map"].delete_many({})

      ins_result = db["coll_impact_map"].insert_one({
        "latlong":latlong,
        "mapData":mapData
      })

      if ins_result is None:
        self.log_error("Couldnot insert new/updated data into coll_impact_map.")
        return 3 #could not insert new data
      else:
        self.log_info("Successfully updated global impact map.")
        return 4 #successfully inserted new data
    except:
      self.log_error("Exception occurred while updating impact map data in db\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return 5 #exception occurred while updating impact map data






  def get_last_update_date(self,db_name):
    '''
    Method to get the last update date
    :param db_name: db name
    :return: last update date
    '''
    metadata = self.get_metadata(db_name)
    if metadata is None:
      self.log_error("Metadata could not be loaded. Look into the database for more details. " +
                     "Will use default datetime as now.")
      return None
    return metadata["last_update_date"]

  def get_metadata(self,db_name):
    '''
    Method to get metadata object from the metadata collection
    :param db_name: name of the db
    :return: metadata object
    '''
    self.log_info("Getting metadata object from db : " + db_name)
    try:
      db = self.client[db_name]
      col_metadata = db["coll_metadata"]
      return col_metadata.find_one()
    except:
      self.log_error("Exception occurred while getting metadata object from db\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return None

  def update_metadata(self,db_name):
    '''
    Method to update the metadata collecton
    :param db_name: name of the database
    :return: update status
    '''
    self.log_info("Updating metadata after inserting new files into db")
    try:
      db = self.client[db_name]
      col_metadata = db["coll_metadata"]
      col_files = db["coll_file_urls"]
      col_cameo_events = db["cameo_events"]
      col_cameo_mentions = db["cameo_mentions"]
      metadata = col_metadata.find_one()

      if metadata is None:
        self.log_info("Some unexpected error. There should always be a metadata object." +
                      "Please check the metadata collection in the db.")
        return False

      latest_added_file = col_files.find().sort('timestamp', pymongo.DESCENDING).limit(1).__getitem__(0)

      # updating the last update date in GDELT format
      if latest_added_file is None:
        metadata["last_update_date"] = self.util.get_present_date_time()
      else:
        metadata["last_update_date"] = latest_added_file["timestamp"]

      # updating the number of old files
      metadata["n_old_files"] = metadata["n_new_files"]

      # updating the number of new files
      col_files.find().count()
      metadata["n_new_files"] = col_files.find().count()

      # updating number of files inserted
      metadata["n_files_inserted"] = metadata["n_new_files"] - metadata["n_old_files"]

      # updating number of pending files
      query = {"$or":[{"mentions.status":0},{"event.status":0}]}
      metadata["n_pending_files"] = col_files.find(query).count()

      # updating number of old events
      metadata["n_old_events"] = metadata["n_new_events"]

      # updating number of new events
      metadata["n_new_events"] = col_cameo_events.find().count()

      # updating number of events inserted
      metadata["n_events_inserted"] = metadata["n_new_events"] - metadata["n_old_events"]

      # updating number of old mentions
      metadata["n_old_mentions"] = metadata["n_new_mentions"]

      # updating number of new mentions
      metadata["n_new_mentions"] = col_cameo_mentions.find().count()

      # updating number of mentions inserted
      metadata["n_mentions_inserted"] = metadata["n_new_mentions"] - metadata["n_old_mentions"]

      #update metadata object
      update_status = col_metadata.update({
        '_id': metadata['_id']
      },{
        '$set': metadata
      }, upsert=True)

      if update_status["nModified"] == 1:
        self.log_info("Successfully updated metadata after inserting new documents")
        return True
      else:
        self.log_info("There was no change in metadata.")
        return True

    except:
      print("Exception occurred while updating metadata after inserting new files\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return False

  def log_info(self,msg):
    '''
    Method that logs info messages either in the log file or on the console
    :param msg: message that needs to be logged
    :param logger: logger object
    :return:
    '''
    if self.logger:self.logger.info(msg)
    else:print("[INFO]: " + msg)

  def log_error(self,msg):
    '''
    Method that logs error messages either in the log file or on the console
    :param msg: message that needs to be logged
    :param logger: logger object
    :return:
    '''
    if self.logger:self.logger.error(msg)
    else:print("[ERROR]: " + msg)

  def log_debug(self,msg):
    '''
    Method that logs debug messages either in the log file or on the console
    :param msg: message that needs to be logged
    :param logger: logger object
    :return:
    '''
    if self.logger:self.logger.debug(msg)
    else:print( "[DEBUG]: " + msg)
