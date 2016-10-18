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

        if n%1000 == 0:
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
        if n_c%500:
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

      result_old = db["coll_impact_map"].find_one({"name":"old"})
      result_new = db["coll_impact_map"].find_one({"name":"new"})
      success = False
      impact_map_dict = {
          "timestamp":self.util.get_present_date_time(),
          "latlong":latlong,
          "mapData":mapData,
          "startDate":start_date,
          "endDate":end_date,
          "name":"new"
        }
      if result_old is None and result_new is None:
        impact_map_dict["name"] = "old"
        ins_result = db["coll_impact_map"].insert_one(impact_map_dict)

        del impact_map_dict["_id"]
        impact_map_dict["name"] = "new"
        ins_result = db["coll_impact_map"].insert_one(impact_map_dict)
        success = True
      else:
        #delete the old record
        del result_new["_id"]
        result_new["name"] = "old"
        del_status = db["coll_impact_map"].remove({"name":"old"})
        del_status = db["coll_impact_map"].insert_one(result_new)

        impact_map_dict["name"] = "new"
        del_status = db["coll_impact_map"].remove({"name":"new"})
        del_status = db["coll_impact_map"].insert_one(impact_map_dict)
        success = True


      if success is False:
        self.log_error("Couldnot insert new/updated data into coll_impact_map.")
        return 3 #could not insert new data
      else:
        self.log_info("Successfully updated global impact map.")
        return 4 #successfully inserted new data
    except:
      self.log_error("Exception occurred while updating impact map data in db\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      self.client[db_name]["coll_impact_map"].delete_many({})
      return 5 #exception occurred while updating impact map data

  def get_total_mentions(self,db_name,end_date,start_date=0):
    '''
    Method to return the number of mentions in a given duration of time
    :param db_name: name of the db
    :param end_date: end date of the duration
    :param start_date: start date of the duration
    :return: number of events
    '''
    self.log_info("Getting number of mentions from " + str(start_date) +
                  " till " + str(end_date))
    try:
      results = self.get_mentions(db_name,end_date,start_date)
      if results is None:
        return -1
      n_events = results.count()
      return n_events
    except:
      self.log_error("Exception occurred while getting total number of mentions till a given date from db\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return -1

  def get_total_events(self,db_name,end_date,start_date=0):
    '''
    Method to return the number of events in a given duration of time
    :param db_name: name of the db
    :param end_date: end date of the duration
    :param start_date: start date of the duration
    :return: number of events
    '''
    self.log_info("Getting number of events from " + str(start_date) +
                  " till " + str(end_date))
    try:
      results = self.get_events(db_name,end_date,start_date)
      if results is None:
        return -1
      n_events = results.count()
      return n_events
    except:
      self.log_error("Exception occurred while total number of events till a given date from db\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return -1

  def get_total_countries(self,db_name,end_date,start_date=0):
    '''
    Method to return the number of events in a given duration of time
    :param db_name: name of the db
    :param end_date: end date of the duration
    :param start_date: start date of the duration
    :return: number of events
    '''
    self.log_info("Getting number of Countries since " + str(start_date) +
                  " till " + str(end_date))
    try:
      results = self.get_events(db_name,end_date,start_date)
      if results is None:
        return -1
      n_actor_1_geo = results.distinct("Actor1CountryCode")
      n_actor_2_geo = results.distinct("Actor2CountryCode")
      n_actor_1_geo_1 = results.distinct("Actor1Geo_CountryCode")
      n_actor_2_geo_1 = results.distinct("Actor2Geo_CountryCode")
      n_action_geo = results.distinct("ActionGeo_CountryCode")
      unique = list(set(n_actor_1_geo+
                        n_actor_2_geo+
                        n_actor_1_geo_1+
                        n_actor_2_geo_1+
                        n_action_geo
                        ))
      n_countries = len(unique)
      return n_countries
    except:
      self.log_error("Exception occurred while getting total number of countries involved till a given date from db\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return -1

  def get_events(self,db_name,end_date,start_date=0):
    '''
    Method to return events in a given date range
    :param db_name:db name
    :param end_date: end date
    :param start_date: start date
    :return: events cursor
    '''
    try:
      db = self.client[db_name]
      col_cameo_events = db["cameo_events"]
      query = {"$and":[{"DATEADDED":{"$lte":end_date}}]}
      if start_date != 0:
        query["$and"].append({"DATEADDED":{"$gt":start_date}})
      results = col_cameo_events.find(query)
      return results
    except:
      self.log_error("Exception occurred while getting events from the DB\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return None

  def get_mentions(self,db_name,end_date,start_date=0):
    '''
    Method to return mentions in a given date range
    :param db_name:db name
    :param end_date: end date
    :param start_date: start date
    :return: events cursor
    '''
    try:
      db = self.client[db_name]
      col_cameo_mentions = db["cameo_mentions"]
      query = {"$and":[{"MentionTimeDate":{"$lte":end_date}}]}
      if start_date != 0:
        query["$and"].append({"MentionTimeDate":{"$gt":start_date}})
      results = col_cameo_mentions.find(query)
      return results
    except:
      self.log_error("Exception occurred while getting mentions from the DB\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return None

  def get_linked_locations(self,db_name,end_date,svg,start_date=0):
    '''
    Method to get all the linked locaions in the given date range
    :param db_name:name of the db
    :param end_date: end date for retrieving events
    :param svg: svg image for the map
    :param start_date: start date for retrieval
    :return: map ditionary
    '''
    try:
      lines = []
      images = []
      results = self.get_events(db_name,end_date,start_date)
      actor_1_geos = results.distinct("Actor1Geo_CountryCode")
      if len(actor_1_geos) is 0:
        return {
          "timestamp":self.util.get_present_date_time(),
          "lines":lines,
          "images":images,
          "status":0,
          "message": "No 1st actors could be found in the selected time duration"
        }
      already_added = []
      for actor_geo in actor_1_geos:
        if len(actor_geo) == 0:
          continue
        a1_geo_name = self.get_country_from_fips_code(db_name,actor_geo)
        if a1_geo_name is None:
            continue
        a1_lat,a1_long = self.get_lat_long(db_name,actor_geo)
        if a1_lat is None or a1_long is None or a1_geo_name is None:
          continue
        linked_events = self.get_linked_countries(db_name,start_date,end_date,actor_geo)
        if linked_events is None:
          continue
        if linked_events.count() == 0:
          continue

        if actor_geo in already_added:
          images[already_added.index(actor_geo)]["scale"] = 1.5
        else:
          images.append({
            "id":actor_geo,
            "svgPath": svg,
            "title": a1_geo_name,
            "latitude": a1_lat,
            "longitude": a1_long,
            "scale": 1.5
          })
          already_added.append(actor_geo)
        for e in linked_events:
          a2_geo_code = e["Actor2Geo_CountryCode"]
          if len(a2_geo_code) is 0:
            continue
          if actor_geo == a2_geo_code:
            continue
          a2_geo_name = self.get_country_from_fips_code(db_name,a2_geo_code)
          if a2_geo_name is None:
            continue
          a2_lat,a2_long = self.get_lat_long(db_name,a2_geo_code)
          if a2_lat is None or a2_long is None:
            continue

          lines.append({
            "latitudes":[a1_lat,a2_lat],
            "longitudes":[a1_long,a2_long]
          })

          if a2_geo_code not in already_added:
            images.append({
              "id":a2_geo_code,
              "svgPath": svg,
              "title": a2_geo_name,
              "latitude": a2_lat,
              "longitude": a2_long,
              "scale": 0.5
            })
            already_added.append(a2_geo_code)

      return {
          "timestamp":self.util.get_present_date_time(),
          "lines":lines,
          "images":images,
          "status":1,
          "message": "Successfully generated linked locations."
        }
    except:
      self.log_error("Exception occurred getting linked locations from the DB\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return None

  def get_linked_countries(self,db_name,start_date,end_date,fips_code):
    '''

    :param db_name:
    :param fips_code:
    :return:
    '''
    self.log_info("Getting all linked locations with : " + fips_code)
    try:
      db = self.client[db_name]
      coll_events = db["cameo_events"]
      query = {"$and":[{"Actor1Geo_CountryCode" : fips_code},{"DATEADDED":{"$lte":end_date}}]}
      if start_date != 0:
        query["$and"].append({"DATEADDED":{"$gt":start_date}})
      events = coll_events.find(query)
      self.log_info("Retrieved linked locations : " + fips_code)
      return events
    except:
      self.log_error("Exception occurred getting linked locations from the DB\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return None

  def get_country_from_fips_code(self,db_name,fips_code):
    '''

    :param db_name:
    :param fips_code:
    :return:
    '''
    try:
      self.log_info("Getting country name from FIPS country code")
      db = self.client[db_name]
      col_fips_country = db["fips_country"]
      query = {"CountryCode" : fips_code}
      country_name = col_fips_country.find_one(query)
      if country_name is None:
        self.log_error("Invalid Country Code : " + fips_code)
        return None
      country_name = country_name["CountryName"]
      return country_name
    except:
      self.log_error("Exception occurred getting country name from FIPS country code\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return None

  def get_lat_long(self,db_name,c_code):
    '''

    :param db_name:
    :param c_code:
    :return:
    '''
    try:
      self.log_info("Getting latitude and longitude for FIPS CODE: " + c_code)
      query = {"Actor1Geo_CountryCode":c_code}
      query1 = {"Actor2Geo_CountryCode":c_code}
      db = self.client[db_name]
      col_cameo_events = db["cameo_events"]
      country_event = col_cameo_events.find_one(query)
      if country_event is None:
        c_e_2 = col_cameo_events.find_one(query1)
        if c_e_2 is None:
          self.log_error("Could not find lat and long for the given location.")
          return None,None
        lat, long = c_e_2["Actor2Geo_Lat"],c_e_2["Actor2Geo_Long"]
        return lat,long
      lat, long = country_event["Actor1Geo_Lat"],country_event["Actor1Geo_Long"]
      return lat,long
      self.log_info("Successfully retrieved lattitude and longitude for : " + c_code)
    except:
      self.log_error("Exception occurred getting Latitude Longitude\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return None,None

  def update_linked_locations(self,db_name,linked_locations):
    '''
    Method to update the linked locations
    :param db_name: name of the db
    :param linked_locations: Linked location object
    :return: update status
    '''
    try:
      self.log_info("Inserting linked locations to db.")
      db = self.client[db_name]
      col_linked_locations = db["coll_linked_locations"]
      insert_status = col_linked_locations.insert_one(linked_locations)
      if insert_status is None:
        self.log_error("Could not insert linked locations into db")
        return False
      else:
        self.log_info("Successfully inserted linked locations into db")
        return True
    except:
      self.log_error("Exception occurred while updating linked locations.\n"+
                      "Exception stacktrace: \n" +
                      traceback.format_exc())
      return -1

  def update_overall_stats(self,db_name,n_events_now,n_events_today,n_events_this_month,
                           e_percent_higher_last_month,n_mentions_now,n_mentions_today,
                           n_mentions_this_month,m_percent_higher_last_month,n_countries_now,
                           n_countries_today,n_countries_this_month,c_percent_higher_last_month):
    '''
    Method to update overall statistics for events countries and mentions
    :param db_name:
    :param n_events_now:
    :param n_events_today:
    :param n_events_this_month:
    :param e_percent_higher_last_month:
    :param n_mentions_now:
    :param n_mentions_today:
    :param n_mentions_this_month:
    :param m_percent_higher_last_month:
    :param n_countries_now:
    :param n_countries_today:
    :param n_countries_this_month:
    :param c_percent_higher_last_month:
    :return:
    '''

    try:
      self.log_info("Inserting overall stats to db")
      db = self.client[db_name]
      col_overall_stats = db["coll_overall_stats"]
      timestamp = self.util.get_present_date_time()
      obj = {
        "n_events_now" :n_events_now,
        "n_events_today" :n_events_today,
        "n_events_this_month" :n_events_this_month,
        "e_percent_higher_last_month" :e_percent_higher_last_month,
        "n_mentions_now" :n_mentions_now,
        "n_mentions_today" :n_mentions_today,
        "n_mentions_this_month" :n_mentions_this_month,
        "m_percent_higher_last_month":m_percent_higher_last_month,
        "n_countries_now":n_countries_now,
        "n_countries_today":n_countries_today,
        "n_countries_this_month" :n_countries_this_month,
        "c_percent_higher_last_month":c_percent_higher_last_month,
        "timestamp":timestamp
      }
      insert_status = col_overall_stats.insert_one(obj)
      if insert_status is None:
        self.log_error("Couldnot insert overall stats into db")
        return False
      else:
        self.log_info("Successfully inserted overall stats into db")
        return True

    except:
      self.log_error("Exception occurred while updating the overall stats.\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return -1

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

  def update_indexes(self,db_name):
    '''
    Method to update indexes for easier access and sorting
    :param db_name: name of the db
    :return: update status
    '''
    self.log_info("Updating indexes for date field in events and mentions collections.")
    try:
      db = self.client[db_name]
      events_index_status = db["cameo_events"].create_index([('DATEADDED', pymongo.ASCENDING),('DATEADDED', pymongo.DESCENDING)],unique=True)
      mentions_index_status = db["cameo_mentions"].create_index([('EventTimeDate', pymongo.ASCENDING),('EventTimeDate', pymongo.DESCENDING)],unique=True)
      if events_index_status is None or mentions_index_status is None:
        self.log_error("Could not update indexes for events or mentions. Check db for further details")
        return False
    except:
      self.log_error("Exception occurred while total number of events till a given date from db\n"+
            "Exception stacktrace: \n"+
            traceback.format_exc())
      return False

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
        self.log_info("No Metadata object found. Creating a new metadata object and updating it.")
        metadata = {
            "last_update_date" : 0,
            "n_old_files" : 0,
            "n_new_files" : 0,
            "n_files_inserted" : 0,
            "n_pending_files" : -1,
            "n_old_events" : 0,
            "n_new_events" : 0,
            "n_events_inserted" : 0,
            "n_old_mentions" : 0,
            "n_new_mentions" : 0,
            "n_mentions_inserted" : 0
        }
        col_metadata.insert_one(metadata)
        if metadata is None:
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
