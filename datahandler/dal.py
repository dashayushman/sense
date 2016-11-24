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

	# the mongodb client
	client = None
	util = None
	logger = None

	def __init__(self, host, port, pool_size, util, log_dir):
		'''
		Constructor for creating a data access layer object.
		:param host: mongodb host
		:param port: mongodb port number
		:param pool_size: pol size for mongodb client
		:param logger: logger object for logging
		'''
		self.client = MongoClient(host, port, maxPoolSize=pool_size)
		self.util = util
		# self.initialize_logger(log_dir)
		return

	def initialize_logger(self, log_path):
		'''
		Method to initialize logger for dal
		:param log_path: path for storing the log file
		:return:
		'''
		try:
			# logging.basicConfig(filename=log_path,format='%(asctime)s %(
            # message)s %(
			# levelname)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)
			self.logger = logging.getLogger("utils")
			self.logger.setLevel(logging.DEBUG)

			# create a file handler
			log_path = os.path.join(log_path, "app.dal.log")
			handler = logging.handlers.RotatingFileHandler(log_path, maxBytes=5000,
				backupCount=5)

			# create a logging format
			formatter = logging.Formatter(
				'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
			handler.setFormatter(formatter)
			# add the handlers to the logger
			self.logger.addHandler(handler)
		except Exception as e:
			print("error({0}): {1}".format(e.errno, e.strerror))
			print(
			"unable to set specified logpath. Setting logpath to default which is "
            "app.log in the application folder itself.")
			# logging.basicConfig(filename="app.log", format='%(asctime)s %(
            # message)s %(
			# levelname)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)
			self.logger = logging.getLogger("dal")
			self.logger.setLevel(logging.DEBUG)

			# create a file handler
			handler = logging.handlers.RotatingFileHandler("app.dal.log",
				maxBytes=5000, backupCount=5)
			# create a logging format
			formatter = logging.Formatter(
				'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
			handler.setFormatter(formatter)
			# add the handlers to the logger
			self.logger.addHandler(handler)

	def insert_new_files(self, current_file_url_list, db_name, logger):
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
			for n, fileobj in enumerate(current_file_url_list):

				if n % 1000 == 0:
					self.log_info("Checking file " + str(n + 1) + " of " + str(
						len(current_file_url_list)) + " file urls")
				'''
				del fileobj["mentions"]["status"]
				del fileobj["event"]["status"]
				del fileobj["kgraph"]["status"]
				'''
				query = {
					"mentions.url": fileobj["mentions"]["url"],
					"event.url": fileobj["event"]["url"],
					"timestamp": fileobj["timestamp"]}
				exists = col_file_url.find_one(query)

				if exists is None:
					fileobj["mentions"]["status"] = 0
					fileobj["event"]["status"] = 0
					fileobj["kgraph"]["status"] = 0

					col_file_url.insert_one(fileobj)
					inserted = inserted + 1

			self.log_info("Number of files inserted: " + str(inserted) +
						  "\nNumber of new files skipped because they already "
                          "exist:" + str(
				len(current_file_url_list) - inserted))
			return True

		except Exception as e:

			self.log_error("DB init Error {0}".format(e.message))
			self.log_error(
				"Error occured while inserting newly added file urls into db")

			return False

	def get_file_urls_by_status(self, db_name, working_dir, status=0, sort=True):
		'''
		dal method for retrieving all file urls with a given status
		:param db_name: name of the database that we will query
		:param logger: logger object
		:param status: status of the file, 0 means not downloaded nor inserted and
		1 means downloaded and inserted
		:return:
		'''
		self.log_info("Getting pending files from db.")
		pending_files = None
		query = {"$or": [{"mentions.status": 0}, {"event.status": 0}]}

		try:
			db = self.client[db_name]
			col_file_url = db["coll_file_urls"]
			if sort:
				pending_files = col_file_url.find(query).sort('timestamp',
					pymongo.DESCENDING)
			else:
				pending_files = col_file_url.find(query)
			print("number of results " + str(pending_files.count()))
			return pending_files

		except Exception as e:
			self.log_error("DB Error ({0}): {1}".format(e.errno, e.strerror))
			self.log_error("Error occured while getting pending file urls from db")
			return None

	def update_file_status(self, db_name, fileobj, category, status):
		'''
		Method to update the status of a file in db
		:param db_name: name of the database
		:param fileobj: The file object that needs to be updated
		:param category: category of file (events, mentions, knowledge graph)
		:param status: new status of the file
		:return: update status (True or False)
		'''
		self.log_info(
			"Updating the status of file in database to inserted (status 1)")
		try:
			fileobj[category]["status"] = status
			db = self.client[db_name]
			col_file_url = db["coll_file_urls"]
			update_status = col_file_url.update({
				'_id': fileobj['_id']
			}, {
				'$set': fileobj
			}, upsert=False)
			if update_status["nModified"] == 1:
				# self.log_info("Successfully updated status of file in database")
				return True
			else:
				# self.log_info("Unable to update status of file in database")
				return False
		except Exception as e:
			self.log_error("DB update Error {0}".format(e.message))
			self.log_error("Exception occured while updating status of file in db")
			return False

	def get_global_impact_map_data(self, db_name, end_date, start_date=0):
		try:
			latlong = {}
			mapData = []
			db = self.client[db_name]
			coll_events = db["cameo_events"]
			agg_pipeline = [{
								"$match": {
									"$and": [{"DATEADDED": {"$lte": end_date}},
											 {"ActionGeo_FullName": {"$ne": ""}},
											 {"ActionGeo_CountryCode": {"$ne": ""}},
											 {"ActionGeo_Lat": {"$ne": ""}},
											 {"ActionGeo_Long": {"$ne": ""}},
											 {"GoldsteinScale": {"$ne": ""}},
											 {"SOURCEURL": {"$ne": ""}}]}}, {
								"$project": {
									"GLOBALEVENTID": 1, "DATEADDED": 1,
									"ActionGeo_FullName": 1,
									"ActionGeo_CountryCode": 1, "ActionGeo_Lat": 1,
									"ActionGeo_Long": 1, "SOURCEURL": 1,
									"GoldsteinScale": 1}}]
			if start_date != 0:
				agg_pipeline[0]["$match"]["$and"].append(
					{"DATEADDED": {"$gt": start_date}})
			results = coll_events.aggregate(agg_pipeline)
			already_added = []
			for e in results:
				GLOBALEVENTID = e["GLOBALEVENTID"]
				DATEADDED = e["DATEADDED"]
				ActionGeo_FullName = e["ActionGeo_FullName"]
				ActionGeo_CountryCode = e["ActionGeo_CountryCode"]
				ActionGeo_Lat = e["ActionGeo_Lat"]
				ActionGeo_Long = e["ActionGeo_Long"]
				SOURCEURL = e["SOURCEURL"]
				GoldsteinScale = e["GoldsteinScale"]
				country_name = self.get_country_from_fips_code(db_name,
					ActionGeo_CountryCode)
				if country_name is None:
					continue
				pseudo_code = ActionGeo_FullName.replace(" ", "_")
				pseudo_code = ''.join(e for e in pseudo_code if e.isalnum())
				code = ActionGeo_CountryCode + "_" + pseudo_code
				hex_color = self.util.convert_goldstein_to_hex_color(GoldsteinScale)
				value = abs(GoldsteinScale)
				self.log_info("Country : " + country_name +
							  "\nCountry Code: " + ActionGeo_CountryCode +
							  "\nlatitude: " + str(ActionGeo_Lat) +
							  "\nlongitude: " + str(ActionGeo_Long) +
							  "\ngoldstein's score: " + str(GoldsteinScale) +
							  "\nhex color code: " + hex_color)
				if code in already_added:
					last_count = mapData[already_added.index(code)]["count"]
					new_count = last_count + 1
					mapData[already_added.index(code)]["gs"] = (((last_count *
																  mapData[
																	  already_added.index(
																		  code)][
																	  "gs"]) / new_count) + (
																GoldsteinScale / new_count))
					mapData[already_added.index(code)]["count"] = new_count
					hex_color = self.util.convert_goldstein_to_hex_color(
						mapData[already_added.index(code)]["gs"])
					mapData[already_added.index(code)]["color"] = hex_color
					mapData[already_added.index(code)]["url"] = SOURCEURL
					print(
					"maja hella : " + str(mapData[already_added.index(code)]["gs"]))
					continue
				already_added.append(code)
				latlong[code] = {
					"latitude": ActionGeo_Lat,
					"longitude": ActionGeo_Long
				}

				map_data_obj = {
					"code": code,
					"name": ActionGeo_FullName,
					"country_name": country_name,
					"value": value,
					"color": hex_color,
					"gs": GoldsteinScale,
					"url": SOURCEURL,
					"count": 1
				}
				mapData.append(map_data_obj)

			dt = self.util.gdelt_date_to_datetime(end_date)
			resp = {
				"day": dt.day,
				"month": dt.month,
				"year": dt.year,
				"mintue": dt.minute,
				"startDate": start_date,
				"endDate": end_date,
				"timestamp": self.util.get_present_date_time(),
				"mapData": mapData,
				"latlong": latlong,
			}

			if len(mapData) == 0:
				resp["status"] = 0
				resp["message"] = "No events found in the given range"
			else:
				resp["status"] = 1
				resp["message"] = "Successfully retrieved global impact map data."

			return resp

		except:
			self.log_error(
				"Exception occurred getting global impact map data from the DB\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return None

	def update_impact_map(self, db_name, impact_map_data, update=False):
		try:
			insert_status = None
			self.log_info("Updating Global impact map to db.")
			db = self.client[db_name]
			coll_impact_map = db["coll_impact_map"]
			if update:
				insert_status = coll_impact_map.update({
					'startDate': impact_map_data['startDate']
				}, {
					'$set': impact_map_data
				}, upsert=True)
			else:
				insert_status = coll_impact_map.insert_one(impact_map_data)
			if insert_status is None:
				self.log_error("Could not insert Global impact map into db")
				return False
			else:
				self.log_info("Successfully inserted Global impact map into db")
				return True
		except:
			self.log_error("Exception occurred while updating Global impact "
                           "map.\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return -1

	def get_total_mentions(self, db_name, end_date, start_date=0):
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
			results = self.get_mentions(db_name, end_date, start_date)
			if results is None:
				return -1
			n_events = results.count()
			return n_events
		except:
			self.log_error(
				"Exception occurred while getting total number of mentions till a "
                "given date from db\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return -1

	def get_total_events(self, db_name, end_date, start_date=0):
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
			results = self.get_events(db_name, end_date, start_date)
			if results is None:
				return -1
			n_events = results.count()
			return n_events
		except:
			self.log_error(
				"Exception occurred while total number of events till a given date "
                "from db\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return -1

	def get_total_countries(self, db_name, end_date, start_date=0):
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
			results = self.get_events(db_name, end_date, start_date)
			if results is None:
				return -1
			n_actor_1_geo = results.distinct("Actor1CountryCode")
			n_actor_2_geo = results.distinct("Actor2CountryCode")
			n_actor_1_geo_1 = results.distinct("Actor1Geo_CountryCode")
			n_actor_2_geo_1 = results.distinct("Actor2Geo_CountryCode")
			n_action_geo = results.distinct("ActionGeo_CountryCode")
			unique = list(set(n_actor_1_geo +
							  n_actor_2_geo +
							  n_actor_1_geo_1 +
							  n_actor_2_geo_1 +
							  n_action_geo
							  ))
			n_countries = len(unique)
			return n_countries
		except:
			self.log_error(
				"Exception occurred while getting total number of countries "
                "involved till a given date from db\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return -1

	def get_events(self, db_name, end_date, start_date=0):
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
			query = {"$and": [{"DATEADDED": {"$lte": end_date}}]}
			if start_date != 0:
				query["$and"].append({"DATEADDED": {"$gt": start_date}})
			results = col_cameo_events.find(query)
			return results
		except:
			self.log_error("Exception occurred while getting events from the DB\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return None

	def get_event_count(self, db_name, end_date, start_date=0):
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
			query = {"$and": [{"DATEADDED": {"$lte": end_date}}]}
			if start_date != 0:
				query["$and"].append({"DATEADDED": {"$gt": start_date}})
			results = col_cameo_events.find(query).count()
			return {'value' : results,
					'date' : self.util.gdelt_date_to_datetime(
						start_date).strftime("%Y-%m-%d")}
		except:
			self.log_error("Exception occurred while getting events from the DB\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return None

	def get_globe_viz(self, db_name, end_date, start_date=0):
		try:
			db = self.client[db_name]
			col_cameo_events = db["cameo_events"]
			query = {"$and": [{"DATEADDED": {"$lte": end_date}}, {"ActionGeo_Long" : {"$ne" : ''}}, {"ActionGeo_Lat" : {"$ne" : ''}}]}
			if start_date != 0:
				query["$and"].append({"DATEADDED": {"$gt": start_date}})

			results = col_cameo_events.find(query)
			return results
		except:
			self.log_error("Exception occurred while getting events from the DB\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return None

	def delete_coll_event_timeline(self, db_name):
		try:
			db = self.client[db_name]
			col_event_timeline = db["coll_event_timeline"]

			results = col_event_timeline.delete_many({})
			return results
		except:
			self.log_error(traceback.format_exc())
			return None

	def get_mentions(self, db_name, end_date, start_date=0):
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
			query = {"$and": [{"MentionTimeDate": {"$lte": end_date}}]}
			if start_date != 0:
				query["$and"].append({"MentionTimeDate": {"$gt": start_date}})
			results = col_cameo_mentions.find(query)
			return results
		except:
			self.log_error(
				"Exception occurred while getting mentions from the DB\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return None

	def get_articles_per_category(self, db_name, end_date, start_date=0):
		'''

		:param db_name:
		:param end_date:
		:param start_date:
		:return:
		'''
		try:
			db = self.client[db_name]
			coll_mentions = db["cameo_mentions"]
			agg_pipeline = [
				{"$match": {"$and": [{"MentionTimeDate": {"$lte": end_date}}]}},
				{"$group": {"_id": "$MentionType", "n_mentions": {"$sum": 1}}}]
			if start_date != 0:
				agg_pipeline[0]["$match"]["$and"].append(
					{"MentionTimeDate": {"$gt": start_date}})
			results = list(coll_mentions.aggregate(agg_pipeline))
			if len(results) == 0:
				return {
					"timestamp": self.util.get_present_date_time(),
					"data": [],
					"status": 0,
					"message": "No mentions were found in the given duration."
				}
			else:
				for i, e in enumerate(results):
					m_code = e["_id"]
					m_name = self.get_mention_type_from_code(db_name, m_code)
					results[i]["source"] = m_name
				return {
					"timestamp": self.util.get_present_date_time(),
					"data": results,
					"status": 1,
					"message": "Successfully retrieved number of mentions in the "
                               "given duration."
				}

		except:
			self.log_error(
				"Exception occurred getting linked locations from the DB\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return None

	def get_high_impact_events(self, db_name, end_date, start_date=0, limit=10):
		'''
		Method to get High impact events in the given range and limit
		:param end_date:
		:param start_date:
		:param limit:
		:return:
		'''
		try:
			db = self.client[db_name]
			coll_events = db["cameo_events"]
			agg_pipeline = [{
								"$match": {
									"$and": [{"DATEADDED": {"$lte": end_date}},
											 {"Actor1Name": {"$ne": ""}},
											 {"Actor2Name": {"$ne": ""}},
											 {"ActionGeo_CountryCode": {"$ne": ""}},
											 {"Actor1Geo_CountryCode": {"$ne": ""}},
											 {"Actor2Geo_CountryCode": {"$ne": ""}},
											 {"Actor2Geo_CountryCode": {"$ne": ""}},
											 {"SOURCEURL": {"$ne": ""}},
											 {"GoldsteinScale": {"$ne": ""}},
											 {"AvgTone": {"$ne": ""}}]}}, {
								"$project": {
									"GLOBALEVENTID": 1, "EventCode": 1,
									"GoldsteinScale": 1, "Actor1Name": 1,
									"Actor2Name": 1, "NumMentions": 1,
									"NumSources": 1, "NumArticles": 1, "AvgTone": 1,
									"Actor1Geo_FullName": 1,
									"Actor1Geo_CountryCode": 1,
									"Actor2Geo_FullName": 1,
									"Actor2Geo_CountryCode": 1,
									"ActionGeo_FullName": 1,
									"ActionGeo_CountryCode": 1, "SOURCEURL": 1,
									"score": {
										"$avg": [{"$abs": "$GoldsteinScale"},
												 "$NumMentions", "$NumSources",
												 "$NumArticles",
												 {"$abs": "$AvgTone"}]}}},
							{"$sort": {"score": -1}}, {"$limit": 10}]
			if start_date != 0:
				agg_pipeline[0]["$match"]["$and"].append(
					{"DATEADDED": {"$gt": start_date}})
			results = list(coll_events.aggregate(agg_pipeline))
			if len(results) == 0:
				return {
					"data": [],
					"status": 0,
					"message": "No events were found in the given duration."
				}
			else:
				for i, e in enumerate(results):
					geo_code = e["ActionGeo_CountryCode"]
					e_code = e["EventCode"]
					geo_name = self.get_country_from_fips_code(db_name, geo_code)
					event_type = self.get_event_type_from_cameo_code(db_name,
                      e_code)
					if event_type is None:
						event_type = ""
					results[i]["FIPS_Country_Name"] = geo_name
					results[i]["EventType"] = event_type
				return {
					"data": results,
					"status": 1,
					"message": "Successfully retrieved High Imapct regions."
				}

		except:
			self.log_error(
				"Exception occurred getting linked locations from the DB\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return None

	def get_high_impact_regions(self, db_name, end_date, start_date=0, limit=10):
		'''
		Method to get High impact regions in the given range and limit
		:param end_date:
		:param start_date:
		:param limit:
		:return:
		'''
		try:
			db = self.client[db_name]
			coll_events = db["cameo_events"]
			agg_pipeline = [{
								"$match": {
									"$and": [{"DATEADDED": {"$lte": end_date}},
											 {"ActionGeo_CountryCode": {"$ne": ""}},
											 {"SOURCEURL": {"$ne": ""}},
											 {"GoldsteinScale": {"$ne": ""}},
											 {"AvgTone": {"$ne": ""}}]}}, {
								"$project": {
									"GLOBALEVENTID": 1, "GoldsteinScale": 1,
									"ActionGeo_FullName": 1,
									"ActionGeo_CountryCode": 1, "SOURCEURL": 1,
									"score": {
										"$avg": [{"$abs": "$GoldsteinScale"},
												 {"$abs": "$AvgTone"}]}}},
							{"$sort": {"score": -1}}, {"$limit": 100}]
			if start_date != 0:
				agg_pipeline[0]["$match"]["$and"].append(
					{"DATEADDED": {"$gt": start_date}})
			results = list(coll_events.aggregate(agg_pipeline))
			final_results = []
			if len(results) == 0:
				return {
					"data": [],
					"status": 0,
					"message": "No events were found in the given duration."
				}
			else:
				already_added = []
				for i, e in enumerate(results):
					if len(already_added) == limit:
						break
					geo_code = e["ActionGeo_CountryCode"]
					_id = e["ActionGeo_FullName"] + "_" + geo_code
					if _id not in already_added:
						already_added.append(_id)
						geo_name = self.get_country_from_fips_code(db_name,
                          geo_code)
						results[i]["FIPS_Country_Name"] = geo_name
						final_results.append(results[i])
					else:
						continue
				return {
					"data": final_results,
					"status": 1,
					"message": "Successfully retrieved High Imapct regions."
				}

		except:
			self.log_error(
				"Exception occurred getting linked locations from the DB\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return None

	def get_actor_network(self, db_name, end_date, start_date=0):
		try:
			nodes = []
			edges = []
			db = self.client[db_name]
			coll_events = db["cameo_events"]
			query = {
				"$and": [{"DATEADDED": {"$lte": end_date}},
						 {"Actor1CountryCode": {"$ne": ""}},
						 {"Actor2CountryCode": {"$ne": ""}},
						 {"Actor1Name": {"$ne": ""}}, {"Actor2Name": {"$ne": ""}}]}
			agg_pipeline = [{
								"$match": {
									"$and": [{"DATEADDED": {"$lte": end_date}},
											 {"Actor1CountryCode": {"$ne": ""}},
											 {"Actor2CountryCode": {"$ne": ""}},
											 {"Actor1Name": {"$ne": ""}},
											 {"Actor2Name": {"$ne": ""}}]}}, {
								"$project": {
									"GLOBALEVENTID": 1, "DATEADDED": 1,
									"Actor1CountryCode": 1, "Actor2CountryCode": 1,
									"Actor1Name": 1, "Actor2Name": 1}}, {
								"$group": {
									"_id": {
										"Actor1CountryCode": "$Actor1CountryCode",
										"Actor2CountryCode": "$Actor2CountryCode",
										"Actor1Name": "$Actor1Name",
										"Actor2Name": "$Actor2Name"},
									"count": {"$sum": 1}}}, {
								"$project": {
									"Actor1CountryCode": "$_id.Actor1CountryCode",
									"Actor2CountryCode": "$_id.Actor2CountryCode",
									"Actor1Name": "$_id.Actor1Name",
									"Actor2Name": "$_id.Actor2Name",
									"Count": "$count", "Actor1Id": {
										"$concat": ["$_id.Actor1Name", "_",
													"$_id.Actor1CountryCode"]},
									"Actor2Id": {
										"$concat": ["$_id.Actor2Name", "_",
													"$_id.Actor2CountryCode"]}}}]
			# actor_ids_agg = [{"$project":{"Actor1Id":{"$concat":["$Actor1Name",
            # "_",
			# "$Actor1CountryCode"]},"Actor2Id":{"$concat":["$Actor2Name","_",
            # "$Actor2CountryCode"]}}}]
			if start_date != 0:
				agg_pipeline[0]["$match"]["$and"].append(
					{"DATEADDED": {"$gt": start_date}})
				query["$and"].append({"DATEADDED": {"$gt": start_date}})
			r_cur = coll_events.aggregate(agg_pipeline)

			events = coll_events.find(query)
			a1_geos = events.distinct("Actor1CountryCode")
			a2_geos = events.distinct("Actor2CountryCode")

			# a1 = events.distinct("Actor1Id")
			# a2 = events.distinct("Actor2Id")

			geo_codebook = list(set(a1_geos + a2_geos))
			actor_codebook = ["_SKIP"]
			actor_scale = {}

			for node_pair in r_cur:
				a1_name = node_pair["Actor1Name"]
				a2_name = node_pair["Actor2Name"]

				a1_id = node_pair["Actor1Id"]
				a2_id = node_pair["Actor2Id"]

				edge_count = node_pair["Count"]

				a1_update = False
				a2_update = False
				if a1_id not in actor_codebook:
					actor_codebook.append(a1_id)
					actor_scale[a1_id] = 1
					a1_update = True
				else:
					actor_scale[a1_id] = actor_scale[a1_id] + 1

				if a2_id not in actor_codebook:
					actor_codebook.append(a2_id)
					actor_scale[a2_id] = 1
					a2_update = True
				else:
					actor_scale[a2_id] = actor_scale[a2_id] + 1

				a1_geo_code = node_pair["Actor1CountryCode"]
				a2_geo_code = node_pair["Actor2CountryCode"]

				a1_geo_name = self.get_country_from_cameo_code(db_name, a1_geo_code)
				a2_geo_name = self.get_country_from_cameo_code(db_name, a2_geo_code)

				if a1_geo_name is None or a2_geo_name is None:
					continue

				a1_codebook_id = actor_codebook.index(a1_id)
				a2_codebook_id = actor_codebook.index(a2_id)

				a1_geo_codebook_id = geo_codebook.index(a1_geo_code)
				a2_geo_codebook_id = geo_codebook.index(a2_geo_code)

				a1_node = {
					"group": a1_geo_codebook_id,
					"id": a1_codebook_id,
					"actor_id": a1_id,
					"label": a1_name,
					"title": "Country: " + a1_geo_name + "<br>Actor: " + a1_name,
					"value": a1_codebook_id
				}
				a2_node = {
					"group": a2_geo_codebook_id,
					"id": a2_codebook_id,
					"actor_id": a2_id,
					"label": a2_name,
					"title": "Country: " + a2_geo_name + "<br>Actor: " + a2_name,
					"value": a2_codebook_id
				}

				edge = {
					"from": a1_codebook_id,
					"to": a2_codebook_id,
					"value": edge_count
				}

				if a1_update: nodes.append(a1_node)
				if a2_update: nodes.append(a2_node)
				if a1_update is False and a2_update is False:
					continue
				edges.append(edge)

			for i, node in enumerate(nodes):
				if node["actor_id"] in actor_scale:
					nodes[i]["value"] = actor_scale[node["actor_id"]]
			return {
				"startDate": start_date,
				"endDate": end_date,
				"timestamp": self.util.get_present_date_time(),
				"nodes": nodes,
				"edges": edges,
				"status": 1,
				"message": "Successfully created the actor network."
			}

		except:
			self.log_error("Exception occurred getting actor network from the "
                           "DB\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return None

	def get_linked_locations(self, db_name, end_date, svg, start_date=0):
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
			results = self.get_events(db_name, end_date, start_date)
			actor_1_geos = results.distinct("Actor1Geo_CountryCode")
			if len(actor_1_geos) is 0:
				return {
					"timestamp": self.util.get_present_date_time(),
					"lines": lines,
					"images": images,
					"status": 0,
					"message": "No 1st actors could be found in the selected time "
                               "duration"
				}
			already_added = []
			for k, actor_geo in enumerate(actor_1_geos):
				if k == 25:
					break
				if len(actor_geo) == 0:
					continue
				a1_geo_name = self.get_country_from_fips_code(db_name, actor_geo)
				if a1_geo_name is None:
					continue
				a1_lat, a1_long = self.get_lat_long(db_name, actor_geo)
				if a1_lat is None or a1_long is None or a1_geo_name is None:
					continue
				linked_events = self.get_linked_countries(db_name, start_date,
					end_date, actor_geo)
				if linked_events is None:
					continue
				if linked_events.count() == 0:
					continue

				if actor_geo in already_added:
					images[already_added.index(actor_geo)]["scale"] = 1.5
				else:
					images.append({
						"id": actor_geo,
						"svgPath": svg,
						"title": a1_geo_name,
						"latitude": a1_lat,
						"longitude": a1_long,
						"scale": 1.0
					})
					already_added.append(actor_geo)
				for l, e in enumerate(linked_events):
					if l == 20:
						break
					a2_geo_code = e["Actor2Geo_CountryCode"]
					if len(a2_geo_code) is 0:
						continue
					if actor_geo == a2_geo_code:
						continue
					a2_geo_name = self.get_country_from_fips_code(db_name,
						a2_geo_code)
					if a2_geo_name is None:
						continue
					a2_lat, a2_long = self.get_lat_long(db_name, a2_geo_code)
					if a2_lat is None or a2_long is None:
						continue

					lines.append({
						"latitudes": [a1_lat, a2_lat],
						"longitudes": [a1_long, a2_long]
					})

					if a2_geo_code not in already_added:
						images.append({
							"id": a2_geo_code,
							"svgPath": svg,
							"title": a2_geo_name,
							"latitude": a2_lat,
							"longitude": a2_long,
							"scale": 0.5
						})
						already_added.append(a2_geo_code)

			return {
				"timestamp": self.util.get_present_date_time(),
				"lines": lines,
				"images": images,
				"status": 1,
				"message": "Successfully generated linked locations."
			}
		except:
			self.log_error(
				"Exception occurred getting linked locations from the DB\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return None

	def get_linked_countries(self, db_name, start_date, end_date, fips_code):
		'''

		:param db_name:
		:param fips_code:
		:return:
		'''
		self.log_info("Getting all linked locations with : " + fips_code)
		try:
			db = self.client[db_name]
			coll_events = db["cameo_events"]
			query = {
				"$and": [{"Actor1Geo_CountryCode": fips_code},
						 {"DATEADDED": {"$lte": end_date}}]}
			if start_date != 0:
				query["$and"].append({"DATEADDED": {"$gt": start_date}})
			events = coll_events.find(query)
			self.log_info("Retrieved linked locations : " + fips_code)
			return events
		except:
			self.log_error(
				"Exception occurred getting linked locations from the DB\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return None

	def get_mention_type_from_code(self, db_name, m_code):
		'''

		:param db_name:
		:param m_code:
		:return:
		'''
		try:
			self.log_info("Getting mention type mention types collection")
			db = self.client[db_name]
			col_gdelt_mentions_type = db["gdelt_mentions_type"]
			query = {"code": m_code}
			mentions_name = col_gdelt_mentions_type.find_one(query)
			if mentions_name is None:
				self.log_error("Invalid Mentions Code : " + m_code)
				return None
			name = mentions_name["name"]
			return name
		except:
			self.log_error("Exception occurred getting Mentions type \n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return None

	def get_country_from_cameo_code(self, db_name, cameo_code):
		'''

		:param db_name:
		:param cameo_code:
		:return:
		'''
		try:
			self.log_info("Getting country name from cameo country code")
			db = self.client[db_name]
			col_cameo_country = db["cameo_countries"]
			query = {"CODE": cameo_code}
			country_name = col_cameo_country.find_one(query)
			if country_name is None:
				self.log_error("Invalid Country Code : " + cameo_code)
				return None
			country_name = country_name["LABEL"]
			return country_name
		except:
			self.log_error(
				"Exception occurred getting country name from cameo country "
                "code\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return None

	def get_event_type_from_cameo_code(self, db_name, cameo_code):
		'''

		:param db_name:
		:param fips_code:
		:return:
		'''
		try:
			self.log_info("Getting country name from FIPS country code")
			db = self.client[db_name]
			col_cameo_type = db["cameo_eventcodes"]
			query = {"CAMEOEVENTCODE": cameo_code}
			event_name = col_cameo_type.find_one(query)
			if event_name is None:
				self.log_error("Invalid Event Code : " + cameo_code)
				return None
			event_name = event_name["EVENTDESCRIPTION"]
			return event_name
		except:
			self.log_error(
				"Exception occurred getting Event type from Cameo event code\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return None

	def get_country_from_fips_code(self, db_name, fips_code):
		'''

		:param db_name:
		:param fips_code:
		:return:
		'''
		try:
			self.log_info("Getting country name from FIPS country code")
			db = self.client[db_name]
			col_fips_country = db["fips_country"]
			query = {"CountryCode": fips_code}
			country_name = col_fips_country.find_one(query)
			if country_name is None:
				self.log_error("Invalid Country Code : " + fips_code)
				return None
			country_name = country_name["CountryName"]
			return country_name
		except:
			self.log_error(
				"Exception occurred getting country name from FIPS country code\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return None

	def get_lat_long(self, db_name, c_code):
		'''

		:param db_name:
		:param c_code:
		:return:
		'''
		try:
			self.log_info("Getting latitude and longitude for FIPS CODE: " + c_code)
			query = {"Actor1Geo_CountryCode": c_code}
			query1 = {"Actor2Geo_CountryCode": c_code}
			db = self.client[db_name]
			col_cameo_events = db["cameo_events"]
			country_event = col_cameo_events.find_one(query)
			if country_event is None:
				c_e_2 = col_cameo_events.find_one(query1)
				if c_e_2 is None:
					self.log_error(
						"Could not find lat and long for the given location.")
					return None, None
				lat, long = c_e_2["Actor2Geo_Lat"], c_e_2["Actor2Geo_Long"]
				return lat, long
			lat, long = country_event["Actor1Geo_Lat"], country_event[
				"Actor1Geo_Long"]
			return lat, long
			self.log_info(
				"Successfully retrieved lattitude and longitude for : " + c_code)
		except:
			self.log_error("Exception occurred getting Latitude Longitude\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return None, None

	def update_articles_per_category(self, db_name, articles_per_category):
		'''

		:param db_name:
		:param articles_per_category:
		:return:
		'''
		try:
			self.log_info("Updating articles_per_category to db.")
			db = self.client[db_name]
			coll_articles_per_category = db["coll_articles_per_category"]
			insert_status = coll_articles_per_category.insert_one(
				articles_per_category)
			if insert_status is None:
				self.log_error("Could not insert articles_per_category into db")
				return False
			else:
				self.log_info("Successfully inserted articles_per_category into db")
				return True
		except:
			self.log_error(
				"Exception occurred while updating articles_per_category.\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return -1

	def update_mentions_timeline(self, db_name, timeline):
		'''

		:param db_name:
		:param timeline:
		:return:
		'''
		try:
			self.log_info("Updating mentions_timeline to db.")
			db = self.client[db_name]
			coll_mentions_timeline = db["coll_mentions_timeline"]
			insert_status = coll_mentions_timeline.insert_one(timeline)
			if insert_status is None:
				self.log_error("Could not insert mentions_timeline into db")
				return False
			else:
				self.log_info("Successfully inserted mentions_timeline into db")
				return True
		except:
			self.log_error("Exception occurred while updating "
                           "mentions_timeline.\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return -1

	def update_high_impact_events(self, db_name, hir_object):
		'''
		Method to update high impact events
		:param db_name:
		:param hir_object:
		:return:
		'''
		try:
			self.log_info("Updating High Impact events to db.")
			db = self.client[db_name]
			col_high_impact_events = db["coll_high_impact_events"]
			insert_status = col_high_impact_events.insert_one(hir_object)
			if insert_status is None:
				self.log_error("Could not insert high impact events into db")
				return False
			else:
				self.log_info("Successfully inserted high impact events into db")
				return True
		except:
			self.log_error(
				"Exception occurred while updating high events regions.\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return -1

	def update_high_impact_regions(self, db_name, hir_object):
		'''
		Method to update high impact regions
		:param db_name:
		:param hir_object:
		:return:
		'''
		try:
			self.log_info("Updating High Impact regions to db.")
			db = self.client[db_name]
			col_high_impact_regions = db["coll_high_impact_regions"]
			insert_status = col_high_impact_regions.insert_one(hir_object)
			if insert_status is None:
				self.log_error("Could not insert high impact regions into db")
				return False
			else:
				self.log_info("Successfully inserted high impact regions into db")
				return True
		except:
			self.log_error(
				"Exception occurred while updating high impact regions.\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return -1

	def update_event_count(self, db_name, event_count):
		try:
			self.log_info("Inserting event timeline to db.")
			db = self.client[db_name]
			coll_event_timeline = db["coll_event_timeline"]
			insert_status = coll_event_timeline.insert_one(event_count)
			if insert_status is None:
				self.log_error("Could not insert event_count into db")
				return False
			else:
				self.log_info("Successfully inserted event_count into db")
				return True
		except:
			self.log_error("Exception occurred while updating event_count.\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return -1

	def update_globe_viz(self, db_name, globe_viz, update=False):
		'''

		:param db_name:
		:param actor_network:
		:return:
		'''
		try:
			self.log_info("Inserting globe_viz to db.")
			db = self.client[db_name]
			coll_globe_viz = db["coll_globe_viz"]
			if update:
				insert_status = coll_globe_viz.update({
					'endDate': globe_viz['startDate']
				}, {
					'$set': globe_viz
				}, upsert=True)
			else:
				insert_status = coll_globe_viz.insert_one(globe_viz)
			if insert_status is None:
				self.log_error("Could not insert globe_viz into db")
				return False
			else:
				self.log_info("Successfully inserted globe_viz into db")
				return True
		except:
			self.log_error("Exception occurred while updating globe_viz.\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return -1

	def update_actor_network(self, db_name, actor_network, update=False):
		'''

		:param db_name:
		:param actor_network:
		:return:
		'''
		try:
			self.log_info("Inserting actor network to db.")
			db = self.client[db_name]
			coll_actor_network = db["coll_actor_network"]
			coll_impact_map = db["coll_actor_network"]
			if update:
				insert_status = coll_impact_map.update({
					'endDate': actor_network['endDate']
				}, {
					'$set': actor_network
				}, upsert=True)
			else:
				insert_status = coll_actor_network.insert_one(actor_network)
			if insert_status is None:
				self.log_error("Could not insert actor network into db")
				return False
			else:
				self.log_info("Successfully inserted actor network into db")
				return True
		except:
			self.log_error("Exception occurred while updating actor network.\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return -1

	def update_linked_locations(self, db_name, linked_locations):
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
			self.log_error("Exception occurred while updating linked locations.\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return -1

	def update_overall_stats(self, db_name, n_events_now, n_events_today,
							 n_events_this_month,
							 e_percent_higher_last_month, n_mentions_now,
							 n_mentions_today,
							 n_mentions_this_month, m_percent_higher_last_month,
							 n_countries_now,
							 n_countries_today, n_countries_this_month,
							 c_percent_higher_last_month):
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
				"n_events_now": n_events_now,
				"n_events_today": n_events_today,
				"n_events_this_month": n_events_this_month,
				"e_percent_higher_last_month": e_percent_higher_last_month,
				"n_mentions_now": n_mentions_now,
				"n_mentions_today": n_mentions_today,
				"n_mentions_this_month": n_mentions_this_month,
				"m_percent_higher_last_month": m_percent_higher_last_month,
				"n_countries_now": n_countries_now,
				"n_countries_today": n_countries_today,
				"n_countries_this_month": n_countries_this_month,
				"c_percent_higher_last_month": c_percent_higher_last_month,
				"timestamp": timestamp
			}
			insert_status = col_overall_stats.insert_one(obj)
			if insert_status is None:
				self.log_error("Couldnot insert overall stats into db")
				return False
			else:
				self.log_info("Successfully inserted overall stats into db")
				return True

		except:
			self.log_error("Exception occurred while updating the overall "
                           "stats.\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return -1

	def get_last_update_date(self, db_name):
		'''
		Method to get the last update date
		:param db_name: db name
		:return: last update date
		'''
		metadata = self.get_metadata(db_name)
		if metadata is None:
			self.log_error(
				"Metadata could not be loaded. Look into the database for more "
                "details. " +
				"Will use default datetime as now.")
			return None
		return metadata["last_update_date"]

	def get_first_update_date_mt(self, db_name):
		'''
		Method to get the last update date
		:param db_name: db name
		:return: last update date
		'''
		self.log_info("Getting first update date from db.")
		try:
			db = self.client[db_name]
			col_events = db["cameo_mentions"]

			first_event = col_events.find({}).sort('MentionTimeDate',
				pymongo.ASCENDING).limit(1)
			if first_event.count() != 0:
				print("First event date:  " + str(first_event.__getitem__(0)['MentionTimeDate']))
				return first_event.__getitem__(0)['MentionTimeDate']
			else:
				return None

		except Exception as e:
			self.log_error("Exception occurred while getting first event from "
                           "db\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return None


	def get_first_update_date_globe_viz(self, db_name):
		'''
		Method to get the last update date
		:param db_name: db name
		:return: last update date
		'''
		self.log_info("Getting first update date from db.")
		try:
			db = self.client[db_name]
			col_events = db["cameo_events"]
			col_impact_map = db["coll_globe_viz"]

			last_updated_map = col_impact_map.find({}).sort('timestamp',
				pymongo.DESCENDING).limit(1)

			if last_updated_map.count() != 0:
				print("Last updated map:  " + str(
					last_updated_map.__getitem__(0)['startDate']))
				return last_updated_map.__getitem__(0)['startDate']

			first_event = col_events.find({}).sort('DATEADDED',
				pymongo.ASCENDING).limit(1)
			if first_event.count() != 0:
				print("First event date:  " + str(first_event.__getitem__(0)['DATEADDED']))
				return first_event.__getitem__(0)['DATEADDED']
			else:
				return None

		except Exception as e:
			self.log_error("Exception occurred while getting first event from "
                           "db\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return None

	def get_first_update_date_act_net(self, db_name):
		'''
		Method to get the last update date
		:param db_name: db name
		:return: last update date
		'''
		self.log_info("Getting first update date from db.")
		try:
			db = self.client[db_name]
			col_events = db["cameo_events"]
			col_impact_map = db["coll_actor_network"]

			last_updated_map = col_impact_map.find({}).sort('timestamp',
				pymongo.DESCENDING).limit(1)

			if last_updated_map.count() != 0:
				print("Last updated map:  " + str(
					last_updated_map.__getitem__(0)['startDate']))
				return last_updated_map.__getitem__(0)['startDate']

			first_event = col_events.find({}).sort('DATEADDED',
				pymongo.ASCENDING).limit(1)
			if first_event.count() != 0:
				print("First event date:  " + str(first_event.__getitem__(0)['DATEADDED']))
				return first_event.__getitem__(0)['DATEADDED']
			else:
				return None

		except Exception as e:
			self.log_error("Exception occurred while getting first event from "
                           "db\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return None

	def get_first_update_date_impact_map(self, db_name):
		'''
		Method to get the last update date
		:param db_name: db name
		:return: last update date
		'''
		self.log_info("Getting first update date from db.")

		try:
			db = self.client[db_name]
			col_events = db["cameo_events"]
			col_impact_map = db["coll_impact_map"]

			last_updated_map = col_impact_map.find({}).sort('timestamp',
				pymongo.DESCENDING).limit(1)

			if last_updated_map.count() != 0:
				print("Last updated map:  " + str(
					last_updated_map.__getitem__(0)['startDate']))
				return last_updated_map.__getitem__(0)['startDate']

			first_event = col_events.find({}).sort('DATEADDED',
				pymongo.ASCENDING).limit(1)

			if first_event.count() != 0:
				print("First event date:  " + str(first_event.__getitem__(0)['DATEADDED']))
				return first_event.__getitem__(0)['DATEADDED']
			else:
				return

		except Exception as e:
			self.log_error("Exception occurred while getting first event from "
                           "db\n" +
						   "Exception stacktrace: \n" +
						   traceback.format_exc())
			return None

	def update_indexes(self, db_name):
		'''
		Method to update indexes for easier access and sorting
		:param db_name: name of the db
		:return: update status
		'''
		self.log_info(
			"Updating indexes for date field in events and mentions collections.")
		try:
			db = self.client[db_name]
			events_index_status = db["cameo_events"].create_index(
				[('DATEADDED', pymongo.ASCENDING),
				 ('DATEADDED', pymongo.DESCENDING)], unique=True)
			mentions_index_status = db["cameo_mentions"].create_index(
				[('EventTimeDate', pymongo.ASCENDING),
				 ('EventTimeDate', pymongo.DESCENDING)], unique=True)
			if events_index_status is None or mentions_index_status is None:
				self.log_error(
					"Could not update indexes for events or mentions. Check db for "
                    "further details")
				return False
		except:
			self.log_error(
				"Exception occurred while total number of events till a given date "
                "from db\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return False

	def get_metadata(self, db_name):
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
			self.log_error(
				"Exception occurred while getting metadata object from db\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return None

	def update_metadata(self, db_name):
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
				self.log_info(
					"No Metadata object found. Creating a new metadata object and "
                    "updating it.")
				metadata = {
					"last_update_date_str": "",
					"last_update_date": 0,
					"n_old_files": 0,
					"n_new_files": 0,
					"n_files_inserted": 0,
					"n_pending_files": -1,
					"n_old_events": 0,
					"n_new_events": 0,
					"n_events_inserted": 0,
					"n_old_mentions": 0,
					"n_new_mentions": 0,
					"n_mentions_inserted": 0
				}
				col_metadata.insert_one(metadata)
				if metadata is None:
					return False

			latest_added_file = col_files.find().sort('timestamp',
				pymongo.DESCENDING).limit(1).__getitem__(0)

			# updating the last update date in GDELT format
			if latest_added_file is None:
				metadata["last_update_date"] = self.util.get_present_date_time()
			else:
				metadata["last_update_date"] = latest_added_file["timestamp"]

			metadata["last_update_date_str"] = str(
				self.util.gdelt_date_to_datetime(metadata["last_update_date"]))

			# updating the number of old files
			metadata["n_old_files"] = metadata["n_new_files"]

			# updating the number of new files
			col_files.find().count()
			metadata["n_new_files"] = col_files.find().count()

			# updating number of files inserted
			metadata["n_files_inserted"] = metadata["n_new_files"] - metadata[
				"n_old_files"]

			# updating number of pending files
			query = {"$or": [{"mentions.status": 0}, {"event.status": 0}]}
			metadata["n_pending_files"] = col_files.find(query).count()

			# updating number of old events
			metadata["n_old_events"] = metadata["n_new_events"]

			# updating number of new events
			metadata["n_new_events"] = col_cameo_events.find().count()

			# updating number of events inserted
			metadata["n_events_inserted"] = metadata["n_new_events"] - metadata[
				"n_old_events"]

			# updating number of old mentions
			metadata["n_old_mentions"] = metadata["n_new_mentions"]

			# updating number of new mentions
			metadata["n_new_mentions"] = col_cameo_mentions.find().count()

			# updating number of mentions inserted
			metadata["n_mentions_inserted"] = metadata["n_new_mentions"] - metadata[
				"n_old_mentions"]

			# update metadata object
			update_status = col_metadata.update({
				'_id': metadata['_id']
			}, {
				'$set': metadata
			}, upsert=True)

			if update_status["nModified"] == 1:
				self.log_info(
					"Successfully updated metadata after inserting new documents")
				return True
			else:
				self.log_info("There was no change in metadata.")
				return True

		except:
			print(
			"Exception occurred while updating metadata after inserting new "
            "files\n" +
			"Exception stacktrace: \n" +
			traceback.format_exc())
			return False

	def log_info(self, msg):
		'''
		Method that logs info messages either in the log file or on the console
		:param msg: message that needs to be logged
		:param logger: logger object
		:return:
		'''
		if self.logger:
			self.logger.info(msg)
		else:
			print("[INFO]: " + msg)

	def log_error(self, msg):
		'''
		Method that logs error messages either in the log file or on the console
		:param msg: message that needs to be logged
		:param logger: logger object
		:return:
		'''
		if self.logger:
			self.logger.error(msg)
		else:
			print("[ERROR]: " + msg)

	def log_debug(self, msg):
		'''
		Method that logs debug messages either in the log file or on the console
		:param msg: message that needs to be logged
		:param logger: logger object
		:return:
		'''
		if self.logger:
			self.logger.debug(msg)
		else:
			print("[DEBUG]: " + msg)
