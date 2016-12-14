from jproperties import Properties
import sys, getopt, traceback
import logging, logging.handlers
import urllib2
from dal import Dal
import os
from urllib2 import urlopen, URLError, HTTPError
import zipfile
import subprocess
from datetime import datetime
import time, math
import datetime as dt
from calendar import monthrange
import json


class Utility:
	'''
	Utility class for processing and restructuring data.
	'''

	# logger object to log progress and application status
	logger = None

	# data access layer object for handling all data access operations
	dal = None

	def _init_(self):
		return

	def load_properties(self, constants):
		'''
		  A method to load the property file. If the property file is missing or
		  any exception occurs,
		  then a default set of properties are loaded and returned.
		:return: Constant c that has all the property values that are shared across
		the application
		'''
		p = Properties()
		c = constants
		try:

			with open(c.prop_file, "rb") as f:
				p.load(f)

			if p.__contains__("url"):
				c.url = p["url"].data
			if p.__contains__("url2"):
				c.mongo_pool_size = p["url2"].data
			if p.__contains__("working_dir"):
				c.working_dir = p["working_dir"].data
			if p.__contains__("download_limit"):
				c.download_limit = int(p["download_limit"].data)
			if p.__contains__("download_sort"):
				c.download_sort = bool(p["download_sort"].data)
			if p.__contains__("log_dir"):
				c.log_dir = p["log_dir"].data
			if p.__contains__("mongo_host"):
				c.mongo_host = p["mongo_host"].data
			if p.__contains__("mongo_port"):
				c.mongo_port = int(p["mongo_port"].data)
			if p.__contains__("db_name"):
				c.db_name = p["db_name"].data
			if p.__contains__("mongo_pool_size"):
				c.mongo_pool_size = int(p["mongo_pool_size"].data)

			return c

		except IOError as e:
			print(
			"I/O error({0}): {1} ;reason {2}".format(e.errno, e.strerror,
				e.message))
			print(
			"error while loading the property file. Returning default properties "
			"from Constants.")
			return c
		except Exception as e:
			print "Unexpected error:", sys.exc_info()[0]
			print(
			"Some unexpected error occured while loading the property file. "
			"Returning default properties from Constants.")
			return c

	def initialize_logger(self, log_path):
		'''
		Method to initialize the logger for logging application progress and events
		:param log_path: path for storing the app logs
		:return:
		'''
		try:
			# logging.basicConfig(filename=log_path,format='%(asctime)s %(
			# message)s %(
			# levelname)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)
			self.logger = logging.getLogger("utils")
			self.logger.setLevel(logging.DEBUG)

			# create a file handler

			log_path = os.path.join(log_path, "app.utils.log")
			handler = logging.handlers.RotatingFileHandler(log_path, maxBytes=5000,
				backupCount=5)

			# create a logging format
			formatter = logging.Formatter(
				'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
			handler.setFormatter(formatter)
			# add the handlers to the logger
			self.logger.addHandler(handler)
		except Exception as e:
			print(e.__str__())
			print(
			"unable to set specified logpath. Setting logpath to default which is "
			"app.log in the application folder itself.")
			# logging.basicConfig(filename="app.log", format='%(asctime)s %(
			# message)s %(
			# levelname)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)
			self.logger = logging.getLogger("utils")
			self.logger.setLevel(logging.DEBUG)

			# create a file handler
			handler = logging.handlers.RotatingFileHandler("app.utils.log",
				maxBytes=5000, backupCount=5)
			# create a logging format
			formatter = logging.Formatter(
				'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
			handler.setFormatter(formatter)
			# add the handlers to the logger
			self.logger.addHandler(handler)

	def parse_cmdline_args(self, argv, constants):
		'''
		Method to parse command line arguments and save them in the constants class
		:param argsv: list of command line arguments
		:return: constants object that holds all the properties that is shared
		across the application
		'''
		try:
			opts, args = getopt.getopt(argv, "hi:o:", ["pfile=", "pfile="])
		except getopt.GetoptError:
			print 'app.py -pfile <property file name> -lfile <log file name>'
			sys.exit(2)
		for opt, arg in opts:
			if opt == '-h':
				print 'app.py -pfile <property file> -lfile <log file>'
				sys.exit()
			elif opt in ("-p", "--pfile"):
				if arg is not None or not arg:
					constants.prop_file = arg
			elif opt in ("-l", "--lfile"):
				if arg is not None or not arg:
					constants.log_dir = arg
		return constants

	def create_cache_file(self, file_path, content):
		try:
			with open(file_path, "w") as text_file:
				text_file.write(content)
			return True
		except Exception as e:
			self.log_error(
				"Exception occurred while writing the cache file to the working "
				"directory.\n" +
				"Error message: " + e.message)
			return False

	def get_cached_file_urls(self, file_path):
		if os.path.isfile(file_path):
			try:
				with open(file_path, 'r') as content_file:
					content = content_file.read()
					return content
			except Exception as e:
				self.log_error(
					"Exception occurred while reading the file url cache file. "
					"Error Message : "
					+ e.message +
					"\n returning None to fetch the file list from the given url")
				return None
		else:
			return None

	def get_file_urls(self, url):
		'''
		Method that makes an HTTP GET request to the given url and extracts all the
		current file names from GDELT2.0
		master file list and converts it into a dictionary
		:param url: url for fetching GDELT2.0 master files list
		:return: structured files list (grouped categorically and chronologically)
		'''
		try:

			str_current_files = urllib2.urlopen(url).read()
			list_current_files = str_current_files.splitlines()
			print("number of lines in the file : " + str(len(list_current_files)))
			grouped_current_files = self.chunks(list_current_files, 3)
			grouped_current_files_dict = []
			for i, x in enumerate(grouped_current_files):
				if len(x[0].split()) < 3 or len(x[1].split()) < 3 or len(
					x[2].split()) < 3:
					print(i)
					continue
				fileobj = {
					"event": {
						"id": x[0].split()[0],
						"chksum": x[0].split()[1],
						"url": x[0].split()[2],
						"filename": x[0].split()[-1].split("/")[-1],
						"status": 0},
					"mentions": {
						"id": x[1].split()[0],
						"chksum": x[1].split()[1],
						"url": x[1].split()[2],
						"filename": x[1].split()[-1].split("/")[-1],
						"status": 0},
					"kgraph": {
						"id": x[2].split()[0],
						"chksum": x[2].split()[1],
						"url": x[2].split()[2],
						"filename": x[2].split()[-1].split("/")[-1],
						"status": 0},
					"timestamp": int(x[0].split()[-1].split("/")[-1].split(".")[0])
					}
				grouped_current_files_dict.append(fileobj)
			return grouped_current_files_dict
		except Exception as e:
			self.log_error("URL Error : {0}".format(e.message))
			self.log_error(
				"Failed to open url. Please provide a valid url for fetching the "
				"files.")
			return None

	def initialize_dal(self, mongo_host, mongo_port, mongo_pool_size, log_dir):
		'''
		Method to initialize the Data Access layer for handling all database
		related operations.
		:param mongo_host: host name for mongodb
		:param mongo_port: port for mongodb
		:param mongo_pool_size: connection pool size for mongodb client
		:param logger: logger object for logging
		:return:
		'''
		try:
			self.dal = Dal(mongo_host, mongo_port, mongo_pool_size, self, log_dir)
			self.log_info("data access layer initialized successfully")
			return True
		except Exception as e:
			self.log_error("DB init Error : {0}".format(e.message))
			return False

	def dump_file_urls(self, current_file_url_list, db_name, logger=None):
		'''
		Utility method to dump newly added file urls to db
		:param current_file_url_list: new list of files
		:param db_name: name of the database that we will query
		:param logger: logger object
		:return: status of insert
		'''
		self.log_info("dumping newly added files to db")
		dump_status = self.dal.insert_new_files(current_file_url_list, db_name,
			self.logger)
		return dump_status

	def download_and_insert_pending_files(self, db_name, working_dir,
										  event_import_command,
										  mentions_import_command, download_limit=0,
										  download_sort=False):
		'''
		Method that downloads all the files with status 0 and inserts them into the
		events and mentions collection
		:param db_name: name of the db
		:return: insert status
		'''

		# Get all the pending files from the database
		pending_files = self.dal.get_file_urls_by_status(db_name, working_dir,
			status=0, sort=download_sort)

		# Check if there are no pending files
		if pending_files.count() is 0:
			return 0, 0  # there are no pending files in db

		# set counter to check the number of inserts later
		insert_count = 0
		if download_limit == 0:
			download_limit = len(pending_files)

		# Iterate through the pending files
		for n, fileobj in enumerate(pending_files):
			if n == download_limit:
				break

			self.log_info(
				"downloading file " + str(n) + " of " + str(pending_files.count()) +
				" files")

			# Url for event files
			events_url = fileobj["event"]["url"]
			# download and insert status of event file
			events_status = fileobj["event"]["status"]

			# Url of the mentions file
			mentions_url = fileobj["mentions"]["url"]

			# status of the mentions file
			mentions_status = fileobj["mentions"]["status"]

			# skip download if the status is 1
			if events_status is 1:
				e_dwn_status = True
			else:
				e_dwn_status, events_comp_file_path = self.download_file(
					working_dir,
					events_url)

			if mentions_status is 1:
				m_dwn_status = True
			else:
				m_dwn_status, mentions_comp_file_path = self.download_file(
					working_dir, mentions_url)

			self.log_info("Successfully downloaded file " + str(n) + " of " + str(
				pending_files.count()) +
						  " files")

			if e_dwn_status and m_dwn_status:
				self.log_info(
					"unzipping files \n" + events_comp_file_path + "\n and \n" +
					mentions_comp_file_path)

				# unzip the files in the working directory
				events_file_path = self.unzip_file(events_comp_file_path,
					working_dir)
				mentions_file_path = self.unzip_file(mentions_comp_file_path,
					working_dir)

				self.log_info(
					"Successfully unzipped files \n" + events_comp_file_path + "\n "
																			   "and"
																			   " \n" +
					mentions_comp_file_path + " to " + events_file_path +
					"\nand\n" +
					mentions_file_path)

				# Check if the files were successfully unzipped and if not then
				# skip the files
				if events_file_path is None or mentions_file_path is None:
					self.log_error("Skipping file " +
								   events_url +
								   " and " +
								   mentions_url +
								   " because one of them could not be unzipped.")
					continue

				self.log_info("Importing files \n" + events_file_path + "\n and "
																		"\n" +
							  mentions_file_path + "\n into database: " + db_name)

				# import the events csv file into the database
				import_events_status = self.import_csv_to_db(events_file_path,
					db_name,
					"cameo_events",
					event_import_command)

				# Check if import was successfull. If not then log error but if
				# successfull the update
				# file status and delete the file from the working directory
				if import_events_status is True:
					self.log_info(
						"Successfully imported files \n" + events_file_path +
						"\n into database: " + db_name)

					self.log_info("Updating status of file \n" + events_file_path +
								  "\n in database: " + db_name + " to 1 (downloaded "
																 "and imported)")

					# Update the file status to 1
					e_update_status = self.dal.update_file_status(db_name, fileobj,
						"event", status=1)

					# Check whether file status update was successfull or not.
					# If success then delete the files from the working directory.
					if e_update_status is True:
						self.log_info(
							"Successfully updated status of file \n" +
							events_file_path +
							"\n in database: " + db_name + " to 1 (downloaded and "
														   "imported)")
						# Delete the files from the working directory
						self.delete_files([events_comp_file_path, events_file_path])

					else:
						self.log_error(
							"Could not update status of file \n" +
							events_file_path +
							"\n in database: " + db_name + " to 1 (downloaded and "
														   "imported)")

				# Import mentions csv into the database
				import_mentions_status = self.import_csv_to_db(mentions_file_path,
					db_name,
					"cameo_mentions",
					mentions_import_command)
				# Check if import was successful. If not then log error but if
				# successful the update
				# file status and delete the file from the working directory
				if import_mentions_status is True:
					self.log_info(
						"Successfully imported files \n" + mentions_file_path + "\n "
																				"into database: " + db_name)

					# update the file ststus to 1
					m_update_status = self.dal.update_file_status(db_name, fileobj,
						"mentions", status=1)

					# Check if update was successful or not.
					# If it was the delete the files from the working directory
					if m_update_status is True:
						self.log_info(
							"Successfully updated status of file \n" +
							events_file_path +
							"\n in database: " + db_name + " to 1 (downloaded and "
														   "imported)")
						# delete the files from the working directory
						self.delete_files(
							[mentions_comp_file_path, mentions_file_path])
					else:
						self.log_error(
							"Could not update status of file \n" +
							events_file_path +
							"\n in database: " + db_name + " to 1 (downloaded and "
														   "imported)")

					# increate the import counter if everything was fine
				if import_events_status and import_mentions_status:
					insert_count = insert_count + 1
			else:
				self.log_error("Skipping file " +
							   events_url +
							   " and " +
							   mentions_url +
							   " because of unsuccessful download.")
				continue
			# check if all the pending files were imported successfully and return
			# a status
			#  accordingly
		if insert_count == download_limit:
			return 1, insert_count  # all files were successfully downloaded and
		# inserted
		else:
			return 2, insert_count  # Not all files were downloaded and inserted
			# successfully

	def update_metadate(self, db_name):
		'''
		Method to update meta information about the entire database and new updates
		:param db_name: name of the database
		:return: update status
		'''
		update_status = self.dal.update_metadata(db_name)
		return update_status

	def update_global_impact_map(self, db_name, end_date, start_date=0,
								 update=False):
		'''
		Method to update the data for the global impact map in the global dashboard
		:param db_name: Name of the DB
		:param end_date: End date for selection
		:param start_date: start date for selection
		:return: Status of update
		'''

		globa_impact_map_data = self.dal.get_global_impact_map_data(db_name,
			end_date, start_date)
		if globa_impact_map_data is None:
			return False
		map_update_status = self.dal.update_impact_map(db_name,
			globa_impact_map_data, update)
		if map_update_status is True:
			return True
		else:
			return False

	def update_linked_locations(self, db_name, svg):
		'''
		Method to update all linked locations in the last one day
		:param db_name:name of the db
		:param svg: image for the map in svg string
		:return: update status
		'''
		last_update_date_gdelt = self.dal.get_last_update_date(db_name)
		if last_update_date_gdelt is None:
			last_update_date_gdelt = self.get_present_date_time()

		end_date = self.gdelt_date_to_datetime(last_update_date_gdelt)
		start_date = self.datetime_to_gdelt_date(end_date - dt.timedelta(
			minutes=15))

		linked_locations = self.dal.get_linked_locations(db_name,
			last_update_date_gdelt, svg, start_date=start_date)
		if linked_locations is None:
			return False
		update_status = self.dal.update_linked_locations(db_name, linked_locations)
		return update_status

	def update_event_count(self, db_name, end_date, start_date):
		'''

		:param db_name:
		:param n_days:
		:return:
		'''

		event_count = self.dal.get_event_count(db_name, end_date,
			start_date=start_date)
		if event_count is None:
			return False
		update_status = self.dal.update_event_count(db_name, event_count)
		return update_status

	def update_globe_viz(self, db_name, end_date, start_date, update=False):
		globe_viz = self.dal.get_globe_viz(db_name, end_date,
			start_date=start_date)

		if globe_viz is None:
			return False
		data = []
		for i, event in enumerate(globe_viz):
			if i != 0 and i%2000 == 0:
				print(str(i) + ' events formatted.')
				break
			e_obj = {}
			e_obj["type"] = 'Feature'
			e_obj["geometry"] = {
				"type": "Point",
				"coordinates": [
				  event['ActionGeo_Long'],
				  event['ActionGeo_Lat'],
				  0
				]}
			del event['ActionGeo_Long']
			del event['_id']
			del event['ActionGeo_Lat']
			e_obj["properties"] = event
			data.append(e_obj)
		update_obj = {
			'features' : data,
			'type' : 'FeatureCollection'
		}
		#json_file_str = json.dumps(update_obj)
		with open('./globe_viz/' + str(start_date) + '.json', 'w') as outfile:
    			json.dump(update_obj, outfile)

		#update_status = self.dal.update_globe_viz(db_name, update_obj, update)
		return True

	def update_actor_network(self, db_name, end_date, start_date, update=False):
		'''

		:param db_name:
		:param n_days:
		:return:
		'''

		actor_network = self.dal.get_actor_network(db_name, end_date,
			start_date=start_date)
		if actor_network is None:
			return False

		update_status = self.dal.update_actor_network(db_name, actor_network, update)
		return update_status

	def update_mentions_timeline(self, db_name):
		last_update_date_gdelt = self.dal.get_last_update_date(db_name)
		if last_update_date_gdelt is None:
			last_update_date_gdelt = self.get_present_date_time()

		first_update_date_gdelt = self.dal.get_first_update_date_mt(db_name)
		if first_update_date_gdelt is None:
			first_update_date_gdelt = self.get_present_date_time() - dt.timedelta(
				weeks=20)
		start_date = self.gdelt_date_to_datetime(first_update_date_gdelt)
		start_date = start_date.replace(hour=0, minute=0, second=0)
		end_date = self.gdelt_date_to_datetime(last_update_date_gdelt)
		days_delta = (end_date - start_date).days
		adder = 0;
		data = []
		sources = []
		for i in range(days_delta + 1):
			temp_end_date = None
			if adder == 0:
				start_date = start_date + dt.timedelta(days=adder)
				adder+=1
			else:
				start_date = start_date + dt.timedelta(days=adder)
			start_date = start_date.replace(hour=0, minute=0, second=0)
			if i == days_delta:
				temp_end_date = start_date.replace(hour=end_date.hour,
					minute=end_date.minute,
					second=end_date.second)
			else:
				temp_end_date = start_date.replace(hour=23, minute=59, second=59)

			print('Mentions Timeline :\nStart Date : ' +
				  str(start_date) + '\nEnd Date : ' + str(end_date))
			articles_per_category = self.dal.get_articles_per_category(db_name,
				self.datetime_to_gdelt_date(temp_end_date),
				self.datetime_to_gdelt_date(start_date))
			m_obj = {
				"year" : start_date.strftime("%d-%m-%Y"),
				'citation_only' : 0,
				'web' : 0
			}

			for cat in articles_per_category["data"]:
				cat_name = cat["source"].replace(" ", "_")
				n_mentions = cat["n_mentions"]
				if cat_name not in sources: sources.append(cat_name)
				m_obj[cat_name] = n_mentions
			data.append(m_obj)
		update_obj = {
			"data": data,
			"timestamp": self.get_present_date_time(),
			"sources": sources
		}
		update_status = self.dal.update_mentions_timeline(db_name, update_obj)
		return update_status
		if update_status is True:
			print("Successfully updated MT for : " +
				  str(start_date))
		else:
			print("Failed to updated MT for : " +
				  str(start_date))



	def update_articles_per_category(self, db_name):
		'''
		articles_per_category:
		:param db_name:
		:return:
		'''
		end_date = self.dal.get_last_update_date(db_name)
		if end_date is None:
			end_date = self.get_present_date_time()
		articles_per_category = self.dal.get_articles_per_category(db_name,
			end_date,
			start_date=0)
		if articles_per_category is None:
			return False
		update_status = self.dal.update_articles_per_category(db_name,
			articles_per_category)
		return update_status

	def update_high_impact_events(self, db_name, limit=10):
		last_update_date_gdelt = self.dal.get_last_update_date(db_name)
		if last_update_date_gdelt is None:
			last_update_date_gdelt = self.get_present_date_time()

		end_date_15 = self.gdelt_date_to_datetime(last_update_date_gdelt)
		start_date_15 = self.datetime_to_gdelt_date(
			end_date_15 - dt.timedelta(minutes=15))

		end_date_today = end_date_15
		start_date_today = self.datetime_to_gdelt_date(datetime(*(
		end_date_today.year, end_date_today.month, end_date_today.day, 0, 0, 0)))
		end_date_today = self.datetime_to_gdelt_date(end_date_today)

		last_month, last_year = self.get_last_month_year(end_date_15)
		days_in_last_month = monthrange(last_year, last_month)[1]
		start_date_month = self.datetime_to_gdelt_date(
			datetime(*(last_year, last_month, days_in_last_month, 0, 0, 0)))
		end_date_month = end_date_today

		h_i_events_15 = self.dal.get_high_impact_events(db_name,
			last_update_date_gdelt, start_date=start_date_15, limit=limit)
		h_i_events_today = self.dal.get_high_impact_events(db_name, end_date_today,
			start_date=start_date_today, limit=limit)
		h_i_events_month = self.dal.get_high_impact_events(db_name, end_date_month,
			start_date=start_date_month, limit=limit)
		if h_i_events_15 is None or h_i_events_today is None or h_i_events_month is\
			None:
			return False

		hir_object = {
			"hie_15": h_i_events_15,
			"hie_today": h_i_events_today,
			"hie_month": h_i_events_month,
			"timestamp": self.get_present_date_time()
		}
		update_status = self.dal.update_high_impact_events(db_name, hir_object)
		return update_status

	def update_high_impact_regions(self, db_name, limit=10):
		last_update_date_gdelt = self.dal.get_last_update_date(db_name)
		if last_update_date_gdelt is None:
			last_update_date_gdelt = self.get_present_date_time()

		end_date_15 = self.gdelt_date_to_datetime(last_update_date_gdelt)
		start_date_15 = self.datetime_to_gdelt_date(
			end_date_15 - dt.timedelta(minutes=15))

		end_date_today = end_date_15
		start_date_today = self.datetime_to_gdelt_date(datetime(*(
		end_date_today.year, end_date_today.month, end_date_today.day, 0, 0, 0)))
		end_date_today = self.datetime_to_gdelt_date(end_date_today)

		last_month, last_year = self.get_last_month_year(end_date_15)
		days_in_last_month = monthrange(last_year, last_month)[1]
		start_date_month = self.datetime_to_gdelt_date(
			datetime(*(last_year, last_month, days_in_last_month, 0, 0, 0)))
		end_date_month = end_date_today

		h_i_regions_15 = self.dal.get_high_impact_regions(db_name,
			last_update_date_gdelt, start_date=start_date_15, limit=limit)
		h_i_regions_today = self.dal.get_high_impact_regions(db_name,
			end_date_today,
			start_date=start_date_today, limit=limit)
		h_i_regions_month = self.dal.get_high_impact_regions(db_name,
			end_date_month,
			start_date=start_date_month, limit=limit)
		if h_i_regions_15 is None or h_i_regions_today is None or h_i_regions_month\
			is None:
			return False

		hir_object = {
			"hir_15": h_i_regions_15,
			"hir_today": h_i_regions_today,
			"hir_month": h_i_regions_month,
			"timestamp": self.get_present_date_time()
		}
		update_status = self.dal.update_high_impact_regions(db_name, hir_object)
		return update_status

	def update_overall_stats(self, db_name):
		'''
		Method to update overall events, mentions and countries statistics
		:param db_name: name of the db
		:return: status of update
		'''
		dt_now = self.dal.get_last_update_date(db_name)
		dt_obj_now = self.gdelt_date_to_datetime(dt_now)
		last_month, last_year = self.get_last_month_year(dt_obj_now)
		days_in_last_month = monthrange(last_year, last_month)[1]
		dt_last_month = self.datetime_to_gdelt_date(
			datetime(*(last_year, last_month, days_in_last_month, 0, 0, 0)))
		dt_today_start = self.datetime_to_gdelt_date(
			datetime(*(dt_obj_now.year, dt_obj_now.month, dt_obj_now.day, 0, 0, 0)))

		self.log_info("last update date : " + str(dt_obj_now) +
					  "\nlast month : " + str(last_month) +
					  "\nLast year  : " + str(last_year) +
					  "\ndate last month : " + str(dt_last_month) +
					  "\ndays in last month : " + str(days_in_last_month) +
					  "\ndate today start : " + str(dt_today_start))

		n_events_now, n_events_today, \
		n_events_this_month, e_percent_higher_last_month = \
			self.get_overall_events_stats(
			db_name, dt_now, dt_last_month, dt_today_start)
		if n_events_now is None or n_events_today is None or n_events_this_month is\
			None or e_percent_higher_last_month is None:
			self.log_error(
				"Failed to get overall statistics for events so aborting.")
			return False

		n_mentions_now, n_mentions_today, \
		n_mentions_this_month, m_percent_higher_last_month = \
			self.get_overall_mentions_stats(
			db_name, dt_now, dt_last_month, dt_today_start)
		if n_mentions_now is None or \
				n_mentions_today is None or \
				n_mentions_this_month is None or \
				m_percent_higher_last_month is None:
			self.log_error(
				"Failed to get overall statistics for mentions so aborting.")
			return False

		n_countries_now, n_countries_today, \
		n_countries_this_month, c_percent_higher_last_month = \
			self.get_overall_countries_stats(
			db_name, dt_now, dt_last_month, dt_today_start)
		if n_countries_now is None or \
				n_countries_today is None or \
				n_countries_this_month is None or \
				c_percent_higher_last_month is None:
			self.log_error(
				"Failed to get overall statistics for countries so aborting.")
			return False

		update_status = self.dal.update_overall_stats(db_name,
			n_events_now,
			n_events_today,
			n_events_this_month,
			e_percent_higher_last_month,
			n_mentions_now,
			n_mentions_today,
			n_mentions_this_month,
			m_percent_higher_last_month,
			n_countries_now,
			n_countries_today,
			n_countries_this_month,
			c_percent_higher_last_month
		)
		return update_status

	def get_overall_countries_stats(self, db_name, dt_now, dt_last_month,
									dt_today_start):
		'''
		Method to get overal countries statistics
		:param db_name: name of the db
		:return: (total_countries_now, total_countries_today,
		total_countries_last_month,percentage_higher)
		'''
		# Get Total Events


		n_countries_now = self.dal.get_total_countries(db_name, dt_now)
		if n_countries_now == -1: return None, None, None, None

		n_countries_till_last_month = self.dal.get_total_countries(db_name,
			dt_last_month)
		if n_countries_till_last_month == -1: return None, None, None, None

		n_countries_this_month = n_countries_now - n_countries_till_last_month

		n_countries_today = self.dal.get_total_countries(db_name, dt_now,
			start_date=dt_today_start)
		if n_countries_today == -1: return None, None, None, None

		percent_higher_last_month = 0.0

		if n_countries_till_last_month == 0:
			percent_higher_last_month = 100.0
		else:
			percent_higher_last_month = (float(
				n_countries_this_month) * 100.0) / n_countries_till_last_month

		self.log_info("countries till last update : " + str(n_countries_now) +
					  "\ncountries til last month : " + str(
			n_countries_till_last_month) +
					  "\ncountries this month  : " + str(n_countries_this_month) +
					  "\ncountries today : " + str(n_countries_today) +
					  "\nIncrease percentage : " + str(
			percent_higher_last_month) + "%")
		return n_countries_now, n_countries_today, n_countries_this_month, \
			   percent_higher_last_month

	def get_overall_mentions_stats(self, db_name, dt_now, dt_last_month,
								   dt_today_start):
		'''
		Method to get overal mentions statistics
		:param db_name: name of the db
		:return: (total_mentions_now, total_mentions_today,
		total_mentions_last_month,percentage_higher)
		'''
		# Get Total Events


		n_mentions_now = self.dal.get_total_mentions(db_name, dt_now)
		if n_mentions_now == -1: return None, None, None, None

		n_mentions_till_last_month = self.dal.get_total_mentions(db_name,
			dt_last_month)
		if n_mentions_till_last_month == -1: return None, None, None, None

		n_mentions_this_month = n_mentions_now - n_mentions_till_last_month

		n_mentions_today = self.dal.get_total_mentions(db_name, dt_now,
			start_date=dt_today_start)
		if n_mentions_today == -1: return None, None, None, None

		percent_higher_last_month = 0.0

		if n_mentions_till_last_month == 0:
			percent_higher_last_month = 100.0
		else:
			percent_higher_last_month = (float(
				n_mentions_this_month) * 100.0) / n_mentions_till_last_month

		self.log_info("mentions till last update : " + str(n_mentions_now) +
					  "\nmentions til last month : " + str(
			n_mentions_till_last_month) +
					  "\nmentions this month  : " + str(n_mentions_this_month) +
					  "\nmentions today : " + str(n_mentions_today) +
					  "\nIncrease percentage : " + str(
			percent_higher_last_month) + "%")
		return n_mentions_now, n_mentions_today, n_mentions_this_month, \
			   percent_higher_last_month

	def get_overall_events_stats(self, db_name, dt_now, dt_last_month,
								 dt_today_start):
		'''
		Method to get overal events statistics
		:param db_name: name of the db
		:return: (total_events_now, total_events_last_month, total_events_today,
		percentage_higher)
		'''
		# Get Total Events

		n_events_now = self.dal.get_total_events(db_name, dt_now)
		if n_events_now == -1: return None, None, None, None

		n_events_till_last_month = self.dal.get_total_events(db_name, dt_last_month)
		if n_events_till_last_month == -1: return None, None, None, None

		n_events_this_month = n_events_now - n_events_till_last_month

		n_events_today = self.dal.get_total_events(db_name, dt_now,
			start_date=dt_today_start)
		if n_events_today == -1: return None, None, None, None

		percent_higher_last_month = 0.0

		if n_events_till_last_month == 0:
			percent_higher_last_month = 100.0
		else:
			percent_higher_last_month = (float(
				n_events_this_month) * 100.0) / n_events_till_last_month

		self.log_info("Events till last update : " + str(n_events_now) +
					  "\nEvents til last month : " + str(n_events_till_last_month) +
					  "\nEvents this month  : " + str(n_events_this_month) +
					  "\nEvents today : " + str(n_events_today) +
					  "\nIncrease percentage : " + str(
			percent_higher_last_month) + "%")
		return n_events_now, n_events_today, n_events_this_month, \
			   percent_higher_last_month

	def update_indexes(self, db_name):
		'''
		Method to update indexes for easier access and sorting
		:param db_name: name of the db
		:return: update status
		'''
		update_status = self.dal.update_indexes(db_name)
		return update_status

	def get_last_month_year(self, dt_now):
		'''
		Method to get the last month and year
		:param dt_now: present datetimeobj
		:return: (last_month, last_year)
		'''
		last_month = dt_now.month - 1
		last_year = dt_now.year
		if last_month == 0:
			last_month = 12
			last_year = last_year - 1
		return last_month, last_year

	def convert_goldstein_to_hex_color(self, g_score):
		'''
		Method to get a corrsponding color for goldstein's score
		:param g_score:
		:return:
		'''
		a1 = 16711680.0
		b1 = 16776960.0

		a2 = 65280.0
		b2 = 16776960.0

		color_range_neg = ["#ff0000", "#ff0000", "#ff3200", "#ff4800", "#ff5d00",
						   "#ff8c00", "#ff9d00", "#ffbb00", "#ffc300", "#ffd400",
						   "#ffe500"]
		color_range_pos = ["#ffee00", "#eeff00", "#ddff00", "#b2ff00", "#9dff00",
						   "#76ff00", "#48ff00", "#3fff00", "#32ff00", "#00ff04",
						   "#00ff04"]

		if g_score == 0.0:
			return ("#ffee00")
		elif g_score < 0.0:
			# val = (((b1-a1)*((g_score+0.2)-(-10.0)))/(0.0-(-10.0))) + a1
			# hex_str = str(hex(int(val))).replace("0x","")
			# if len(hex_str) < 6:
			#  while len(hex_str) == 6:
			#    hex_str = "0" + hex_str
			# hex_str = "#" + hex_str
			if abs(int(g_score)) > 10:
				g_score = -10
			hex_str = color_range_neg[int(math.floor(g_score))]
			return hex_str
		elif g_score > 0.0:
			'''
			g_score = -1 * g_score
			val = (((b2-a2)*((g_score+0.2)-(-10.0)))/(0.0-(-10.0))) + a2
			hex_str = str(hex(int(val))).replace("0x","")
			if len(hex_str) < 6:
			  while len(hex_str) == 6:
				hex_str = "0" + hex_str
			hex_str = "#" + hex_str
			'''
			if abs(int(g_score)) > 10:
				g_score = 10
			hex_str = color_range_pos[int(math.ceil(g_score))]
			return hex_str

	def get_present_date_time(self, type="gdelt", d_type="int", timezone="utc"):
		'''
		Method to return present date and time in different formats
		:param type: type of date (format) ["gdelt" , "default"]
		:param d_type: data type of date to return ["int" , "string" , "timestamp"]
		:param timezone: which timezone to consider ["utc" , "local"]
		:return: datetime in the selected format and data type
		'''
		dt_now = None
		if timezone == "utc":
			dt_now = datetime.utcnow()
		elif timezone == "local":
			dt_now = datetime.now()
		else:
			dt_now = datetime.now()

		if type == "gdelt":
			if d_type == "string":
				return dt_now.strftime("%Y%m%d%H%M%S")
			elif d_type == "int":
				return int(dt_now.strftime("%Y%m%d%H%M%S"))
			elif d_type == "tuple":
				return dt_now
			else:
				return dt_now.strftime("%Y%m%d%H%M%S")
		elif type == "default":
			if d_type == "string":
				return dt_now.strftime("%Y-%m-%d %H:%M:%S")
			elif d_type == "timestamp":
				return time.mktime(
					time.strptime(dt_now.strftime("%Y%m%d%H%M%S"), "%Y%m%d%H%M%S"))
			else:
				return dt_now.strftime("%Y-%m-%d %H:%M:%S")
		else:
			return dt_now.strftime("%Y-%m-%d %H:%M:%S")

	def gdelt_date_to_datetime(self, gdelt_date):
		'''
		Method to convert date in GDELT format to python datetime object
		:param gdelt_date: date in gdelt format in integer
		:return: corresponding datetime
		'''
		return datetime.strptime(str(gdelt_date), "%Y%m%d%H%M%S")

	def datetime_to_gdelt_date(self, datetime):
		'''
		convert python datetime object to GDELT date
		:param datetime: datetime object
		:return: GDELT date in int
		'''
		return int(datetime.strftime("%Y%m%d%H%M%S"))

	def delete_files(self, files):
		'''
		Method to remove files from wrodking directory
		:param files: list of file paths
		:return:
		'''
		for file in files:
			try:
				os.remove(file)
			except Exception as e:
				print("could not remove file " + file + "\nException: " + e.message)

	def download_file(self, working_dir, url):
		'''
		Method for downloading files from a URL
		:param working_dir: directory to download the file to
		:param url:
		:return: download_status, downloaded_file_path
		'''
		# Open the url
		self.log_info("Downloading file from url " + url + " to " + working_dir)
		try:
			f = urlopen(url)
			print("downloading " + url)
			filename = url.split("/")[-1]
			download_path = os.path.join(working_dir, filename)
			# Open our local file for writing
			with open(download_path, "wb") as local_file:
				local_file.write(f.read())

			return True, download_path
		# handle errors
		except HTTPError, e:
			self.log_error(
				"Exception occurred while Downloading file from URL: " + str(
					url) + ".\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			# self.log_error("HTTP Error:"+ str(url))
			return False, None
		except URLError, e:
			self.log_error(
				"Exception occurred while downloading file from url: " + str(
					url) + "\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return False, None
		except Exception, e:
			self.log_error(
				"Exception occurred while downloading file from url: " + str(
					url) + "\n" +
				"Exception stacktrace: \n" +
				traceback.format_exc())
			return False, None

	def unzip_file(self, file_path, working_dir):
		'''
		Method to unzip zip files and extract them in a given working directory
		:param file_path: path to the zip file
		:param working_dir: directory to extract the file to
		:return: unzipped_file_path
		'''
		self.log_info("Unzipping file " + str(file_path))
		try:
			zip_ref = zipfile.ZipFile(file_path, 'r')
			zip_ref.extractall(working_dir)
			zip_ref.close()
			extracted_file_name = zip_ref.filelist[0].filename
			extracted_file_path = os.path.join(working_dir, extracted_file_name)
			return extracted_file_path
		except Exception as e:
			self.log_error(
				"Exception Occurred while unzipping file:" + str(e.message))
			return None

	def import_csv_to_db(self, file_path, db_name, coll_name, import_command):
		'''
		Method to import csv into db
		:param file_path: path to csv file
		:param db_name: name of database
		:param coll_name: name of collection to import data to
		:param import_command: raw shell command for importing events data
		:return: import status (True if success and False if failure)
		'''
		self.log_info("Importing file " + str(file_path) + " to collection " +
					  coll_name + " in database " + db_name)
		try:
			command = self.create_mongo_import_command(db_name,
				file_path,
				coll_name,
				import_command)
			status = subprocess.call(command.split(" "))
			if status is 0:
				# self.log_info("Successfully imported file " + str(file_path) +
				#              " to collection " + coll_name + " in database " +
				# db_name)
				return True
			else:
				self.log_error("Failed file " + str(file_path) +
							   " to collection " + coll_name + " in database " +
							   db_name)
				return False
		except Exception as e:
			self.log_error("Mongoimport Error {0}".format(e.message))
			self.log_error(
				"Exception occured while running import command for " + file_path)
			return False

	def create_mongo_import_command(self, db_name, file_path, coll_name,
									import_command):
		'''
		Method to create a mongo import command for importing csv/tsv into the db
		:param db_name: name of the database
		:param file_path: path to csv file
		:param coll_name: name of the collection to import data into
		:param import_command: raw import command for generating real import
		command by adding arguments
		:return: command string
		'''
		cmd = import_command.replace("DBNAME", db_name)
		cmd = cmd.replace("FILEPATH", file_path)
		return cmd

	def chunks(self, l, n):
		'''
		Yield successive n-sized chunks from l.
		:param n: size of a chunk
		:return:
		'''
		for i in range(0, len(l), n):
			yield l[i:i + n]

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
