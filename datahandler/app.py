from utils import Utility
from constants import Constants
import sys, os, datetime
import datetime as dt

# utility object (application logic layer)
util = Utility()

# constants object that holds all the default propertiesexists
const = Constants()

# application logger
logger = None


def initialize(mongo_host, mongo_port, mongo_pool_size, log_dir):
	print("initializing logger")
	util.initialize_logger(const.log_dir)

	print("initializing data access layer")
	db_init_status = util.initialize_dal(mongo_host,
		mongo_port,
		mongo_pool_size,
		log_dir)
	if db_init_status is False:
		print("Could not initialize data access layer. Aborting!!!")
		sys.exit()


def download_insert_new_files(url, db_name, working_dir, event_import_command,
							  mentions_import_command, download_limit,
							  download_sort):
	# fetch current list of files
	print("getting current files from url")
	current_file_url_list = util.get_file_urls(url)
	if current_file_url_list is None:
		print("Could not get current files from url. Aborting!!!")
		sys.exit()
	elif current_file_url_list is 0:
		print("There are no new files to work on. Exitting!!!")
		sys.exit()

	# dump newly added files to the file url collection
	print("dumping newly added files into database")
	insert_status = util.dump_file_urls(current_file_url_list, db_name)
	if insert_status is False:
		print("could not dump newly added files into database. Aborting!!!")
		sys.exit()

	# download pending files with status 0
	print(
	"downloading pending files with status 0 and inserting them into the database")
	insert_status, n_files_inserted = util.download_and_insert_pending_files(
		db_name,
		working_dir,
		event_import_command,
		mentions_import_command,
		download_limit,
		download_sort)
	if insert_status is 0:
		print("there are no pending files in db")
	elif insert_status is 1:
		print("all files were successfully downloaded and inserted")
	elif insert_status is 2:
		print(
		"Not all files were downloaded and inserted successfully. Few files are "
		"still pending." +
		"please check logs for ")


def update_metadata(db_name):
	# Update metadata after downloading new files
	print("Updating metadata after adding new files")
	update_status = util.update_metadate(db_name)
	if update_status is False:
		print(
		"Unable to update metadata due to exceptions. Look into it in the log "
		"files.")


def update_global_dashboard(db_name):
	# Update world impact map for the last 1 day since the last update
	last_update_date_gdelt = util.dal.get_last_update_date(db_name)
	if last_update_date_gdelt is None:
		last_update_date_gdelt = util.get_present_date_time()

	end_date = util.gdelt_date_to_datetime(last_update_date_gdelt)
	start_date = util.datetime_to_gdelt_date(end_date - dt.timedelta(days=1))
	impact_map_update_status = util.update_global_impact_map(db_name,
		last_update_date_gdelt, start_date)
	if impact_map_update_status is True:
		print("Successfully updated global impact map data.")


def update_overall_stats(db_name):
	# Update overall statistics of events and mentions
	overall_stats_update_status = util.update_overall_stats(db_name)
	if overall_stats_update_status is True:
		print("Successfully updated Overall statistics.")


def update_indexes(db_name):
	update_status = util.update_indexes(db_name)
	if update_status is True:
		print("Successfully updated indexes for events and mentions")


def update_linked_locations(db_name, svg):
	update_status = util.update_linked_locations(db_name, svg)
	if update_status is True:
		print("Successfully updated linked locations")


def update_high_impact_regions(db_name):
	update_status = util.update_high_impact_regions(db_name, limit=10)
	if update_status is True:
		print("Successfully updated high impact regions")


def update_high_impact_events(db_name):
	update_status = util.update_high_impact_events(db_name, limit=10)
	if update_status is True:
		print("Successfully updated high impactevents")


def update_articles_per_category(db_name):
	update_status = util.update_articles_per_category(db_name)
	if update_status is True:
		print("Successfully updated articles per category")


def update_mentions_timeline(db_name):
	update_status = util.update_mentions_timeline(db_name)
	if update_status is True:
		print("Successfully updated articles per category")


def update_actor_network(db_name):
	last_update_date_gdelt = util.dal.get_last_update_date(db_name)
	if last_update_date_gdelt is None:
		last_update_date_gdelt = util.get_present_date_time()

	end_date = util.gdelt_date_to_datetime(last_update_date_gdelt)
	start_date = util.datetime_to_gdelt_date(
		end_date - dt.timedelta(days=1))

	update_status = util.update_actor_network(db_name, last_update_date_gdelt,
		start_date)
	if update_status is True:
		print("Successfully updated actor network")


def main(argv):
	start = datetime.datetime.now()
	print("parsing command line arguments")
	const = Constants()
	const = util.parse_cmdline_args(argv, const)

	print("loading property file")
	const = util.load_properties(const)

	initialize(const.mongo_host, const.mongo_port, const.mongo_pool_size,
		const.log_dir)
	'''
	download_insert_new_files(const.url,
							  const.db_name,
							  const.working_dir,
							  const.event_import_command,
							  const.mentions_import_command,
							  const.download_limit,
							  const.download_sort)
'''
	download_insert_new_files(const.url2,
							  const.db_name,
							  const.working_dir,
							  const.event_import_command,
							  const.mentions_import_command,
							  const.download_limit,
							  const.download_sort)


	update_metadata(const.db_name)


	#update_indexes(const.db_name)

	#update_global_dashboard(const.db_name)

	update_overall_stats(const.db_name)
	# update_linked_locations(const.db_name,const.svg)
	update_high_impact_regions(const.db_name)
	update_high_impact_events(const.db_name)
	#update_articles_per_category(const.db_name)
	update_mentions_timeline(const.db_name)
	#update_actor_network(const.db_name)

	update_impact_map(const.db_name)
	update_actor_network_viz(const.db_name)
	update_event_timeline(const.db_name)
	#format_globe_viz(const.db_name)
	end = datetime.datetime.now()
	print("total time taken: " + str(end - start))

def format_globe_viz(db_name):
	last_update_date_gdelt = util.dal.get_last_update_date(db_name)
	if last_update_date_gdelt is None:
		last_update_date_gdelt = util.get_present_date_time()

	first_update_date_gdelt = util.dal.get_first_update_date_globe_viz(db_name)
	if first_update_date_gdelt is None:
		first_update_date_gdelt = util.get_present_date_time() - dt.timedelta(
			weeks=20)
	start_date = util.gdelt_date_to_datetime(first_update_date_gdelt)
	start_date = start_date.replace(hour=0, minute=0, second=0)
	end_date = util.gdelt_date_to_datetime(last_update_date_gdelt)
	days_delta = (end_date - start_date).days
	adder = 0;
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
			temp_end_date = start_date.replace(hour=23, minute=56, second=59)
		if i == 0:
			impact_map_update_status = util.update_globe_viz(db_name,
				util.datetime_to_gdelt_date(temp_end_date),
				util.datetime_to_gdelt_date(start_date),
				update=True)
		else:
			impact_map_update_status = util.update_globe_viz(db_name,
				util.datetime_to_gdelt_date(temp_end_date),
				util.datetime_to_gdelt_date(start_date))
		if impact_map_update_status is True:
			print("Successfully updated globe viz data for : " +
				  str(start_date))
		else:
			print("Failed to uupdated globe viz data for : " +
				  str(start_date))

def update_event_timeline(db_name):
	last_update_date_gdelt = util.dal.get_last_update_date(db_name)
	if last_update_date_gdelt is None:
		last_update_date_gdelt = util.get_present_date_time()

	first_update_date_gdelt = util.dal.get_first_update_date_mt(db_name)
	if first_update_date_gdelt is None:
		first_update_date_gdelt = util.get_present_date_time() - dt.timedelta(
			weeks=20)
	start_date = util.gdelt_date_to_datetime(first_update_date_gdelt)
	start_date = start_date.replace(hour=0, minute=0, second=0)
	end_date = util.gdelt_date_to_datetime(last_update_date_gdelt)
	days_delta = (end_date - start_date).days
	adder = 0
	util.dal.delete_coll_event_timeline(db_name)
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
			temp_end_date = start_date.replace(hour=23, minute=56, second=59)

		impact_map_update_status = util.update_event_count(db_name,
			util.datetime_to_gdelt_date(temp_end_date),
			util.datetime_to_gdelt_date(start_date))

		if impact_map_update_status is True:
			print("Successfully updated global impact map data for : " +
				  str(start_date))
		else:
			print("Failed to updated global impact map data for : " +
				  str(start_date))
def update_actor_network_viz(db_name):
	last_update_date_gdelt = util.dal.get_last_update_date(db_name)
	if last_update_date_gdelt is None:
		last_update_date_gdelt = util.get_present_date_time()

	first_update_date_gdelt = util.dal.get_first_update_date_act_net(db_name)
	if first_update_date_gdelt is None:
		first_update_date_gdelt = util.get_present_date_time() - dt.timedelta(
			weeks=20)
	start_date = util.gdelt_date_to_datetime(first_update_date_gdelt)
	start_date = start_date.replace(hour=0, minute=0, second=0)
	end_date = util.gdelt_date_to_datetime(last_update_date_gdelt)
	days_delta = (end_date - start_date).days
	adder = 0;
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
			temp_end_date = start_date.replace(hour=23, minute=56, second=59)
		if i == 0:
			impact_map_update_status = util.update_actor_network(db_name,
				util.datetime_to_gdelt_date(temp_end_date),
				util.datetime_to_gdelt_date(start_date),
				update=True)
		else:
			impact_map_update_status = util.update_actor_network(db_name,
				util.datetime_to_gdelt_date(temp_end_date),
				util.datetime_to_gdelt_date(start_date))
		if impact_map_update_status is True:
			print("Successfully updated global impact map data for : " +
				  str(start_date))
		else:
			print("Failed to updated global impact map data for : " +
				  str(start_date))

def update_impact_map(db_name):
	# Update world impact map for the last 1 day since the last update
	last_update_date_gdelt = util.dal.get_last_update_date(db_name)
	if last_update_date_gdelt is None:
		last_update_date_gdelt = util.get_present_date_time()

	first_update_date_gdelt = util.dal.get_first_update_date_impact_map(db_name)
	if first_update_date_gdelt is None:
		first_update_date_gdelt = util.get_present_date_time() - dt.timedelta(
			weeks=20)
	start_date = util.gdelt_date_to_datetime(first_update_date_gdelt)
	start_date = start_date.replace(hour=0, minute=0, second=0)
	end_date = util.gdelt_date_to_datetime(last_update_date_gdelt)
	days_delta = (end_date - start_date).days
	adder = 0
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
			temp_end_date = start_date.replace(hour=23, minute=56, second=59)
		#start_date = util.datetime_to_gdelt_date(end_date - dt.timedelta(days=1))
		if i == 0:
			impact_map_update_status = util.update_global_impact_map(db_name,
				util.datetime_to_gdelt_date(temp_end_date),
				util.datetime_to_gdelt_date(start_date), update=True)
		else:
			impact_map_update_status = util.update_global_impact_map(db_name,
				util.datetime_to_gdelt_date(temp_end_date),
				util.datetime_to_gdelt_date(start_date))
		if impact_map_update_status is True:
			print("Successfully updated global impact map data for : " +
				  str(start_date))
		else:
			print("Failed to updated global impact map data for : " +
				  str(start_date))


def log_info(self, msg, logger=None):
	'''
	Method that logs info messages either in the log file or on the console
	:param msg: message that needs to be logged
	:param logger: logger object
	:return:
	'''
	if logger:
		logger.info(msg)
	else:
		print(msg)


def log_error(self, msg, logger=None):
	'''
	Method that logs error messages either in the log file or on the console
	:param msg: message that needs to be logged
	:param logger: logger object
	:return:
	'''
	if logger:
		logger.error(msg)
	else:
		print(msg)


def log_debug(self, msg, logger=None):
	'''
	Method that logs debug messages either in the log file or on the console
	:param msg: message that needs to be logged
	:param logger: logger object
	:return:
	'''
	if logger:
		logger.debug(msg)
	else:
		print(msg)


if __name__ == '__main__':
	main(sys.argv[1:])
