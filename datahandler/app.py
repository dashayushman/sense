from utils import Utility
from constants import Constants
import sys,os,datetime

#utility object (application logic layer)
util = Utility()

#constants object that holds all the default propertiesexists
const = Constants()

#application logger
logger = None

def initialize(mongo_host,mongo_port,mongo_pool_size,log_dir):
  print("initializing logger")
  #util.initialize_logger(const.log_dir)

  print("initializing data access layer")
  db_init_status = util.initialize_dal(mongo_host,
                                       mongo_port,
                                       mongo_pool_size,
                                       log_dir)
  if db_init_status is False:
    print("Could not initialize data access layer. Aborting!!!")
    sys.exit()

def download_insert_new_files(url,db_name,working_dir,event_import_command,mentions_import_command,download_limit,download_sort):
  #fetch current list of files
  print("getting current files from url")
  current_file_url_list = util.get_file_urls(url)
  if current_file_url_list is None:
    print("Could not get current files from url. Aborting!!!")
    sys.exit()
  elif current_file_url_list is 0:
    print("There are no new files to work on. Exitting!!!")
    sys.exit()

  #dump newly added files to the file url collection
  print("dumping newly added files into database")
  insert_status = util.dump_file_urls(current_file_url_list,db_name)
  if insert_status is False:
    print("could not dump newly added files into database. Aborting!!!")
    sys.exit()

  #download pending files with status 0
  print("downloading pending files with status 0 and inserting them into the database")
  insert_status, n_files_inserted = util.download_and_insert_pending_files(db_name,
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
    print("Not all files were downloaded and inserted successfully. Few files are still pending."+
          "please check logs for ")

def update_metadata(db_name):
  # Update metadata after downloading new files
  print("Updating metadata after adding new files")
  update_status = util.update_metadate(db_name)
  if update_status is False:
    print("Unable to update metadata due to exceptions. Look into it in the log files.")

def update_global_dashboard(db_name):
  # Update world impact map for the last 1 day since the last update
  impact_map_update_status = util.update_global_impact_map(db_name)
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

def update_linked_locations(db_name,svg):
  update_status = util.update_linked_locations(db_name,svg)
  if update_status is True:
    print("Successfully updated linked locations")

def main(argv):
  start = datetime.datetime.now()
  print("parsing command line arguments")
  const = Constants()
  const = util.parse_cmdline_args(argv,const)

  print("loading property file")
  const = util.load_properties(const)

  initialize(const.mongo_host,const.mongo_port,const.mongo_pool_size,const.log_dir)
  '''
  download_insert_new_files(const.url,
                            const.db_name,
                            const.working_dir,
                            const.event_import_command,
                            const.mentions_import_command,
                            const.download_limit,
                            const.download_sort)
  download_insert_new_files(const.url2,
                            const.db_name,
                            const.working_dir,
                            const.event_import_command,
                            const.mentions_import_command,
                            const.download_limit,
                            const.download_sort)

  update_metadata(const.db_name)
  '''
  #update_indexes(const.db_name)
  #update_global_dashboard(const.db_name)
  #update_overall_stats(const.db_name)
  update_linked_locations(const.db_name,const.svg)
  end = datetime.datetime.now()
  print("total time taken: " + str(end-start))




def log_info(self,msg,logger=None):
  '''
  Method that logs info messages either in the log file or on the console
  :param msg: message that needs to be logged
  :param logger: logger object
  :return:
  '''
  if logger:logger.info(msg)
  else:print(msg)

def log_error(self,msg,logger=None):
  '''
  Method that logs error messages either in the log file or on the console
  :param msg: message that needs to be logged
  :param logger: logger object
  :return:
  '''
  if logger:logger.error(msg)
  else:print(msg)

def log_debug(self,msg,logger=None):
  '''
  Method that logs debug messages either in the log file or on the console
  :param msg: message that needs to be logged
  :param logger: logger object
  :return:
  '''
  if logger:logger.debug(msg)
  else:print(msg)


if __name__ == '__main__':
  main(sys.argv[1:])


