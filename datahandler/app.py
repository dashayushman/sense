from utils import Utility
from constants import Constants
import sys,os

#utility object (application logic layer)
util = Utility()

#constants object that holds all the default propertiesexists
const = Constants()

#application logger
logger = None

def main(argv):
  print("parsing command line arguments")
  const = Constants()
  const = util.parse_cmdline_args(argv,const)

  print("loading property file")
  const = util.load_properties(const)

  print("initializing logger")
  #util.initialize_logger(const.log_dir)

  print("initializing data access layer")
  db_init_status = util.initialize_dal(const.mongo_host,
                                       const.mongo_port,
                                       const.mongo_pool_size,
                                       const.log_dir)
  if db_init_status is False:
    print("Could not initialize data access layer. Aborting!!!")
    sys.exit()

  #fetch current list of files
  print("getting current files from url")
  cache_file_path = os.path.join(const.working_dir,const.cache_file)
  current_file_url_list = util.get_file_urls(const.url,cache_file_path)
  if current_file_url_list is None:
    print("Could not get current files from url. Aborting!!!")
    sys.exit()
  elif current_file_url_list is 0:
    print("There are no new files to work on. Exitting!!!")
    sys.exit()

  #dump newly added files to the file url collection
  print("dumping newly added files into database")
  insert_status = util.dump_file_urls(current_file_url_list,const.db_name)
  if insert_status is False:
    print("could not dump newly added files into database. Aborting!!!")
    sys.exit()

  #download pending files with status 0
  print("downloading pending files with status 0 and inserting them into the database")
  insert_status = util.download_and_insert_pending_files(const.db_name,
                                                         const.working_dir,
                                                         const.event_import_command,
                                                         const.mentions_import_command,
                                                         const.download_limit,
                                                         const.download_sort)
  if insert_status is 0:
    print("there are no pending files in db")
  elif insert_status is 1:
    print("all files were successfully downloaded and inserted")
  elif insert_status is 2:
    print("Not all files were downloaded and inserted successfully. Few files are still pending."+
          "please check logs for ")

  # Update metadata after downloading new files
  print("Updating metadata after adding new files")

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


