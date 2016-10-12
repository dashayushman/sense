from constants import Constants
from jproperties import Properties
import sys, getopt
import logging,logging.handlers
import urllib2
from dal import Dal
import os
from urllib2 import urlopen, URLError, HTTPError
import zipfile
import subprocess
from difflib import unified_diff

class Utility:
  '''
  Utility class for processing and restructuring data.
  '''

  #logger object to log progress and application status
  logger = None

  #data access layer object for handling all data access operations
  dal = None

  def _init_(self):
    return

  def load_properties(self,constants):
    '''
      A method to load the property file. If the property file is missing or any exception occurs,
      then a default set of properties are loaded and returned.
    :return: Constant c that has all the property values that are shared across the application
    '''
    p = Properties()
    c = constants
    try:

      with open(c.prop_file, "rb") as f:
        p.load(f)

      if p.__contains__("url"):
        c.url = p["url"].data
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
      print("I/O error({0}): {1} ;reason {2}".format(e.errno, e.strerror, e.message))
      print("error while loading the property file. Returning default properties from Constants.")
      return c
    except Exception as e:
      print "Unexpected error:", sys.exc_info()[0]
      print("Some unexpected error occured while loading the property file. Returning default properties from Constants.")
      return c

  def initialize_logger(self,log_path):
    '''
    Method to initialize the logger for logging application progress and events
    :param log_path: path for storing the app logs
    :return:
    '''
    try:
      #logging.basicConfig(filename=log_path,format='%(asctime)s %(message)s %(levelname)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)
      self.logger = logging.getLogger("utils")
      self.logger.setLevel(logging.DEBUG)

      # create a file handler
      log_path = os.path.join(log_path,"app.utils.log")
      handler = logging.handlers.RotatingFileHandler(log_path, maxBytes=5000, backupCount=5)

      # create a logging format
      formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
      handler.setFormatter(formatter)
      # add the handlers to the logger
      self.logger.addHandler(handler)
    except Exception as e:
      print(e.__str__())
      print("unable to set specified logpath. Setting logpath to default which is app.log in the application folder itself.")
      #logging.basicConfig(filename="app.log", format='%(asctime)s %(message)s %(levelname)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)
      self.logger = logging.getLogger("utils")
      self.logger.setLevel(logging.DEBUG)

      # create a file handler
      handler = logging.handlers.RotatingFileHandler("app.utils.log", maxBytes=5000, backupCount=5)
      # create a logging format
      formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
      handler.setFormatter(formatter)
      # add the handlers to the logger
      self.logger.addHandler(handler)

  def parse_cmdline_args(self,argv,constants):
    '''
    Method to parse command line arguments and save them in the constants class
    :param argsv: list of command line arguments
    :return: constants object that holds all the properties that is shared across the application
    '''
    try:
      opts, args = getopt.getopt(argv,"hi:o:",["pfile=","pfile="])
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
  def create_cache_file(self,file_path,content):
    try:
      with open(file_path, "w") as text_file:
        text_file.write(content)
      return True
    except Exception as e:
      self.log_error("Exception occurred while writing the cache file to the working directory.\n"+
                     "Error message: " + e.message)
      return False

  def get_cached_file_urls(self,file_path):
    if os.path.isfile(file_path):
      try:
        with open(file_path, 'r') as content_file:
          content = content_file.read()
          return content
      except Exception as e:
        self.log_error("Exception occurred while reading the file url cache file. Error Message : "
                       + e.message +
                       "\n returning None to fetch the file list from the given url")
        return None
    else:
      return None

  def get_newly_added_fileurls(self,cache_content,new_content):
    try:
      lines = unified_diff(cache_content, new_content, fromfile='before.py', tofile='after.py')
      added = []
      for i,line in enumerate(lines):
        if i <=2:
          continue
        splt_line = line.split()
        if splt_line[0] == '+':
          added.append(" ".join(splt_line[2:]))
      if len(added) is 0:
        return None
      else:
        return "\n".join(added)
    except Exception as e:
      print("Some exception occurred while creating a file diff and extracting newly added file urls. Returning the new content.\n"+
            "Exception " + e.message)
      return new_content


  def get_file_urls(self,url,cache_file_path):
    '''
    Method that makes an HTTP GET request to the given url and extracts all the current file names from GDELT2.0
    master file list and converts it into a dictionary
    :param url: url for fetching GDELT2.0 master files list
    :return: structured files list (grouped categorically and chronologically)
    '''
    try:

      '''
      str_current_files = self.get_cached_file_urls(cache_file_path)
      if str_current_files is None:
        str_current_files = urllib2.urlopen(url).read()
        cache_ststus = self.create_cache_file(cache_file_path,str_current_files)
      else:
        new_content = urllib2.urlopen(url).read()
        str_current_files = self.get_newly_added_fileurls(str_current_files,new_content)

        if str_current_files is None:
          self.log_info("No new files have been added to the GDELT2.0 master list. Exitting!!!")
          return 0
      '''

      str_current_files = urllib2.urlopen(url).read()
      list_current_files = str_current_files.splitlines()
      print("number of lines in the file : "+ str(len(list_current_files)))
      grouped_current_files = self.chunks(list_current_files,3)
      grouped_current_files_dict = []
      for i,x in enumerate(grouped_current_files):
        if len(x[0].split()) < 3 or len(x[1].split()) < 3 or  len(x[2].split()) < 3:
          print(i)
          continue
        fileobj = {"event":{"id":x[0].split()[0],
                         "chksum":x[0].split()[1],
                         "url":x[0].split()[2],
                         "filename": x[0].split()[-1].split("/")[-1],
                         "status":0 },
                "mentions":{"id":x[1].split()[0],
                            "chksum":x[1].split()[1],
                            "url":x[1].split()[2],
                            "filename": x[1].split()[-1].split("/")[-1],
                            "status":0 },
                "kgraph":{"id":x[2].split()[0],
                          "chksum":x[2].split()[1],
                          "url":x[2].split()[2],
                          "filename": x[2].split()[-1].split("/")[-1],
                          "status":0 },
                "timestamp":int(x[0].split()[-1].split("/")[-1].split(".")[0])
                }
        grouped_current_files_dict.append(fileobj)
      return grouped_current_files_dict
    except Exception as e:
      self.log_error("URL Error : {0}".format(e.message))
      self.log_error("Failed to open url. Please provide a valid url for fetching the files.")
      return None

  def initialize_dal(self,mongo_host,mongo_port,mongo_pool_size,log_dir):
    '''
    Method to initialize the Data Access layer for handling all database related operations.
    :param mongo_host: host name for mongodb
    :param mongo_port: port for mongodb
    :param mongo_pool_size: connection pool size for mongodb client
    :param logger: logger object for logging
    :return:
    '''
    try:
      self.dal = Dal(mongo_host,mongo_port,mongo_pool_size,self,log_dir)
      self.log_info("data access layer initialized successfully")
      return True
    except Exception as e:
      self.log_error("DB init Error : {0}".format(e.message))
      return False

  def dump_file_urls(self,current_file_url_list,db_name,logger=None):
    '''
    Utility method to dump newly added file urls to db
    :param current_file_url_list: new list of files
    :param db_name: name of the database that we will query
    :param logger: logger object
    :return: status of insert
    '''
    self.log_info("dumping newly added files to db")
    dump_status = self.dal.insert_new_files(current_file_url_list,db_name,self.logger)
    return dump_status

  def download_and_insert_pending_files(self,db_name,working_dir,event_import_command,mentions_import_command,download_limit=0,download_sort=False):
    '''
    Method that downloads all the files with status 0 and inserts them into the events and mentions collection
    :param db_name: name of the db
    :return: insert status
    '''

    # Get all the pending files from the database
    pending_files = self.dal.get_file_urls_by_status(db_name,working_dir,status=0,sort=download_sort)

    # Check if there are no pending files
    if pending_files.count() is 0:
      return 0 #there are no pending files in db

    # set counter to check the number of inserts later
    insert_count = 0
    if download_limit == 0:
      download_limit = len(pending_files)

    # Iterate through the pending files
    for n,fileobj in enumerate(pending_files):
      if n == download_limit:
        break

      self.log_info("downloading file "+str(n) + " of "+ str(pending_files.count()) +
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
        e_dwn_status,events_comp_file_path = self.download_file(working_dir,events_url)

      if mentions_status is 1:
        m_dwn_status = True
      else:
        m_dwn_status,mentions_comp_file_path = self.download_file(working_dir,mentions_url)

      self.log_info("Successfully downloaded file "+str(n) + " of "+ str(pending_files.count()) +
                      " files")

      if e_dwn_status and m_dwn_status:
        self.log_info("unzipping files \n"+  events_comp_file_path + "\n and \n" +
                        mentions_comp_file_path)

        #unzip the files in the working directory
        events_file_path = self.unzip_file(events_comp_file_path,working_dir)
        mentions_file_path = self.unzip_file(mentions_comp_file_path,working_dir)

        self.log_info("Successfully unzipped files \n"+  events_comp_file_path + "\n and \n" +
                      mentions_comp_file_path + " to " + events_file_path + "\nand\n" +
                      mentions_file_path)

        # Check if the files were successfully unzipped and if not then skip the files
        if events_file_path is None or mentions_file_path is None:
          self.log_error("Skipping file " +
                         events_url +
                         " and " +
                         mentions_url +
                         " because one of them could not be unzipped.")
          continue

        self.log_info("Importing files \n"+  events_file_path + "\n and \n" +
                        mentions_file_path + "\n into database: " + db_name)

        #import the events csv file into the database
        import_events_status = self.import_csv_to_db(events_file_path,
                                                     db_name,
                                                     "cameo_events",
                                                     event_import_command)

        # Check if import was successfull. If not then log error but if successfull the update
        # file status and delete the file from the working directory
        if import_events_status is True:
          self.log_info("Successfully imported files \n"+  events_file_path +
                        "\n into database: " + db_name)

          self.log_info("Updating status of file \n"+  events_file_path +
                        "\n in database: " + db_name + " to 1 (downloaded and imported)")

          # Update the file status to 1
          e_update_status = self.dal.update_file_status(db_name,fileobj,"event",status=1)

          # Check whether file status update was successfull or not.
          # If success then delete the files from the working directory.
          if e_update_status is True:
            self.log_info("Successfully updated status of file \n"+  events_file_path +
                        "\n in database: " + db_name + " to 1 (downloaded and imported)")
            # Delete the files from the working directory
            self.delete_files([events_comp_file_path,events_file_path])

          else:
            self.log_error("Could not update status of file \n"+  events_file_path +
                        "\n in database: " + db_name + " to 1 (downloaded and imported)")

        # Import mentions csv into the database
        import_mentions_status = self.import_csv_to_db(mentions_file_path,
                                                       db_name,
                                                       "cameo_mentions",
                                                       mentions_import_command)
        # Check if import was successful. If not then log error but if successful the update
        # file status and delete the file from the working directory
        if import_mentions_status is True:
          self.log_info("Successfully imported files \n"+  mentions_file_path + "\n into database: " + db_name)

          # update the file ststus to 1
          m_update_status = self.dal.update_file_status(db_name,fileobj,"mentions",status=1)

          # Check if update was successful or not.
          # If it was the delete the files from the working directory
          if m_update_status is True:
            self.log_info("Successfully updated status of file \n"+  events_file_path +
                        "\n in database: " + db_name + " to 1 (downloaded and imported)")
            #delete the files from the working directory
            self.delete_files([mentions_comp_file_path,mentions_file_path])
          else:
            self.log_error("Could not update status of file \n"+  events_file_path +
                        "\n in database: " + db_name + " to 1 (downloaded and imported)")

        #increate the import counter if everything was fine
        if import_events_status and import_mentions_status:
          insert_count = insert_count+1
      else:
        self.log_error("Skipping file " +
                       events_url +
                       " and " +
                       mentions_url +
                       " because of unsuccessful download.")
        continue
    #check if all the pending files were imported successfully and return a status accordingly
    if insert_count == download_limit:
      return 1 #all files were successfully downloaded and inserted
    else:
      return 2 #Not all files were downloaded and inserted successfully

  def delete_files(self,files):
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

  def download_file(self,working_dir,url):
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
      download_path = os.path.join(working_dir,filename)
      # Open our local file for writing
      with open(download_path, "wb") as local_file:
        local_file.write(f.read())

      return True, download_path
    #handle errors
    except HTTPError, e:
      self.log_error("HTTP Error:"+ str(e.code) + " " + str(url))
      return False, None
    except URLError, e:
      self.log_error("URL Error:",+ str(e.reason) + " " + str(url))
      return False , None
    except Exception, e:
      self.log_error("Exception Occurred:", + str(e.strerror) )
      return False , None

  def unzip_file(self,file_path,working_dir):
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
      extracted_file_path = os.path.join(working_dir,extracted_file_name)
      return extracted_file_path
    except Exception as e:
      self.log_error("Exception Occurred while unzipping file:" +  str(e.message))
      return None

  def import_csv_to_db(self,file_path,db_name,coll_name,import_command):
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
        #self.log_info("Successfully imported file " + str(file_path) +
        #              " to collection " + coll_name + " in database " + db_name)
        return True
      else:
        self.log_error("Failed file " + str(file_path) +
                       " to collection " + coll_name + " in database " + db_name)
        return False
    except Exception as e:
      self.log_error("Mongoimport Error {0}".format(e.message))
      self.log_error("Exception occured while running import command for " + file_path)
      return False

  def create_mongo_import_command(self,db_name,file_path,coll_name,import_command):
    '''
    Method to create a mongo import command for importing csv/tsv into the db
    :param db_name: name of the database
    :param file_path: path to csv file
    :param coll_name: name of the collection to import data into
    :param import_command: raw import command for generating real import command by adding arguments
    :return: command string
    '''
    cmd = import_command.replace("DBNAME",db_name)
    cmd = cmd.replace("FILEPATH",file_path)
    return cmd

  def chunks(self,l, n):
    '''
    Yield successive n-sized chunks from l.
    :param n: size of a chunk
    :return:
    '''
    for i in range(0, len(l), n):
        yield l[i:i + n]

  def log_info(self,msg):
    '''
    Method that logs info messages either in the log file or on the console
    :param msg: message that needs to be logged
    :param logger: logger object
    :return:
    '''
    if self.logger:self.logger.info(msg)
    else:print(msg)

  def log_error(self,msg):
    '''
    Method that logs error messages either in the log file or on the console
    :param msg: message that needs to be logged
    :param logger: logger object
    :return:
    '''
    if self.logger:self.logger.error(msg)
    else:print(msg)

  def log_debug(self,msg):
    '''
    Method that logs debug messages either in the log file or on the console
    :param msg: message that needs to be logged
    :param logger: logger object
    :return:
    '''
    if self.logger:self.logger.debug(msg)
    else:print(msg)
