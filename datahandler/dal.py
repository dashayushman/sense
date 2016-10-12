from pymongo import MongoClient
import pymongo
import logging
import os
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
