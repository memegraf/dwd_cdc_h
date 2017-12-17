#!/usr/bin/env python
# encoding = utf-8

# dwd_cdd_hourly_ftp
# script to gather the free climate hourly station observations
# from Deutscher Wetter (DWD) Dienst Climate Data Center (CDC)


from ftplib import FTP
import traceback

from dwd_cdc_core import *
from dwd_cdc_radio import *


def dwd_main(config, config_folders):
    """ runs through the ftp folder, downloads files, extracts infos and creates output"""
    # go to folder where to store the downloads
    os.chdir(config['ftp_local_storage'])

    # open ftp connection
    try:
        ftp = FTP(config['ftp_host'])  # connect to host, default port
        ftp.login()  # user anonymous, passwd anonymous@
    except IOError:
        logging.error('ftp connection errrorrr' + str(sys.exc_info()))

    # get all the 'zip' (default) files and extract weather data by configured sourcetype
    for key in config_folders:

        # only run if the last run is longer ago then 1 hour, or just run
        ok_run = bool(0)
        if config['check_against_last_run']:
            older1h = get_last_run(key, config)
            if older1h:
                ok_run=bool(1)

        if ok_run:
            # holds the data for each sourcetype
            datastore = {}

            try:
                ftp.cwd(config_folders[key])
            except (TypeError, NameError):
                logging.error('can not switch ftp directory ' + str(traceback.print_tb()))
            else:

                for filename in ftp.nlst():
                    logging.info('filename: ' + filename)

                    # open file from ftp server / folder
                    try:
                        fhandle = open(filename, 'wb')
                        # get files
                        ftp.retrbinary('RETR ' + filename, fhandle.write)
                        fhandle.close()
                    except(TypeError, NameError):
                        logging.error('error while reading file ' + str(sys.exc_info()))
                    else:
                        # process file and store its relevant contents into dict
                        try:
                            logging.info("sourcetype:" + str(key))

                            if filename.endswith('.zip'):
                                mydata = dwd_extract_station_observation(filename, key)
                                logging.debug('dwd main mydata : ' + json.dumps(mydata))

                            if filename.endswith('.tar.gz'):
                                mydata = dwd_extract_radolan_asc(filename, key)
                                logging.debug('dwd main mydata : ' + json.dumps(mydata))

                            if filename.endswith('bin.gz'):
                                mydata = dwd_extract_radolan_bin(filename, key)
                                print mydata
                                logging.debug('dwd main mydata : ' + str(mydata))

                        except(TypeError, NameError):
                            logging.error(str('error while processing file ' + str(traceback.print_exc())))
                        else:
                            # send data as events and store these events in dictionary to do some batch processing later
                            try:
                                datastore[filename] = dwd_make_event(mydata, config, config[key + '_st'], str(key))

                                logging.debug('dwd main datastore filename : ' + json.dumps(datastore[filename]))

                            except(TypeError, NameError):
                                logging.error(str('error while sending file ' + str(traceback.print_exc())))
            finally:
                try:
                    # do batch saving into json files for each sourcetype
                    with open(os.path.abspath(config['json_local_storage'] + '/' + key + '.json'), 'w') as myfile:
                        myfile.write(json.dumps(datastore))

                    # save last run file
                    with open(os.path.abspath(key + config['last_run_file']), 'w') as myfile:
                        myfile.write(str(time.time()))

                except IOError:
                    logging.error('can not save datafile' + str(sys.exc_info()))


########################################################################################################################
#
#
#
########################################################################################################################

print ("start")

# set up logging
ds = str(datetime.datetime.now()).replace('-', '').replace(':', '').replace('.', '')
LOG_FILENAME = os.path.abspath(os.getcwd() + '/log' + '/' + ds + '.log')

try:
    os.makedirs(os.path.dirname(LOG_FILENAME))
except OSError:
    if not os.path.isdir(os.path.dirname(LOG_FILENAME)):
        raise

FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(funcName)s - line %(lineno)d"
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG, format=FORMAT)
logging.info(str(datetime.datetime.now()) + '_' + "dwd script started")

# get & set config
dwd_config, dwd_config_folders = get_config()

# log config values
logging.debug(str(datetime.datetime.now()) + '_' + 'config=' + json.dumps(dwd_config))
logging.debug(str(datetime.datetime.now()) + '_' + 'datasources=' + json.dumps(dwd_config_folders))

# start working
dwd_main(dwd_config, dwd_config_folders)

logging.info(str(datetime.datetime.now()) + '_' + "dwd script ended")
print ("end")
