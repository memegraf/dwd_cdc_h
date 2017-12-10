#!/usr/bin/env python
# encoding = utf-8

# dwd_cdd_hourly_ftp
# script to gather the free climate hourly station observations
# from Deutscher Wetter (DWD) Dienst Climate Data Center (CDC)


import datetime
import simplejson as json
import logging
import os
import sys
import zipfile
from ftplib import FTP
import re
import tarfile
import ConfigParser


def dwd_makenode(header, value):
    logging.debug('making nodes')
    values = value.split(';')
    headers = header.split(';')
    data = dict(zip(headers, values))
    return data


def dwd_extract_station_observation(dwd_file, key):
    # extract data from the zip files and return extracted info as dict
    dwd_data = {}
    with zipfile.ZipFile(dwd_file) as z:
        for filename in z.namelist():

            dwd_data['OriginalFilename'] = str(filename)
            dwd_data['Read_Timestamp'] = str(datetime.datetime.now())

            if '.txt' in filename:

                logging.debug('watching at file:')
                logging.debug(filename)

                # in file *Stationsname* are some basic infos like location and name of the station
                # we extract these from the first and last (usually) line of the file and return a node
                if 'Metadaten_Stationsname_' in filename:

                    with z.open(filename) as f:

                        logging.debug('working at file:')
                        logging.debug(filename)

                        lines = f.read().decode('latin1').splitlines()

                        # Station Header
                        dwd_data['Station_h'] = lines[0]

                        # Station Data
                        c = len(lines) - 1
                        if ';' in lines[c]:
                            dwd_data['Station'] = lines[c]
                        else:
                            for i in lines:
                                if ';' in lines[i] and i != 0:
                                    dwd_data['Station'] = lines[i]

                        # create node
                        try:
                            dwd_data['meta'] = dwd_makenode(dwd_data['Station_h'].replace(' ', ''),
                                                            dwd_data['Station'].replace(' ', ''))
                            logging.debug('meta: ' + str(dwd_data['meta']))

                        except ValueError:
                            logging.error('can not make node' + str(sys.exc_info()))

                # get latest weatherdata measurement
                if 'produkt_' in filename:
                    with z.open(filename) as f:

                        logging.debug('working at file:')
                        logging.debug(filename)

                        lines = f.read().decode('latin1').splitlines()

                        # Header
                        dwd_data['header'] = lines[0]

                        # Data
                        c = len(lines) - 1
                        if ';' in lines[c]:
                            dwd_data[key] = lines[c]
                        else:
                            for i in lines:
                                if ';' in lines[i] and i != 0:
                                    dwd_data[key] = lines[i]

                        # create node
                        try:
                            dwd_data['data'] = dwd_makenode(dwd_data['header'].replace(' ', ''),
                                                            dwd_data[key].replace(' ', ''))
                            logging.debug('data: ' + str(dwd_data['data']))
                        except (RuntimeError, TypeError, NameError):
                            logging.error('can not make node' + str(sys.exc_info()))
    return dwd_data


def dwd_extract_radio(dwd_file, key):
    # extract data from the tar.gz files and return extracted info as dict

    # http://wradlib.org/wradlib-docs/0.9.0/notebooks/radolan/radolan_quickstart.html

    dwd_data = {}

    print(dwd_file + key)

    tar = tarfile.open(dwd_file, "r:gz")
    for tarinfo in tar:
        print tarinfo.name, "is", tarinfo.size, "bytes in size and is",
        if tarinfo.isreg():
            print "a regular file."
        elif tarinfo.isdir():
            print "a directory."
        else:
            print "something else."
    tar.close()


def dwd_send_event(dwd_event, config, sourcetype, key):
    # lookup fieldnames, description, uom
    def get_names(fi):
        # named_data = {}
        lookup = open(fi)
        named_data = json.loads(lookup.read())
        return named_data

    # data store, given back to caller for batch saving
    datastore = []

    try:
        dwd_event['sourcetype'] = sourcetype

        # add or remove data from the dict which gets exported
        if config['create_raw_dump'] == 'false':
            dwd_event.pop('Station', None)
            dwd_event.pop('header', None)
            dwd_event.pop('Station_h', None)
            dwd_event.pop(key, None)

        if config['create_names_dump'] == 'True':
            dwd_event['named_data'] = get_names(config['lookup'])

        if config['create_fields_dump'] == 'false':
            dwd_event.pop('data', None)
            dwd_event.pop('meta', None)

        if config['create_radio_dump'] == 'True':
            dwd_event.pop('data', None)
            dwd_event.pop('meta', None)

        # send single events
        # jdata = {'event': json.dumps(dwd_event)}

        # append to list for batch export
        datastore.append(dwd_event)

    except IOError:
        logging.error('file error while writing event' + str(sys.exc_info()))
    else:
        logging.debug('wrote event: ' + json.dumps(dwd_event))
    finally:
        return datastore


def dwd_main(config, config_folders):
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

        # holds the data for each sourcetype
        datastore = {}

        try:
            ftp.cwd(config_folders[key])
        except (TypeError, NameError):
            logging.error('can not switch ftp directory ' + str(sys.exc_info()))
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

                        if filename.endswith('.tar.gz'):
                            mydata = dwd_extract_radio(filename,key)

                    except(TypeError, NameError):
                        logging.error(str('error while processing file ' + str(sys.exc_info())))
                    else:
                        # send data as events and store these events in dictionary to do some batch processing later
                        try:
                            datastore[filename] = dwd_send_event(mydata, config, config[key + '_st'], str(key))
                        except(TypeError, NameError):
                            logging.error(str('error while sending file ' + str(sys.exc_info())))
        finally:
            try:
                # do batch saving into json files for each sourcetype
                with open(os.path.abspath(config['json_local_storage'] + '/' + key + '.json'), 'w') as myfile:
                    myfile.write(json.dumps(datastore))
            except IOError:
                logging.error('can not save datafile' + str(sys.exc_info()))


def get_config():

    # read configuration file and set defaults
    local_conf = ConfigParser.SafeConfigParser()

    local_conf.read('local.conf')

    config = {
        'scriptname': os.path.basename(__file__),
        'ftp_local_storage': os.path.abspath(os.getcwd() + str(local_conf.get('folders', 'ftp_local_storage'))),
        'json_local_storage': os.path.abspath(os.getcwd() + str(local_conf.get('folders', 'json_local_storage'))),
        'lookup': os.path.abspath(os.getcwd() + str(local_conf.get('folders', 'lookup'))),
        'ftp_host': local_conf.get('ftp', 'ftp_host'),
        'create_raw_dump': local_conf.getboolean("functions", "create_raw_dump"),
        'create_names_dump': local_conf.getboolean("functions", "create_names_dump"),
        'create_fields_dump': local_conf.getboolean("functions", "create_fields_dump"),
    }

    #add sourcetype to general config store
    config.update(dict(local_conf.items('sourcetypes')))


    #getting ftp folders
    config_folders = dict(local_conf.items("ftp_folders"))
    print("hi")
    print(dict(local_conf.items('ftp_folders')))

    local_folders = {k: v for k, v in config.iteritems() if '_local_storage' in k}
    for key in local_folders:
        try:
            os.makedirs(local_folders[key])
        except OSError:
            if not os.path.isdir(local_folders[key]):
                raise

    return config, config_folders


# start working

# set up logging
ds = str(datetime.datetime.now()).replace('-', '').replace(':', '').replace('.', '')
LOG_FILENAME = os.path.abspath(os.getcwd() + '/log' + '/' + ds + '.log')
try:
    os.makedirs(os.path.dirname(LOG_FILENAME))
except OSError:
    if not os.path.isdir(os.path.dirname(LOG_FILENAME)):
        raise

logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)

logging.info(str(datetime.datetime.now()) + '_' + "dwd script started")
# get & set config
dwd_config, dwd_config_folders = get_config()

#log config values
logging.debug(str(datetime.datetime.now()) + '_' + 'config=' + json.dumps(dwd_config))
logging.debug(str(datetime.datetime.now()) + '_' + 'datasources=' + json.dumps(dwd_config_folders))

dwd_main(dwd_config, dwd_config_folders)

logging.info(str(datetime.datetime.now()) + '_' + "dwd script ended")
