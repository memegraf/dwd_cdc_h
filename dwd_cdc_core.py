#!/usr/bin/env python
# encoding = utf-8

import zipfile
import datetime
import simplejson as json
import logging
import os
import sys
import ConfigParser
import time
import tarfile
import re



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

    logging.debug('dwd_extract_station_observation dwd_data : ' + json.dumps(dwd_data))

    return dwd_data

# get last run
def get_last_run(key, config):
    try:
        lr = open(key + config['last_run_file'])
        last_run = float(lr.readline())
    except IOError:
        last_run = config['this_run']

    logging.debug('get last run  : ' + str(last_run))

    if (last_run - config['this_run']) < 6000 :
        older1h = bool(1)
    else :
        older1h = bool(0)

    logging.debug(last_run - config['this_run'])

    return older1h

def dwd_make_event(dwd_event, config, sourcetype, key):
    # lookup fieldnames, description, uom
    def get_names(fi):
        # named_data = {}
        lookup = open(fi)
        named_data = json.loads(lookup.read())
        return named_data

    # data store, given back to caller for batch saving
    datastore = []

    logging.debug('dwd_make_event dwd_event: ' + str(dwd_event))
    logging.debug('dwd_make_event sourcetype : ' + sourcetype)

    try:
        dwd_event['sourcetype'] = sourcetype
        logging.debug('prepare export')
        #print(config['create_raw_dump'])
        # add or remove data from the dict which gets exported
        logging.debug('dwd_make_event config raw dump : ' + str(config['create_raw_dump']))
        if config['create_raw_dump'] == 'False':
            dwd_event.pop('Station', None)
            dwd_event.pop('header', None)
            dwd_event.pop('Station_h', None)
            dwd_event.pop(key, None)

        logging.debug('dwd_make_event config names dump : ' + str(config['create_names_dump']))
        if config['create_names_dump'] :
            dwd_event['named_data'] = get_names(config['lookup'])

        print(config['create_fields_dump'])
        logging.debug('dwd_make_event config fields dump : ' + str(config['create_fields_dump']))

        if not config['create_fields_dump']:
            dwd_event.pop('data', None)
            dwd_event.pop('meta', None)

        logging.debug('dwd_make_event config radio dump : ' + str(config['create_radio_dump']))
        if config['create_radio_dump'] :
            dwd_event.pop('data', None)
            dwd_event.pop('meta', None)

        # send single events
        # data_to_send = {'event': json.dumps(dwd_event)}

        # append to list for batch export
        datastore.append(dwd_event)

        logging.debug('dwd_make_event datastore : ' + json.dumps(dwd_event))

    except :
        logging.error('file error while writing event' + str(sys.exc_info()))
    else:
        logging.debug('wrote event: ' + json.dumps(dwd_event))
    finally:
        return datastore


def get_config():
    """ getr local and default configs. is local config is not set, default config file is used
    note: if you set up a local config, by now it has to have all options.

    """

    # read configuration file and set defaults
    actual_config = ConfigParser.SafeConfigParser()
    default_config = ConfigParser.SafeConfigParser()

    actual_config.read('local.conf')
    default_config.read('default.conf')

    #simple check if local config is set.
    try:
        teststring = str(actual_config.get('folders', 'ftp_local_storage'))
        print(teststring)
    except:

        print('no local config, choosing default config instead')
        actual_config = default_config

    finally:
        config = {
            'scriptname': os.path.basename(__file__),
            'ftp_local_storage': os.path.abspath(os.getcwd() + str(actual_config.get('folders', 'ftp_local_storage'))),
            'json_local_storage': os.path.abspath(os.getcwd() + str(actual_config.get('folders', 'json_local_storage'))),
            'lookup': os.path.abspath(os.getcwd() + str(actual_config.get('folders', 'lookup'))),
            'ftp_host': actual_config.get('ftp', 'ftp_host'),
            'create_raw_dump': actual_config.getboolean("functions", "create_raw_dump"),
            'create_names_dump': actual_config.getboolean("functions", "create_names_dump"),
            'create_fields_dump': actual_config.getboolean("functions", "create_fields_dump"),
            'create_radio_dump': actual_config.getboolean("functions", "create_radio_dump"),
            'last_run_file': actual_config.get('folders', 'last_run_file'),
            'this_run': float(time.time()),
            'check_against_last_run': actual_config.getboolean("functions", "check_against_last_run")
        }

    #add sourcetype to general config store
    config.update(dict(actual_config.items('sourcetypes')))

    #getting ftp folders
    config_folders = dict(actual_config.items("ftp_folders"))

    # create local folders to store data
    local_folders = {k: v for k, v in config.iteritems() if '_local_storage' in k}
    for key in local_folders:
        try:
            os.makedirs(local_folders[key])
        except OSError:
            if not os.path.isdir(local_folders[key]):
                raise

    return config, config_folders


# this function extracts th etextual infor from the ASC dwd file
def dwd_extract_radolan_asc(dwd_file, key):
    # extract textual data from the tar.gz files and return as dict

    # http://wradlib.org/wradlib-docs/0.9.0/notebooks/radolan/radolan_quickstart.html

    # holds the extracted data from the tar.gz
    dwd_data = {}

    """
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
    """

    with tarfile.open(dwd_file, "r:gz") as tar:
        """ we need to extract the time and date from the filename to find the most recent one"""
        myfiles = tar.getnames()

        # stores filenames and their extracted datetime YYYYMMDDHHMMSS
        myfiledates={}

        for i in myfiles:
            mydatetime=re.search("RW_(\d{4})(\d{2})(\d{2})(-)(\d{2})(\d{2})",i)
            myfiledates[str(i)] = (
                mydatetime.group(1) +
                mydatetime.group(2) +
                mydatetime.group(3) +
                mydatetime.group(5) +
                mydatetime.group(6) )

        # get the latest file
        latest= sorted(myfiledates.keys())[-1]

        # and read its contents into dict
        dwd_data['filename'] = latest
        dwd_data['data'] = tar.extractfile(latest).read()

    return dwd_data
