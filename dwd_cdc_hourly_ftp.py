#!/usr/bin/env python
# encoding = utf-8

# dwd_cdd_hourly_ftp
# script to gather the free climate hourly station observations
# from Deutscher Wetter (DWD) Dienst Climate Data Center (CDC)


import datetime
import json
import logging
import os
import sys
import zipfile
from ftplib import FTP


def dwd_makenode(header, value):
    logging.debug('making nodes')
    values = value.split(';')
    headers = header.split(';')
    data = dict(zip(headers, values))
    return data


def dwd_extract_actual(dwd_file, key):
    # extract data from the zip files
    with zipfile.ZipFile(dwd_file) as z:
        for filename in z.namelist():
            if not os.path.isdir(filename):
                dwd_data = {}

                # station data
            if '.txt' in filename:

                logging.debug('watching at file:')
                logging.debug(filename)

                # get station metadata
                if 'Metadaten_Stationsname_' in filename:
                    with z.open(filename) as f:

                        logging.debug('working at file:')
                        logging.debug(filename)

                        lines = f.read().decode('latin1').splitlines()

                        dwd_data['Station_h'] = lines[0]

                        c = len(lines) - 1
                        if ';' in lines[c]:
                            dwd_data['Station'] = lines[c]
                        else:
                            for i in lines:
                                if ';' in lines[i]:
                                    dwd_data['Station'] = lines[i]

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

                        dwd_data['header'] = lines[0]

                        c = len(lines) - 1
                        if ';' in lines[c]:
                            dwd_data[key] = lines[c]
                        else:
                            for i in lines:
                                if ';' in lines[i]:
                                    dwd_data[key] = lines[i]

                        try:
                            dwd_data['data'] = dwd_makenode(dwd_data['header'].replace(' ', ''),
                                                            dwd_data[key].replace(' ', ''))
                            logging.debug('data: ' + str(dwd_data['data']))
                        except (RuntimeError, TypeError, NameError):
                            logging.error('can not make node' + str(sys.exc_info()))

                dwd_data['OriginalFilename'] = str(filename)
                dwd_data['Read_Timestamp'] = str(datetime.datetime.now())
    return dwd_data


def dwd_send_event(dwd_event, config, sourcetype, key):
    # lookup fieldnames
    def get_names(fi):
        # named_data = {}
        lookup = open(fi)
        named_data = json.loads(lookup.read())
        return named_data

    datastore = []

    try:
        dwd_event['sourcetype'] = sourcetype

        if config['create_raw_dump'] == 'false':
            dwd_event.pop('Station', None)
            dwd_event.pop('header', None)
            dwd_event.pop('Station_h', None)

        if config['create_names_dump'] == 'true':
            dwd_event['named_data'] = get_names(config['lookup'])

        if config['create_fields_dump'] == 'false':
            dwd_event.pop('data', None)

            dwd_event.pop(key, None)

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


def dwd_cleanup():
    # clear log, datafiles and downloads
    logging.debug('cleanup: ')
    pass


def dwd_main(config, config_folders):
    # go to folder where to store the downloads
    os.chdir(config['ftp_local_storage'])

    datastore = {}

    # open up ftp connection
    try:
        ftp = FTP(config['ftp_host'])  # connect to host, default port
        ftp.login()  # user anonymous, passwd anonymous@
    except IOError:
        logging.error('ftp connection errrorrr' + str(sys.exc_info()))

    # extract data for each configured folder in dwd_config_folders[]
    for key in config_folders:
        try:
            ftp.cwd(config_folders[key])
        except (RuntimeError, TypeError, NameError):
            logging.error('can not switch ftp directory' + str(sys.exc_info()))
        else:
            for filename in ftp.nlst(config['filematch']):
                logging.debug('Processing ' + filename)
                # mydata = []
                # open file from ftp serveer
                try:
                    fhandle = open(filename, 'wb')
                    # get files
                    ftp.retrbinary('RETR ' + filename, fhandle.write)
                    fhandle.close()
                except(RuntimeError, TypeError, NameError):
                    logging.error('error while reading file' + str(sys.exc_info()))
                else:
                    # process file
                    try:
                        print(filename + str(key))
                        mydata = dwd_extract_actual(filename, key)
                    except(RuntimeError, TypeError, NameError):
                        logging.error(
                            str('error while processing file' + str(sys.exc_info())))
                    else:
                        # create json and send or save data as event
                        try:
                            # helper.log_debug(mydata)
                            datastore[filename] = dwd_send_event(mydata, config, config[key + '_st'], str(key))
                        except(RuntimeError, TypeError, NameError):
                            logging.error(
                                str('error while sending file' + str(sys.exc_info())))
        finally:
            try:
                with open(os.path.abspath(config['json_local_storage'] + '/' + key + '.json'), 'w') as myfile:
                    myfile.write(json.dumps(datastore))
            except IOError:
                logging.error('can not save datafile' + str(sys.exc_info()))


def validate_input():
    # todo:
    #   - move settings to config file
    #   - validate inputs :-)

    config = {'air_temperature_st': 'dwd:air:temperature:act:h',
              'cloudiness_st': 'dwd:cloudiness:act:h',
              'precipitation_st': 'dwd:precipitation:act:h',
              'pressure_st': 'dwd:pressure:act:h',
              'soil_temperature_st': 'dwd:soil:temperature:act:h',
              'solar_st': 'dwd:solar:act:h',
              'sun_st': 'dwd:sun:act:h',
              'wind_st': 'dwd:wind:act:h',
              # basic config
              'scriptname': os.path.basename(__file__),
              'ftp_local_storage': os.path.abspath(os.getcwd() + '\downloads'),
              'json_local_storage': os.path.abspath(os.getcwd() + '\json'),
              'lookup': os.path.abspath(os.getcwd() + '\lookup_fieldnames_DE.json'),
              'filematch': '*.zip',
              'ftp_host': 'ftp-cdc.dwd.de',
              # function selection
              'create_raw_dump': 'false',
              'create_names_dump': 'false',  # false, short, mid, long # not implemented yet
              'create_fields_dump': 'true',
              'cleanup_before_perform': 'false',  # not implemented yet
              }

    config_folders = {'air_temperature': '/pub/CDC/observations_germany/climate/hourly/air_temperature/recent/',
                      'cloudiness': '/pub/CDC/observations_germany/climate/hourly/cloudiness/recent/',
                      'precipitation': '/pub/CDC/observations_germany/climate/hourly/precipitation/recent/',
                      'pressure': '/pub/CDC/observations_germany/climate/hourly/pressure/recent/',
                      'soil_temperature': '/pub/CDC/observations_germany/climate/hourly/soil_temperature/recent/',
                      'solar': '/pub/CDC/observations_germany/climate/hourly/solar/',
                      'sun': '/pub/CDC/observations_germany/climate/hourly/sun/recent/',
                      'wind': '/pub/CDC/observations_germany/climate/hourly/wind/recent/'}

    return config, config_folders


# start working
LOG_FILENAME = os.path.abspath(
    os.getcwd() + '\log' + '/' + str(datetime.datetime.now()).replace('-',
                                                                      '').replace(':', '') + '_' + 'log.log')
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
logging.info(str(datetime.datetime.now()) + '_' + "dwd script started")

# get & set config
dwd_config, dwd_config_folders = validate_input()

logging.info(str(datetime.datetime.now()) + '_' + 'config=' + json.dumps(dwd_config))
logging.info(str(datetime.datetime.now()) + '_' + 'datasources=' + json.dumps(dwd_config_folders))

dwd_main(dwd_config, dwd_config_folders)

logging.info(str(datetime.datetime.now()) + '_' + "dwd script ended")
