#!/usr/bin/env python
# encoding = utf-8

# dwd_cdd_hourly_free_ftp
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

    logging.debug(data)
    return data


def dwd_extract_actual(dwd_file, key):
    # extract data from the zip files
    with zipfile.ZipFile(dwd_file) as z:
        for filename in z.namelist():
            if not os.path.isdir(filename):
                dwd_data = {}

                # station data
                # noinspection PyStatementEffect
                [i for i, s in enumerate(filename) if 'Metadaten_Stationsname_' in s]
                if 'Metadaten_Stationsname_' and '.txt' in filename:
                    with z.open(filename) as f:
                        lines = f.read().decode('latin1').splitlines()

                        # 'raw_data'
                        if dwd_config['create_raw_dump']:
                            dwd_data['Station_h'] = lines[0]

                        if 'generiert' or 'Legende:' in lines[-1]:
                            dwd_data['Station'] = lines[-2]
                        else:
                            dwd_data['Station'] = lines[-1]
                        try:
                            dwd_data['fields'] = dwd_makenode(dwd_data['Station_h'], dwd_data['Station'])
                            logging.debug(dwd_data['fields'])
                        except ValueError:
                            logging.debug('can not make node, maybe no or corrupt data?' + str(sys.exc_info()))
                        finally:
                            logging.debug('---' + dwd_data['Station_h'] + '---' + dwd_data['Station'])

                # get latest weatherdata measurement
                if 'produkt_tu_stunde_' and '.txt' in filename:
                    with z.open(filename) as f:
                        lines = f.read().decode('latin1').splitlines()

                        dwd_data['header'] = lines[0]

                        if 'generiert' or 'Legende:' in lines[-1]:
                            dwd_data[key] = lines[-2]
                        else:
                            dwd_data[key] = lines[-1]
                        try:
                            dwd_data['data'] = dwd_makenode(dwd_data['header'], dwd_data[key])
                            logging.debug(dwd_data['data'])
                        except (RuntimeError, TypeError, NameError):
                            logging.debug('can not make node' + str(sys.exc_info()))

                dwd_data['OriginalFilename'] = str(filename)
                dwd_data['Read_Timestamp'] = str(datetime.datetime.now())
    return dwd_data


def dwd_send_event(dwd_event, config, sourcetype, key):
    # lookup fieldnames
    def get_names(fi):
        # named_data = {}
        lookup = open(fi)
        named_data = json.loads(lookup.read().decode('utf-8'))
        return named_data

    try:
        dwd_event['sourcetype'] = sourcetype

        if config['create_raw_dump'] == 'false':
            dwd_event.pop('Station', None)
            dwd_event.pop(key, None)
            dwd_event.pop('header', None)
            dwd_event.pop('data', None)
            dwd_event.pop('Station_h', None)

        if config['create_names_dump'] == 'true':
            dwd_event['named_data'] = get_names(config['lookup'])

        if config['create_fields_dump'] == 'false':
            dwd_event.pop('data', None)

        jdata = {'event': json.dumps(dwd_event)}

        # append to file (per sourcetype)
        filename = dwd_event['sourcetype'].replace(':', '_')
        with open(os.path.abspath(config['json_local_storage'] + '/' + filename + '.json'), 'a+') as myfile:
            myfile.write(json.dumps(jdata) + ",".encode('utf-8'))
    except IOError:
        logging.error('file error while writing event' + str(sys.exc_info()))
    else:
        logging.debug('wrote event: ' + json.dumps(dwd_event))


def dwd_cleanup():
    # clear log, datafiles and downloads
    logging.debug('cleanup: ')

    try:
        rc = 'myrc'
    except IOError:
        logging.error('file error while cleaning up' + str(sys.exc_info()))
    finally:
        return rc


def dwd_jsonformat(config):
    # wrap data files into json array
    logging.debug('jsonformat: ')
    try:
        rc = 'myrc'
        print(str(config))
    except IOError:
        logging.error('file error while formatting data files' + str(sys.exc_info()))
    finally:
        return rc


def dwd_main(config, config_folders):
    # go to folder where to store the downloads
    os.chdir(config['ftp_local_storage'])

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
                    raise
                else:
                    # process file
                    try:
                        print(filename + str(key))
                        mydata = dwd_extract_actual(filename, key)
                    except(RuntimeError, TypeError, NameError):
                        logging.error('error while processing file' + str(sys.exc_info()))
                    else:
                        # create json and send or save data as event
                        try:
                            # helper.log_debug(mydata)
                            dwd_send_event(mydata, config, config[key + '_st'], str(key))
                        except(RuntimeError, TypeError, NameError):
                            logging.error('error while sending file' + str(sys.exc_info()))
        finally:
            # check if data files need to get reformated
            if config['wrap_json'] == 'true':
                dwd_jsonformat(config)


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
              'wrap_json': 'false'  # wrap data export into json array # not implemented yet
              }

    config_folders = {'air_temperature': '/pub/CDC/observations_germany/climate/hourly/air_temperature/recent/',
                      'cloudiness': '/pub/CDC/observations_germany/climate/hourly/cloudiness/recent/',
                      'precipitation': '/pub/CDC/observations_germany/climate/hourly/precipitation/recent/',
                      'pressure': '/pub/CDC/observations_germany/climate/hourly/pressure/recent/',
                      'soil_temperature': '/pub/CDC/observations_germany/climate/hourly/soil_temperature/recent/',
                      'solar': '/pub/CDC/observations_germany/climate/hourly/solar/recent/',
                      'sun': '/pub/CDC/observations_germany/climate/hourly/sun/recent/',
                      'wind': '/pub/CDC/observations_germany/climate/hourly/wind/recent/'}

    return config, config_folders


# start working
LOG_FILENAME = 'log.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
logging.info("dwd script started")

# get & set config
dwd_config, dwd_config_folders = validate_input()

logging.info('config=' + json.dumps(dwd_config))
logging.info('datasources=' + json.dumps(dwd_config_folders))

dwd_main(dwd_config, dwd_config_folders)

logging.info("dwd script ended")
