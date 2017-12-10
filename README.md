# dwd_cdc_h

Get latest climate station observations from 
Deutscher Wetter Dienst (DWD) - Climate Data Center (CDC) 
https://www.dwd.de/DE/klimaumwelt/cdc/cdc_node.html

This script downloads the latest weather station observation data, 
extracts the measurements and saves these as json files grouped by 
sourcetype (like air temperature or pressure). 

You may also download RADIOLAN data and plot images.

The data on the ftp server is updated hourly, so schedule the script to get 
updated data. 

Locally created data files will get overwritten. 

Data docu: ftp://ftp-cdc.dwd.de/pub/CDC/

example output

        {
          "stundenwerte_TU_00390_akt.zip": [
            {
              "sourcetype": "dwd:air:temperature:act:h",
              "Read_Timestamp": "2017-12-02 00:00:45.082000",
              "meta": {
                "Stations_ID": "390",
                "Von_Datum": "19861126",
                "Stationsname": "Berleburg,Bad-St\u00fcnzel",
                "Bis_Datum": ""
              },
              "data": {
                "QN_9": "1",
                "eor": "eor",
                "STATIONS_ID": "390",
                "TT_TU": "-1.8",
                "MESS_DATUM": "2017113023",
                "RF_TU": "99.0"
              },
              "OriginalFilename": "produkt_tu_stunde_20160530_20171130_00390.txt"
           ... 
           ... 
           ...      
          "stundenwerte_TU_03289_akt.zip": [
            {
              "sourcetype": "dwd:air:temperature:act:h",
              "Read_Timestamp": "2017-12-01 23:59:54.082000",
              "meta": {
                "Stations_ID": "3289",
                "Von_Datum": "20041101",
                "Stationsname": "Schmieritz-Weltwitz",
                "Bis_Datum": ""
              },
              "data": {
                "QN_9": "1",
                "eor": "eor",
                "STATIONS_ID": "3289",
                "TT_TU": "-0.1",
                "MESS_DATUM": "2017113023",
                "RF_TU": "85.0"
              },
              "OriginalFilename": "produkt_tu_stunde_20160530_20171130_03289.txt"
            }
          ]
        }

## install
see dwd_install.sh

## config
configuration is done in file local.conf. 



 The chapter ftp_folder and chapter sourcetypes are dependent on each other. these must get set according to the
 functionality switches

example settings to gather radiolan data and plot images:
    
     [ftp_folders]
     radio_data = /pub/CDC/grids_germany/hourly/radolan/recent/asc/
    
     [sourcetypes]
     radio_data_st = dwd:radio:act:h
    
     [functions]
     create_radio_dump = true


example settings to get air_temperature and cloudiness data only:
    
     [ftp_folders]
     air_temperature = /pub/CDC/observations_germany/climate/hourly/air_temperature/recent/
     cloudiness = /pub/CDC/observations_germany/climate/hourly/cloudiness/recent/
    
     [sourcetypes]
     air_temperature_st = dwd:air:temperature:act:h
     cloudiness_st = dwd:cloudiness:act:h
    
     [functions]
     create_fields_dump = true

config options:

    # ftp host
    [ftp]
    ftp_host = ftp-cdc.dwd.de
    
    
    # FTP folders to gather data from
    [ftp_folders]
    #radio_data = /pub/CDC/grids_germany/hourly/radolan/recent/asc/
    air_temperature = /pub/CDC/observations_germany/climate/hourly/air_temperature/recent/
    cloudiness = /pub/CDC/observations_germany/climate/hourly/cloudiness/recent/
    precipitation = /pub/CDC/observations_germany/climate/hourly/precipitation/recent/
    pressure = /pub/CDC/observations_germany/climate/hourly/pressure/recent/
    soil_temperature = /pub/CDC/observations_germany/climate/hourly/soil_temperature/recent/
    solar = /pub/CDC/observations_germany/climate/hourly/solar/
    sun = /pub/CDC/observations_germany/climate/hourly/sun/recent/
    wind = /pub/CDC/observations_germany/climate/hourly/wind/recent/
    
    
    # Sourcetypes (type of the data, like temperature, pressure) that get extracted
    # sourcetypes have to be the same as the ftp_folder keys, with _st at the end
    [sourcetypes]
    #radio_data_st = dwd:radio:act:h
    air_temperature_st = dwd:air:temperature:act:h
    cloudiness_st = dwd:cloudiness:act:h
    precipitation_st = dwd:precipitation:act:h
    pressure_st = dwd:pressure:act:h
    soil_temperature_st = dwd:soil:temperature:act:h
    solar_st = dwd:solar:act:h
    sun_st = dwd:sun:act:h
    wind_st = dwd:wind:act:h
    
        
    # Switch functionallity
    [functions]
    create_raw_dump = false # return comma seperated lines from the source files
    create_names_dump = false   # add nice fieldnames, based on lookup_fieldnames_<LANGUAGE>.json
                                # false, short, mid, long # not implemented yet
    create_fields_dump = true #return measurements as fields
    create_radio_dump = false # download radiolan data and plot images
    check_against_last_run = true # set false if in experimenting
    
    
    # Paths and Urls, Filetypes
    [folders]
    lookup = /lookup_fieldnames_DE.json
    ftp_local_storage = /downloads
    json_local_storage = /json
    img_local_storage = /img
    last_run_file = last_run.time




## run
python dwd_cdc_hourly_ftp.py
						
# todo
- [ ] add send event
- [X] write basic docu
- [X] move config to file
- [ ] do input validation
- [ ] implement lookup for nice fieldnames and merge with measurements
- [ ] download radio data
- [ ] convert radio data
- [ ] plot radio images


