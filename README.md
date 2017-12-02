# dwd_cdc_h

Get latest climate station observations from 
Deutscher Wetter Dienst (DWD) - Climate Data Center (CDC) 
https://www.dwd.de/DE/klimaumwelt/cdc/cdc_node.html

This script downloads the latest weather station observation data, extracts the measurements and saves these as json files grouped by sourcetype (like air temperature or pressure). 
The data on the ftp server is updated hourly, so schedule the script to get updated data. locally created data files will get overwritten. 

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
pip install simplejson

## config
configuration is done within the scipt. 

## run
python dwd_cdc_hourly_ftp.py

Sourcetypes (type of the data, like temperature, pressure) that get extracted

    'air_temperature_st': 'dwd:air:temperature:act:h',
    'cloudiness_st': 'dwd:cloudiness:act:h',
    'precipitation_st': 'dwd:precipitation:act:h',
    'pressure_st': 'dwd:pressure:act:h',
    'soil_temperature_st': 'dwd:soil:temperature:act:h',
    'solar_st': 'dwd:solar:act:h',
    'sun_st': 'dwd:sun:act:h',
    'wind_st': 'dwd:wind:act:h',
	      
Paths and Urls, Filetypes

    'ftp_local_storage':  where to store the downloaded zip files
    'json_local_storage': and the json data files
    'lookup': lookup_fieldnames_DE.json contains nice fieldnames in german
    'ftp_host': dwd ftp url

Functions

    'create_raw_dump': 'false',
return comma seperated lines from the source files
 
    'create_names_dump': 'false',  # false, short, mid, long # not implemented yet
add nice fieldnames, based on lookup_fieldnames_<LANGUAGE>.json
    
    'create_fields_dump': 'true', 
return measurements as fields


## config_folders
FTP folders to gather data from

    'air_temperature': '/pub/CDC/observations_germany/climate/hourly/air_temperature/recent/',
    'cloudiness': '/pub/CDC/observations_germany/climate/hourly/cloudiness/recent/',
    'precipitation': '/pub/CDC/observations_germany/climate/hourly/precipitation/recent/',
    'pressure': '/pub/CDC/observations_germany/climate/hourly/pressure/recent/',
    'soil_temperature': '/pub/CDC/observations_germany/climate/hourly/soil_temperature/recent/',
    'solar': '/pub/CDC/observations_germany/climate/hourly/solar/recent/',
    'sun': '/pub/CDC/observations_germany/climate/hourly/sun/recent/',
    'wind': '/pub/CDC/observations_germany/climate/hourly/wind/recent/'
							
# todo
- [ ] add send event
- [X] write basic docu
- [ ] move config to file
- [ ] do input validation
- [ ] implement lookup for nice fieldnames and merge with measurements


