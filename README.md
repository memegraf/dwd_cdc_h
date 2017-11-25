# dwd_cdc_h
Download Weather Data

Script to gather the free climate hourly station observations
from Deutscher Wetter (DWD) Dienst Climate Data Center (CDC)

## config

              'air_temperature_st': 'dwd:air:temperature:act:h',
              'cloudiness_st': 'dwd:cloudiness:act:h',
              'precipitation_st': 'dwd:precipitation:act:h',
              'pressure_st': 'dwd:pressure:act:h',
              'soil_temperature_st': 'dwd:soil:temperature:act:h',
              'solar_st': 'dwd:solar:act:h',
              'sun_st': 'dwd:sun:act:h',
              'wind_st': 'dwd:wind:act:h',
							
              'scriptname': os.path.basename(__file__),
              'ftp_local_storage': os.path.abspath(os.getcwd() + '\downloads'),
              'json_local_storage': os.path.abspath(os.getcwd() + '\json'),
              'lookup': os.path.abspath(os.getcwd() + '\lookup_fieldnames_DE.json'),
              'filematch': '*.zip',
              'ftp_host': 'ftp-cdc.dwd.de',
							
              'create_raw_dump': 'false',
              'create_names_dump': 'false',  # false, short, mid, long # not implemented yet
              'create_fields_dump': 'true',
              'cleanup_before_perform': 'false',  # not implemented yet
              'wrap_json': 'false'  # wrap data export into json array # not implemented yet


## config_folders
	      
	      'air_temperature': '/pub/CDC/observations_germany/climate/hourly/air_temperature/recent/',
              'cloudiness': '/pub/CDC/observations_germany/climate/hourly/cloudiness/recent/',
              'precipitation': '/pub/CDC/observations_germany/climate/hourly/precipitation/recent/',
              'pressure': '/pub/CDC/observations_germany/climate/hourly/pressure/recent/',
              'soil_temperature': '/pub/CDC/observations_germany/climate/hourly/soil_temperature/recent/',
              'solar': '/pub/CDC/observations_germany/climate/hourly/solar/recent/',
              'sun': '/pub/CDC/observations_germany/climate/hourly/sun/recent/',
              'wind': '/pub/CDC/observations_germany/climate/hourly/wind/recent/'
							
