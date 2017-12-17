#!/usr/bin/env python
#encoding = utf-8

import wradlib as wrl
import matplotlib.pyplot as pl
import warnings

warnings.filterwarnings('ignore')
try:
    get_ipython().magic("matplotlib inline")
except:
    pl.ion()
import numpy as np


def dwd_extract_radolan_asc(dwd_file, key):
    # extract data from the tar.gz files and return extracted info as dict and save images

    # http://wradlib.org/wradlib-docs/0.9.0/notebooks/radolan/radolan_quickstart.html

    # holds the extracted data from the tar.gz
    dwd_data = {}

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

    return dwd_data


def dwd_extract_radolan_bin(dwd_file, key):


    try:
        # load radolan files
        rwdata, rwattrs = wrl.io.read_RADOLAN_composite(dwd_file)
        # print the available attributes
        print("RW Attributes:", rwattrs)

        #holds the extracted data
        dwd_data = {}
        dwd_data['data'] = rwattrs
    except:
        print("argh")

    # In[ ]:


    # do some masking
    sec = rwattrs['secondary']
    rwdata.flat[sec] = -9999
    rwdata = np.ma.masked_equal(rwdata, -9999)

    # In[ ]:


    # Get coordinates
    radolan_grid_xy = wrl.georef.get_radolan_grid(900, 900)
    x = radolan_grid_xy[:, :, 0]
    y = radolan_grid_xy[:, :, 1]

    # In[ ]:


    # plot function
    # pl.pcolormesh(x, y, rwdata, cmap="spectral")
    # cb = pl.colorbar(shrink=0.75)
    # cb.set_label("mm/h")
    # pl.title('RADOLAN RW Product Polar Stereo \n' + rwattrs['datetime'].isoformat())
    # pl.grid(color='r')

    # http://wradlib.org/wradlib-docs/0.9.0/notebooks/radolan/radolan_quickstart.html


    #print(dwd_file + key)

    #with gzip.open(dwd_file, 'rb') as f:
    #    file_content = f.read()
    #    print(file_content)

    return dwd_data

