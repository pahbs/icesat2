#! /usr/bin/env python

''' Author: Nathan Thomas, Paul Montesano
    Date: 02/003/2020
    Version: 1.0
    THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.'''

"""
# This script was copied from extract_filter_atl08.py, edited to work with 
  # v005 ATL08 data and enable the option to extract 20m 
  # segment information in addition to 100m info
  
  # See *do_20m and *v005 for changes made from the original extract script
    
# Example call with extracting 20m segment info and logging:
  # python extract_filter_atl08_v005.py -i /css/icesat-2/ATLAS/ATL08.005/
    2018.12.26/ATL08_20181226222354_13640102_005_01.h5 -o <outDir> --do_20m --log
    
  # Difference between --do_20m and default (100m) is the former will have a 
    # few extra columns and 5x as many rows as 100m run of same input

# Log changes:
# 1/20/22 - *v005:
    # Created from extract_filter_atl08.py and edited to work with v005 data
    # Changed default min/max month from 6-9 to 1-12
    # Changed default min/max latitude from 45-75 to 30-90

    # Commented/removed references to tcc_flg everywhere
    # Updated flag to get tcc_prc (and removed references to tcc_prc)
    
    # Set SUBSET_COLS = False in call to filter
    
# 1/21/22 - *do_20m: 
    # Changing if do_30 to do_20 and making several other edits 
      # to work with 20m segments

# 1/24/22 - *do_20m:
    # Changed the way if do_20m works with code...
    # Instead of replacing 100m stuff, 20m info gets added in addition to
      # the 100m info. This will multiply the number of output rows by 5,
      # but is the best solution since not much info for 20m segments
      # is provided. Unnecessary/redundant info can be pared down later
    
# 1/25/22 - *do_20m:
    # Adding code to get uniqueIDs (for 100m and 20m)
    # Adding code to get min/max of unfiltered data and return (outCSV, extent)
      # so it can be written to .csv in wrapper code
    # Adding logging option for runs
    
# 2/2/22 - *v005/*do_20m::
    # Removed fid field because it was pointless with our uID field
    # Changed field names to be a little more descriptive - very few changes
    # Added include_lowest=True to tcc_bin mapping portion so 0% tcc gets 
      # binned to 10 not NaN --> NVM, no more tcc binning
    # Cleaned up old code from previous version (can reference 
      # extract_filter_atl08.py if need be)
"""

import h5py
#from osgeo import gdal
import numpy as np
import pandas as pd
#import subprocess
import os, sys
import math

import argparse

import time
from datetime import datetime

def calculateElapsedTime(start, end, unit = 'minutes'):
    
    # start and end = time.time()
    
    if unit == 'minutes':
        elapsedTime = round((time.time()-start)/60, 4)
    elif unit == 'hours':
        elapsedTime = round((time.time()-start)/60/60, 4)
    else:
        elapsedTime = round((time.time()-start), 4)  
        unit = 'seconds'
        
    #print("\nEnd: {}\n".format(time.strftime("%m-%d-%y %I:%M:%S %p")))
    print("Elapsed time: {} {}".format(elapsedTime, unit))
    
    return None

# From a dataframe's columns, reorder them based on hardcoded list
# Hardcoded list will not have all the columns*, and it may have items that 
#  aren't in df's columns [eg if not running 20m segments]
# *Hardcoded list will focus on columns i know are important, any columns not
#  in hardcoded list will reman in current order after the hardcoded ones
def getOrderedColumns(df):

    actualCols = df.columns.tolist() # columns actually in df
 
    # Put these columns first, then append remaining fields to end
    desiredOrder = ['id_unique', 'year', 'month', 'day', 
                    'lon_20m', 'lat_20m', 'h_can_20m', 'h_te_best_20m', 
                    'lon', 'lat', 'h_can', 'h_max_can', 'h_can_quad', 
                    'h_can_unc', 'h_te_best', 'h_te_unc', 'rh10', 
                    'rh15', 'rh20', 'rh25', 'rh30', 'rh35','rh40', 'rh45', 
                    'rh50', 'rh55', 'rh60', 'rh65', 'rh70', 'rh75', 'rh80',
                    'rh85', 'rh90', 'rh95', 'can_rh_conf', 'h_dif_ref',
                    'seg_landcov', 'seg_cover', 'seg_water', 'night_flg',
                    'sol_el', 'asr', 'ter_slp', 'ter_flg', 'can_open', 
                    'gt', 'beam_type', 'dt', 'id_100m', 'id_20m', 
                    'granule_name']
 
    # Same as desiredOrder, as long as column was in df
    colsOrdered = [d for d in desiredOrder if d in actualCols]
    
    # Add left out columns to list at end
    remainingCols = [c for c in actualCols if c not in colsOrdered]
    colsOrdered.extend(remainingCols)
    
    if len(colsOrdered) != len(actualCols):
        print("Ordered columns are not same size as dataframe columns")
        import pdb; pdb.set_trace()
    
    return colsOrdered    
    
#*do_20m - (1/25/22) - revise as necessary
def get100mSegId(lon, lat, date):
    
    import math

    # Get yyyymmdd:
    date = time.strptime(date.decode("utf-8").split('T')[0], '%Y-%m-%d')
    idDate = '{}{}{}'.format(date.tm_year, date.tm_mon, date.tm_mday)
    
    # Configure lat/lon portion of ID:
    # We want to always get 6 digits after decimal point to be safe (prob not necessary)
    # Depending on number of digits in whole part, decimal part may be padded with 0s
    lonSplit = math.modf(lon) # split into whole and decimal parts
    lonWhole = str(lonSplit[1]).strip('-').split('.')[0] # probably a better way to do this but oh well
    lonDec = str(lonSplit[0]).split('.')[1][0:6]
    
    latSplit = math.modf(lat)
    latWhole = str(latSplit[1]).strip('-').split('.')[0]
    latDec = str(latSplit[0]).split('.')[1][0:6]
    
    if lon < 0: lonDir = 'W'
    elif lon > 0: lonDir = 'E'
    else: lonDir = '0' # if (should never happen) lon = 0, 0.0000000 -> 00000000
        
    if lat < 0: latDir = 'S'
    elif lat > 0: latDir = 'N'
    else: latDir = '0' # if (should never happen) lon = 0, 0.0000000 -> 00000000
    
    idLon = '{}{}{}'.format(lonWhole, lonDir, lonDec)   
    idLat = '{}{}{}'.format(latWhole, latDir, latDec)

    # To ensure all uniqueIDs are the same length, pad end with 0s
    while len(idLon) < 10: idLon = '{}0'.format(idLon)
    while len(idLat) < 10: idLat = '{}0'.format(idLat)
        
    # Put them together
    #108W324536-76N2836387-20190828 --> center lon is -108.324536; center lat is 76.2836387; date is 2019-08-28
    ID = '{}-{}-{}'.format(idLon, idLat, idDate)
    
    return ID

def logOutput(logdir, bname, mode):

    os.system('mkdir -p {}'.format(logdir))
    logfile = os.path.join(logdir, '{}__extract-atl08_Log.txt'.format(bname))
    sys.stdout = open(logfile, mode)

    return sys.stdout

def reFormatArrays(inDict):
    
    outDict = {}
    repeat = 5 # 5 20m segments in 100m segment
    
    for key, inArr in inDict.items():
        outArr = np.array([])
        
        # 100m segment arrays need to have values multiplied
        if inArr.ndim == 1: 
            for i in inArr:
                for r in range(repeat):
                    outArr = np.append(outArr, i)
        
        # 20m segment arrays need to have their 2nd dimension flattened
        elif inArr.ndim == 2: 
            outArr = inArr.flatten()
            
        # Build up the new dict
        outDict[key] = outArr
        
    return outDict

def rec_merge1(d1, d2):
    '''return new merged dict of dicts'''
    for k, v in d1.items(): # in Python 2, use .iteritems()!
        if k in d2:
            d2[k] = rec_merge1(v, d2[k])
    d3 = d1.copy()
    d3.update(d2)
    return d3

def extract_atl08(args):
   
    # Input sanitization
    if str(args.input).endswith('.h5'):
        pass
    else:
        print("INPUT ICESAT2 FILE MUST END '.H5'")
        os._exit(1)
    if args.output == None:
        print("\n OUTPUT DIR IS NOT SPECIFIED (OPTIONAL). OUTPUT WILL BE PLACED IN THE SAME LOCATION AS INPUT H5 \n\n")
    else:
        pass
    if args.resolution == None:
        print("SPECIFY OUTPUT RASTER RESOLUTION IN METERS'")
        os._exit(1)
    else:
        pass 

    TEST = args.TEST
    do_20m = args.do_20m

    # File path to ICESat-2h5 file
    H5 = args.input
    
    # Get the filepath where the H5 is stored and filename
    inDir = '/'.join(H5.split('/')[:-1])
    granule_fname = H5.split('/')[-1]
    Name = granule_fname.split('.')[0]
    
    # Set up output and logging directories
    if args.output == None:
        outbase = os.path.join(inDir, Name)
        logdir  = os.path.join(inDir, '_logs')
    else:
        outbase = os.path.join(args.output, Name)
        logdir  = os.path.join(args.output, '_logs')
        
    # Need this block now so we can get output .csv name and check for existence
    land_seg_path = '/land_segments/' # Now, everything point-specific (for 100m or 20m segments) is within this tag
    if do_20m:
        segment_length = 20
    else:
        segment_length = 100
    fn_tail = '_' + str(segment_length) + 'm.csv'
    out_csv_fn = os.path.join(outbase + fn_tail)

    # Check file existence before logging:
    if args.overwrite:
        # Overwite is True (on)
        pass
    else:
        if os.path.isfile(out_csv_fn):
            # Overwite is False (off) and file exists
            print(" FILE EXISTS AND WE'RE NOT OVERWRITING\n")
            os._exit(1)
        else:
            # Overwite is False (off) but file DOES NOT exist
            pass
        
    # Log output if arg supplied    
    if args.logging:
        sys.stdout = logOutput(logdir, Name, mode = "a")
        sys.stdout.flush()

    # Start clock
    start = time.time()
    print("\nBegin: {}".format(time.strftime("%m-%d-%y %I:%M:%S %p")))
    print("\nATL08 granule name: \t{}".format(Name))
    print("Input dir: \t\t{}".format(inDir))
    print("\nSegment length: {}m".format(segment_length)) 
    
    if args.filter_geo:
        print("\nMin lat: {}".format(args.minlat))
        print("Max lat: {}".format(args.maxlat))
        print("Min lon: {}".format(args.minlon))
        print("Max lon: {}\n\n".format(args.maxlon))
    
    # open file
    f = h5py.File(H5,'r')

    # Set up acq date
    dt, yr, m, d = ([] for i in range(4))

    # Set up orbit info fields
    gt, orb_num, rgt, orb_orient = ([] for i in range(4))

    # Set the names of the 6 lasers
    lines = ['gt1r', 'gt1l', 'gt2r', 'gt2l', 'gt3r', 'gt3l']

    # set up blank lists
    latitude, longitude, segid_beg, segid_end = ([] for i in range(4))

    # Canopy fields - 100m
    # no RH % specific fields, for 100m or otherwise
    #can_h_met_0, can_h_met_1, can_h_met_2, can_h_met_3, can_h_met_4, can_h_met_5, can_h_met_6, can_h_met_7, can_h_met_8 = ([] for i in range(9))
    can_h_met = []   # Relative	(RH--)	canopy height	metrics calculated	at	the	following	percentiles: 25,50,	60,	70,	75,	80,	85,	90,	95
    h_max_can = []
    h_can = []      # 98% height of all the individual canopy relative heights for the segment above the estimated terrain surface. Relative canopy heights have been computed by differencing the canopy photon height from the estimated terrain surface.
    h_can_quad = []
    h_can_unc = []
    
    n_ca_ph = []
    n_toc_ph = []
    can_open = []    # stdv of all photons classified as canopy within segment
    can_rh_conf = [] # Canopy relative height confidence flag based on percentage of ground and canopy photons within a segment: 0 (<5% canopy), 1 (>5% canopy, <5% ground), 2 (>5% canopy, >5% ground)
   
    #*v005 - remove tcc_flg and tcc_prc ref - use seg_cover
    #tcc_flg = [] # Flag indicating that more than 50% of the Landsat Continuous Cover product have values > 100 for the L-Km segment.  Canopy is assumed present along the L-km segment if landsat_flag is 1.
    seg_cover = [] # Average percentage value of the valid (value <= 100) Copernicus fractional cover product for each 100 m segment

    #*do_20m - additional fields for 20m segments:
    if do_20m:
        latitude_20m  = []
        longitude_20m = []
        h_can_20m = []
        h_te_best_20m = []
        seg20_id = []

    # Uncertainty fields
    n_seg_ph = []   # Number of photons within each land segment.
    cloud_flg = []     # Valid range is 0 - 10. Cloud confidence flag from ATL09 that indicates the number of cloud or aerosol layers identified in each 25Hz atmospheric profile. If the flag is greater than 0, aerosols or clouds could be present.
    msw_flg = []    # Multiple Scattering warning flag. The multiple scattering warning flag (ATL09 parameter msw_flag) has values from -1 to 5 where zero means no multiple scattering and 5 the greatest. If no layers were detected, then msw_flag = 0. If blowing snow is detected and its estimated optical depth is greater than or equal to 0.5, then msw_flag = 5. If the blowing snow optical depth is less than 0.5, then msw_flag = 4. If no blowing snow is detected but there are cloud or aerosol layers detected, the msw_flag assumes values of 1 to 3 based on the height of the bottom of the lowest layer: < 1 km, msw_flag = 3; 1-3 km, msw_flag = 2; > 3km, msw_flag = 1. A value of -1 indicates that the signal to noise of the data was too low to reliably ascertain the presence of cloud or blowing snow. We expect values of -1 to occur only during daylight.
    night_flg = []
    seg_landcov = [] # IGBP Land Cover Surface type classification as reference from MODIS Land Cover(ANC18) at the 0.5 arcsecond res. flag_values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 flag_meanings : Water Evergreen_Needleleaf_Forest Evergreen_Broadleaf_Forest Deciduous_Needleleaf_Forest Deciduous_Broadleaf_Forest Mixed_Forest Closed_Shrublands Open_Shrubla

    seg_snow = []  # 0=ice free water; 1=snow free land;  2=snow; 3=ice. Daily snow/ice cover from ATL09 at the 25 Hz rate(275m) indicating likely presence of snow and ice within each segment.
    seg_water = []  # no_water=0, water=1. Water mask(i.e. flag) indicating inland water as referenced from the Global Raster Water Mask(ANC33) at 250 m spatial resolution.
    sig_vert = []    # Total vertical geolocation error due to ranging and local surface slope.  The parameter is computed for ATL08 as described in equation 1.2.
    sig_acr = []       # Total cross-track uncertainty due to PPD and POD knowledge.  Read from ATL03 product gtx/geolocation/sigma_across. Sigma_atlas_y is reported on ATL08 as the uncertainty of the center-most reference photon of the 100m ATL08 segment.
    sig_along = []        # Total along-track uncertainty due to PPD and POD knowledge.  Read from ATL03 product gtx/geolocation/sigma_along. Sigma_atlas_x is reported on ATL08 as the uncertainty of the center-most reference photon of the 100m ATL08 segment.
    sig_h = []            # Estimated uncertainty for the reference photon bounce point ellipsoid height: 1- sigma (m) provided at the geolocation segment rate on ATL03.  Sigma_h is reported on ATL08 as the uncertainty of the center-most reference photon of the 100m ATL08 segment.
    sig_topo = []         # Total uncertainty that include sigma_h plus geolocation uncertainty due to local slope (equation 1.3).  The local slope is multiplied by the geolocation uncertainty factor. This will be used to determine the total vertical geolocation error due to ranging and local slope.

    # Terrain fields
    n_te_ph = []
    h_te_best = []  # The best fit terrain elevation at the the mid-point location of each 100m segment. The mid-segment terrain elevation is determined by selecting the best of three fits- linear, 3rd order and 4th order polynomials - to the terrain photons and interpolating the elevation at the mid-point location of the 100 m segment. For the linear fit, a slope correction and weighting is applied to each ground photon based on the distance to the slope height at the center of the segment.
    h_te_unc = []    # Uncertainty of the mean terrain height for the segment. This uncertainty incorporates all systematic uncertainties(e.g. timing orbits, geolocation,etc.) as well as uncertainty from errors of identified photons.  This parameter is described in section 1, equation 1.4
    ter_slp = []        # The along-track slope of terrain, within each segment;computed by a linear fit of terrain classified photons. Slope is in units of delta height over delta along track distance.
    snr = []        # The signal to noise ratio of geolocated photons as determined by the ratio of the superset of ATL03 signal and DRAGANN found signal photons used for processing the ATL08 segments to the background photons (i.e. noise) within the same ATL08 segments.
    sol_az = []     # The direction, eastwards from north, of the sun vector as seen by an observer at the laser ground spot.
    sol_el = []     # Solar Angle above or below the plane tangent to the ellipsoid surface at the laser spot. Positive values mean the sun is above the horizon, while  negative values mean it is below the horizon. The effect of atmospheric refraction is not included. This is a low precision value, with approximately TBD degree accuracy.

    asr = []		# Apparent surface reflectance
    h_dif_ref = []	# height difference from reference DEM
    ter_flg = []
    ph_rem_flg = []
    dem_rem_flg = []
    seg_wmask = []
    lyr_flg = []

    # NEED TO ADD THESE
    #h_canopy_uncertainty = []
    #h_canopy_quad = []

    if False:
        # Granule level info
        granule_dt = datetime.strptime(Name.split('_')[1], '%Y%m%d%H%M%S')

        YEAR = granule_dt.year
        MONTH = granule_dt.month
        DOY = granule_dt.timetuple().tm_yday

        Arrtemp = f['/orbit_info/orbit_number/'][...,]

        temp = np.empty_like(Arrtemp, dtype='a255')
        temp[...] = YEAR
        yr.append(temp)

        temp = np.empty_like(Arrtemp, dtype='a255')
        temp[...] = YEAR
        yr.append(temp)

        temp = np.empty_like(Arrtemp, dtype='a255')
        temp[...] = MONTH
        m.append(temp)

        temp = np.empty_like(Arrtemp, dtype='a255')
        temp[...] = DOY
        d.append(temp)

    dt.append(f['/ancillary_data/granule_end_utc/'][...,].tolist())
    orb_orient.append(f['/orbit_info/sc_orient/'][...,].tolist())
    orb_num.append(f['/orbit_info/orbit_number/'][...,].tolist())
    rgt.append(f['/orbit_info/rgt/'][...,].tolist())

    dt          =np.array([dt[l][k] for l in range(1) for k in range(len(dt[l]))] )    
    orb_orient  =np.array([orb_orient[l][k] for l in range(1) for k in range(len(orb_orient[l]))] )
    orb_num     =np.array([orb_num[l][k] for l in range(1) for k in range(len(orb_num[l]))] )
    rgt         =np.array([rgt[l][k] for l in range(1) for k in range(len(rgt[l]))] )

    # Beam level info
    # For each laser read the data and append to its list
    for line in lines:

        # It might be the case that a specific line/laser has no members in the h5 file.
        # If so, catch the error and skip - MW 3/31
        try:
            latitude.append(f['/' + line    + land_seg_path + 'latitude/'][...,].tolist())
        except KeyError:
            continue # No info for laser/line, skip it and move on to next line

        longitude.append(f['/' + line   + land_seg_path + 'longitude/'][...,].tolist())

        # Get ground track
        Arrtemp = f['/' + line  + land_seg_path + 'latitude/'][...,]
        temp = np.empty_like(Arrtemp, dtype='a255')
        temp[...] = line
        gt.append(temp)

        segid_beg.append(f['/' + line   + land_seg_path + 'segment_id_beg/'][...,].tolist())
        segid_end.append(f['/' + line   + land_seg_path + 'segment_id_end/'][...,].tolist())
        
        # Canopy fields

        #*do_20m - build 20m lists from .h5 data
          # None of these fields exist for 20m segments as of now, except
          # RH98 (h_canopy_20m), lat/lon (_20m), h_te_best_fit_20m
          
        if do_20m:
            latitude_20m.append(f['/' + line    + land_seg_path + 'latitude_20m/'][...,].tolist())
            longitude_20m.append(f['/' + line   + land_seg_path + 'longitude_20m/'][...,].tolist())
            h_can_20m.append(f['/' + line       + '/land_segments/canopy/h_canopy_20m'][...,].tolist())

            # Build the segment ID column using latitude array:            
            n_20m = len(f['/' + line    + land_seg_path + 'latitude_20m/'][...,].tolist()) # number of 20m segment shots in line
            segList = [1, 2, 3, 4, 5]
            seg20_id.append([segList for i in range(n_20m)]) # list created in append will be same size/shape as other 20m lists 
            
            
        #*do_20m - Instead of if/else, we want 20m-specific info to be 
          # extracted in addition to 100m, not instead of 
        can_h_met.append(f['/' + line   + '/land_segments/canopy/canopy_h_metrics/'][...,].tolist())
        
        h_max_can.append(f['/' + line   + '/land_segments/canopy/h_max_canopy/'][...,].tolist())
        h_can.append(f['/' + line       + '/land_segments/canopy/h_canopy/'][...,].tolist())
        h_can_quad.append(f['/' + line  + '/land_segments/canopy/h_canopy_quad'][...,].tolist())
        h_can_unc.append(f['/' + line   + '/land_segments/canopy/h_canopy_uncertainty'][...,].tolist())
            
        n_ca_ph.append(f['/' + line     + '/land_segments/canopy/n_ca_photons/'][...,].tolist())
        n_toc_ph.append(f['/' + line    + '/land_segments/canopy/n_toc_photons/'][...,].tolist())
        can_open.append(f['/' + line    + '/land_segments/canopy/canopy_openness/'][...,].tolist())
        can_rh_conf.append(f['/' + line + '/land_segments/canopy/canopy_rh_conf/'][...,].tolist())
        
        #*v005 - seg_cover replaces tcc
        seg_cover.append(f['/' + line     + '/land_segments/canopy/segment_cover/'][...,].tolist())
    
        # Uncertainty fields
        cloud_flg.append(f['/' + line   + land_seg_path + 'cloud_flag_atm/'][...,].tolist())
        msw_flg.append(f['/' + line     + land_seg_path + 'msw_flag/'][...,].tolist())
        n_seg_ph.append(f['/' + line    + land_seg_path + 'n_seg_ph/'][...,].tolist())
        night_flg.append(f['/' + line   + land_seg_path + 'night_flag/'][...,].tolist())
        seg_landcov.append(f['/' + line + land_seg_path + 'segment_landcover/'][...,].tolist())
        seg_snow.append(f['/' + line    + land_seg_path + 'segment_snowcover/'][...,].tolist())
        seg_water.append(f['/' + line   + land_seg_path + 'segment_watermask/'][...,].tolist())
        sig_vert.append(f['/' + line    + land_seg_path + 'sigma_atlas_land/'][...,].tolist())
        sig_acr.append(f['/' + line     + land_seg_path + 'sigma_across/'][...,].tolist())
        sig_along.append(f['/' + line   + land_seg_path + 'sigma_along/'][...,].tolist())
        sig_h.append(f['/' + line       + land_seg_path + 'sigma_h/'][...,].tolist())
        sig_topo.append(f['/' + line    + land_seg_path + 'sigma_topo/'][...,].tolist())

        # Terrain fields
        #*do_20m - Grab the only terrain field for 20m segment
        if do_20m:
            h_te_best_20m.append(f['/' + line   + '/land_segments/terrain/h_te_best_fit_20m'][...,].tolist())

        n_te_ph.append(f['/' + line     + '/land_segments/terrain/n_te_photons/'][...,].tolist())
        h_te_best.append(f['/' + line   + '/land_segments/terrain/h_te_best_fit/'][...,].tolist())
        h_te_unc.append(f['/' + line    + '/land_segments/terrain/h_te_uncertainty/'][...,].tolist())
        ter_slp.append(f['/' + line     + '/land_segments/terrain/terrain_slope/'][...,].tolist())
            
        snr.append(f['/' + line         + land_seg_path + 'snr/'][...,].tolist())
        sol_az.append(f['/' + line      + land_seg_path + 'solar_azimuth/'][...,].tolist())
        sol_el.append(f['/' + line      + land_seg_path + 'solar_elevation/'][...,].tolist())

        asr.append(f['/' + line         + land_seg_path + 'asr/'][...,].tolist())
        h_dif_ref.append(f['/' + line   + land_seg_path + 'h_dif_ref/'][...,].tolist())
        ter_flg.append(f['/' + line     + land_seg_path + 'terrain_flg/'][...,].tolist())
        ph_rem_flg.append(f['/' + line  + land_seg_path + 'ph_removal_flag/'][...,].tolist())
        dem_rem_flg.append(f['/' + line + land_seg_path + 'dem_removal_flag/'][...,].tolist())
        seg_wmask.append(f['/' + line   + land_seg_path + 'segment_watermask/'][...,].tolist())
        lyr_flg.append(f['/' + line     + land_seg_path + 'layer_flag/'][...,].tolist())
    
    # MW 3/31/20: Originally a length of 6 was hardcoded into the below calculations because the
    #          assumption was made that 6 lines/lasers worth of data was stored in the arrays. With
    #	       the above changes made to the beginning of the 'for line in lines' loop on 3/31, this
    #          assumption is no longer always true. Adding nLines var to replace range(6) below
    nLines = len(latitude)

    # Be sure at least one of the lasers/lines for the h5 file had data points - MW added block 3/31
    if nLines == 0:
        return None # No usable points in h5 file, can't process
        
    latitude    =np.array([latitude[l][k] for l in range(nLines) for k in range(len(latitude[l]))] )
    longitude   =np.array([longitude[l][k] for l in range(nLines) for k in range(len(longitude[l]))] )

    gt          =np.array([gt[l][k] for l in range(nLines) for k in range(len(gt[l]))] )

    segid_beg   =np.array([segid_beg[l][k] for l in range(nLines) for k in range(len(segid_beg[l]))] )
    segid_end   =np.array([segid_end[l][k] for l in range(nLines) for k in range(len(segid_end[l]))] )

    
    h_max_can   =np.array([h_max_can[l][k] for l in range(nLines) for k in range(len(h_max_can[l]))] )
    h_can       =np.array([h_can[l][k] for l in range(nLines) for k in range(len(h_can[l]))] )
    h_can_unc   =np.array([h_can_unc[l][k] for l in range(nLines) for k in range(len(h_can_unc[l]))] ) # new as of 2022
    h_can_quad  =np.array([h_can_quad[l][k] for l in range(nLines) for k in range(len(h_can_quad[l]))] )
    
    #*do_20m: No more can_h_met_0 etc for 20m segments - 
      # will make separate RH % arrays later on 
      # erased can_met array stuff, changed if-else block to only if
    if do_20m:
        latitude_20m  = np.array([latitude_20m[l][k] for l in range(nLines) for k in range(len(latitude_20m[l]))])
        longitude_20m = np.array([longitude_20m[l][k] for l in range(nLines) for k in range(len(longitude_20m[l]))])
        h_can_20m     = np.array([h_can_20m[l][k] for l in range(nLines) for k in range(len(h_can_20m[l]))])
        h_te_best_20m = np.array([h_te_best_20m[l][k] for l in range(nLines) for k in range(len(h_te_best_20m[l]))])
        seg20_id      = np.array([seg20_id[l][k] for l in range(nLines) for k in range(len(seg20_id[l]))])        

    can_h_met = np.array([can_h_met[l][k] for l in range(nLines) for k in range(len(can_h_met[l]))])
        
    n_ca_ph     =np.array([n_ca_ph[l][k] for l in range(nLines) for k in range(len(n_ca_ph[l]))] )
    n_toc_ph    =np.array([n_toc_ph[l][k] for l in range(nLines) for k in range(len(n_toc_ph[l]))] )
    can_open    =np.array([can_open[l][k] for l in range(nLines) for k in range(len(can_open[l]))] )
    can_rh_conf =np.array([can_rh_conf[l][k] for l in range(nLines) for k in range(len(can_rh_conf[l]))] )
    seg_cover   =np.array([seg_cover[l][k] for l in range(nLines) for k in range(len(seg_cover[l]))] )
    
    #tcc_flg     =np.array([tcc_flg[l][k] for l in range(nLines) for k in range(len(tcc_flg[l]))] )
    #tcc_prc     =np.array([tcc_prc[l][k] for l in range(nLines) for k in range(len(tcc_prc[l]))] )

    cloud_flg   =np.array([cloud_flg[l][k] for l in range(nLines) for k in range(len(cloud_flg[l]))] )
    msw_flg     =np.array([msw_flg[l][k] for l in range(nLines) for k in range(len(msw_flg[l]))] )
    n_seg_ph    =np.array([n_seg_ph[l][k] for l in range(nLines) for k in range(len(n_seg_ph[l]))] )
    night_flg    =np.array([night_flg[l][k] for l in range(nLines) for k in range(len(night_flg[l]))] )
    seg_landcov =np.array([seg_landcov[l][k] for l in range(nLines) for k in range(len(seg_landcov[l]))] )
    seg_snow    =np.array([seg_snow[l][k] for l in range(nLines) for k in range(len(seg_snow[l]))] )
    seg_water   =np.array([seg_water[l][k] for l in range(nLines) for k in range(len(seg_water[l]))] )
    sig_vert    =np.array([sig_vert[l][k] for l in range(nLines) for k in range(len(sig_vert[l]))] )
    sig_acr     =np.array([sig_acr[l][k] for l in range(nLines) for k in range(len(sig_acr[l]))] )
    sig_along   =np.array([sig_along[l][k] for l in range(nLines) for k in range(len(sig_along[l]))] )
    sig_h       =np.array([sig_h[l][k] for l in range(nLines) for k in range(len(sig_h[l]))] )
    sig_topo    =np.array([sig_topo[l][k] for l in range(nLines) for k in range(len(sig_topo[l]))] )

    n_te_ph     =np.array([n_te_ph[l][k] for l in range(nLines) for k in range(len(n_te_ph[l]))] )
    h_te_best   =np.array([h_te_best[l][k] for l in range(nLines) for k in range(len(h_te_best[l]))] )
    h_te_unc    =np.array([h_te_unc[l][k] for l in range(nLines) for k in range(len(h_te_unc[l]))] )
    ter_slp     =np.array([ter_slp[l][k] for l in range(nLines) for k in range(len(ter_slp[l]))] )
    snr         =np.array([snr[l][k] for l in range(nLines) for k in range(len(snr[l]))] )
    sol_az      =np.array([sol_az[l][k] for l in range(nLines) for k in range(len(sol_az[l]))] )
    sol_el      =np.array([sol_el[l][k] for l in range(nLines) for k in range(len(sol_el[l]))] )

    asr 		=np.array([asr[l][k] for l in range(nLines) for k in range(len(asr[l]))] )
    h_dif_ref 	=np.array([h_dif_ref[l][k] for l in range(nLines) for k in range(len(h_dif_ref[l]))] )
    ter_flg 	=np.array([ter_flg[l][k] for l in range(nLines) for k in range(len(ter_flg[l]))] )
    ph_rem_flg	=np.array([ph_rem_flg[l][k] for l in range(nLines) for k in range(len(ph_rem_flg[l]))] )
    dem_rem_flg =np.array([dem_rem_flg[l][k] for l in range(nLines) for k in range(len(dem_rem_flg[l]))] )
    seg_wmask	=np.array([seg_wmask[l][k] for l in range(nLines) for k in range(len(seg_wmask[l]))] )
    lyr_flg		=np.array([lyr_flg[l][k] for l in range(nLines) for k in range(len(lyr_flg[l]))] )

    # Handle nodata
    val_invalid = np.finfo('float32').max
    val_nan = np.nan
    val_nodata_src = np.max(h_can)
    print("Find src nodata value using max of h_can: \t{}".format(val_nodata_src))
    
    if TEST:       
        # Testing with 'h_can'
        
        print("\n\tNan used for 30m version, not for 100m version.")
        print("\t\tCheck max of h_can...")
        print("\t\tnp.nanmax: \t{}".format(np.nanmax(h_can)) )
        print("\t\tnp.max: \t{}".format(np.max(h_can)) )
        
        print('[BEFORE] # of nan ATL08 obs of h_can: \t{}'.format(len( h_can[np.isnan(h_can) ] )))
        h_can = np.array([val_invalid if math.isnan(x) else x for x in h_can])
        print("Set h_can max to float32 max; np.max: \t {}".format(np.max(h_can)))
        print('[AFTER] # of nan ATL08 obs of h_can: \t{}'.format(len( h_can[np.isnan(h_can) ] )))
        
        print("\nData type: \t{}".format(h_can.dtype))
        print("# of nan ATL08 obs of  h_can: \t{}".format( np.count_nonzero(np.isnan(h_can)) ))
        print('# of invalid ATL08 obs of h_can: \t{}'.format(len( h_can[h_can == val_invalid ] )))
        
        print('# of ATL08 obs: \t\t{}'.format(len(latitude)))
        print('# of ATL08 obs (can pho.>0): \t{}'.format(len(n_ca_ph[n_ca_ph>0])))
        print('# of ATL08 obs (toc pho.>0): \t{}'.format(len(n_toc_ph[n_toc_ph>0])))
        print('# of ATL08 obs (h_can>0): \t{}'.format(len(h_can[h_can>0])))

    # Get approx path center Lat
    #CenterLat = latitude[len(latitude)/2]
    CenterLat = latitude[int(len(latitude)/2)]

    if False:
        # Calc args.resolution in degrees
        ellipse = [6378137.0, 6356752.314245]
        radlat = np.deg2rad(CenterLat)
        Rsq = (ellipse[0]*np.cos(radlat))**2+(ellipse[1]*np.sin(radlat))**2
        Mlat = (ellipse[0]*ellipse[1])**2/(Rsq**1.5)
        Nlon = ellipse[0]**2/np.sqrt(Rsq)
        pixelSpacingInDegreeX = float(args.resolution) / (np.pi/180*np.cos(radlat)*Nlon)
        pixelSpacingInDegreeY = float(args.resolution) / (np.pi/180*Mlat)
        print('Raster X (' + str(args.resolution) + ' m) Resolution at ' + str(CenterLat) + ' degrees N = ' + str(pixelSpacingInDegreeX))
        print('Raster Y (' + str(args.resolution) + ' m) Resolution at ' + str(CenterLat) + ' degrees N = ' + str(pixelSpacingInDegreeY))

    #*do_20m - Useless now with 100m/20m unique ID fields
    # Create a handy ID label for each point
    #fid = np.arange(1, len(h_max_can)+1, 1)


    if TEST:
        print("\nSet up a dataframe dictionary...")
    
    #*do_20m - Instead of having separate 100m/20m dicts, add to 
      # dictionaries if do_20m is True    
    dict_orb_gt_seg = {
                    #'fid_100m'  :fid,
                    'lon'       :longitude,
                    'lat'       :latitude,
                    #'yr'        :np.full(longitude.shape, yr[0]),
                    #'m'         :np.full(longitude.shape, m[0]),
                    #'d'         :np.full(longitude.shape, d[0]),
                    'dt'        :np.full(longitude.shape, dt[0]),
                    'orb_orient':np.full(longitude.shape, orb_orient[0]),
                    'orb_num'   :np.full(longitude.shape, orb_num[0]),
                    'rgt'       :np.full(longitude.shape, rgt[0]),
                    'gt'        :gt,

                    'segid_beg' :segid_beg,
                    'segid_end' :segid_end
    }
    
    dict_rh_metrics = {
                    'h_max_can' :h_max_can,
                    'h_can'     :h_can,
                    'h_can_quad':h_can_quad,
                    'h_can_unc' :h_can_unc,                    
                    
                    
                    # RHs: 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95%
                    'rh10'      :can_h_met[:,0],
                    'rh15'      :can_h_met[:,1],
                    'rh20'      :can_h_met[:,2],
                    'rh25'      :can_h_met[:,3],
                    'rh30'      :can_h_met[:,4],
                    'rh35'      :can_h_met[:,5],
                    'rh40'      :can_h_met[:,6],
                    'rh45'      :can_h_met[:,7],
                    'rh50'      :can_h_met[:,8],
                    'rh55'      :can_h_met[:,9],
                    'rh60'      :can_h_met[:,10],
                    'rh65'      :can_h_met[:,11],
                    'rh70'      :can_h_met[:,12],
                    'rh75'      :can_h_met[:,13],
                    'rh80'      :can_h_met[:,14],
                    'rh85'      :can_h_met[:,15],
                    'rh90'      :can_h_met[:,16],
                    'rh95'      :can_h_met[:,17]
    }
    
    dict_other_fields = {
                    'n_ca_ph'   :n_ca_ph,
                    'n_toc_ph'  :n_toc_ph,
                    'can_open'  :can_open,
                    'can_rh_conf':can_rh_conf,
                    'seg_cover'  :seg_cover,
                    #'tcc_flg'   :tcc_flg,
                    #'tcc_prc'   :tcc_prc,

                    'cloud_flg' :cloud_flg,
                    'msw_flg'   :msw_flg,
                    'n_seg_ph'  :n_seg_ph,
                    'night_flg' :night_flg,
                    'seg_landcov':seg_landcov,
                    'seg_snow'  :seg_snow,
                    'seg_water' :seg_water,
                    'sig_vert'  :sig_vert,
                    'sig_acr'   :sig_acr,
                    'sig_along' :sig_along,
                    'sig_h'     :sig_h,
                    'sig_topo'  :sig_topo,

                    'n_te_ph'   :n_te_ph,
                    'h_te_best' :h_te_best,
                    'h_te_unc'  :h_te_unc,
                    'ter_slp'   :ter_slp,
                    'snr'       :snr,
                    'sol_az'    :sol_az,
                    'sol_el'    :sol_el,

                    'asr'       :asr,
                    'h_dif_ref' :h_dif_ref,
                    'ter_flg'   :ter_flg,
                    'ph_rem_flg':ph_rem_flg,
                    'dem_rem_flg':dem_rem_flg,
                    'seg_wmask' :seg_wmask,
                    'lyr_flg'   :lyr_flg
    }
    
    #*do_20m - Now add 20m segment arrays to dict
    if do_20m:
        dict_orb_gt_seg['lon_20m'] = longitude_20m
        dict_orb_gt_seg['lat_20m'] = latitude_20m
        dict_orb_gt_seg['id_20m'] = seg20_id
        
        dict_rh_metrics['h_can_20m'] = h_can_20m
        
        dict_other_fields['h_te_best_20m'] = h_te_best_20m            
    
    # Erased large chunk of old logic for building dicts

  

    # First, get one big dictionary for all value types
    outDict = rec_merge1(dict_orb_gt_seg, rec_merge1(dict_rh_metrics, dict_other_fields))
  
    # This is redunant filtering, but for large datasets reconfiguring the arrays takes a while
    # There are two filters that are usually the cause of returning empty datasets
    # If there are no points that independently do not meet these criteria, there is no point in continuing
    # TBD whether or not this will speed things up - it will
    if 0 not in outDict['msw_flg'].tolist() or 1 not in outDict['seg_snow'].tolist():
        print("\nPre-filtering step determined there are no good points in dataset. Exiting")
        return None
    
    print("\nBuilding pandas dataframe...")


    #*do_20m new
    # Configure arrays if do_20m is True: 

    # If only extracting 100m segments, all arrays in outDict will be of shape
    # (X,) [aka ndims = 1], so no edits need to be made before DF creation
    
    # But if extracting 20m segments, 1D arrays need to be reformatted to
    # repeat the 100m info for each 20m segment. This will multiply the size of 
    # all 1D (aka 100m segment) arrays by 5 and therefore the output .csv, but 
    # this way we can still use existing filters and avoid losing the 100m 
    # info associated with each 20m segment
    # The redundant info can be cleaned up in zonal stats outputs and/or can
    # be taken care of if using an actual database, with a 100m segment 
    # db table that can be joined with 20m table using id_100m (combine with 
    # 20m seg id id_20m for unique 20m IDs)
    
    if do_20m:
        outDict = reFormatArrays(outDict)
    
    # Create DF from dictionary
    out = pd.DataFrame(outDict)
  
    print("Setting pandas df nodata values to np.nan for some basic eval.")
    out = out.replace(val_nodata_src, np.nan)
   
    print('# of ATL08 obs: \t\t{}'.format(len(out.lat[out.lat.notnull()])))
    print('# of ATL08 obs (can pho.>=0): \t{}'.format(len(out.n_ca_ph[
                                                      (out.h_can.notnull() ) & 
                                                      (out.n_ca_ph >= 0) 
                                                                        ])))
    print('# of ATL08 obs (toc pho.>=0): \t{}'.format(len(out.n_toc_ph[
                                                      (out.h_can.notnull() ) & 
                                                      (out.n_toc_ph >= 0) 
                                                                        ])))
    print('# of ATL08 obs (h_can>=0): \t{}'.format(len(out.h_can[
                                                      (out.h_can.notnull() ) & 
                                                      (out.h_can >= 0) 
                                                                       ])))
    print('# of ATL08 obs (h_can<0): \t{}'.format(len(out.h_can[
                                                      (out.h_can.notnull() ) & 
                                                                (out.h_can < 0)
                                                                        ])))
            
    # if set_nodata_nan is True (default False) set NoData to nan
    # if False, set it to the output NoData value (val_invalid)
    if args.set_nodata_nan:
        val_nodata_out = val_nan
    else:
        val_nodata_out = val_invalid
    print("Setting out pandas df nodata values: \t{}".format(val_nodata_out))
    out = out.replace(np.nan, val_nodata_out)
    
    #*v005 - seg_landcover changed to Copernicus
    # set_flag_names is False but if we want it to be True, need to update
    # seg_landcover (and others?) using the following:
    """
    [0, 111, 113, 112, 114, 115, 116, 121, 123, 122, 124, 125, 126,
     20, 30, 90, 100, 60, 40, 50, 70, 80, 200] corresponds with --> 
    ['NoData',
    'Closed_forest_evergreen_needle_leaf',
    'Closed_forest_deciduous_needle_leaf',
    'Closed_forest_evergreen_broad_leaf',
    'Closed_forest_deciduous_broad_leaf',
    'Closed_forest_mixed', 'Closed_forest_unknown',
    'Open_forest_evergreen_needle_leaf',
    'Open_forest_deciduous_needle_leaf',
    'Open_forest_evergreen_broad_leaf',
    'Open_forest_deciduous_broad_leaf',
    'Open_forest_mixed', 'Open_forest_unknown', 'Shrubs',
    'Herbaceous', 'Herbaceous_wetleand',
    'Moss_and_lichen', 'Bare_sparse_vegetation',
    'Cultivated_and_managed_vegetation_agriculture',
    'Urban_built_up', 'Snow_and_ice',
    'Permanent_water_bodies', 'Open_sea']
    """
    
    # Set flag names - meaning, convert the numerical code to the descriptor. Default False - the below are wrong anyways now
    if args.set_flag_names:
        #*v005 - Paul translated the above lists to replace the old landcover 
          # fields, copy here:
        class_values = [ 0, 111, 113, 112, 114, 115, 116, 121, 123, 122, 124, 
                        125, 126, 20, 30, 90, 100, 60, 40, 50, 70, 80, 200]
        class_names = ['No data','Closed forest\nevergreen needle',
                       'Closed forest\ndeciduous needle',
                       'Closed forest\nevergreen_broad',
                       'Closed forest\ndeciduous broad','Closed forest\nmixed', 
                       'Closed forest\nunknown','Open forest\nevergreen needle',
                'Open forest deciduous needle','Open forest evergreen_broad',
                'Open forest deciduous_broad','Open forest mixed', 
                'Open forest unknown', 'Shrubs','Herbaceous', 
                'Herbaceous\nwetleand','Moss/lichen', 'Bare/sparse','Cultivated/managed',
                'Urban/built', 'Snow/ice','Permanent\nwater', 'Open sea']
        out['seg_landcov'] = out['seg_landcov'].map(dict(zip(class_values, 
                                                                 class_names)))
        
        """
        # old method (pre v005)
        out['seg_landcov'] = out['seg_landcov'].map({0: "water", 1: "evergreen needleleaf forest", 2: "evergreen broadleaf forest", \
                                                     3: "deciduous needleleaf forest", 4: "deciduous broadleaf forest", \
                                                     5: "mixed forest", 6: "closed shrublands", 7: "open shrublands", \
                                                     8: "woody savannas", 9: "savannas", 10: "grasslands", 11: "permanent wetlands", \
                                                     12: "croplands", 13: "urban-built", 14: "croplands-natural mosaic", \
                                                     15: "permanent snow-ice", 16: "barren"})
        """
        out['seg_snow'] = out['seg_snow'].map({0: "ice free water", 1: "snow free land", 2: "snow", 3: "ice"})
        out['cloud_flg'] = out['cloud_flg'].map({0: "High conf. clear skies", 1: "Medium conf. clear skies", 2: "Low conf. clear skies", \
                                                 3: "Low conf. cloudy skies", 4: "Medium conf. cloudy skies", 5: "High conf. cloudy skies"})
        out['night_flg'] = out['night_flg'].map({0: "day", 1: "night"})
        #out['tcc_flg'] = out['tcc_flg'].map({0: "=<5%", 1: ">5%"}) #*v005
                                         
    #*v005: Bin tcc values - added include_lowest = True so tcc of 0% 
            #gets mapped to 10 bin rather than NaN   
    # nvm lol no longer needed                              
    #tcc_bins = [0,10,20,30,40,50,60,70,80,90,100]
    #out['tcc_bin'] = pd.cut(out['tcc_prc'], bins=tcc_bins, labels=tcc_bins[1:], include_lowest=True)
    
    # Add granule name to table 2/2/22
    out['granule_name'] = granule_fname

    if args.filter_qual:

        print('Quality Filtering: \t\t[ON]')

        import FilterUtils
	
        # These filters are customized for boreal
        out = FilterUtils.prep_filter_atl08_qual(out)
        out = FilterUtils.filter_atl08_qual_v2(out, SUBSET_COLS=False, DO_PREP=False,
        subset_cols_list=['rh25','rh50','rh60','rh70','rh75','rh80','rh90',
                          'h_can','h_max_can','h_can_quad','h_can_unc', 'h_te_best',
                          'h_te_unc', 'granule_name','can_rh_conf', 'h_dif_ref',
                          'seg_landcov','seg_cover','night_flg','seg_water',
                          'sol_el','asr','ter_slp', 'ter_flg','year','month','day'], 
        filt_cols=['h_can','h_dif_ref','month','msw_flg','beam_type','seg_snow','sig_topo'], 
                        thresh_h_can=100, thresh_h_dif=25, thresh_sig_topo=2.5, 
                            month_min=args.minmonth, month_max=args.maxmonth)
    else:
        print('Quality Filtering: \t[OFF] (do downstream)')

    if args.filter_geo:
        print('Geographic Filtering: \t[ON] xmin = {}, xmax = {}, ymin = {}, ymax = {}'.format(args.minlon, args.maxlon, args.minlat, args.maxlat))        
        # These filters are customized for boreal 
        out = out[ (out['lon']     >= args.minlon) & 
                   (out['lon']     <= args.maxlon) & 
                   (out['lat']     >= args.minlat) & 
                   (out['lat']     <= args.maxlat)]
    else:
        print('Geographic Filtering: \t[OFF] (do downstream)')

    # After filtering, output dataframe may be empty. If so, exit program
    if out.empty:
        print('\nFile is empty after filtering. Exiting')
        return None

    #*do_20m - GET UNIQUE ID FIELDS
    # If not doing 20m segments, unique_id = 100m segment ID
    # If doing 20m segments, unique_id = 100m segment ID + 20m segment ID
    # So 100m.csv will have just unique ID and 100m segment ID 
      # (which = unique ID)
    # 20m.csv will have unique ID, 100m seg ID, 20m seg ID
      # (id_20m already exists as column if do_20m)
  
    # Start by getting 100m segment ID
    out['id_100m'] = list(map(lambda ln, lt, dt: get100mSegId(ln, lt, dt), \
                                            out['lon'], out['lat'], out['dt']))
     
    # If doing 20m segments, then unique ID is combo of id_100m and id_20m
    if do_20m: 
        out['id_unique'] = list(map(lambda i1, i2: \
                        '{}-{}'.format(i1, i2), out['id_100m'], out['id_20m']))

    # Otherwise, unique ID is just 100m id (yes this will add a duplicate
    # column but it will enable a consisent unique ID column name. 
    # un-likely to process 100m with this code anyways so w/e)
    else:
        out['id_unique'] = out['id_100m']

    # Lastly, try and reorder the columns before writing to .csv. 
    # This part is partially hardcoded according to expected column names
    colsOrdered = getOrderedColumns(out)
    out = out[colsOrdered]

    # At this point we know out DF is not empty - Write out to csv
    print('\nWriting {} rows to CSV: {}'.format(len(out), out_csv_fn))
    out.to_csv(out_csv_fn,index=False, encoding="utf-8-sig")
        
    print("\nEnd: {}\n".format(time.strftime("%m-%d-%y %I:%M:%S %p")))        
    calculateElapsedTime(start, time.time())
    
    sys.stdout.close() # Close logging file

def main():
    #print("\nWritten by:\n\tNathan Thomas\t| @Nmt28\n\tPaul Montesano\t| paul.m.montesano@nasa.gov\n")
                                         
    class Range(object):
        def __init__(self, start, end):
            self.start = start
            self.end = end

        def __eq__(self, other):
            return self.start <= other <= self.end

        def __contains__(self, item):
            return self.__eq__(item)

        def __iter__(self):
            yield self

        def __str__(self):
            return '[{0},{1}]'.format(self.start, self.end)                                           

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, help="Specify the input ICESAT H5 file")
    parser.add_argument("-r", "--resolution", type=str, default='100', help="Specify the output raster resolution (m)")
    parser.add_argument("-o", "--output", type=str, help="Specify the output directory (optional)")
    parser.add_argument("-v", "--out_var", type=str, default='h_max_can', help="A selected variable to rasterize")
    parser.add_argument("-prj", "--out_epsg", type=str, default='102001', help="Out raster prj (default: Canada Albers Equal Area)")
    parser.add_argument("--max_h_can" , type=float, choices=[Range(0.0, 100.0)], default=30.0, help="Max value of h_can to include")
    parser.add_argument("--min_n_toc_ph" , type=int, default=1, help="Min number of top of canopy classified photons required for shot to be output")
    parser.add_argument("--minlon" , type=float, choices=[Range(-180.0, 180.0)], default=-180.0, help="Min longitude of ATL08 shots for output to include") 
    parser.add_argument("--maxlon" , type=float, choices=[Range(-180.0, 180.0)], default=180.0, help="Max longitude of ATL08 shots for output to include")
    parser.add_argument("--minlat" , type=float, choices=[Range(-90.0, 90.0)], default=30.0, help="Min latitude of ATL08 shots for output to include") 
    parser.add_argument("--maxlat" , type=float, choices=[Range(-90.0, 90.0)], default=90.0, help="Max latitude of ATL08 shots for output to include")
    parser.add_argument("--minmonth" , type=int, choices=[Range(1, 12)], default=1, help="Min month of ATL08 shots for output to include")
    parser.add_argument("--maxmonth" , type=int, choices=[Range(1, 12)], default=12, help="Max month of ATL08 shots for output to include")
    parser.add_argument('--no-overwrite', dest='overwrite', action='store_false', help='Turn overwrite off (To help complete big runs that were interrupted)')
    parser.set_defaults(overwrite=True)
    parser.add_argument('--no-filter-qual', dest='filter_qual', action='store_false', help='Turn off quality filtering (To control filtering downstream)')
    parser.set_defaults(filter_qual=True)
    parser.add_argument('--no-filter-geo', dest='filter_geo', action='store_false', help='Turn off geographic filtering (To control filtering downstream)')
    parser.set_defaults(filter_geo=True)
    parser.add_argument('--do_20m', dest='do_20m', action='store_true', help='Turn on 20m segment ATL08 extraction')
    parser.set_defaults(do_20m=False)
    parser.add_argument('--set_flag_names', dest='set_flag_names', action='store_true', help='Set the flag values to meaningful flag names')
    parser.set_defaults(set_flag_names=False)
    parser.add_argument('--set_nodata_nan', dest='set_nodata_nan', action='store_true', help='Set output nodata to nan')
    parser.set_defaults(set_nodata_nan=False)
    parser.add_argument('--TEST', dest='TEST', action='store_true', help='Turn on testing')
    parser.set_defaults(TEST=False)
    parser.add_argument('--log', dest='logging', action='store_true', help='Turn on output logging to file')
    parser.set_defaults(logging=False)    

    args = parser.parse_args()

    # Moved data validation bit to function

#    print(f'Month range: {args.minmonth}-{args.maxmonth}')

    extract_atl08(args)


if __name__ == "__main__":
    main()
