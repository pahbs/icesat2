#!/bin/bash
#
# EXTRACT AND FILTER ATL08 v004 data on ADAPT
# ---QUALITY FILTERING APPLIED BY DEFAULT FROM FILTERUTILS.py
# ------ need to re-run this script if you change the default filtering in FilterUtils.py
# ------ to turn off filter, add '--no-filter-qual' to extract_filter_atl08.py call
#
# pdsh -g ecotone,forest do_extract_filter_atl08.sh 2021 /att/nobackup/pmontesa/userfs02/data/icesat2/list_atl08.004_jjas boreal
# pdsh -g ilab,forest do_extract_filter_atl08.sh \"2018 2019 2020 2021\" /att/nobackup/pmontesa/userfs02/data/icesat2/list_atl08.005 senegal
# do_extract_filter_atl08.sh "2018 2019 2020 2021" /att/nobackup/pmontesa/userfs02/data/icesat2/list_atl08.005 hi_lat_na_latest
#
# First, create the ATL08 v4 data list like this:
# parallel 'ls /att/pubrepo/IceSAT-2/ATLAS/ATL08.004/{}.0[6-9].*/*h5 >> /att/nobackup/pmontesa/userfs02/data/icesat2/list_atl08.004_jjas_{}' ::: 2018 2019 2020 2021
# parallel 'ls /att/pubrepo/IceSAT-2/ATLAS/ATL08.005/{}*/*h5 >> /att/nobackup/pmontesa/userfs02/data/icesat2/list_atl08.005_{}' ::: 2018 2019 2020 2021

# Second, chunk up by nodes
# source activate py2
# gen_chunks.py list_atl08.004_jjas_2021 nodes_all

# CONDA env needs h5py and geopandas

module load gnu_parallel/20210422
module load anaconda/3-2021.11
conda activate earthml

# This works but wont when something about that path changes
#module load anaconda
#source /att/gpfsfs/home/appmgr/app/anaconda/platform/x86_64/centos/7/3-2020.07/etc/profile.d/conda.sh
#conda activate geopy

YEARS_LIST=${1:-"2018 2019 2020 2021"}
FILE_LIST_STEM=${2:-'/att/nobackup/pmontesa/userfs02/data/icesat2/list_atl08.004_jjas'}

GEO_DOMAIN=${3:-'boreal'}
OUTDIR=${4:-'/att/nobackup/pmontesa/userfs02/data/icesat2/atl08.005'}

hostN=`/bin/hostname -s`

# Make GEO_DOMAIN and YEAR subdirs
OUTDIR=${OUTDIR}/${GEO_DOMAIN}
mkdir -p ${OUTDIR}

echo; echo $YEARS_LIST ; echo

for YEAR in ${YEARS_LIST} ; do
    echo "YEAR: ${YEAR}"
    mkdir -p ${OUTDIR}/${YEAR}

    FILE_LIST=${FILE_LIST_STEM}_${YEAR}_${hostN}
    FILE_LIST=$(cat ${FILE_LIST})

    if [[ "$GEO_DOMAIN" == "boreal" ]] ; then
        # These bounds are good for boreal
        parallel --progress 'extract_filter_atl08.py -i {1} -o {2}/{3} --minlon -180 --maxlon 180 --minlat 45 --maxlat 75 --minmonth 6 --maxmonth 9' ::: ${FILE_LIST} ::: ${OUTDIR} ::: ${YEAR}
    fi
    if [[ "$GEO_DOMAIN" == "senegal" ]] ; then
        # This LC h_can thresh list is good for Senegal: it gives Shrubland and Cropland a threshold of 15; otherwise same as boreal.
        parallel --progress 'extract_filter_atl08.py --list_lc_h_can_thresh 0 60 60 60 60 60 60 50 50 50 50 50 50 15 10 10 5 5 15 0 0 0 0 --i {1} -o {2}/{3} --minlon -18 --maxlon -11 --minlat 12 --maxlat 17 --minmonth 1 --maxmonth 12' ::: ${FILE_LIST} ::: ${OUTDIR} ::: ${YEAR}
    fi
    if [[ "$GEO_DOMAIN" == "howland" ]] ; then
        # ---NOT YET RUN OR TESTED
        # TODO: 
        # work in args for lc thresh and output to pandas into extract_filter_atl08_v005.py
        # update FilterUtils to match icesat2_boreal repo version
        # update to use the filter qual v3 from FilterUtils
        # This LC h_can thresh list is same as boreal.
        parallel --progress 'extract_filter_atl08_v005.py --list_lc_h_can_thresh 0 60 60 60 60 60 60 50 50 50 50 50 50 15 10 10 5 5 5 0 0 0 0 --i {1} -o {2}/{3} --minlon -18 --maxlon -11 --minlat 12 --maxlat 17 --minmonth 1 --maxmonth 12' ::: ${FILE_LIST} ::: ${OUTDIR} ::: ${YEAR}
    fi
    if [[ "$GEO_DOMAIN" == "senegal_no_filt" ]] ; then
        # This LC h_can thresh list is good for Senegal: it gives Shrubland and Cropland a threshold of 15; otherwise same as boreal.
        parallel --progress 'extract_filter_atl08.py --no-filter-qual --i {1} -o {2}/{3} --minlon -18 --maxlon -11 --minlat 12 --maxlat 17 --minmonth 1 --maxmonth 12' ::: ${FILE_LIST} ::: ${OUTDIR} ::: ${YEAR}
    fi
    if [[ "$GEO_DOMAIN" == "test" ]] ; then
        # testing...
        parallel --progress 'extract_filter_atl08.py -i {1} -o {2}/{3} --minlon -180 --maxlon 180 --minlat 55 --maxlat 60 --minmonth 1 --maxmonth 12' ::: ${FILE_LIST} ::: ${OUTDIR} ::: ${YEAR}
    fi
    if [[ "$GEO_DOMAIN" == "hi_lat_na" ]] ; then
        # testing...
        parallel --progress 'extract_filter_atl08.py -i {1} -o {2}/{3} --minlon -166 --maxlon -140 --minlat 65 --maxlat 70 --minmonth 1 --maxmonth 12' ::: ${FILE_LIST} ::: ${OUTDIR} ::: ${YEAR}
    fi
    if [[ "$GEO_DOMAIN" == "hi_lat_na_no_filt" ]] ; then
        # testing...
        parallel --progress 'extract_filter_atl08.py --no-filter-qual -i {1} -o {2}/{3} --minlon -166 --maxlon -140 --minlat 65 --maxlat 70 --minmonth 1 --maxmonth 12' ::: ${FILE_LIST} ::: ${OUTDIR} ::: ${YEAR}
    fi
    if [[ "$GEO_DOMAIN" == "hi_lat_norway_no_filt" ]] ; then
        # testing...
        parallel --progress 'extract_filter_atl08.py --no-filter-qual -i {1} -o {2}/{3} --minlon 4 --maxlon 32 --minlat 57  --maxlat 70 --minmonth 1 --maxmonth 12' ::: ${FILE_LIST} ::: ${OUTDIR} ::: ${YEAR}
    fi
    if [[ "$GEO_DOMAIN" == "hi_lat_sib" ]] ; then
        # testing...
        parallel --progress 'extract_filter_atl08.py -i {1} -o {2}/{3} --minlon 95 --maxlon 110 --minlat 65  --maxlat 75 --minmonth 1 --maxmonth 12' ::: ${FILE_LIST} ::: ${OUTDIR} ::: ${YEAR}
    fi

done
