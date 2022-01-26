#!/bin/bash
#
# EXTRACT AND FILTER ATL08 v004 data on ADAPT
#
# pdsh -g ecotone,forest do_extract_filter_atl08.sh 2021 /att/nobackup/pmontesa/userfs02/data/icesat2/list_atl08.004_jjas boreal
# pdsh -g ilab,forest do_extract_filter_atl08.sh \"2018 2019 2020 2021\" /att/nobackup/pmontesa/userfs02/data/icesat2/list_atl08.005 senegal
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
        # These bounds are good for Senegal
        parallel --progress 'extract_filter_atl08.py -i {1} -o {2}/{3} --minlon -18 --maxlon -11 --minlat 12 --maxlat 17 --minmonth 1 --maxmonth 12' ::: ${FILE_LIST} ::: ${OUTDIR} ::: ${YEAR}
    fi
    if [[ "$GEO_DOMAIN" == "test" ]] ; then
        # testing...
        parallel --progress 'extract_filter_atl08.py -i {1} -o {2}/{3} --minlon -180 --maxlon 180 --minlat 55 --maxlat 60 --minmonth 1 --maxmonth 12' ::: ${FILE_LIST} ::: ${OUTDIR} ::: ${YEAR}
    fi


done

# Senegal
