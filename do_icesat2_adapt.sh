#! /bin/bash
#
# This will run a ICESat-2 ATL08 processing in parallel using ADAPT ATL08 H5 files
# launch like this:
# pupsh "hostname ~ 'ecotone'" "do_icesat2_adapt.sh /path/to/file/list_mypairs"
# pdsh -g ecotone,crane do_icesat2_adapt.sh /path/to/file/list_mypairs
#
# Get input h5 list like this:
# find $PWD -name 'ATL08*003_01.h5' > atl08_h5_na_files
# Get input csv list like this:
# find $PWD -name 'ATL08*003_01.csv' > atl08_csv_files

# Find all h5 files, then chunk up on a node
# ATL08_DIR='/att/pubrepo/IceSAT-2/ATLAS/ATL08.003'
# find ${ATL08_DIR} -name 'ATL08*.h5' > $(basename ${outdir})/atl08v003_h5_files
# gen_chunks.py 


# Path to input v3 ATL08 csv files
# /att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ATL08/flight_csvs

# The full path list name for a given VM that will run its list of files in parallel
listname=${1:-'NULL'}
CONTINENT=${2:-'na'}
DO_MERGE=${3:-'false'}
outdir=${4:-'/att/nobackup/pmontesa/userfs02/data/icesat2/atl08.003'}  #/att/nobackup/pmontesa/userfs02/data/icesat2/atl08/v3/csv

VM_DO_MERGE='crane111'


if [[ "$CONTINENT" == "na" ]] || [[ "$CONTINENT" == "eu" ]] ; then 
    if [[ "$CONTINENT" == "na" ]] ; then
        MINLON=-180
        MAXLON=-13
    fi
    if [[ "$CONTINENT" == "eu" ]] ; then
        MINLON=-13
        MAXLON=180
    fi
    outdir=${outdir}/$CONTINENT
    echo ; echo "Continent: $CONTINENT" ; echo

else
    echo ; echo "Processing all ATL08...." ; echo

fi


mkdir -p $outdir

if [ -z $outdir ] ; then
    echo "Specify an output dir."
    exit 1
fi

# GET LISTS
hostN=`/bin/hostname -s`

if [ -e ${listname}_${hostN} ] ; then
    listname+="_${hostN}"
fi

echo "Running script in parallel on $listname ..."
#if [[ "$hostN" == *"ecotone"* ]] ; then
#    echo "Activate python....."
#    source ~/anaconda3/bin/activate sibbork
#fi

source ~/anaconda3/bin/activate sibbork_pyproj


if [[ "$DO_MERGE" != "true" ]] ; then

    list=$(cat ${listname})
    
    echo ; echo "Running individual files with GNU parallel..." ; echo

    # No overwrite; No filtering
    parallel --progress --delay 1 'extract_atl08.py --no-overwrite --no-filter-qual -i {1} -o {2} --minlon {3} --maxlon {4}' ::: ${list} ::: ${outdir} ::: $MINLON ::: $MAXLON

    parallel --progress --delay 1 'Rscript /home/pmontesa/code/icesat2/filter_atl08.R --atl08_csv_fn={1.}.csv --outdir={2} --filt_minlon={3} --filt_maxlon={4}' ::: $list ::: $outdir ::: $MINLON ::: $MAXLON

else

    # If your input list is of the csv files, then run this
    echo "Get R going..."
    source /att/opt/other/centos/modules/init/bash
    export MODULEPATH=/att/opt/other/centos/modules/modulefiles
    module load stretch/anaconda3
    module load jessie/R-3.6.1


    # Next, run the merge.
    # Merge all filtered CSVs for North America
    #Rscript /home/pmontesa/code/icesat2/merge_atl08.R --indir="/att/nobackup/pmontesa/userfs02/data/icesat2/atl08/v3/csv_na" --csv_search_str="003_01_filt_45_90_-180_-13"
    # Merge all filtered CSVs for Eurasia
    #Rscript /home/pmontesa/code/icesat2/merge_atl08.R --indir="/att/nobackup/pmontesa/userfs02/data/icesat2/atl08/v3/csv_eu" --csv_search_str="003_01_filt_45_90_-13_180"

    if [[ "$hostN" == "$VM_DO_MERGE" ]] ; then

        Rscript /home/pmontesa/code/icesat2/merge_atl08.R --indir=${outdir} --csv_search_str="003_01_filt_45_90_"${MINLON}"_"${MAXLON}

    else
        echo "merge_atl08.R not running...need to lauch from ${VM_DO_MERGE}"
    fi

fi
