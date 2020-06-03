#! /bin/bash
#
# This will run a ICESat-2 ATL08 processing in parallel
# launch like this:
# pupsh "hostname ~ 'ecotone'" "do_icesat2.sh /path/to/file/list_mypairs"
#
# Get input h5 list like this:
# find $PWD -name 'ATL08*003_01.h5' > atl08_h5_na_files
# Get input csv list like this:
# find $PWD -name 'ATL08*003_01.csv' > atl08_csv_files


# Path to input v3 ATL08 csv files
# /att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ATL08/flight_csvs

# The full path list name for a given VM that will run its list of files in parallel
listname=${1}
CONTINENT=${2:-'na'}
outdir=${3:-'/att/nobackup/pmontesa/userfs02/data/icesat2/atl08/v3/csv'}

if [[ "$CONTINENT" == "na" ]] ; then
    MINLON=-180
    MAXLON=-13
fi
if [[ "$CONTINENT" == "eu" ]] ; then
    MINLON=-13
    MAXLON=180
fi

echo ; echo "Continent: $CONTINENT" ; echo

if [ -z $outdir ] ; then
    echo "Specify an output dir."
    exit 1
fi

# GET LISTS
hostN=`/bin/hostname -s`

if [ -e ${listname}__${hostN} ] ; then
    listname+="_${hostN}"
fi

list=$(cat ${listname})

echo "Running script in parallel on $list_name ..."
if [[ "$hostN" == *"ecotone"* ]] ; then
    echo "Activate python....."
    source ~/anaconda/bin/activate sibbork
fi

# If your input list is of the h5 files, then run this
parallel --progress --delay 1 'extract_atl08.py -i {1} -o {2}' ::: $list ::: $outdir

# If your input list is of the csv files, then run this
echo "Get R going..."
source /att/opt/other/centos/modules/init/bash
export MODULEPATH=/att/opt/other/centos/modules/modulefiles
module load stretch/anaconda3
module load jessie/R-3.6.1

#parallel --progress --delay 1 'Rscript /home/pmontesa/code/icesat2/filter_atl08.R --atl08_csv_fn={1} --outdir={2} --filt_minlon={3} --filt_maxlon={4}' ::: $list ::: $outdir ::: $MINLON ::: $MAXLON

# Next, run the merge.
# Merge all filtered CSVs for North America
#Rscript /home/pmontesa/code/icesat2/merge_atl08.R --indir="/att/nobackup/pmontesa/userfs02/data/icesat2/atl08/v3/csv" --csv_search_str="003_01_filt_45_90_-180_-13"
