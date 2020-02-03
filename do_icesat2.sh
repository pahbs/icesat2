#! /bin/bash
#
# This will run a ICESat-2 ATL08 gridding in parallel
# launch like this:
# pupsh "hostname ~ 'ecotone'" "do_icesat2.sh /path/to/file/list_mypairs"
#
# Get input list like this:
# find $PWD -name 'ATL08*.h5' > atl08_files
#


# The full path list name for a given VM that will run its list of files in parallel
listname=${1}
outdir=${2}

if [ -z $outdir ] ; then
    echo "Specify an output dir."
    exit 1
fi

hostN=`/bin/hostname -s`

if [ -e ${listname}__${hostN} ] ; then
    listname+="_${hostN}"
fi

list=$(cat ${listname})

# Get # of CPUs (aka sockets)
ncpu=$(lscpu | awk '/^Socket.s.:/ {sockets=$NF} END {print sockets}')

echo "Running script in parallel on $list_name ..."
if [ "$hostN" == *"ecotone"* ] ; then
    echo "Activate python....."
    source ~/anaconda/bin/activate sibbork
fi
parallel --progress -j $ncpu --delay 1 'extract_atl08.py -i {1} -o {2}' ::: $list ::: $outdir

