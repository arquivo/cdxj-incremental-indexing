#!/bin/bash
# 
# Script that incrementally run cdxj indexing on a collection.
# For each warc run cdxj indexing if not exists
# Concatenate and sort every warc cdxj.
#
# Example of usage:
#     ./cdxj-incremental-indexing.sh -w /data/collections/RAQ2018 -x /data/cdxj_incremental/RAQ2018 -o /data/RAQ2018.cdxj
#
function usage() {
    {
        echo "Usage: $0 -w /path_for_collection_with_warcs -x /path_for_cdxj_incremental_path -o /path_to_cdxj_file [-c collection_name] [-d] [-P] [-p 2] ";
        echo "  -d                       debug mode, print more often"
        echo "  -P                       run in parallel by default is the number of cpus "
        echo "  -p PARALLEL_JOBS_COUNT   number of jobs that is run in parallel"
    } 1>&2; 
    exit 1; 
}

function readoptions() {
    while getopts ":w:x:o:c:p:dP" OPTION; do
        case "${OPTION}" in
            w) COLLECTION_WARC_PATH=${OPTARG} ;;
            x) CDXJ_INCREMENTAL_PATH=${OPTARG} ;;
            o) CDXJ_FINAL_PATH=${OPTARG} ;;
            c) COLLECTION_NAME=${OPTARG} ;;
            p) PARALLEL=true; PARALLEL_N=${OPTARG} ;;
            d) DEBUG=true ;;
            P) PARALLEL=true ;;
            :) echo -e "No argument value for option $OPTARG\n"; usage ;;
            *) usage ;;
        esac
    done
    shift $((OPTIND-1))
}

function check_options() {
    [ -d "${COLLECTION_WARC_PATH}" ] || echo "warc path '${COLLECTION_WARC_PATH}' not found"
    [ -d "${COLLECTION_WARC_PATH}" ] || usage
    [ -d "${CDXJ_INCREMENTAL_PATH}" ] || echo "cdxj incremental path '${CDXJ_INCREMENTAL_PATH}' not found"
    [ -d "${CDXJ_INCREMENTAL_PATH}" ] || usage
}

# print a message if in debug mode
function debug(){
    if [ $DEBUG ]; then echo -e $*; fi
}

# print a message if debug and run other arguments
function print_run() {
    echo $1;
    shift;
    run "$@";
}

# run command
# if debug print command that will be running
# if error running command exit immediatelly and print was have run
function run() {
    if [ $DEBUG ]; then echo "  --> $@"; fi
    $@
    RETVAL=$?
    if [[ $RETVAL != 0 ]]; then
        echo "Error running command: $@"
        exit $RETVAL
    fi
}

function wait_run_in_parallel()
{
    local number_to_run_concurrently=$1
    if [ `jobs -np | wc -l` -gt $number_to_run_concurrently ]; then
        wait `jobs -np | head -1` # wait for the oldest one to finish
    fi
}

function warc_cdxj_incremental_indexing() {
    collection_warc_path_length=${#COLLECTION_WARC_PATH}
    warc_path=$1
    debug "cdxj incremental indexing for warc: $warc_path"
    
    warc_path_length=${#warc_path}
    debug "warc_path_length ${warc_path_length}"
    
    warc_relative_path=${warc_path:$collection_warc_path_length:$warc_path_length}
    debug "warc_relative_path ${warc_relative_path}"

    warc_relative_path_no_file_extension=${warc_relative_path:0:${#warc_relative_path}-8}
    debug "warc_relative_path_no_file_extension ${warc_relative_path_no_file_extension}"

    warc_cdxj_file_path=${CDXJ_INCREMENTAL_PATH}${warc_relative_path_no_file_extension}.cdxj

    # skip if cdxj exist but reindex if warc is more recent than its previously indexed cdxj.
    if [ -f "$warc_cdxj_file_path" ] && [ "$warc_cdxj_file_path" -nt "$warc_path" ]; then
        echo "Skipping... cdxj already exists warc: ${warc_path} cdxj: ${warc_cdxj_file_path}"
    else 
        run mkdir -p "$(dirname "${warc_cdxj_file_path}")"
        
        echo "Indexing... ${warc_path} to ${warc_cdxj_file_path}"
        
        touch ${warc_cdxj_file_path}_tmp
        cdxj-indexer --post-append ${warc_path} -o ${warc_cdxj_file_path}_tmp

        RETVAL=$?
        if [[ $RETVAL != 0 ]]; then
            echo "Error indexing... ${warc_path} to ${warc_cdxj_file_path}"
            rm ${warc_cdxj_file_path}_tmp
        fi
        # prevent the cdxj to have a not finished indexed state. It could have it if it's killed in the middle of the indexing
        mv ${warc_cdxj_file_path}_tmp ${warc_cdxj_file_path}
    fi

    debug ""
}

readoptions "$@"

check_options

# define more variables
CDXJ_TEMP_PATH="${CDXJ_FINAL_PATH}_temp"
CDXJ_TEMP2_PATH="${CDXJ_FINAL_PATH}_temp2"
BLACKLIST_CDXJ_PATH=${COLLECTION_WARC_PATH}/blacklist_cdxj.txt
WARCS_FILE_PATH=${CDXJ_INCREMENTAL_PATH}/warcs.txt

if [ -z ${PARALLEL+x} ]; then 
    PARALLEL_N=0
else 
    if [ -z ${PARALLEL_N+x} ]; then 
        PARALLEL_N=`nproc`
    fi
fi

echo "Run a cdxj incremental indexing for each warc of ${COLLECTION_WARC_PATH}"

# write warcs to a temporary file
find "${COLLECTION_WARC_PATH}" -type f -regextype egrep -regex '.*\.(w|)arc\.gz$' > "${WARCS_FILE_PATH}"

# read each warc from temporary file and execute a function
while read line; do
    warc_cdxj_incremental_indexing "$line" &

    wait_run_in_parallel $PARALLEL_N
done <"${WARCS_FILE_PATH}"

# wait for pending jobs
wait

echo "Concatenate each warc cdxj file and sort it." 

# define this variable to sort correcly
export LC_ALL=C

find "${CDXJ_INCREMENTAL_PATH}" -type f -name "*.cdxj" -exec cat {} > "${CDXJ_TEMP2_PATH}" \;

echo "Concatenate all cdxj files and sort them" 

# use the cdxj folder to put the temporary file during the sort
cat "${CDXJ_TEMP2_PATH}" | sort -T $(dirname $CDXJ_FINAL_PATH)/ > "${CDXJ_TEMP_PATH}"

if [ ! -z ${COLLECTION_NAME+x} ]; then 
    echo "Add collection to each line" 
    # can not prefix it with run or print_run function
    sed -i "s/}$/, \"collection\": \"${COLLECTION_NAME}\"}/g"  "${CDXJ_TEMP_PATH}"
fi

print_run "Remove temp 2 file" rm "${CDXJ_TEMP2_PATH}"

if [ -f "$BLACKLIST_CDXJ_PATH" ]; then
    echo "Remove blacklist cdxj records" 
    run comm -23 "${CDXJ_TEMP_PATH}" "${BLACKLIST_CDXJ_PATH}" > "${CDXJ_FINAL_PATH}"
    
    debug "Remove cdxj temporary file" 
    run rm -f "${CDXJ_TEMP_PATH}"
else
    echo "No blacklist cdxj file found"
    debug "Move temporary cdxj file to final." 
    run mv "${CDXJ_TEMP_PATH}" ${CDXJ_FINAL_PATH}
fi

echo "Done!"
