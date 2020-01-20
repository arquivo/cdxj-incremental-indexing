# cdxj-incremental-indexing
A wrap of the pywb cdxj-indexer command line tool that offers incremental and parallel indexing of a collection.


## Setup
Clone this repository, install virtualenv and activate it.
```bash
git clone git@github.com:arquivo/cdxj-incremental-indexing.git
cd cdxj-incremental-indexing
pip install --upgrade virtualenv
virtualenv -p python3 venv
source venv/bin/activate
pip install -r requirements.txt
```

## Parameters
<pre>
Usage: ./cdxj-incremental-indexing.sh -w /path_for_collection_with_warcs -x /path_for_cdxj_incremental_path -o /path_to_cdxj_file [-d] [-P] [-p 2] 
  -d                       debug mode, print more often
  -P                       run in parallel by default is the number of cpus 
  -p PARALLEL_JOBS_COUNT   number of jobs that is run in parallel
</pre>

## Run
```bash
/opt/cdxj-incremental-indexing/cdxj-incremental-indexing.sh -w /data/collections/PATCHING2020 -x /data/cdxj_incremental/PATCHING2020 -o /data/PATCHING2020.cdxj -P
```
