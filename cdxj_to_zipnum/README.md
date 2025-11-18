# CDXJ to ZipNum converted

Build local ZipNum-style index files and .loc entries from a single CDX/CDXJ
input (or stdin).

It can read a single uncompressed or gzip compressed CDXJ file.

It can generate a single shard or multiple shards.
By default each shard has a maximum of 100MB.
WARN that by default pywb only read up to 10 shards / blocks.


## 

Example to merge CDXJ files and convert the merge to ZipNum format.

```bash
python merge/merge_sorted_files.py - /data/indexes_cdx/Roteiro.cdxj /data/indexes_cdx/RAQ2017.cdxj 2> errors_merge.log | python cdxj_to_zipnum/cdxj_to_zipnum.py -o outdir2 -i - --base Roteiro-RAQ2017
```
