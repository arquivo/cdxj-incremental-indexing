# CDXJ to ZipNum converted

Build local ZipNum-style index files and .loc entries from a single CDX/CDXJ
input (or stdin).

It can read a single uncompressed or gzip compressed CDXJ file.

It can generate a single shard or multiple shards.
By default each shard has a maximum of 100MB.
WARN that by default pywb only read up to 10 shards / blocks.
