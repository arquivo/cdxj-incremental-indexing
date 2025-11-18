# Efficient CDXJ file merger

The [merge_sorted_files.py](merge_sorted_files.py) script merges multiple sorted
files into a single sorted output file using
a min-heap based k-way merge algorithm. It's optimized for handling large files
with minimal memory usage by reading files line-by-line rather than loading them
entirely into memory.

The script can be used to merge multiple CDXJ files, instead of merging them
using `cat` and `sort`. This new method/script is more efficient because it takes
advanced that the existing CDXJ files are already sorted.

On one of high performant servers with SSD disks of Arquivo.pt it could merge
1.3TB from 12 CDXJ files during 2h20 minutes.

Usage example, that redirect stderr to a file.

```bash
time python merge_sorted_files.py FAWP48-59.cdxj /data/indexes_cdx/FAWP48.cdxj /data/indexes_cdx/FAWP49.cdxj /data/indexes_cdx/FAWP50.cdxj /data/indexes_cdx/FAWP51.cdxj /data/indexes_cdx/FAWP52.cdxj /data/indexes_cdx/FAWP53.cdxj /data/indexes_cdx/FAWP54.cdxj /data/indexes_cdx/FAWP55.cdxj /data/indexes_cdx/FAWP56.cdxj /data/indexes_cdx/FAWP57.cdxj /data/indexes_cdx/FAWP58.cdxj /data/indexes_cdx/FAWP59.cdxj 2> errors.log
```
