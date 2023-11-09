#!/bin/bash
PROGNAME=$0

usage() {
  cat << EOF >&2
Usage: $PROGNAME [-n <threshold>] [-f <cdx file>]

 -n <threshold>: How many instances of a SURT in a CDX file before it's considered problematic (default 1000)
 -f <cdx file>: cdx file to count surts. Equivalent to:  cat <cdx file> | $PROGNAME
EOF
  exit 1
}

THRESHOLD=1000
FILE=''

while getopts f:n: o; do
  case $o in
    (f) FILE=${OPTARG};;
    (n) THRESHOLD="${OPTARG}";;
    (*) usage
  esac
done
shift $((OPTIND-1))


if test -n "$FILE"; then
  cut -d' ' -f1 $FILE | uniq -c | awk -v N="$THRESHOLD" '{if($1 > N) print $2,$1}'
elif [ -p /dev/stdin ]; then
  cat | cut -d' ' -f1 | uniq -c | awk -v N="$THRESHOLD" '{if($1 > N) print $2,$1}'
else
  usage
fi