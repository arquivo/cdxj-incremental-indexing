#!/bin/bash
PROGNAME=$0

usage() {
  cat << EOF >&2
Usage: $PROGNAME <problematic urls file> <cdx file>

 <problematic_urls_file>: A file with the problematic urls and their frequency, as output by find_problematic_urls.sh.
 <cdx file>: cdx file to filter out the problematic surts.
EOF
  exit 1
}

if  [ $# -ne 2 ]; then
  usage;
fi


START_BYTE=1
while read -r line
do
#  >&2 echo "Reading file: $1"
  #Sanitize input
  #\.+*?^$()[]{}|
  SURT=$(echo "$line" | cut -d' ' -f1)
#  >&2 echo "SURT: $SURT"
  I="$SURT"
  I=$(echo "$I" | sed 's/\\/\\\\/g')
  I=$(echo "$I" | sed 's/\./\\\./g')
  I=$(echo "$I" | sed 's/\+/\\\+/g')
  I=$(echo "$I" | sed 's/\*/\\\*/g')
  I=$(echo "$I" | sed 's/\?/\\\?/g')
  I=$(echo "$I" | sed 's/\^/\\\^/g')
  I=$(echo "$I" | sed 's/\$/\\\$/g')
  I=$(echo "$I" | sed 's/(/\\(/g')
  I=$(echo "$I" | sed 's/)/\\)/g')
  I=$(echo "$I" | sed 's/\[/\\\[/g')
  I=$(echo "$I" | sed 's/\]/\\\]/g')
  I=$(echo "$I" | sed 's/{/\\{/g')
  I=$(echo "$I" | sed 's/}/\\}/g')
  I=$(echo "$I" | sed 's/|/\\|/g')

#  >&2 echo "SANITIZED: '$I'"

  #Regex: ^SANITIZED_SURT\s
  REGEX="^$I\\s"
  MATCH=$(tail -c +"$START_BYTE" $2 | grep -Eba "$REGEX" | head -n1)
#  >&2 echo "Surt: $SURT"
#  >&2 echo "Grep: tail -c +$START_BYTE $2 | grep -Enb $REGEX | head -n1"
#  >&2 echo "Match: $MATCH"
  TEXT_MATCH=$(echo "$MATCH" | cut -d: -f2-)
  BYTE_MATCH=$(echo "$MATCH" | cut -d: -f1)

#  >&2 echo "Command: tail -c +$START_BYTE $2 | head -c $BYTE_MATCH"

  tail -c +"$START_BYTE" $2 | head -c "$BYTE_MATCH"

  echo "$TEXT_MATCH"

  OFFSET=$(echo "$line" | cut -d' ' -f2)
  END_BYTE=$(echo "$START_BYTE + $BYTE_MATCH" | bc)
  BYTE_OFFSET=$(tail -c +"$END_BYTE" $2 | head -n "$OFFSET" | wc -c)
  START_BYTE=$(echo "$END_BYTE + $BYTE_OFFSET" | bc)

#  >&2 echo "$LINE_MATCH - $SURT - $OFFSET"

done < $1

tail -c +"$START_BYTE" $2