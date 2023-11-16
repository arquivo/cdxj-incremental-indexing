#!/bin/sh
set -e

CDXJ_INPUT_FILE="$1"
CDXJ_OUTPUT_FILE="$2"
BLACKLIST_PATTERNS_FILE="$3"
PARALLEL_N="$4"

if [ -z "${PARALLEL_N}" ]; then
    grep -a -E -v -f ${BLACKLIST_PATTERNS_FILE} "${CDXJ_INPUT_FILE}" > "${CDXJ_OUTPUT_FILE}"
else
    workdir="$(dirname "$CDXJ_OUTPUT_FILE")/blacklist_tmp"
    mkdir -p "$workdir"
    cd "$workdir"
    
    split -d -n l/"${PARALLEL_N}"  "$CDXJ_INPUT_FILE" "$workdir/part.$(basename "$CDXJ_INPUT_FILE")"
    ls "part.$(basename "$CDXJ_INPUT_FILE")"* | xargs -P ${PARALLEL_N} -I file sh -c "grep -a -E -v -f ${BLACKLIST_PATTERNS_FILE} file > filtered.file"
    rm "part.$(basename ${CDXJ_INPUT_FILE})"*
    cat "filtered.part.$(basename ${CDXJ_INPUT_FILE})"* > "${CDXJ_OUTPUT_FILE}"´

    cd $OLDPWD
    rm -rf $workdir
fi