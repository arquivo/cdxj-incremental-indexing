#!/bin/sh
set -e

CDXJ_INPUT_FILE="$1"
CDXJ_OUTPUT_FILE="$2"
BLACKLIST_PATTERNS_FILE="$3"
PARALLEL_N="$4"

workdir="$(dirname "$CDXJ_OUTPUT_FILE")/blacklist_tmp"
mkdir -p "$workdir"

split -d -n l/"${PARALLEL_N}"  "$CDXJ_INPUT_FILE" "$workdir/part.$(basename "$CDXJ_INPUT_FILE")"
ls "${workdir}/part.$(basename "$CDXJ_INPUT_FILE")"* | xargs -I file sh -c "grep -E -v -f ${BLACKLIST_PATTERNS_FILE} file > ${workdir}/filtered.file"
rm "${workdir}/part.$(basename "$CDXJ_INPUT_FILE")"*
cat "filtered.part.${CDXJ_INPUT_FILE}"* > "${CDXJ_OUTPUT_FILE}"
rm -rf "${workdir}/blacklist_tmp"