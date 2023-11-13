#!/bin/sh
set -e

CDXJ_INPUT_FILE="$1"
CDXJ_OUTPUT_FILE="$2"
BLACKLIST_PATTERNS_FILE="$3"
BLACKLIST_WORKDIR="$4"

mkdir -p "$BLACKLIST_WORKDIR"
cd "$BLACKLIST_WORKDIR"

split "$CDXJ_INPUT_FILE" -d -n l/10  "$CDXJ_INPUT_FILE" "part.$(basename $CDXJ_INPUT_FILE)"
ls part.$(basename $CDXJ_INPUT_FILE)* | xargs grep -E -v -f "${BLACKLIST_PATTERNS_FILE}" "{}" > "filtered.{}"
cat filtered.{} > "${CDXJ_OUTPUT_FILE}"