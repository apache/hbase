#!/usr/bin/env bash

usage() {
  echo "Usage: $0 <table_name> <column_family> [-n|--num-rows NUM_ROWS] [-i|--initial-row-value INITIAL_ROW_VALUE]"
  exit 1
}

if [ "$#" -lt 2 ]; then
  usage
fi

TABLE_NAME=$1
COLUMN_FAMILY=$2
shift 2

NUM_ROWS=""
INITIAL_ROW_VALUE=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    -n|--num-rows)
      NUM_ROWS="$2"
      shift 2
      ;;
    -i|--initial-row-value)
      INITIAL_ROW_VALUE="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

TSV_GENERATOR_ARGS=""
if [ -n "$NUM_ROWS" ]; then
  TSV_GENERATOR_ARGS="$TSV_GENERATOR_ARGS -n $NUM_ROWS"
fi
if [ -n "$INITIAL_ROW_VALUE" ]; then
  TSV_GENERATOR_ARGS="$TSV_GENERATOR_ARGS -i $INITIAL_ROW_VALUE"
fi

# Clean up any existing bulkload directories
rm -rf /tmp/bulkload

# Re-create the necessary directory structure
mkdir -p /tmp/bulkload/tsvdata

# Generate TSV data and save to the specified directory
python3 /opt/utils/tsv_generator.py /tmp/bulkload/tsvdata $TSV_GENERATOR_ARGS

# Import TSV data to create HFiles for bulk loading
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -Dimporttsv.columns=HBASE_ROW_KEY,$COLUMN_FAMILY:col0,$COLUMN_FAMILY:col1,$COLUMN_FAMILY:col2,$COLUMN_FAMILY:col3,$COLUMN_FAMILY:col4,$COLUMN_FAMILY:col5,$COLUMN_FAMILY:col6,$COLUMN_FAMILY:col7,$COLUMN_FAMILY:col8,$COLUMN_FAMILY:col9 \
  -Dimporttsv.bulk.output=/tmp/bulkload/HFiles \
  $TABLE_NAME /tmp/bulkload/tsvdata/output.tsv

# Bulk load the generated HFiles into the HBase table
hbase completebulkload /tmp/bulkload/HFiles $TABLE_NAME
