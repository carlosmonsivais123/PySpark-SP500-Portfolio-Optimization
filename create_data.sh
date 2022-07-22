#!/bin/bash
function parse_yaml {
   local prefix=$2
   local s='[[:space:]]*' w='[a-zA-Z0-9_]*' fs=$(echo @|tr @ '\034')
   sed -ne "s|^\($s\):|\1|" \
        -e "s|^\($s\)\($w\)$s:$s[\"']\(.*\)[\"']$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p"  $1 |
   awk -F$fs '{
      indent = length($1)/2;
      vname[indent] = $2;
      for (i in vname) {if (i > indent) {delete vname[i]}}
      if (length($3) > 0) {
         vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
         printf("%s%s%s=\"%s\"\n", "'$prefix'",vn, $2, $3);
      }
   }'
}
eval $(parse_yaml "$PWD/config.yaml")

# Extracting Data
python retreive_main.py

# Logging into GCP
gcloud auth activate-service-account $SERVICE_ACCOUNT --key-file=$JSON_KEY_FILE --project=$PROJECT_ID

# Uploading Data to GCP
echo "Uploading S&P 500 Full Stock Data $DATA_BUCKET/$FULL_DATASET_NAME"
gsutil cp "$PWD/$FULL_DATASET_NAME" "$DATA_BUCKET/$FULL_DATASET_NAME"
rm "$PWD/$FULL_DATASET_NAME" "$PWD/S&P_500_Ticks.csv"
echo "Data Upload Completed to $DATA_BUCKET/$FULL_DATASET_NAME"

. ./execute_pyspark.sh