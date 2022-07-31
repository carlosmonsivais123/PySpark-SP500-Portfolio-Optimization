#!/bin/bash

# Will uncomment when running process end to end
# sh ./script.sh

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


# Logging into GCP
gcloud auth activate-service-account $SERVICE_ACCOUNT --key-file=$JSON_KEY_FILE --project=$PROJECT_ID

# Uploading Folders and file to GCP Bucket
Upload_Folders=(
  GCP_Functions
  PySpark_Data
  PySpark_EDA
)

for folder in "${Upload_Folders[@]}";do
    gsutil -m cp "$PWD/$folder/*" $PYTHON_FILES_BUCKET/$folder/
done

gsutil -m cp "/Users/CarlosMonsivais/Desktop/PySpark-SP500-Portfolio-Optimization/main.py" $PYTHON_FILES_BUCKET/


# # Creating and DataProc cluster on Compute Engine in GCP
# gcloud dataproc clusters create stock-cluster \
# --enable-component-gateway \
# --region us-central1 \
# --zone us-central1-a \
# --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
# --num-workers 2 \
# --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
# --image-version 2.0-debian10 \
# --optional-components JUPYTER --scopes 'https://www.googleapis.com/auth/cloud-platform' \
# --project airy-digit-356101


# Using this current cluster
# # Creating and DataProc cluster on Compute Engine in GCP
# gcloud dataproc clusters create stock-cluster \
# --enable-component-gateway \
# --region us-central1 \
# --zone us-central1-a \
# --master-machine-type n1-standard-8 --master-boot-disk-size 500 \
# --num-workers 2 \
# --worker-machine-type n1-standard-8 --worker-boot-disk-size 500 \
# --image-version 2.0-debian10 \
# --optional-components JUPYTER --scopes 'https://www.googleapis.com/auth/cloud-platform' \
# --project airy-digit-356101


# Executing PySpark Files that were uploaded above
gcloud dataproc jobs submit pyspark 'gs://stock-sp500/Spark_Files/main.py' \
--project=$PROJECT_ID \
--cluster='stock-cluster' \
--region='us-central1' \
--py-files='gs://stock-sp500/Spark_Files/GCP_Functions/upload_to_gcp.py',\
'gs://stock-sp500/Spark_Files/PySpark_Data/data_cleaning.py',\
'gs://stock-sp500/Spark_Files/PySpark_Data/data_schema.py',\
'gs://stock-sp500/Spark_Files/PySpark_EDA/stock_plots.py'