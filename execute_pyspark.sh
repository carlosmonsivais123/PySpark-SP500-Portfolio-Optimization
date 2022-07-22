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

local_folder_list=(
  GCP_Functions
  PySpark_Data
  PySpark_EDA
)

local_file_list=(
  main.py
)


for folder in "${local_folder_list[@]}"; do
    zip -r "$PWD/$folder.zip" "$PWD/$folder"
done

for folder in "${local_folder_list[@]}"; do
    gsutil -m cp -r "$folder.zip" $PYTHON_FILES_BUCKET/
done

for folder in "${local_folder_list[@]}"; do
    rm "$PWD/$folder.zip"
done

for file in "${local_file_list[@]}"; do
    gsutil -m cp $file $PYTHON_FILES_BUCKET/
done

# Creating and DataProc cluster on Compute Engine in GCP to execute PySpark Files that were uploaded above
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


# gsutil cp "/Users/CarlosMonsivais/Desktop/PySpark-SP500-Portfolio-Optimization/GCP_Functions.zip" $PYTHON_FILES_BUCKET/
# gsutil cp "/Users/CarlosMonsivais/Desktop/PySpark-SP500-Portfolio-Optimization/PySpark_Data.zip" $PYTHON_FILES_BUCKET/
# gsutil cp "/Users/CarlosMonsivais/Desktop/PySpark-SP500-Portfolio-Optimization/PySpark_EDA.zip" $PYTHON_FILES_BUCKET/


# gcloud dataproc jobs submit pyspark 'gs://stock-sp500/Spark_Files/main.py' \
# --cluster='stock-cluster' \
# --region='us-central1' \
# --py-files='gs://stock-sp500/Spark_Files/GCP_Functions.zip','gs://stock-sp500/Spark_Files/PySpark_Data.zip','gs://stock-sp500/Spark_Files/PySpark_EDA.zip'




# For loop zip the files
# upload them to gcp
# run the job on dataproc
# fix the error.