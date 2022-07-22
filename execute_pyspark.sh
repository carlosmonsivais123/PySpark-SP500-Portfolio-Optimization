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

# Uploading zipped PySpark files into DataProc along with the main.py file that will be run in the cluster.
Upload_Files=(
  PySpark_Files.zip
  main.py
)

zip -r "$PWD/PySpark_Files.zip" "$PWD/GCP_Functions" "$PWD/PySpark_Data" "$PWD/PySpark_EDA"

for file in "${Upload_Files[@]}"; do
    gsutil -m cp "$file" $PYTHON_FILES_BUCKET/
done

rm "$PWD/PySpark_Files.zip"

# Creating and DataProc cluster on Compute Engine in GCP
gcloud dataproc clusters create stock-cluster \
--enable-component-gateway \
--region us-central1 \
--zone us-central1-a \
--master-machine-type n1-standard-4 --master-boot-disk-size 500 \
--num-workers 2 \
--worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
--image-version 2.0-debian10 \
--optional-components JUPYTER --scopes 'https://www.googleapis.com/auth/cloud-platform' \
--project airy-digit-356101

# Executing PySpark Files that were uploaded above
gcloud dataproc jobs submit pyspark 'gs://stock-sp500/Spark_Files/main.py' \
--cluster='stock-cluster' \
--region='us-central1' \
--py-files='gs://stock-sp500/Spark_Files/Archive.zip'