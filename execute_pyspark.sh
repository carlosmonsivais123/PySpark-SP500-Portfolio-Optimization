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


# Logging into GCP
gcloud auth activate-service-account $SERVICE_ACCOUNT --key-file=$JSON_KEY_FILE --project=$PROJECT_ID

# Uploading Folders and file to GCP Bucket
Upload_Folders=(
  Package_Installation
  PySpark_Retrieve_Data
  GCP_Functions
  PySpark_Data
  PySpark_EDA
  PySpark_Clustering
  PySpark_ML_Models
)

for folder in "${Upload_Folders[@]}";do
    gsutil cp "$PWD/$folder/*" $PYTHON_FILES_BUCKET/$folder/
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
# --project airy-digit-356101 \
# --metadata='PIP_PACKAGES=tensorflow==2.8.2 tensorflow_probability==0.16.0' \
# --initialization-actions='gs://stock-sp500/Spark_Files/Package_Installation/pip_install.sh'\


# Executing PySpark Files that were uploaded above
gcloud dataproc jobs submit pyspark 'gs://stock-sp500/Spark_Files/main.py' \
--project=$PROJECT_ID \
--cluster='stock-cluster' \
--region='us-central1' \
--py-files=\
'gs://stock-sp500/Spark_Files/PySpark_Retrieve_Data/retrieve_data.py',\
'gs://stock-sp500/Spark_Files/GCP_Functions/upload_to_gcp.py',\
'gs://stock-sp500/Spark_Files/PySpark_Data/data_cleaning.py',\
'gs://stock-sp500/Spark_Files/PySpark_Data/data_schema.py',\
'gs://stock-sp500/Spark_Files/PySpark_Data/read_data_source.py',\
'gs://stock-sp500/Spark_Files/PySpark_EDA/stock_plots.py',\
'gs://stock-sp500/Spark_Files/PySpark_Clustering/k_means.py',\
'gs://stock-sp500/Spark_Files/PySpark_ML_Models/data_transforms.py',\
'gs://stock-sp500/Spark_Files/PySpark_ML_Models/linear_regression_models.py'\

# # Tableau Data Download
# gsutil cp "gs://stock-sp500/Clustering/k_means_clustered_data.csv" $PWD/Tableau_Data
# gsutil cp "gs://stock-sp500/Data/Correlation_Data/Daily_Returns_Symbol_Correlation_Data.csv" $PWD/Tableau_Data
# gsutil cp "gs://stock-sp500/Data/Correlation_Data/Daily_Returns_Sector_Correlation_Data.csv" $PWD/Tableau_Data
gsutil cp "gs://stock-sp500/Modeling/predictions.csv" $PWD/Tableau_Data/lr_model_preds.csv