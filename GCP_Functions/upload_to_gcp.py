from google.cloud import storage

class Upload_To_GCP:
    def upload_string_message(bucket_name, contents, destination_blob_name):
        """Uploads a file to the bucket."""

        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"

        # The contents to upload to the file
        # contents = "these are my contents"

        # The ID of your GCS object
        # destination_blob_name = "storage-object-name"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(contents)

        print(f"{destination_blob_name} with contents {contents} uploaded to {bucket_name}.")


    def upload_filename(bucket_name, file_name, destination_blob_name):
        """Uploads a file to the bucket."""

        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"

        # The filename of the file you will upload, it can be eny type of file
        # filename = "filename.extension"

        # The ID of your GCS object
        # destination_blob_name = "storage-object-name"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(file_name)

        print(f"{destination_blob_name} with files {file_name} uploaded to {bucket_name}.")