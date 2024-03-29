from google.cloud import storage

class Upload_To_GCP:
    def upload_string_message(self, bucket_name, contents, destination_blob_name):
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


    def upload_filename(self, bucket_name, file_name, destination_blob_name):
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


    def list_blobs(self, bucket_name):
        """Lists all the blobs in the bucket."""
        # bucket_name = "your-bucket-name"
        files_in_bucket=[]

        storage_client = storage.Client()

        # Note: Client.list_blobs requires at least package version 1.17.0.
        blobs = storage_client.list_blobs(bucket_name)

        for blob in blobs:
            files_in_bucket.append(blob.name)

        return files_in_bucket