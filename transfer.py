import os
from azure.storage.blob import BlobServiceClient

# Connection string for Source Storage Account
source_connection_string = "DefaultEndpointsProtocol=https;AccountName=atraccount01;AccountKey=L1U2RVDgia7y+9wcYjm4J/YHWokx5Y8Pvw9OAsB2JKZYTTrU7xrI7jONNnwL0yR3hdHEczy4HmT//V+AStR0GRYg==;EndpointSuffix=core.windows.net
"
# Connection string for Destination Storage Account
destination_connection_string = "DefaultEndpointsProtocol=https;AccountName=atraccount02;AccountKey=Klbe1Sj2H/jdhymwgDiBinz9UzNspu3lYTKFMSff391scIw/z9d8aU1DQpccDJQ1apqz7kuX34Fb+AStrwqJ1w==;EndpointSuffix=core.windows.net
"
# Source container name
source_container_name = "source-container"
# Destination container name
destination_container_name = "<dest-container"
# Number of blobs to copy
num_blobs_to_copy = 100

# Create a BlobServiceClient for Source Storage Account
source_blob_service_client = BlobServiceClient.from_connection_string(source_connection_string)
# Create a BlobServiceClient for Destination Storage Account
destination_blob_service_client = BlobServiceClient.from_connection_string(destination_connection_string)

# Get the source container client
source_container_client = source_blob_service_client.get_container_client(source_container_name)
# Get the destination container client
destination_container_client = destination_blob_service_client.get_container_client(destination_container_name)

# List blobs in the source container
blob_list = source_container_client.list_blobs()

# Iterate over the blobs and copy them to the destination container
for i, blob in enumerate(blob_list):
    if i >= num_blobs_to_copy:
        break

    # Get the blob name
    blob_name = blob.name
    print(f"Copying blob: {blob_name}")

    # Get the blob client for the source blob
    source_blob_client = source_blob_service_client.get_blob_client(container=source_container_name, blob=blob_name)
    
    # Create a new blob URL for the destination blob
    destination_blob_url = f"{destination_blob_service_client.primary_endpoint}/{destination_container_name}/{blob_name}"

    # Start the blob copy operation
    destination_blob_client = destination_blob_service_client.get_blob_client(container=destination_container_name, blob=blob_name)
    destination_blob_client.start_copy_from_url(source_blob_client.url)

    # Monitor the copy operation status
    copy_status = destination_blob_client.get_blob_properties().copy.status
    while copy_status == "pending":
        copy_status = destination_blob_client.get_blob_properties().copy.status

    if copy_status == "success":
        print(f"Blob copied successfully: {blob_name}")
    else:
        print(f"Failed to copy blob: {blob_name}")

print("Blob copy completed.")
