# breweries_bees


The objective of the test is to consume data from an API, persist it in a data lake architecture with three layers, with the first being raw data, the second being selected and partitioned by location, and the third containing aggregated analytical data.

Bronze layer: Raw data/uncurated data usually persists in its native format (but it's up to you)
Silver: Transformed into a columnar storage format, such as Parquet or Delta, partitioned by brewery location
Gold: Create an aggregated view with the number of stores by type and location


## Solution 1 - Azure Stack 
### Architecture

![breweries_arq](https://user-images.githubusercontent.com/82526635/236075843-477c0fb0-069c-4e64-a8f5-e0cb11b2b65a.PNG)

1 - In your Microsoft Azure account, create a resource group containing the following resources:

Azure Data Lake Storage: as datalake

Azure Data Factory: as the project orchestrator

Azure Databricks: as the development environment/platform for Python/PySpark code

Azure Key Vault: for secure storage of passwords/credentials, connection strings through "secrets"

https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/manage-resource-groups-portal


![resource_group](https://user-images.githubusercontent.com/82526635/236101832-68028dd3-7c83-43df-914d-91e8f96a3d47.PNG)


2 - Create a container in Azure Data Lake Storage with directories bronze, silver, and gold for storing data in their respective layers.
https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-quickstart-create-account

3 - Use Azure Key Vault to store the access token for the Azure Databricks instance and the passwords and connection string for Azure Data Lake Storage.
https://docs.microsoft.com/en-us/azure/key-vault/secrets/quick-create-portal

4 - Create an access policy in Key Vault for the Data Factory to access the Key Vault secrets.
https://docs.microsoft.com/en-us/azure/key-vault/general/secure-your-key-vault

5 - Start the Azure Databricks instance and create a scope to access the Azure Data Lake Storage files and generate the access token to be stored in Azure Key Vault.
https://docs.databricks.com/security/secrets/using-secrets/scopes.html#create-a-scope
https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-token

6 - Make a mount point to access the directories in your container on Azure Data Lake Storage.
https://docs.databricks.com/data/data-sources/azure/azure-datalake-gen2.html#mount-azure-data-lake-gen2

7 - Create 3 notebooks using Python and PySpark to perform the required transformations requested by the case, each notebook referring to a layer: bronze, silver, gold.
https://docs.databricks.com/notebooks/notebooks-use-case-data-transformation.html

8 - Access the Azure Data Factory instance, create a pipeline, in the activities panel, expand the databricks option, select notebook, and drag 3 to the plan. Configure linked services for Azure Key Vault and Azure Databricks, configure the notebook paths, and link the sequential execution of notebooks in case of successful execution.
https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-portal

9 - Validate the pipeline, save, and debug.
https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-portal
https://docs.microsoft.com/en-us/azure/data-factory/transform-data-using-databricks-notebook

![pipeline_adf](https://user-images.githubusercontent.com/82526635/236101950-c00d3195-fab5-4509-a895-b023533a9483.jpg)


10 - You can check that the data has been saved in each respective directory in Azure Data Lake Storage.

![dir](https://user-images.githubusercontent.com/82526635/236102854-5bc3a135-979e-4cb0-8fdf-b40b36c41067.PNG)


You can check the code by clicking [here](https://github.com/kh4r00n/breweries_bees/tree/main/bees_azure_databricks_notebooks).



## Solution 2 - Colab and Cloud Storage
### Architecture
![breweries_arq_3](https://user-images.githubusercontent.com/82526635/236099995-fed0d510-fce4-4bb8-b913-4829c022c291.PNG)


1 - Create a Bucket in Cloud Storage by following the steps described in Google's official documentation: https://cloud.google.com/storage/docs/creating-buckets. It's important to create a directory for each layer of the data pipeline (bronze, silver, and gold). To create directories, you can follow the official Google documentation: https://cloud.google.com/storage/docs/creating-folders.

2 - Create and configure a service account with permissions for Cloud Storage, following the instructions described in Google's official documentation: https://cloud.google.com/storage/docs/access-control/iam-roles. Be sure to grant the necessary permissions for the service account to access the bucket created in step 1.

3 - Create and run Python/PySpark notebooks in the Google Colab environment by following the instructions described in Google's official documentation: https://colab.research.google.com/notebooks/intro.ipynb. Remember that you need to install the PySpark library and configure the credentials of the service account created in step 2 to access Cloud Storage. For this, you can follow Google's official documentation: https://cloud.google.com/dataproc/docs/tutorials/jupyter-notebook.

You can check the code by clicking [here](https://github.com/kh4r00n/breweries_bees/tree/main/bees_google_colab).

