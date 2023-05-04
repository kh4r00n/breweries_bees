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



## Solution 2 - Google Cloud: Cloud Storage, Apache Beam and Dataflow
### Architecture
![breweries_arq_2](https://user-images.githubusercontent.com/82526635/236099887-47497eb7-f96e-4c5d-8967-22acbf8bb9ba.PNG)



## Solution 3 - Colab and Cloud Storage
### Architecture
![breweries_arq_3](https://user-images.githubusercontent.com/82526635/236099995-fed0d510-fce4-4bb8-b913-4829c022c291.PNG)


