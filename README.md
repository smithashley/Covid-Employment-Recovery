# Covid-19 Employment Recovery

This project explores the economic recovery of different wage groups in Virginia with the use of Azure Blob Storage, Azure Databricks, Azure SQL Database, and Tableau.

![](https://github.com/smithashley/Covid-Employment-Recovery/blob/main/images/newazurediag.png)

The dataset was provided by Opportunity Insights and it lists daily employment changes for each state. https://tracktherecovery.org/ 

## Steps to load data to Azure Blob Storage

1. Log-in to Azure in the command line interface

az login

2. Create resource group

az group create --location "eastus2" --name [insert resource group here]

3. Create a storage account

az storage account create --name [insert account name here] --resource-group [insert resource group here] 

4. Create a container

az storage container create --account-name [insert account name here] --name [insert container name here]

5. Upload a blob

az storage blob upload --account-name [insert account name here] --container-name [insert container name here] --file '[insert file path here]' --name [insert file name here]

## Steps for ETL in Databricks
1. Connect blob to Azure Databricks
2. Define the schema
3. Read in .csv file
4. Transform data (clean up columns and data types) then filter down to Virginia
5. Write files to Azure SQL Database
![](https://github.com/smithashley/Covid-Employment-Recovery/blob/main/images/dboimage.png)

## Data Visualization

![](https://github.com/smithashley/Covid-Employment-Recovery/blob/main/images/newplot.png)
This chart shows the effect that Covid-19 has had on job growth for Virginians at different salary levels.
The market for high income workers has recovered much faster, while low income job growth appears to have stalled.  
[Link to interactive chart](https://public.tableau.com/views/Project1_16033597504640/Sheet2?:language=en&:display_count=y&publish=yes&:origin=viz_share_link)

Timeline provided for context
- First case January 20, 2020
- National emergency declared March 13, 2020
- VA schools close March 16, 2020
- Nonessential businesses close March 24, 2020
- VA stay at home order March 30, 2020
- CARES Act April 15, 2020
- Selective reopening by region May 15, 2020
- Selective business reopened statewide May 29, 2020
- VA stay at home order ends June 10, 2020

