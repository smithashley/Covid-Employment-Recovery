# Covid-19 Employment Recovery

This project explores the effect Covid-19 has had on job growth for Virginians at different income levels with the use of Azure Blob Storage, Azure Databricks, Azure SQL Database, and Tableau.

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
5. Write the 4 Dataframes that were created to Azure SQL Database
![](https://github.com/smithashley/Covid-Employment-Recovery/blob/main/images/dboimage.png)

## Data Visualization
The job market for high income workers has recovered while low income job growth has taken a much bigger hit from the effects of the pandemic.  
- Low income is defined as less than $27,000.
- Middle income is defined as $27,000 to $60,000.
- High income is defined as more than $60,000.

![](https://github.com/smithashley/Covid-Employment-Recovery/blob/main/images/newplot.png)
[Link to interactive chart](https://public.tableau.com/views/Project1_16033597504640/Sheet2?:language=en&:display_count=y&publish=yes&:origin=viz_share_link)

Timeline provided for context
- First case in United States confirmed on January 20, 2020
- First case in Virgina confirmed on March 7, 2020
- National emergency declared March 13, 2020
- Nonessential businesses and schools closed in Virginia on March 24, 2020
- CARES Act signed on March 27, 2020
- Virginia stay at home order March 30, 2020
- Selective reopening by region on May 15, 2020
- Selective business reopening statewide on May 29, 2020
- Virginia stay at home order ended on June 10, 2020

