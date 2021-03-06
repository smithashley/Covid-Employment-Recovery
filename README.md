# Covid-19 Employment Recovery

This project explores the effect Covid-19 has had on job growth for Virginians at different income levels with the use of Azure Data Lake Storage, Azure Databricks, Azure Synapse Analytics, and Tableau.

![](https://github.com/smithashley/Covid-Employment-Recovery/blob/main/images/azurec_diag.png)

The dataset was provided by Opportunity Insights and it lists daily employment changes for each state. https://tracktherecovery.org/ 

## Steps to load data to Azure Data Lake Storage

- Log-in to Azure in the command line interface

az login

- Create resource group

az group create --location "eastus2" --name [insert resource group here]

- Create the data lake account

az dls fs create --account [insert account name here] --resource-group [insert resource group here] 

- Create folder

az dls fs create --account [insert account name here] --folder --path /[insert folder name here]

- Upload data to data lake account

az dls fs upload --account-name [insert account name here] --source-path "[insert file path here]" --destination-path "[insert file name here]"

## Steps for ETL in Databricks
- Define the schema
- Read in .csv file from Azure Data Lake Storage using credential passthrough
- Transform data 
  - Dropped unneeded columns
  - Merged columns and fixed data types for datetime
  - Renamed columns
  - Performed mathematical and windowing funtions
- Configure connection to Azure Synapse Analytics
- Write the data to Azure Synapse Analytics

## Query data and filter down to Virginia
![](https://github.com/smithashley/Covid-Employment-Recovery/blob/main/images/VA_query.png)

## Data Visualization
The job market for high income workers has recovered while low income job growth has taken a much bigger hit from the effects of the pandemic.  
- Low income is defined as less than $27,000.
- Middle income is defined as $27,000 to $60,000.
- High income is defined as more than $60,000.

![](https://github.com/smithashley/Covid-Employment-Recovery/blob/main/images/tableau_screenshot.png)
[Link to Tableau dashboard](https://public.tableau.com/views/Project1_16033597504640/Sheet22?:language=en&:display_count=y&:origin=viz_share_link)

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
