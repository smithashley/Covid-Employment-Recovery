# Covid-19 Employment Recovery

This project explores the effect Covid-19 has had on job growth for Virginians at different income levels with the use of Azure Data Lake Storage, Azure Databricks, Azure Synapse Analytics, and Tableau.

![](https://github.com/smithashley/Covid-Employment-Recovery/blob/main/images/azurec_diag.png)

The dataset was provided by Opportunity Insights and it lists daily employment changes for each state. https://tracktherecovery.org/ 

## Steps to load data to Azure Data Lake Storage

1. Log-in to Azure in the command line interface

az login

2. Create resource group

az group create --location "eastus2" --name [insert resource group here]

3. Create the data lake account

az dls fs create --account [insert account name here] --resource-group [insert resource group here] 

4. Create folder

az dls fs create --account [insert account name here] --folder --path /[insert folder name here]

5. Upload data to data lake account

az dls fs upload --account-name [insert account name here] --source-path "[insert file path here]" --destination-path "[insert file name here]"

## Steps for ETL in Databricks
1. Define the schema
2. Read in .csv file from Azure Data Lake Storage
3. Transform data (clean up columns and data types) 
4. Write the 3 Dataframes that were created to Azure Synapse Analytics

## Query data and filter down to Virginia
![](https://github.com/smithashley/Covid-Employment-Recovery/blob/main/images/VAquery.png)

## Data Visualization
The job market for high income workers has recovered while low income job growth has taken a much bigger hit from the effects of the pandemic.  
- Low income is defined as less than $27,000.
- Middle income is defined as $27,000 to $60,000.
- High income is defined as more than $60,000.

![](https://github.com/smithashley/Covid-Employment-Recovery/blob/main/images/newplot.png)
[Link to interactive chart](https://public.tableau.com/views/Project1_16033597504640/Sheet22?:language=en&:display_count=y&:origin=viz_share_link)

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
