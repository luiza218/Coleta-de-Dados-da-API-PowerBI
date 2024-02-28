# Power BI Data Collector for Python 📊🐍

This Python script simplifies the process of retrieving data from Microsoft Power BI using the Power BI REST API. 
Seamlessly integrate this tool into your workflow to automate data extraction, enabling efficient and timely analysis of your Power BI datasets.

## Key Features
- **Easy Setup**: Quickly authenticate and connect to the Power BI API with straightforward configuration.
- **Data Extraction**: Retrieve datasets, reports, and other relevant information effortlessly.
- **Scheduled Automation**: Set up automated data collection tasks using your preferred scheduling tool.
- **Customizable**: Adapt the script to fit your specific data collection requirements and parameters.
- **Enhance Analysis**: Empower your data-driven decisions by effortlessly integrating Power BI data into your Python-based analytical workflows.

## Getting Started
1. Clone this repository.
2. Follow the documentation and usage instructions. You might want to look into the [API DOCUMENTATION](/https://learn.microsoft.com/en-us/rest/api/power-bi/).

## Details
This script has a **raw-zone**, **staging-zone** and **consumer zone**.

-**RAW ZONE**: Retrieves the JSONs for the GROUPS, APPS and REFRESHABLES and create its DataFrames. These DFs will be used later for retrieving the info for ALL REPORTS PER GROUP, 
ALL REPORTS PER APP and ALL DATASETS PER GROUP. In this stage, we will only upload the JSON for each request in our s3 bucket.

-**STAGING-ZONE**: Creates the DataFrames based on each JSON and adapts each one of them to fit better into our data visualization. As I said before, 
we will select the columns that fit best into our scope and transform the DataFrames into Parquet files. The files uploaded into our s3 bucket are in a Parquet format.

-**CONSUMER-ZONE**: In this stage we will retrieve our files from the STAGING-ZONE, copy them into our CONSUMER-ZONE and register a manifest link for each one of them.

## Contribution
Feel free to contribute and enhance the functionality of this tool! 🌐💻
