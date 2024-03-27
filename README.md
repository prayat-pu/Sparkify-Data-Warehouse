# Sparkify Data Warehouse

This project, designed by Udacity, focuses on practicing data warehousing with AWS. It simulates a real-world scenario encountered by data engineers in the industry.

## Overview

A music streaming startup, Sparkify, has seen significant growth in their user base and song database. They aim to transfer their processes and data onto the cloud. Currently, their data is stored in S3, within a directory of JSON logs detailing user activity on the app, alongside a directory containing JSON metadata about the songs in their app.

As a data engineer, you are tasked with constructing an ETL pipeline. This pipeline will extract data from S3, stage it in Redshift, and transform it into a series of dimensional tables. This facilitates the analytics team in deriving insights about the songs their users are listening to.

![App Screenshot](https://video.udacity-data.com/topher/2022/May/62770f73_sparkify-s3-to-redshift-etl/sparkify-s3-to-redshift-etl.png)

## Installation

This project requires Python for installation.

```bash
pip install matplotlib pandas psycopg2 configparser
```

## Workflow

To successfully complete this project, the following steps are necessary:

- Create an IAM role and Security Group for Redshift.
- Create a Redshift cluster. This can be done using either code or the Amazon Web Service interface.
- Design the schema for the database and data warehouse.
- Load data into Redshift and the data warehouse.
- Analyze the data using Python.

## Data Warehouse and Database Schema

### Database Schema

![Database Schema](https://github.com/prayat-pu/Sparkify-Data-Warehouse/blob/main/img/Sparkify%20Data%20Warehouse-Database.png?raw=true)

### Data Warehouse Schema

![Data Warehouse Schema](https://github.com/prayat-pu/Sparkify-Data-Warehouse/blob/main/img/Sparkify%20Data%20Warehouse.png?raw=true)

## Running Locally

To run this project locally, follow these steps:

Clone the project:

```bash
git clone https://github.com/prayat-pu/Sparkify-Data-Warehouse.git
```

Navigate to the project directory:

```bash
cd Sparkify-Data-Warehouse
```

Install dependencies:

```bash
pip install matplotlib pandas psycopg2 configparser
```

Ensure Redshift is available and add the required information in `dwh.cfg`.

Execute the scripts:

```bash
python create_tables.py  # To create tables in Redshift
python etl.py            # To load data into Redshift
```

## Usage/Examples

Start by opening a Jupyter notebook:

```bash
jupyter notebook
```

Data can be fetched from Redshift for analysis using Python. This project includes an example notebook, `insight_from_DWH.ipynb`, showcasing analyses such as:

- Top 5 Songs of this dataset
![Top 5 Songs](https://github.com/prayat-pu/Sparkify-Data-Warehouse/blob/main/img/top5songs.png?raw=true)

- Top 5 Users of this dataset
![Top 5 Users](https://github.com/prayat-pu/Sparkify-Data-Warehouse/blob/main/img/top5users.png?raw=true)

- Top 5 Artists by song publication volume
![Top Artists](https://github.com/prayat-pu/Sparkify-Data-Warehouse/blob/main/img/MostPublishArtist.png?raw=true)

## Author

- [@prayat](https://www.github.com/prayat-pu)
