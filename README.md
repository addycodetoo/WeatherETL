# Weather ETL POC

This project demonstrates a proof of concept for an **ETL (Extract, Transform, Load)** process that fetches weather data, transforms it, and stores it in a structured format for further analysis. The project is designed to be modular and can easily be extended to work with different data sources or formats.

## Folder Structure
weather_etl_poc/
├── config/
│   └── config.yml
├── data/
│   ├── pre_transform/
│   └── transformed/
├── scripts/
│   └── etl_job.py
└── utils/
    └── helpers.py

## Prerequisites

Before running the project, ensure you have the following software installed:

- **Java 8 (Update 431 or later)** - Required for running Apache Spark and Hadoop.
- **Apache Spark** - The distributed data processing engine used in the project.
- **Hadoop 3.3.6** - Used for storing large amounts of data and managing distributed file systems.
- **Python 3.10.11** - Required for running the Python ETL scripts.

### Install Dependencies

1. Install the necessary software and tools:
   - [Download and install Java 8 (Update 431 or later)](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html).
   - [Download and install Apache Spark](https://spark.apache.org/downloads.html).
   - [Download and install Hadoop 3.3.6](https://hadoop.apache.org/releases.html).
   - [Install Python 3.10.11](https://www.python.org/downloads/release/python-31011/).

2. Install Python dependencies:

```bash
pip install -r requirements.txt

Setup and Installation
1. Clone the Repository
git clone https://github.com/addycodetoo/WeatherETL.git
cd WeatherETL

2. Set Up the Configuration
Ensure that your config/config.yml file contains the correct API key and other necessary configurations for the weather API.
api_key: "your_api_key_here"

3. Run the ETL Job
Execute the etl_job.py script to start the ETL process.
python scripts/etl_job.py

Folder Details
config/config.yml: Contains the configuration details, such as the API key.

data/pre_transform/: Directory where raw data (e.g., API responses) is stored before any transformation.

data/transformed/: Directory where the transformed and processed data will be stored.

scripts/etl_job.py: The main script that orchestrates the ETL process, from data extraction to loading.

utils/helpers.py: Contains helper functions used throughout the ETL process.