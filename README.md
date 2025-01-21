# Data Pipeline: UN Population Analysis

## Overview
This repository contains an Airflow-based data pipeline that extracts UN Population data, performs data transformations, and loads it into a PostgreSQL database. The pipeline consists of 3 primary steps:
- **Extract**: Downloads the dataset.
- **Transform**: Processes the dataset to calculate specific insights and standardizes column names.
- **Load**: Stores the transformed data in a PostgreSQL database.

## Prerequisites
- Python 3.8+
- Apache Airflow 2.x+
- PostgreSQL Database

## Setup and Running Locally

### 1. Install the dependencies:
```bash
pip install -r requirements.txt
