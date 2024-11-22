### Network Security Projects For Phising Data

Setup github secrets:
AWS_ACCESS_KEY_ID=

AWS_SECRET_ACCESS_KEY=

AWS_REGION = us-east-1

AWS_ECR_LOGIN_URI = 788614365622.dkr.ecr.us-east-1.amazonaws.com/networkssecurity
ECR_REPOSITORY_NAME = networkssecurity


Docker Setup In EC2 commands to be Executed
#optinal

sudo apt-get update -y

sudo apt-get upgrade

#required

curl -fsSL https://get.docker.com -o get-docker.sh

sudo sh get-docker.sh

sudo usermod -aG docker ubuntu

newgrp docker

Project Structure Overview
The project is organized into a modular, pipeline-driven workflow with distinct stages for data processing and machine learning model operations. The components follow an ETL (Extract, Transform, Load) pipeline structure, focusing on streamlined data flow from ingestion to deployment.

High-Level Pipeline Flow
Data Ingestion Config:

This module is responsible for fetching data from various sources (e.g., CSV files, APIs, databases, and S3 buckets).
The output of this component is the Data Ingestion Artifacts, which are passed to the next stage.
Data Validation Config:

Ensures the integrity and validity of the ingested data.
Data validation artifacts are created and passed to the transformation module.
Data Transformation Config:

Handles basic preprocessing and cleaning of raw data.
Converts the cleaned data into a structured format (e.g., JSON).
Produces Data Transformation Artifacts as outputs.
Model Trainer Config:

This component trains machine learning models using the transformed data.
Generates Model Trainer Artifacts.
Model Evaluation Config:

Evaluates trained models based on predefined metrics.
Produces Model Evaluation Artifacts to determine model performance.
Model Pusher Config:

Deploys the accepted model to cloud services such as AWS, Azure, or other environments.
Generates Model Pusher Artifacts for tracking.
Detailed ETL Pipeline Description
1. Extract
Source:
The data originates from multiple sources:
Local CSV Dataset: Static data stored in CSV files.
APIs: Open, paid, or custom APIs provide real-time or static data.
S3 Buckets: Data from cloud storage repositories.
Internal Databases: Proprietary or existing databases within the organization.
Extracted data is raw and needs validation and cleaning in subsequent stages.
2. Transform
Transformation Processes:
Basic preprocessing to handle missing values, anomalies, or inconsistencies.
Data cleaning to produce a structured JSON format.
Output:
The transformed data is formatted as JSON objects, ready for storage or further analysis.
3. Load
Destination:
The transformed data is loaded into scalable storage solutions, including:
MongoDB (Atlas): A NoSQL database suited for JSON-like document storage.
AWS DynamoDB: A fully managed NoSQL database service.
MySQL: A relational database for structured data.
S3 Buckets: Cloud storage for backup or analysis-ready data.
Dataset Representation
Input Dataset:
Sample tabular structure with columns A, B, and C. Each row contains numerical values.
Example:
css
Code kopieren
A   B   C  
100 120 140  
Transformation Process:
Converts the tabular data into JSON format.
Example:
json
Code kopieren
[
  {"A": 100, "B": 120, "C": 140},
  {...},
  {...}
]
Destination:
The JSON data is stored in MongoDB Atlas or other configured destinations.
Key Features
Scalability:
The project uses cloud-based solutions like MongoDB Atlas, AWS, and Azure for large-scale data handling and deployment.

Modularity:
Each pipeline stage is distinct, allowing independent development and debugging.

Flexibility:
The ETL pipeline supports multiple data sources and destinations, making it adaptable to diverse use cases.

Extensibility:
The modular architecture facilitates easy integration of new features, such as additional data validation rules or new model evaluation criteria.

Let me know if you'd like further refinements or explanations!