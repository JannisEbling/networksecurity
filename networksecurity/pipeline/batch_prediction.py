import os
import sys
from datetime import datetime
import pandas as pd
import numpy as np
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.storage.blob import BlobServiceClient
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.utils.main_utils.utils import load_object

class BatchPrediction:
    def __init__(self, input_file_path):
        try:
            self.input_file_path = input_file_path
            self.model_path = os.path.join("final_model", "model.pkl")
            
            # Azure Batch configuration
            self.batch_account_name = os.getenv("AZURE_BATCH_ACCOUNT_NAME")
            self.batch_account_key = os.getenv("AZURE_BATCH_ACCOUNT_KEY")
            self.batch_account_url = os.getenv("AZURE_BATCH_ACCOUNT_URL")
            
            # Azure Storage configuration
            self.storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            self.container_name = "network-security-predictions"
            
            # Initialize Azure clients
            self.batch_client = self._create_batch_client()
            self.blob_client = self._create_blob_client()
            
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def _create_batch_client(self):
        """Create Azure Batch client"""
        credentials = SharedKeyCredentials(
            self.batch_account_name,
            self.batch_account_key
        )
        return BatchServiceClient(credentials, batch_url=self.batch_account_url)
    
    def _create_blob_client(self):
        """Create Azure Blob Storage client"""
        return BlobServiceClient.from_connection_string(self.storage_connection_string)
    
    def _upload_to_blob(self, file_path, blob_name):
        """Upload file to Azure Blob Storage"""
        container_client = self.blob_client.get_container_client(self.container_name)
        with open(file_path, "rb") as data:
            container_client.upload_blob(name=blob_name, data=data)
        return f"https://{self.batch_account_name}.blob.core.windows.net/{self.container_name}/{blob_name}"
    
    def _create_batch_pool(self):
        """Create Azure Batch pool for processing"""
        pool_id = f"networksecurity-pool-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        pool = {
            'id': pool_id,
            'vm_size': 'STANDARD_D2_V2',
            'target_dedicated_nodes': 2,
            'virtual_machine_configuration': {
                'image_reference': {
                    'publisher': 'microsoft-azure-batch',
                    'offer': 'ubuntu-server-container',
                    'sku': '20-04-lts',
                    'version': 'latest'
                },
                'node_agent_sku_id': 'batch.node.ubuntu 20.04'
            },
            'start_task': {
                'command_line': '/bin/bash -c "apt-get update && apt-get install -y python3-pip && pip3 install pandas numpy scikit-learn xgboost mlflow azure-storage-blob"',
                'wait_for_success': True,
                'user_identity': {
                    'auto_user': {
                        'scope': 'pool',
                        'elevation_level': 'admin'
                    }
                }
            }
        }
        
        self.batch_client.pool.add(pool)
        return pool_id
    
    def _create_batch_job(self, pool_id):
        """Create Azure Batch job"""
        job_id = f"networksecurity-job-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        job = {
            'id': job_id,
            'pool_info': {'pool_id': pool_id}
        }
        self.batch_client.job.add(job)
        return job_id
    
    def _create_batch_task(self, job_id, input_url, model_url):
        """Create Azure Batch task for prediction"""
        task_id = f"prediction-task-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        # Resource files (upload batch_task.py to batch node)
        task_script_path = os.path.join(os.path.dirname(__file__), "batch_task.py")
        script_resource = self._upload_to_blob(task_script_path, "batch_task.py")
        
        task = {
            'id': task_id,
            'resource_files': [
                {'http_url': script_resource, 'file_path': 'batch_task.py'}
            ],
            'command_line': f'python3 batch_task.py "{input_url}" "{model_url}"',
            'environment_settings': [
                {'name': 'AZ_STORAGE_CONNECTION_STRING', 
                 'value': self.storage_connection_string}
            ]
        }
        
        self.batch_client.task.add(job_id=job_id, task=task)
        return task_id
    
    def start_batch_prediction(self):
        """Start batch prediction process"""
        try:
            logging.info("Starting batch prediction process")
            
            # Upload input file and model to blob storage
            input_blob_name = f"input-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
            model_blob_name = f"model-{datetime.now().strftime('%Y%m%d-%H%M%S')}.pkl"
            
            input_url = self._upload_to_blob(self.input_file_path, input_blob_name)
            model_url = self._upload_to_blob(self.model_path, model_blob_name)
            
            # Create pool, job and task
            pool_id = self._create_batch_pool()
            job_id = self._create_batch_job(pool_id)
            task_id = self._create_batch_task(job_id, input_url, model_url)
            
            logging.info(f"Batch prediction job submitted. Job ID: {job_id}, Task ID: {task_id}")
            
            # Monitor task status
            task = self.batch_client.task.get(job_id, task_id)
            while task.state != 'completed':
                task = self.batch_client.task.get(job_id, task_id)
            
            # Download results
            container_client = self.blob_client.get_container_client(self.container_name)
            blob_client = container_client.get_blob_client("predictions.csv")
            
            predictions_path = os.path.join("predictions", f"predictions_{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv")
            os.makedirs("predictions", exist_ok=True)
            
            with open(predictions_path, "wb") as f:
                f.write(blob_client.download_blob().readall())
            
            logging.info(f"Batch prediction completed. Results saved to {predictions_path}")
            
            # Clean up
            self.batch_client.job.delete(job_id)
            self.batch_client.pool.delete(pool_id)
            
            return predictions_path
            
        except Exception as e:
            raise NetworkSecurityException(e, sys)

def start_batch_prediction(input_file_path: str) -> str:
    """
    Start batch prediction process
    Args:
        input_file_path (str): Path to input file
    Returns:
        str: Path to predictions file
    """
    try:
        batch_prediction = BatchPrediction(input_file_path=input_file_path)
        return batch_prediction.start_batch_prediction()
        
    except Exception as e:
        raise NetworkSecurityException(e, sys)