import os
import sys
import pandas as pd
import pickle
from azure.storage.blob import BlobClient

def run_prediction(input_url: str, model_url: str):
    try:
        # Download input file and model
        input_blob = BlobClient.from_blob_url(input_url)
        model_blob = BlobClient.from_blob_url(model_url)

        with open('input.csv', 'wb') as f:
            f.write(input_blob.download_blob().readall())
        with open('model.pkl', 'wb') as f:
            f.write(model_blob.download_blob().readall())

        # Load data and model
        data = pd.read_csv('input.csv')
        with open('model.pkl', 'rb') as f:
            model = pickle.load(f)

        # Make predictions
        predictions = model.predict(data)
        results = pd.DataFrame(predictions, columns=['prediction'])
        results.to_csv('predictions.csv', index=False)

        # Upload results
        output_blob = BlobClient.from_connection_string(
            os.environ['AZ_STORAGE_CONNECTION_STRING'],
            'network-security-predictions',
            'predictions.csv'
        )
        with open('predictions.csv', 'rb') as f:
            output_blob.upload_blob(f)

    except Exception as e:
        print(f"Error in batch task: {str(e)}", file=sys.stderr)
        raise e

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python batch_task.py <input_url> <model_url>")
        sys.exit(1)
    
    input_url = sys.argv[1]
    model_url = sys.argv[2]
    run_prediction(input_url, model_url)
