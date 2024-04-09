from google.cloud import storage
from confluent_kafka import Producer
import pandas as pd
import numpy as np
import json
import io

storage_client = storage.Client.from_service_account_json('path/to/your/credentials.json')
bucket = storage_client.get_bucket('ind_dataset')


kafka_config = {
    'bootstrap.servers': '',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '',
    'sasl.password': '',
    'message.max.bytes': 100485760  
}
producer = Producer(**kafka_config)

def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message {msg.topic()} {msg.partition()} {msg.offset()} delivered")

file_tracker = {}

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

def calculate_accident_risk(recording_meta_df, tracks_meta_df, tracks_df):
    merged_data = pd.merge(tracks_df, tracks_meta_df, on=['recordingId', 'trackId'])
    merged_data = pd.merge(merged_data, recording_meta_df, on='recordingId')
    merged_data['avgSpeed'] = np.sqrt(merged_data['xVelocity']**2 + merged_data['yVelocity']**2)
    numVehicles = merged_data['trackId'].nunique()
    avgSpeed = merged_data['avgSpeed'].mean()
    estimatedRisk = numVehicles * avgSpeed
    locationId = merged_data['locationId'].iloc[0]
    weekday = merged_data['weekday'].iloc[0]
    startTime = merged_data['startTime'].iloc[0]
    duration = merged_data['duration'].iloc[0]
    latLocation = merged_data['latLocation'].iloc[0]
    lonLocation = merged_data['lonLocation'].iloc[0]
    result_dict = {
        'locationId': locationId,
        'numVehicles': numVehicles,
        'avgSpeed': avgSpeed,
        'estimatedRisk': estimatedRisk,
        'weekday': weekday,
        'startTime': startTime,
        'duration': duration,
        'latLocation': latLocation,
        'lonLocation': lonLocation
    }
    
    return result_dict


def process_file(blob):
    recording_id = blob.name.split('_')[0]
    if recording_id not in file_tracker:
        file_tracker[recording_id] = {'recordingMeta': None, 'tracksMeta': None, 'tracks': None, 'data_loaded': False}

    blob_data = blob.download_as_string()
    if blob.name.endswith('.csv'):
        data = io.StringIO(blob_data.decode('utf-8'))
        df = pd.read_csv(data)
        if 'recordingMeta.csv' in blob.name:
            file_tracker[recording_id]['recordingMeta'] = df
        elif 'tracksMeta.csv' in blob.name:
            file_tracker[recording_id]['tracksMeta'] = df
        elif 'tracks.csv' in blob.name:
            file_tracker[recording_id]['tracks'] = df
        
        if all([file_tracker[recording_id][part] is not None for part in ['recordingMeta', 'tracksMeta', 'tracks']]):
            file_tracker[recording_id]['data_loaded'] = True
            process_data(recording_id)  
        
    elif blob.name.endswith('.png'):
        print(f"Skipping binary file: {blob.name}")
    else:
        print(f"Unsupported file type: {blob.name}")


def process_data(recording_id):
    if file_tracker[recording_id]['data_loaded']:
        for analysis_type, analysis_func in analysis_functions.items():
            if analysis_func:  
                df_recordingMeta = file_tracker[recording_id]['recordingMeta']
                df_tracksMeta = file_tracker[recording_id]['tracksMeta']
                df_tracks = file_tracker[recording_id]['tracks']
                result_dict = analysis_func(df_recordingMeta, df_tracksMeta, df_tracks)
                publish_to_kafka(analysis_type, result_dict)
        del file_tracker[recording_id]

def publish_to_kafka(topic, data):
    try:
        message = json.dumps(data, cls=NpEncoder)
        producer.produce(topic, value=message, callback=acked)
        producer.poll(0)
    except Exception as e:
        print(f"Exception while publishing message: {str(e)}")


analysis_functions = {
    'accident_risk': calculate_accident_risk,
}

def main():
    blobs = bucket.list_blobs()
    for blob in blobs:
        print(f"Processing file: {blob.name}")
        process_file(blob)

    producer.flush()

if __name__ == '__main__':
    main()