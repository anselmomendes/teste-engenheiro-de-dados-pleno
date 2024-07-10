import pandas as pd

class TransformFiles:
    def __init__(self, files_dir):
        self.files_dir = files_dir

    def order(self) -> pd.DataFrame:
        try:

            data = pd.read_csv(self.files_dir, sep=',', encoding='utf-8')
            order = pd.DataFrame(columns=["op", "oid_id", "created_at", "updated_at", "last_sync_tracker"])
            
            data['createdAt'] = pd.to_datetime(data['createdAt'], unit='s')
            data['updatedAt'] = pd.to_datetime(data['updatedAt'], unit='s')
            data['lastSyncTracker'] = pd.to_datetime(data['lastSyncTracker'], unit='s')  
            data.drop(columns=['array_trackingEvents'], inplace=True)
            df = data.rename(columns={"Op":"op", 
                                    "oid__id":"oid_id", 
                                    "createdAt":"created_at",
                                    "updatedAt":"updated_at",
                                    "lastSyncTracker":"last_sync_tracker"})

            #Garantindo que o nome das colunas estão corretos
            order = pd.concat([df, order], axis=0, ignore_index=False)
            return order
        except Exception as e:
            print(f"Error transforming order. {e}")
            return None
    
    def tracking(self) -> pd.DataFrame:
        try:
            data = pd.read_csv(self.files_dir, sep=',', encoding='utf-8')
            array_trackingEvents = data['array_trackingEvents'].apply(lambda x: x.replace("'", '"').replace('None', 'null').replace('$date', 'date'))
            
            tracking_events = pd.DataFrame(columns=['created_at', 
                                                    'tracking_code', 
                                                    'status', 
                                                    'description', 
                                                    'tracker_type', 
                                                    'from', 
                                                    'to'])

            for trackingEvents in array_trackingEvents:
                df = pd.read_json(trackingEvents)
                df.rename(columns={'createdAt':'created_at', 'trackingCode':'tracking_code', 'trackerType':'tracker_type',}, inplace=True)
                tracking_events = pd.concat([tracking_events, df], axis=0, ignore_index=False)

            tracking_events.reset_index(drop=True, inplace=True)

            tracking_events['created_at'] = tracking_events['created_at'].apply(lambda x: pd.to_datetime(x.get('date'), unit='ms'))
            tracking_events['status'] = tracking_events['status'].apply(lambda x: None if pd.isnull(x) else x)
            return tracking_events
        except Exception as e:
            print(f"Error transforming tracking. {e}")
            return None

        