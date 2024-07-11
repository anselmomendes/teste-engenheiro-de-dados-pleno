import pandas as pd

class TransformFiles:
    def __init__(self, files_name):
        self.files_name = files_name

    def order(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.files_name, sep=',', encoding='utf-8')

            order = pd.DataFrame(columns=["op", 
                                            "oid_id", 
                                            "created_at", 
                                            "updated_at", 
                                            "last_sync_tracker",
                                            "order_created_at",
                                            "order_tracking_code", 
                                            "order_status", 
                                            "order_description", 
                                            "order_tracker_type", 
                                            "order_from", 
                                            "order_to"])

            for index, data in df.iterrows():
                data['createdAt'] = pd.to_datetime(data['createdAt'], unit='s')
                data['updatedAt'] = pd.to_datetime(data['updatedAt'], unit='s')
                data['lastSyncTracker'] = pd.to_datetime(data['lastSyncTracker'], unit='s')  

                data['array_trackingEvents'] = data['array_trackingEvents'].replace("'", '"').replace('None', 'null').replace('$date', 'date')

                #data = data.rename(columns={"Op":"op", 
                #                        "oid__id":"oid_id", 
                #                        "createdAt":"created_at",
                #                        "updatedAt":"updated_at",
                #                        "lastSyncTracker":"last_sync_tracker",
                #                        "array_trackingEvents":"array_tracking_events"})

                tracking = pd.read_json(data['array_trackingEvents'])
                tracking.rename(columns={'createdAt':'order_created_at', \
                                        'trackingCode':'order_tracking_code', \
                                        'status':'order_status', \
                                        'description':'order_description', \
                                        'trackerType':'order_tracker_type', \
                                        'from':'order_from', \
                                        'to':'order_to'},
                                        inplace=True)
                #data.drop(columns=['array_tracking_events'], inplace=True)
                tracking['op'] = data['Op']
                tracking['oid_id'] = data['oid__id']
                tracking['created_at'] = data['createdAt']
                tracking['updated_at'] = data['updatedAt']
                tracking['last_sync_tracker'] = data['lastSyncTracker']

            order = pd.concat([tracking, order], axis=0, ignore_index=True)
            order.reset_index(drop=True, inplace=True)

            order['order_created_at'] = order['order_created_at'].apply(lambda x: pd.to_datetime(x.get('date'), unit='ms'))
            order['order_status'] = order['order_status'].apply(lambda x: None if pd.isnull(x) else x)
            order = order[['op', \
                        'oid_id', \
                        'created_at', \
                        'updated_at', \
                        'last_sync_tracker', \
                        'order_created_at', \
                        'order_tracking_code', \
                        'order_status', \
                        'order_description', \
                        'order_tracker_type', \
                        'order_from', \
                        'order_to']]
            return order
        except Exception as e:
            print(f"Error transforming order. {e}")
            return None