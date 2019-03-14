import pika
from bson import json_util, ObjectId
import asyncio
import pymongo
from pymongo import MongoClient, CursorType
import json
import requests
import ast

client = MongoClient('mongodb://root_DB:cjdk69RvQy5b5VDL@159.203.191.182:5009/DelivX')
db = client.DelivX

credentials = pika.PlainCredentials('admin', 'cw4NNFd3nhKgcUBe')
parameters = pika.ConnectionParameters('18.228.179.231', 5672, '/', credentials, socket_timeout=300)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='PushNotifictionSend')
url = 'https://fcm.googleapis.com/fcm/send'

headers = {"Content-Type":"application/json",
        "Authorization": "key=AAAARNrad8o:APA91bHkxG8dPMF-thCRkh_nZzCu9JDfEjvsQjuD7woRFtYBTM-RI7xrcq2bwW1RUv9jIIolZyPTRh2b9Rbe48jmDpd7yndpgkNvZqzkIAvddC027VdzmhMdDLvZPMsaEzRTztbSMO3z"}

def callback(ch, method, properties, body):
    body = body.decode("utf-8")
    body = json.loads(body)
    extra_data = body['extra']
    data_body = body['notification']['body']
    data_title = body['notification']['title']
    device_type = extra_data['deviceType']
    push_type = extra_data['pushType']
    know_more_url = extra_data['knowMoreUrl']
    id = extra_data['uniqId']
    topic = extra_data['topic']
    user_id = extra_data['userId']
    user_name = extra_data['userName']
    user_type = extra_data['userType']
    imageurl = extra_data['imageurl']
    fcm_response = requests.post(url, data=json.dumps(body), headers=headers)
    message_data = db.fcmNotification.find({'_id': ObjectId(id)})
    if fcm_response.status_code == 200:
        messageid = (fcm_response.json())['message_id']
    else:
        messageid = "Error while sending notification"

    if message_data.count() == 0:
        if fcm_response.status_code == 200:
            success_data = {
                "success": [
                    {
                        "id": str(ObjectId()),
                        "topic": topic,
                        "userId": user_id,
                        "userName": user_name,
                        "userType": user_type,
                        "deviceType": device_type,
                        "messsageId": str(messageid),
                        "status": 1,
                        "statusMsg": "Sent"
                    }
                ],
                "error": [],
                "title": data_title,
                "body": data_body,
                "imageUrl": imageurl,
                "pusyType": push_type,
                "knowMoreUrl": know_more_url,
                "_id": ObjectId(id)
            }
        else:
            success_data = {
                "error": [
                    {
                        "id": str(ObjectId()),
                        "topic": topic,
                        "userId": user_id,
                        "userName": user_name,
                        "userType": user_type,
                        "deviceType": device_type,
                        "messsageId": str(messageid),
                        "status": 1,
                        "statusMsg": "Sent"
                    }
                ],
                "success": [],
                "title": data_title,
                "body": data_body,
                "imageUrl": imageurl,
                "pusyType": push_type,
                "knowMoreUrl": know_more_url,
                "_id": ObjectId(id)
            }
        db.fcmNotification.insert(success_data)
    else:
        success_data = {
            "id": str(ObjectId()),
            "topic": topic,
            "userId": user_id,
            "userName": user_name,
            "userType": user_type,
            "deviceType": device_type,
            "messsageId": str(messageid),
            "status": 1,
            "statusMsg": "Sent"
        }
        if fcm_response.status_code == 200:
            db.fcmNotification.update({'_id': ObjectId(id)}, {'$push':{"success": success_data}}, upsert=False)
        else:
            db.fcmNotification.update({'_id': ObjectId(id)}, {'$push': {"error": success_data}}, upsert=False)

channel.basic_consume(callback, queue='PushNotifictionSend', no_ack=True)
channel.start_consuming()


