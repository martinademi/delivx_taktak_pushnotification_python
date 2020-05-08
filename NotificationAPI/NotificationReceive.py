import json
import requests
import ast
import os
import pika

from bson import json_util, ObjectId
from pymongo import MongoClient, CursorType
from dotenv import load_dotenv, find_dotenv

load_dotenv(dotenv_path="/usr/etc/.env")

mongo_url = os.getenv("MONGO_URL")
mongo_db = os.getenv("MONGO_DATABASE")

client = MongoClient(mongo_url)
db = client[str(mongo_db)]

rabbit_host = os.getenv("RABBIT_HOST")
rabbit_user = os.getenv("RABBIT_USER")
rabbit_pass = os.getenv("RABBIT_PASS")
rabbit_queue = os.getenv("RABBIT_QUEUE")
server_key = os.getenv("SERVER_KEY")

print(db, rabbit_host, rabbit_user, rabbit_pass, rabbit_queue, server_key)

credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
parameters = pika.ConnectionParameters(rabbit_host, 5672, '/', credentials, socket_timeout=300)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=rabbit_queue)
url = 'https://fcm.googleapis.com/fcm/send'

headers = {"Content-Type": "application/json",
           "Authorization": server_key}

def callback(ch, method, properties, body):
    body = body.decode("utf-8")
    body = json.loads(body)
    extra_data = body['extra']
    data_body = body['notification']['body']
    data_title = body['notification']['title']
    device_type = extra_data['deviceType'] if 'deviceType' in extra_data else ""
    push_type = extra_data['pushType']
    know_more_url = extra_data['knowMoreUrl']
    id = extra_data['uniqId']
    topic = extra_data['topic']
    user_id = extra_data['userId']
    user_name = extra_data['userName']
    user_type = extra_data['userType']
    notificationtype = body['notificationType']
    notificationmsg = body['notificationTypeMsg']
    createdTimeStamp = body['createdTimeStamp']
    userTypeStatus = body['userTypeStatus']
    imageurl = extra_data['imageurl']
    fcm_response = requests.post(url, data=json.dumps(body), headers=headers)
    print(fcm_response.json())
    message_data = db.fcmNotification.find({'_id': ObjectId(id)})
    if fcm_response.status_code == 200:
        messageid = (fcm_response.json())['message_id']
    else:
        messageid = "Error while sending notification"
    # print("Called")
    if message_data.count() == 0:
        if fcm_response.status_code == 200:
            print("success")
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
                "notificationType":notificationtype,
                "notificationTypeMsg":notificationmsg ,
                "createdTimeStamp": createdTimeStamp,
                "userTypeStatus":userTypeStatus,
                "_id": ObjectId(id)
            }
        else:
            print("error")
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
                "notificationType": notificationtype,
                "notificationTypeMsg": notificationmsg,
                "createdTimeStamp": createdTimeStamp,
                "userTypeStatus": userTypeStatus,
                "_id": ObjectId(id)
            }
        db.fcmNotification.insert(success_data)
        # print("new")
    else:
        print("success")
        success_data = {
            "id": str(ObjectId()),
            "topic": topic,
            "userId": user_id,
            "userName": user_name,
            "userType": user_type,
            "deviceType": device_type,
            "messsageId": str(messageid),
            "notificationType": notificationtype,
            "notificationTypeMsg": notificationmsg,
            "createdTimeStamp": createdTimeStamp,
            "userTypeStatus": userTypeStatus,
            "status": 1,
            "statusMsg": "Sent"
        }
        if fcm_response.status_code == 200:
            print("success")
            db.fcmNotification.update({'_id': ObjectId(id)}, {'$push': {"success": success_data}}, upsert=False)
        else:
            print("error")
            db.fcmNotification.update({'_id': ObjectId(id)}, {'$push': {"error": success_data}}, upsert=False)
        print("update")


channel.basic_consume(callback, queue=rabbit_queue, no_ack=True)
channel.start_consuming()


