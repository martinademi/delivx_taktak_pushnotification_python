import pika
from bson import json_util, ObjectId
import asyncio
import pymongo
from pymongo import MongoClient, CursorType
import json
import requests
import ast
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(dotenv_path="/usr/etc/.env")

mongo_url = os.getenv("MONGO_URL")
mongo_db = os.getenv("MONGO_DATABASE")
server_key = os.getenv("SERVER_KEY")
rabbitHost = os.getenv("RABBITMQ_HOST")
rabbitUser = os.getenv("RABBITMQ_USER")
rabbitPass = os.getenv("RABBITMQ_PASS")

client = MongoClient(mongo_url)
db = client[str(mongo_db)]

credentials = pika.PlainCredentials(rabbitUser, rabbitPass)
parameters = pika.ConnectionParameters(rabbitHost, 5672, '/', credentials, socket_timeout=300)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='PushNotifictionSend')
url = 'https://fcm.googleapis.com/fcm/send'

server_key = "key={}".format(server_key)
headers = {"Content-Type": "application/json",
           "Authorization": server_key}

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


