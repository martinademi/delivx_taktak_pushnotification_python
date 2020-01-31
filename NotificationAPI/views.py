from django.shortcuts import render
# Create your views here.
from django.http import HttpResponse, JsonResponse
import json
from django.shortcuts import render, redirect
from rest_framework.views import APIView
from datetime import timedelta
from pytz import timezone
import datetime
import requests
import time
from bson import json_util, ObjectId
import asyncio
import pymongo
from pymongo import MongoClient, CursorType
from bson.objectid import ObjectId
import pika
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

server_key = "key={}".format(server_key)
url = 'https://fcm.googleapis.com/fcm/send'

server_key = "key={}".format(server_key)
headers = {"Content-Type": "application/json",
           "Authorization": server_key}

# Create your views here.


class PushNotification(APIView):
    '''
        API for the Send the Push Notification To the USER, DRIVER or STORES
        data in Array or List [
            id = userid or cityid or driverid
            topic = topic of userid or driverid or cityid
            deviceType = 1(iOS) , 2(Android), 3(Web)
            uniqId = for every user
            messsageId = message id from the getting by google response
            status = 0 for sent, 1 for view
        ]
        radius = radius of area
        address = address
        title = title of the notification
        body = message of the notification
        imageUrl = image url
        pusyType = 1 > Normal Notification
                   2 > RIACH Notification
        knowMoreUrl = CallBack Url
        userType = 1 for user, 2 for driver
        type = 0 for individual, 1 for city, 2 for zone, 3 for the Cordinates(By addrss)
    '''
    def post(self, request):
        credentials = pika.PlainCredentials(rabbitUser, rabbitPass)
        parameters = pika.ConnectionParameters(rabbitHost, 5672, '/', credentials, socket_timeout=300)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue='PushNotifictionSend')
        data = request.data
        print(data)
        data_type = data['type']
        data_body = data['body']
        data_title = data['title']
        topics = data['topics']
        topics = topics.split(",")
        image_url = data['imageUrl']
        push_type = data['pusyType'] if 'pusyType' in data else 1
        user_type = data['userType']
        know_more_url = data['knowMoreUrl']
        longitude = float(data['longitude']) if data['longitude'] != "" else 0
        latitude = float(data['latitude']) if 'latitude' in data else 0
        dispatchRadius = data['radius']
        if data_type != 3:
            if int(data_type) == 1:
                for i in topics:
                    aggregateobj = [
                        {
                            '$match':
                                {
                                    'pushToken': i
                                }
                        }
                    ]

            elif int(data_type) == 2: # get the city data
                aggregateobj = [
                    {
                        '$match':
                            {
                                'cityId': {
                                    '$in': topics
                                },
                            }
                    }
                ]
            elif int(data_type) == 3: # get the Zone data when the city is blank
                aggregateobj = [
                    {
                        '$match':
                            {
                                'zones': {
                                    '$in': topics
                                }
                            }
                    }
                ]
            else: # when the cityid and zone id both are not blank
                aggregateobj = [
                    {
                        '$match':{
                            "status": 0
                        }
                    }
                ]
        else:
            query = {
                'near': {
                    "longitude": float(longitude),
                    "latitude": float(latitude)
                },
                "spherical": True,
                'distanceField': 'dist',
                # "maxDistance": dispatchRadius / 6378.137,
                "maxDistance": dispatchRadius,
                "distanceMultiplier": 6378.137
            }

            aggregateobj = [
                {
                    "$geoNear": query
                },
                {
                    '$sort': {
                        'dist': -1
                    }
                }

            ]
        print('=====================',aggregateobj)
        print(user_type)
        if int(user_type) == 1:
            response_data = db.customer.aggregate(aggregateobj)
        elif int(user_type) == 2:
            response_data = db.driver.aggregate(aggregateobj)

        uniqId = str(ObjectId()) # generate the objectid or the mongo id from here
        for i in response_data:
            if 'fcmTopic' in i:
                if i['fcmTopic'] == None or i['fcmTopic'] == "":
                    pass
                else:
                    fcm_token = i['fcmTopic']
                    if int(push_type) == 1:
                        body = {
                            "notification": {
                                "title": data_title,
                                "body": data_body,
                                "sound": "default",
                            },
                            "data": {
                                "title": data_title,
                                "body": data_body,
                            },
                            "collapse_key": 'your_collapse_key',
                            "priority": 'high',
                            "delay_while_idle": True,
                            "dry_run": False,
                            "time_to_live": 3600,
                            "badge": "1",
                            "extra": {
                                "uniqId": uniqId,
                                "deviceType": i['mobileDevices']['deviceType'],
                                "pushType": push_type,
                                "knowMoreUrl": know_more_url,
                                "topic": str(i['fcmTopic']),
                                "imageurl": image_url,
                                "userId": str(i['_id']),
                                "userName": i['name'],
                                "userType": i['userTypeMsg']
                            },
                            "to": "/topics/" + str(i['fcmTopic'])
                        }
                    elif int(push_type) == 2:
                        body = {
                            "notification": {
                                "title": data_title,
                                "body": data_body,
                                "sound": "default",
                                'media_url': image_url,
                                "content_available": True,
                                'isMediaType': True
                            },
                            "collapse_key": 'your_collapse_key',
                            "priority": 'high',
                            "delay_while_idle": True,
                            "dry_run": False,
                            "time_to_live": 3600,
                            "badge": "1",
                            "to": "/topics/" + str(i['fcmTopic'])
                        }
                    channel.basic_publish(exchange='', routing_key='PushNotifictionSend',
                                          body=json.dumps(body))

            else:
                if 'pushToken' in i:
                    fcm_token = i['pushToken']
                    if int(push_type) == 1:
                        body = {
                            "notification": {
                                "title": data_title,
                                "body": data_body,
                                "sound": "default",
                            },
                            "data": {
                                "title": data_title,
                                "body": data_body,
                            },
                            "collapse_key": 'your_collapse_key',
                            "priority": 'high',
                            "delay_while_idle": True,
                            "dry_run": False,
                            "time_to_live": 3600,
                            "badge": "1",
                            "extra": {
                                "uniqId": uniqId,
                                "deviceType": i['mobileDevices']['deviceType'] if 'mobileDevices' in i else 0,
                                "pushType": push_type,
                                "knowMoreUrl": know_more_url,
                                "topic": str(i['pushToken']),
                                "imageurl": image_url,
                                "userId": str(i['_id']),
                                "userName": i['firstName'] + " " + i['lastName'],
                                "userType": "Driver"
                            },
                            "to": "/topics/" + str(i['pushToken'])
                        }
                    elif int(push_type) == 2:
                        body = {
                            "notification": {
                                "title": data_title,
                                "body": data_body,
                                "sound": "default",
                                'media_url': image_url,
                                "content_available": True,
                                'isMediaType': True
                            },
                            "collapse_key": 'your_collapse_key',
                            "priority": 'high',
                            "delay_while_idle": True,
                            "dry_run": False,
                            "time_to_live": 3600,
                            "badge": "1",
                            "to": "/topics/" + str(i['pushToken'])
                        }
                    print(body)
                    channel.basic_publish(exchange='', routing_key='PushNotifictionSend',
                                          body=json.dumps(body))
                else:
                    pass
        connection.close()
        message = {
            "message": "Data In Processing"
        }
        return JsonResponse(message, safe=False, status=200)


