import json
import datetime
import requests
import time
import pymongo
import pika
import os

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from rest_framework.views import APIView
from datetime import timedelta
from pytz import timezone
from bson import json_util, ObjectId
from pymongo import MongoClient, CursorType
from bson.objectid import ObjectId
from dotenv import load_dotenv, find_dotenv

load_dotenv(dotenv_path="/usr/etc/.env")

mongo_url = os.getenv("MONGO_URL")
mongo_db = os.getenv("MONGO_DATABASE")
server_key = os.getenv("FCM_SERVER_KEY")

client = MongoClient(mongo_url)
db = client[str(mongo_db)]

rabbit_host = os.getenv("RABBIT_HOST")
rabbit_user = os.getenv("RABBIT_USER")
rabbit_pass = os.getenv("RABBIT_PASS")
rabbit_queue = "PushNotifictionSendPython"

print(db, rabbit_host, rabbit_user, rabbit_pass, rabbit_queue, server_key)

url = 'https://fcm.googleapis.com/fcm/send'

headers = {"Content-Type": "application/json",
           "Authorization": "key="+str(server_key)}


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
                   2 > RICH Notification
        knowMoreUrl = CallBack Url
        userType = 1 for user, 2 for driver
        type = 0 for individual, 1 for city, 2 for zone, 3 for the Cordinates(By addrss)
    '''

    def post(self, request):
        try:
            credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
            parameters = pika.ConnectionParameters(rabbit_host, 5672, '/', credentials, socket_timeout=300)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue=rabbit_queue)
            data = request.data
            print("request data: ",data)
            data_type = data['topicType']
            data_body = data['body']
            data_title = data['title']
            topics = data['topics'] if len(data['topics']) != 0 else []
            if len(topics) > 0:
                topics = topics.split(",")
            image_url = data['imageUrl']
            push_type = data['type'] if 'type' in data else 1
            user_type = data['userType']
            know_more_url = data['knowMoreUrl']
            longitude = float(data['longitude']) if data['longitude'] != "" else 0
            latitude = float(data['latitude']) if 'latitude' in data else 0
            dispatchRadius = data['radius']
            createdTimestamp = datetime.datetime.now().timestamp()
            if int(data_type) == 1:
                pushmsg = "Individual"
            elif int(data_type) == 2 or int(data_type) == 3:
                pushmsg = "City"
            elif int(data_type) == 4 or int(data_type) == 5:
                pushmsg = "Zone"
            else:
                pushmsg = "Radius"
            if int(data_type) != 6:
                if int(data_type) == 1:
                    if int(user_type) == 1:
                        aggregateobj = [
                            {
                                '$match':
                                    {
                                        'fcmTopic':  {
                                            "$in": topics
                                        }
                                    }
                            }
                        ]
                    elif int(user_type) == 2:
                        aggregateobj = [
                            {
                                '$match':
                                    {
                                        'pushToken':  {
                                            "$in": topics
                                        }
                                    }
                            }
                        ]
                elif int(data_type) == 2 or int(data_type) == 3:  # get the city data
                    if int(user_type) == 1:
                        strTopics = topics
                        topics  = [ObjectId(i) for i in topics]
                        topics += strTopics 
                    # print("city : ",topics)
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
                elif int(data_type) == 4 or int(data_type) == 5:  # get the Zone data when the city is blank
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
                else:  # when the cityid and zone id both are not blank
                    aggregateobj = [
                        {
                            '$match':{
                                "guestToken": False
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
            if len(topics) > 0:
                print("aggregation query: ", aggregateobj)
            else:
                print("no topics found")
            if int(user_type) == 1:
                userTypeStatus = 1
                response_data = db.customer.aggregate(aggregateobj)
            elif int(user_type) == 2:
                userTypeStatus = 2
                response_data = db.driver.aggregate(aggregateobj)
            else:
                print("user type not found")

            uniqId = str(ObjectId())  # generate the objectid or the mongo id from here
            for i in response_data:
                if 'fcmTopic' in i.keys():
                    if i['fcmTopic'] == None or i['fcmTopic'] == "":
                        print("fcmtopic is none")
                        pass
                    else:
                        # print("correct fcm topic")
                        fcm_token = i['fcmTopic']
                        print(i['fcmTopic'])
                        if int(push_type) == 1:
                            body = {
                                "notification": {
                                    "title": data_title,
                                    "body": data_body,
                                    "sound": "default",
                                },
                                "data": {
                                    "title": data_title,
                                    "msg": data_body,
                                },
                                "collapse_key": 'your_collapse_key',
                                "priority": 'high',
                                "delay_while_idle": True,
                                "notificationType": int(data_type),
                                "notificationTypeMsg": pushmsg,
                                "createdTimeStamp":int(createdTimestamp),
                                "userTypeStatus":userTypeStatus,
                                "dry_run": False,
                                "time_to_live": 3600,
                                "badge": "1",
                                "extra": {
                                    "uniqId": uniqId,
                                    #"deviceType": i['mobileDevices']['deviceType'],
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
                                "extra": {
                                    "uniqId": uniqId,
                                    #"deviceType": i['mobileDevices']['deviceType'],
                                    "pushType": push_type,
                                    "knowMoreUrl": know_more_url,
                                    "topic": str(i['fcmTopic']),
                                    "imageurl": image_url,
                                    "userId": str(i['_id']),
                                    "userName": i['name'],
                                    "userType": i['userTypeMsg']
                                },
                                "collapse_key": 'your_collapse_key',
                                "priority": 'high',
                                "delay_while_idle": True,
                                "dry_run": False,
                                "notificationType":int(data_type),
                                "notificationTypeMsg":pushmsg,
                                "createdTimeStamp": int(createdTimestamp),
                                "userTypeStatus": userTypeStatus,
                                "time_to_live": 3600,
                                "badge": "1",
                                "to": "/topics/" + str(i['fcmTopic'])
                            }
                        else:
                            print("wrong push_type")
                        if connection.is_open:
                            print('OK')
                        else:
                            print("Not OK")
                        # print("Body: ", body)
                        channel.basic_publish(exchange='', routing_key=rabbit_queue,
                                            body=json.dumps(body))

                else:
                    if 'pushToken' in i.keys():
                        fcm_token = i['pushToken']
                        if int(push_type) == 1:
                            body = {
                                "notification": {
                                    "title": data_title,
                                    "body": data_body,
                                    "sound": "default",
                                    "imageurl": image_url,
                                },
                                "data": {
                                    "title": data_title,
                                    "msg": data_body
                                },
                                "collapse_key": 'your_collapse_key',
                                "priority": 'high',
                                "delay_while_idle": True,
                                "notificationType": int(data_type),
                                "notificationTypeMsg": pushmsg,
                                "createdTimeStamp": int(createdTimestamp),
                                "userTypeStatus": userTypeStatus,
                                "dry_run": False,
                                "time_to_live": 3600,
                                "badge": "1",
                                "extra": {
                                    "uniqId": uniqId,
                                    # "deviceType": i['mobileDevices']['deviceType'] if 'mobileDevices' in i else 0,
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
                                "data": {
                                    "title": data_title,
                                    "msg": data_body,
                                    "imageurl": image_url,
                                },
                                "collapse_key": 'your_collapse_key',
                                "priority": 'high',
                                "delay_while_idle": True,
                                "notificationType": int(data_type),
                                "notificationTypeMsg": pushmsg,
                                "createdTimeStamp": int(createdTimestamp),
                                "userTypeStatus": userTypeStatus,
                                "dry_run": False,
                                "time_to_live": 3600,
                                "badge": "1",
                                "extra": {
                                    "uniqId": uniqId,
                                    # "deviceType": i['mobileDevices']['deviceType'] if 'mobileDevices' in i else 0,
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
                        if connection.is_open:
                            print('OK')
                        # print("Body: ", body)   
                        channel.basic_publish(exchange='', routing_key=rabbit_queue,
                                            body=json.dumps(body))
                    else:
                        print("pushToken not found")
                        pass
            
            connection.close()
            message = {
                "message": "Data In Processing"
            }
            return JsonResponse(message, safe=False, status=200)
        except:
            error_message = {
                "data": [],
                "message": "Exception occured",
            }
            return JsonResponse(error_message, status=500)
