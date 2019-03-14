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

client = MongoClient('mongodb://root_DB:cjdk69RvQy5b5VDL@159.203.191.182:5009/DelivX')
db = client.DelivX

credentials = pika.PlainCredentials('admin', 'cw4NNFd3nhKgcUBe')
parameters = pika.ConnectionParameters('18.228.179.231', 5672, '/', credentials, socket_timeout=300)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='PushNotifictionSend')
url = 'https://fcm.googleapis.com/fcm/send'


# headers = {"Content-Type":"application/json",
#         "Authorization": "key=AAAAdRdVxxw:APA91bGutSyKYrRpeP_SdWFpkZUWoNm5IPGqUs-ZJhUtLMj7SRMnnvieMp2s9s7HBjM1ax6Z9TNSYJqxY0AezyyK7TIaE5m3VgXe3M2CLPH0kB7jF-Uf1tAu-TNoppIqB5U-Wfcy_Q6H"}
headers = {"Content-Type":"application/json",
           "Authorization": "key=AAAARNrad8o:APA91bHkxG8dPMF-thCRkh_nZzCu9JDfEjvsQjuD7woRFtYBTM-RI7xrcq2bwW1RUv9jIIolZyPTRh2b9Rbe48jmDpd7yndpgkNvZqzkIAvddC027VdzmhMdDLvZPMsaEzRTztbSMO3z"}
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
        type = 1 for city, 2 for zone, 3 for the Cordinates(By addrss)
    '''
    def post(self, request):
        data = request.data
        data_type = data['type']
        data_body = data['body']
        data_title = data['title']
        cityId = data['cityId']
        zoneId = data['zoneId']
        image_url = data['imageUrl']
        push_type = data['pusyType']
        user_type = data['userType']
        know_more_url = data['knowMoreUrl']
        longitude = data['longitude']
        latitude = data['latitude']
        dispatchRadius = data['radius']
        if data_type != 3:
            if len(zoneId) == 0 and len(cityId)!= 0: # get the city data
                aggregateobj = [
                    {
                        '$match':
                            {
                                'cityId': {
                                    '$in': cityId
                                },
                            }
                    }
                ]
            elif len(cityId) == 0 and len(zoneId) != 0: # get the Zone data when the city is blank
                aggregateobj = [
                    {
                        '$match':
                            {
                                'zones': {
                                    '$in': zoneId
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
        print(aggregateobj)
        if int(user_type) == 1:
            response_data = db.customer.aggregate(aggregateobj)
        elif int(user_type) == 2:
            response_data = db.driver.aggregate(aggregateobj)

        uniqId = str(ObjectId()) # generate the objectid or the mongo id from here
        for i in response_data:
            print("-----------------", i['fcmTopic'])
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
                pass
        message = {
            "message": "Data In Processing"
        }
        return JsonResponse(message, safe=False, status=200)


