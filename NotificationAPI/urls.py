from django.conf.urls import url
from NotificationAPI import views

app_name = "NotificationAPI"

urlpatterns = [
    url(r'^pushnotificaton/$', views.PushNotification.as_view(), name='userDetails'),
]
