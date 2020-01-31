module.exports = {
    apps : [{
      name: 'pushnotification',
      script: 'manage.py',
      args: 'runserver 0.0.0.0:8008',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      interpreter:'/opt/pythonApi/pushNotification/venv/bin/python3.6'
    }]
  };
  