module.exports = {
  apps : [{
    name: 'pushnotification-callback',
    script: 'NotificationReceive.py',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '1G',
    interpreter:'/opt/pythonAPI/venv/bin/python3'
  }]
};
