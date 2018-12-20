from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from datetime import timedelta
from task.tasks import temp_data_crawler, temp_update_check, temp_send_cache, send_ohlcv_cache

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'keystone.settings')

app = Celery('proj')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))

# REFERENCE: https://www.revsys.com/tidbits/celery-and-django-and-docker-oh-my/
# Celerybeat 태스크 추가/정의
from celery.schedules import crontab

app.conf.beat_schedule = {
    'update_check': {
        'task': 'task.tasks.temp_update_check',
        'schedule': crontab(minute='0', hour='17', day_of_week='mon-fri'),
        'args': (),
    },
    'data_crawler': {
        'task': 'task.tasks.temp_data_crawler',
        'schedule': crontab(minute='30', hour='2', day_of_week='tue-sat'),
        'args': (),
    },
    'send_cache': {
        'task': 'task.tasks.temp_send_cache',
        'schedule': crontab(minute='10', hour='3', day_of_week='tue-sat'),
        'args': (),
    },
    'send_cache': {
        'task': 'task.tasks.send_ohlcv_cache',
        'schedule': crontab(minute='10', hour='4', day_of_week='tue-sat'),
        'args': (),
    },
}
