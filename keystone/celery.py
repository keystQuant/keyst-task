from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from datetime import timedelta
from task.tasks import (
    temp_data_crawler,
    temp_update_check,
    temp_send_cache,
    send_ohlcv_cache,
    send_mktcap_cache,
    send_buysell_cache,
    send_factor_cache,
    send_buysell_mkt,
    )

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
        'schedule': crontab(minute='0', hour='3', day_of_week='tue-sat'),
        'args': (),
    },
    'make_ohlcv_cache': {
        'task': 'task.tasks.send_ohlcv_cache',
        'schedule': crontab(minute='45', hour='4', day_of_week='tue-sat'),
        'args': (),
    },
    'make_mktcap_cache': {
        'task': 'task.tasks.send_mktcap_cache',
        'schedule': crontab(minute='45', hour='4', day_of_week='tue-sat'),
        'args': (),
    },
    'make_buysell_cache': {
        'task': 'task.tasks.send_buysell_cache',
        'schedule': crontab(minute='45', hour='4', day_of_week='tue-sat'),
        'args': (),
    },
    'etf_data_crawler': {
        'task': 'task.tasks.temp_etf_crawler',
        'schedule': crontab(minute='0', hour='6', day_of_week='tue-sat'),
        'args': (),
    },
    'buysell_mkt_data_crawler': {
        'task': 'task.tasks.send_buysell_mkt',
        'schedule': crontab(minute='10', hour='6', day_of_week='tue-sat'),
        'args': (),
    },
    'make_factor_cache': {
        'task': 'task.tasks.send_factor_cache',
        'schedule': crontab(minute='0', hour='1', day_of_week='sun'),
        'args': (),
    },
}
