from celery import shared_task

import time
import requests
from .cache import RedisClient

GOBBLE_URL = 'http://45.77.31.8:3000/task/'
API_URL = 'http://45.76.202.71:3000/api/v1/stocks/task/?type='

def update_tasks(redis_client):
    task_ran = redis_client.key_exists('TASK_IN_PROGRESS')
    if not task_ran:
        return 1
    else:
        print('Redis 캐시에서 TASK_IN_PROGRESS 키값을 확인합니다.')

    task_in_progress = redis_client.get_key('TASK_IN_PROGRESS')
    task_in_progress = 1 if task_in_progress == 'True' else 0
    if not task_in_progress:
        # task가 실행중이 아니면, 실행
        return 1
    else:
        return 0

def starting_tasks(redis_client):
    redis_client.set_key('TASK_IN_PROGRESS', 'True')

def ending_tasks(redis_client):
    redis_client.set_key('TASK_IN_PROGRESS', 'False')

@shared_task
def data_update_task():
    print('데이터 수집 작업 시작')

    r = RedisClient()
    update = update_tasks(r)

    if update == 1:
        print('같은 프로세스가 현재 실행중이지 않습니다. 태스크 시작')
        starting_tasks(r)
        ### 날짜부터 업데이트한다
        ### 하지만 날짜가 업데이트되기 전에 레디스에서 그 상태를 알 수 있도록 업데이트 안 되었다고 한다
        print('날짜 정보를 수집합니다')
        r.set_key('DATE_JUST_UPDATED_TO_DB', 'False')
        task = requests.get(GOBBLE_URL + 'DATE')

        set_other_tasks_ready = False
        while not set_other_tasks_ready:
            date_just_updated_in_db = r.get_key('DATE_JUST_UPDATED_TO_DB')
            if date_just_updated_in_db == 'True':
                print('날짜가 업데이트되었습니다. 다음 태스크로 넘어갑니다.')
                set_other_tasks_ready = True
            else:
                print('dates still not updated, waiting...')
                continue
            time.sleep(4) # 4초씩 쉬고 레디스 서버에 요청을 보낸다

        print('업데이트가 필요한 정보를 DB에서 확인합니다.')
        task = requests.get(API_URL + 'SET_UPDATE_TASKS')

        if task.json()['status'] == 'DONE':
            # 날짜가 업데이트되었다면, 업데이트할 모든 데이터를 FnGuide로부터 수집해온다
            task = requests.get(GOBBLE_URL + 'FNGUIDE')

        ending_tasks(r)
    else:
        print('TASK ALREADY IN PROGRESS: 같은 태스크가 이미 실행중입니다. 조금 있다 다시 시도합니다.')
    return True
