from celery import Celery

celergy_async='oii.workflow.async'
WAKEUP_TASK=celergy_async+'.wakeup'

"""
define a wakeup task in your worker code like this:

from oii.workflow.celergy_async import async, WAKEUP_TASK

@celergy_async.task(name=WAKEUP_TASK)
def my_function():
    pass

the function can also accept a single argument which is an arbitrary payload
although said payload must be serializable

default config is in oii.workflow.celergy_async_config

any config needs to map the celergy_async_wakeup task to a broadcast queue
like this:

from kombu.common import Broadcast

from oii.workflow.celergy_async import WAKEUP_TASK

WAKEUP_QUEUE='celergy_async_wakeup'

CELERY_QUEUES = (Broadcast(WAKEUP_QUEUE),)
CELERY_ROUTES = { WAKEUP_TASK: { 'queue': WAKEUP_QUEUE } }

(in above, does not matter what 'wakeup queue' is)

run a worker like this

celery --config=oii.workflow.celergy_async_config -A my.worker.module worker -n [unique name] [params]

params include -c n for n threads
"""

celergy_async = Celery(ASYNC)

DEFAULT_celergy_async_CONFIG_MODULE='oii.workflow.async_config'

def celergy_async_config(config_module=DEFAULT_ASYNC_CONFIG_MODULE):
    """use to configure client applications from a module,
    see configuration notes above"""
    if config_module is None:
        config_module = DEFAULT_celergy_async_CONFIG_MODULE
    celergy_async.config_from_object(config_module)

def celergy_async_wakeup(payload=None):
    """celergy_asynchronously wake up all workers. must be configged"""
    if payload is None:
        celergy_async.send_task(WAKEUP_TASK)
    else:
        celergy_async.send_task(WAKEUP_TASK, [payload])

# decorator for celergy_async.task(name=WAKEUP_TASK)
def wakeup_task(func):
    @celergy_async.task(name=WAKEUP_TASK)
    def func_wrapper(*a,**kw):
        try: # try calling with key
            func(*a,**kw)
        except TypeError:
            try:
                func(None,**kw)
            except TypeError:
                func()
    return func_wrapper

if __name__=='__main__':
    # configure client
    celergy_async_config()
    # wake up all workers
    celergy_async_wakeup()
