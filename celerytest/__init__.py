from celerytest.config import CELERY_TEST_CONFIG, CELERY_TEST_CONFIG_MEMORY
from celerytest.worker import CeleryWorkerThread


def setup_celery_worker(app, config=CELERY_TEST_CONFIG_MEMORY, concurrency=1):
    conf = dict(list(CELERY_TEST_CONFIG.__dict__.items()) + list(config.__dict__.items()))
    conf['CELERYD_CONCURRENCY'] = concurrency
    app.config_from_object(conf)


def start_celery_worker(app, config=CELERY_TEST_CONFIG_MEMORY, concurrency=1):
    setup_celery_worker(app, config=config, concurrency=concurrency)

    worker = CeleryWorkerThread(app)
    worker.daemon = True
    worker.start()
    worker.ready.wait()
    return worker
