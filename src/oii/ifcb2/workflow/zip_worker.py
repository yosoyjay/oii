import logging
import tempfile
import requests

from oii.ioutils import upload, exists

from oii.ifcb2 import get_resolver
from oii.ifcb2 import PID, LID, TS_LABEL, NAMESPACE, BIN_LID
from oii.ifcb2.workflow import RAW2BINZIP, accepts_product
from oii.ifcb2.identifiers import as_product, parse_pid
from oii.ifcb2.represent import binpid2zip

from oii.workflow import COMPLETED, AVAILABLE, ERROR
from oii.workflow.client import WorkflowClient
from oii.rbac.utils import secure_upload

from worker_config import WORKFLOW_URL, API_KEY

client = WorkflowClient(WORKFLOW_URL)

def do_binzip(pid, job):
    def log_callback(msg):
        logging.warn('BINZIP %s' % msg)
        client.heartbeat(pid,message=msg)
    if not accepts_product(pid):
        log_callback('skipping %s, not accepted' % pid)
        client.wakeup()
        return
    parsed = parse_pid(pid)
    binzip_url = '%s%s_binzip.zip' % (parsed[NAMESPACE], parsed[BIN_LID])
    log_callback('creating zipfile for %s' % pid)
    with tempfile.NamedTemporaryFile() as zip_tmp:
        zip_path = zip_tmp.name
        binpid2zip(pid, zip_path)
        # construct binzip URL
        log_callback('depositing %s' % binzip_url)
        secure_upload(zip_path, binzip_url, API_KEY)
    log_callback('deposited %s' % binzip_url)
    client.wakeup()

@wakeup_task
def binzip_wakeup(wakeup_key):
    client.do_all_work(
        roles=[RAW2BINZIP],
        callback=do_binzip,
        ttl=217,
        message='deposited bin zip')
