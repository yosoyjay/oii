from oii.procutil import Process
import sys
from oii.utils import Struct, md5_file, gen_id
import os
import re
from oii.workflow.rabbit import Job, JobExit, WIN, FAIL, SKIP, PASS
from oii.workflow.workers import ProcessWorker
from oii.times import iso8601
from oii.config import get_config
from oii.psql import xa
import json

class ImageStackWorker(ProcessWorker):
    """messages should be absolute paths to image names.
    expects the following in config:
    queue_name - RabbitMQ queue basename
    amqp_host - RabbitMQ host
    imagestack_exec_path - path to imagestack executable
    in_prefix - prefix regex to strip off of output paths
    out_dir - top-level dir to deposit output"""
    def __init__(self,config):
        super(ImageStackWorker,self).__init__(config)
        script = self.get_script()
        self.imagestack = Process(script)
    def get_script(self):
        return ''
    def get_parameters(self,message):
        img_in = message
        img_out_file = re.sub(self.config.in_prefix,'',re.sub(r'\.tif','.png',img_in)).lstrip('/')
        print 'img_out_file = %s' % img_out_file
        img_out = os.path.join(self.config.out_dir,img_out_file)
        print 'img_out = %s' % img_out
        if os.path.exists(img_out):
            self.log('SKIP already exists: %s %s %s' % (self.config.out_dir, img_in, img_out))
            raise JobExit('Output file already exists: %s' % img_out, SKIP)
        od = os.path.dirname(img_out)
        if not os.path.exists(od):
            os.makedirs(od)
        return dict(img_in=img_in,img_out=img_out,start_time=iso8601())
    def win_callback(self,params):
        start_time = params['start_time']
        end_time = iso8601()
        img_in = params['img_in']
        img_out = params['img_out']
        process_id = gen_id()
        if not os.path.exists(img_out):
            self.log('FAIL output file does not exist: %s' % img_out)
            return FAIL
        in_md5 = md5_file(img_in)
        out_md5 = md5_file(img_out)
        in_length = os.stat(img_in).st_size
        out_length = os.stat(img_out).st_size
        used = {
            'process_id': process_id,
            'algorithm_id': '201203_ic', # FIXME uncontrolled
            'direction': 'used', # FIXME local id
            'pathname': img_in, # FIXME local pathname
            'no_earlier_than': start_time,
            'no_later_than': end_time,
            'fixity_md5': in_md5,
            'fixity_length': in_length
            }
        generated_by = {
            'process_id': process_id,
            'algorithm_id': '201203_ic', # FIXME uncontrolled
            'direction': 'generated by', # FIXME local id
            'pathname': img_out, # FIXME local pathname
            'no_earlier_than': start_time,
            'no_later_than': end_time,
            'fixity_md5': out_md5,
            'fixity_length': out_length
            }
        # FIXME emit provenance record
        prov_qname = '%s_prov' % self.qname
        self.enqueue(json.dumps(used), prov_qname)
        self.enqueue(json.dumps(generated_by), prov_qname)
        return WIN
    def fail_callback(self,params):
        return FAIL

class ProvenanceLogger(Job):
    def __init__(self,config):
        super(ProvenanceLogger,self).__init__('%s_prov' % config.queue_name)
        self.config = config
    def run_callback(self,message):
        try:
            r = Struct(json.loads(message))
            r.imagename = re.sub(r'.*/','',r.pathname)
            with xa(self.config.psql_connect) as (c,db):
                db.execute("set session time zone 'UTC'")
                db.execute("insert into provenance_test (process_id, algorithm_id, direction, imagename, no_earlier_than, no_later_than, fixity_md5, fixity_length) values (%s,%s,%s,%s,%s,%s,%s,%s)",(r.process_id, r.algorithm_id, r.direction, r.imagename, r.no_earlier_than, r.no_later_than, r.fixity_md5, r.fixity_length))
                c.commit()
            return WIN
        except:
            return FAIL

def enqueue_from_stdin(hl):
    batch = []
    while True:
        line = sys.stdin.readline()
        if line == '':
            break
        line = line.strip()
        batch = batch + [line]
        if len(batch) > 1000:
            print batch
            hl.enqueue(batch)
            batch = []
    hl.enqueue(batch)

def cli(job,argv):
    cmd = argv[2]
    if cmd == 'q':
        if argv[3] == '-':
            enqueue_from_stdin(job)
        else:
            job.enqueue(argv[3:])
    elif cmd == 'r':
        job.retry_failed()
    elif cmd == 'w':
        job.work()
    elif cmd == 'log':
        job.consume_log()

