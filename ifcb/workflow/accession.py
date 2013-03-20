import sys
import logging
import time
import json
import datetime

from oii.config import get_config
from oii.psql import xa
from oii.iopipes import LocalFileSource
from oii.ifcb.db import IfcbFeed, IfcbFixity
from oii.resolver import parse_stream, Solution
from oii.ifcb.formats import integrity
from oii.times import text2utcdatetime

from celery import Celery
from celery.signals import after_setup_task_logger

MODULE='oii.ifcb.workflow.accession'

celery = Celery(MODULE, broker='amqp://guest@localhost//', backend='amqp')

logger = logging.getLogger(MODULE)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

def celery_logging(**kw):
    logger = logging.getLogger(MODULE)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

after_setup_task_logger.connect(celery_logging)
    
# example config file
# resolver = oii/ifcb/mvco.xml
# [ditylum]
# psql_connect = user=foobar password=bazquux dbname=ditylum

def list_adcs(time_series,resolver,after_year=2012):
    r = parse_stream(resolver)
    for s in r['list_adcs'].resolve_all(time_series=time_series): # FIXME hardcoded
        date = time.strptime(s.date, s.date_format)
        if date.tm_year > after_year:
            yield s
        else:
            logger.info('%s SKIP, out of date range' % s.pid)

def list_new_filesets(time_series,psql_connect,resolver,after_year=2012):
    feed = IfcbFeed(psql_connect)
    r = parse_stream(resolver)
    for s in list_adcs(time_series,resolver,after_year):
        if feed.exists(s.pid):
            logger.info('%s EXISTS in time series %s' % (s.pid, time_series))
        else:
            logger.info('%s NEW, not already in time series %s' % (s.pid, time_series))
            fs = r['fileset'].resolve(pid=s.pid,product='raw',time_series=time_series,day_dir=s.day_dir)
            if fs is None:
                logger.warn('%s UNRESOLVABLE cannot find raw files' % s.pid)
            else:
                yield fs

def check_integrity(pid, hdr_path, adc_path, roi_path, schema_version):
    integrity.check_hdr(LocalFileSource(hdr_path))
    logger.info('%s PASS integrity check %s' % (pid, hdr_path))
    targets = list(integrity.check_adc(LocalFileSource(adc_path), schema_version=schema_version))
    logger.info('%s PASS integrity check %s' % (pid, adc_path))
    integrity.check_roi(LocalFileSource(roi_path), targets)
    logger.info('%s PASS integrity check %s' % (pid, roi_path))

@celery.task
def accede(config_file, time_series):
    config = get_config(config_file, time_series)
    fx = IfcbFixity(config.psql_connect)
    feed = IfcbFeed(config.psql_connect)
    with xa(config.psql_connect) as (c, db):
        for s in list_new_filesets(time_series,config.psql_connect,config.resolver,after_year=2005): # FIXME hardcoded
            try:
                check_integrity(s.pid, s.hdr_path, s.adc_path, s.roi_path, s.schema_version)
            except Exception, e:
                logger.warn('%s FAIL integrity checks: %s' % (s.pid, e))
                continue
            # hot diggity, we've got some good data
            # compute fixity
            try:
                fx.fix(s.pid, s.hdr_path, cursor=db, filetype='hdr')
                logger.info('%s FIXITY computed for %s' % (s.pid, s.hdr_path))
                fx.fix(s.pid, s.adc_path, cursor=db, filetype='adc')
                logger.info('%s FIXITY computed for %s' % (s.pid, s.adc_path))
                fx.fix(s.pid, s.roi_path, cursor=db, filetype='roi')
                logger.info('%s FIXITY computed for %s' % (s.pid, s.roi_path))
            except:
                logger.error('%s FAIL fixity cannot be computed!' % s.pid)
                c.rollback()
                continue
            # register bin
            try:
                ts = text2utcdatetime(s.date, s.date_format)
                feed.create(s.pid, ts, cursor=db)
                c.commit()
                logger.info('%s DONE' % s.pid)
            except:
                logger.error('%s FAILED' % s.pid)
                continue

if __name__=='__main__':
    try:
        config_file = sys.argv[1]
        time_series=sys.argv[2]
    except:
        sys.stderr.write('usage: [python] oii/ifcb/workflow/accession.py [config file] [time series name]\n')
        sys.exit(-1)
    accede.apply_async((config_file, time_series))
