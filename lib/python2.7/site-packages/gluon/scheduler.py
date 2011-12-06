USAGE = """
## Example

For any existing app

Create File: app/models/scheduler.py ======
from gluon.scheduler import Scheduler

def demo1(*args,**vars):
    print 'you passed args=%s and vars=%s' % (args, vars)
    return 'done!'

def demo2():
    1/0

scheduler = Scheduler(db,dict(demo1=demo1,demo2=demo2))
=====================================

Create File: app/modules/scheduler.py ======
scheduler.worker_loop()
=====================================

## run worker nodes with:

   python web2py.py -S app -M -N -R applications/app/modules/scheduler.py

or without web2py

   python gluon/scheduler.py -u sqlite://storage.sqlite \
                             -f applications/myapp/databases/ \
                             -t mytasks.py
(-h for info)
python scheduler.py -h

## schedule jobs using
http://127.0.0.1:8000/scheduler/appadmin/insert/db/task_scheduled

## monitor scheduled jobs
http://127.0.0.1:8000/scheduler/appadmin/select/db?query=db.task_scheduled.id>0

## view completed jobs
http://127.0.0.1:8000/scheduler/appadmin/select/db?query=db.task_run.id>0

## API
You can schedule, update and delete jobs programmatically via

   id = scheduler.db.tast_scheduler.insert(....)
   scheduler.db(query).update(...)
   scheduler.db(query).delete(...)

(using normal DAL syntax)

## Comments

Works very much like celery & django-celery in web2py with some differences:
- it has no dependendecies but web2py (a small subset of it actually)
- it is much simpler to use and runs everywhere web2py runs
  as long as you can at last one backrgound task
- it uses a database (via DAL) instead of rabbitMQ for message passing
  (this is not really a limitation for ~10 worker nodes)
- it does not allow stopping of running tasks
- it does not allow managed starting and stopping of worker nodes
- it does not allow tasksets (but it does allow a task to submit another task)
"""

import optparse
import os
import uuid
import socket
import traceback
import logging
import time
import sys
import cStringIO
import threading
from datetime import datetime, timedelta

if 'WEB2PY_PATH' in os.environ:
    sys.path.append(os.environ['WEB2PY_PATH'])

from gluon import * # needs DAL, Field, current.T and validators
from gluon.contrib.simplejson import loads,dumps


STATUSES = (QUEUED,
            ALLOCATED,
            RUNNING,
            COMPLETED,
            FAILED,
            TIMEOUT,
            OVERDUE,
            STOPPED) = ('queued',
                        'allocated',
                        'running',
                        'completed',
                        'failed',
                        'timeout',
                        'overdue',
                        'stopped')

class TYPE(object):
    """
    validator that check whether field is valid json and validate its type
    """

    def __init__(self,myclass=list,parse=False):
        self.myclass = myclass
        self.parse=parse

    def __call__(self,value):
        try:
            obj = loads(value)
        except:
            return (value,current.T('invalid json'))
        else:
            if isinstance(obj,self.myclass):
                if self.parse:
                    return (obj,None)
                else:
                    return (value,None)
            else:
                return (value,current.T('Not of type: %s') % self.myclass)


class TimeoutException(Exception): pass
class StoppedException(Exception): pass

def timeout_run(func, args=(), kwargs={}, timeout_duration=1):
    """
    runs ret=fun(*args,**kwargs) and returns (status, ret, caputured_output, traceback)
    status can be COMPLETED, FAILED, TIMEOUT, or  STOPPED
    http://code.activestate.com/recipes/473878-timeout-function-using-threading/
    """
    class InterruptableThread(threading.Thread):

        def __init__(self):
            threading.Thread.__init__(self)
            self.result = self.output = self.traceback = None

        def run(self):
            try:
                stdout, sys.stdout = sys.stdout, cStringIO.StringIO()
                self.result = func(*args, **kwargs)
                self.status = COMPLETED
            except StoppedException:
                self.status = STOPPED
            except:
                self.status = FAILED
                self.result = None
                self.traceback = traceback.format_exc()
            sys.stdout, self.output = stdout, sys.stdout.getvalue()
    it = InterruptableThread()
    it.start()
    it.join(timeout_duration)
    if it.isAlive():
        return TIMEOUT, it.result, it.output, it.traceback
    else:
        return it.status, it.result, it.output, it.traceback

class Scheduler(object):
    """
    this is the main scheduler class, it also implement the worker_loop!
    """

    def __init__(self,db,tasks,worker_name=None,migrate=True):
        if not worker_name:
            worker_name = self.guess_worker_name()
        self.db = db
        self.tasks = tasks
        self.worker_name = worker_name
        now = datetime.now()
        db.define_table(
            'task_scheduled',
            Field('name',requires=IS_NOT_EMPTY()),
            Field('group_name',default='main',writable=False),
            Field('status',requires=IS_IN_SET(STATUSES),default=QUEUED,writable=False),
            Field('func',requires=IS_IN_SET(sorted(self.tasks.keys()))),
            Field('args','text',default='[]',requires=TYPE(list)),
            Field('vars','text',default='{}',requires=TYPE(dict)),
            Field('enabled','boolean',default=True),
            Field('start_time','datetime',default=now),
            Field('next_run_time','datetime',default=now),
            Field('stop_time','datetime',default=now+timedelta(days=1)),
            Field('repeats','integer',default=1,comment="0=unlimted"),
            Field('period','integer',default=60,comment='seconds'),
            Field('timeout','integer',default=60,comment='seconds'),
            Field('times_run','integer',default=0,writable=False),
            Field('last_run_time','datetime',writable=False,readable=False),
            Field('assigned_worker_name',default=None,writable=False),
            migrate=migrate,format='%(name)s')
        db.define_table(
            'task_run',
            Field('task_scheduled','reference task_scheduled'),
            Field('status',requires=IS_IN_SET((RUNNING,COMPLETED,FAILED))),
            Field('start_time','datetime'),
            Field('output','text'),
            Field('result','text'),
            Field('traceback','text'),
            Field('worker_name',default=worker_name),
            migrate=migrate)
        db.define_table(
            'worker_heartbeat',
            Field('name'),
            Field('last_heartbeat','datetime'),
            migrate=migrate)
        try:
            current._scheduler = self
        except:
            pass

    def form(self,id,**args):
        """
        generates an entry form to submit a new task, for debugging
        """
        return SQLFORM(self.db.task_schedule,id,**args)

    def assign_next_task(self,group_names=['main']):
        """
        find next task that needs to be executed
        """
        from datetime import datetime
        db = self.db
        queued = (db.task_scheduled.status==QUEUED)
        allocated = (db.task_scheduled.status==ALLOCATED)
        due = (db.task_scheduled.enabled==True)
        due &= (db.task_scheduled.group_name.belongs(group_names))
        due &= (db.task_scheduled.next_run_time<datetime.now())
        assigned_to_me = (db.task_scheduled.assigned_worker_name==self.worker_name)
        not_assigned = (db.task_scheduled.assigned_worker_name=='')|\
            (db.task_scheduled.assigned_worker_name==None)
        # grab all queue tasks
        counter = db(queued & due & (not_assigned|assigned_to_me)).update(
            assigned_worker_name=self.worker_name,status=ALLOCATED)
        db.commit()
        if counter:
            # pick the first
            row = db(allocated & due & assigned_to_me).select(
                orderby=db.task_scheduled.next_run_time,limitby=(0,1)).first()
            # release others if any
            if row:
                row.update_record(status=RUNNING,last_run_time=datetime.now())
                db(allocated & due & assigned_to_me).update(
                    assigned_worker_name=None,status=QUEUED)
                db.commit()
        else:
            row = None
        return row

    def run_next_task(self,group_names=['main']):
        """
        get and execute next task
        """
        db = self.db
        task = self.assign_next_task(group_names=group_names)
        if task:
            logging.info('running task %s' % task.name)
            task_id = db.task_run.insert(
                task_scheduled=task.id,status=RUNNING,
                start_time=task.last_run_time)
            db.commit()
            times_run = task.times_run+1
            try:
                func = self.tasks[task.func]
                args = loads(task.args)
                vars = loads(task.vars)
                status, result, output, tb = \
                    timeout_run(func,args,vars,timeout_duration=task.timeout)
            except:
                status, result, output = FAILED, None, None
                tb = 'SUBMISSION ERROR:\n%s' % traceback.format_exc()
            next_run_time = task.last_run_time + timedelta(seconds=task.period)
            status_repeat = status
            if status==COMPLETED:
                if (not task.repeats or times_run<task.repeats) and \
                        (not next_run_time or next_run_time<task.stop_time):
                    status_repeat = QUEUED
                    logging.info('task %s %s' % (task.name,status))
            while True:
                try:
                    db(db.task_run.id==task_id).update(status=status,
                                                       output=output,
                                                       traceback=tb,
                                                       result=dumps(result))
                    task.update_record(status=status_repeat,
                                       next_run_time=next_run_time,
                                       times_run=times_run,
                                       assigned_worker_name=None)
                    db.commit()
                    return True
                except db._adapter.driver.OperationalError:
                    db.rollback()
                    # keep looping until you can log task!
        else:
            return False

    def log_heartbeat(self):
        """
        logs a worker heartbeat
        """
        db = self.db
        now = datetime.now()
        host = self.worker_name
        if not db(db.worker_heartbeat.name==host).update(last_heartbeat=now):
            db.worker_heartbeat.insert(name=host,last_heartbeat=now)
        db.commit()

    def fix_failures(self):
        """
        find all tasks that have been running than they should and sets them to OVERDUE
        """
        db = self.db
        tasks = db(db.task_scheduled.status==RUNNING).select()
        ids = [task.id for task in tasks if \
                   task.last_run_time+timedelta(seconds=task.timeout) \
                   <datetime.now()]
        db(db.task_scheduled.id.belongs(ids)).update(status=OVERDUE)
        db(db.task_scheduled.status==QUEUED).update(assigned_worker_name=None)
        db.commit()

    def cleanup_scheduled(self, statuses=[COMPLETED],expiration=24*3600):
        """
        delete task_scheduled that were completed long ago
        can be run as a task by adding:
            tasks['scheduler.cleanup_scheduled']=scheduler.cleanup_scheduled
        """
        db = self.db
        now = datetime.now()
        db(db.task_scheduled.status.belongs(statuses))\
            (db.task_scheduled.last_run_time+expiration<now).delete()
        db.commit()

    def cleanup_run(self, statuses=[COMPLETED],expiration=24*3600):
        """
        delete task_run that were completed long ago
        can be run as a task by adding:
            tasks['scheduler.cleanup_run']=scheduler.cleanup_run
        """
        db = self.db
        now = datetime.now()
        db(db.task_run.status.belongs(statuses))\
            (db.task_run.start_time+expiration<now).delete()
        db.commit()

    @staticmethod
    def guess_worker_name():
        """
        some times we may not know the name of a worker so we make one up
        """
        try:
            worker_name = current.request.env.http_host
        except:
            worker_name = socket.gethostname()
        worker_name += '#'+str(uuid.uuid4())
        return worker_name

    def worker_loop(self,
                    logger_level='INFO',
                    heartbeat=10,
                    group_names=['main']):
        """
        this implements a worker process and should only run as worker
        it loops and logs (almost) everything
        """
        db = self.db
        try:
            level = getattr(logging,logger_level)
            logging.basicConfig(format="%(asctime)-15s %(levelname)-8s: %(message)s")
            logging.getLogger().setLevel(level)
            logging.info('worker_name = %s' % self.worker_name)
            while True:
                try:
                    if 'main' in group_names:
                        self.fix_failures()
                    logging.info('checking for tasks...')
                    self.log_heartbeat()
                    while self.run_next_task(group_names=group_names): pass
                    time.sleep(heartbeat)
                except db._adapter.driver.OperationalError:
                    db.rollback()
                    # whatever happened, try again
        except KeyboardInterrupt:
            logging.info('[ctrl]+C')

def main():
    """
    allows to run worker without python web2py.py .... by simply python this.py
    """
    parser = optparse.OptionParser()
    parser.add_option("-w", "--worker_name", dest="worker_name", default=None,
                      help="start a worker with name")
    parser.add_option("-b", "--heartbeat",dest="heartbeat", default = 10,
                      help="heartbeat time in seconds (default 10)")
    parser.add_option("-L", "--logger_level",dest="logger_level",
                      default = 'INFO',
                      help="level of logging (DEBUG, INFO, WARNING, ERROR)")
    parser.add_option("-g", "--group_names",dest="group_names",
                      default = 'main',
                      help="comma separated list of groups to be picked by the worker")
    parser.add_option("-f", "--db_folder",dest="db_folder",
                      default = None,
                      help="location of the dal database folder")
    parser.add_option("-u", "--db_uri",dest="db_uri",
                      default = None,
                      help="database URI string (web2py DAL syntax)")
    parser.add_option("-m", "--db_migrate",dest="db_migrate",
                      action = "store_true", default=False,
                      help="create tables if missing")
    parser.add_option("-t", "--tasks",dest="tasks",default=None,
                      help="file containing task files, must define tasks = {'task_name':(lambda: 'output')} or similar set of tasks")
    (options, args) = parser.parse_args()
    if not options.tasks or not options.db_uri:
        print USAGE
    path,filename = os.path.split(options.tasks)
    if filename.endswith('.py'):
        filename = filename[:-3]
    sys.path.append(path)
    print 'importing tasks...'
    tasks = __import__(filename, globals(), locals(), [], -1).tasks
    print 'tasks found: '+', '.join(tasks.keys())
    group_names = [x.strip() for x in options.group_names.split(',')]
    print 'groups for this worker: '+', '.join(group_names)
    print 'connecting to database in folder: ' + options.db_folder or './'
    print 'using URI: '+options.db_uri
    db = DAL(options.db_uri,folder=options.db_folder)
    print 'instantiating scheduler...'
    scheduler=Scheduler(db = db,
                        worker_name = options.worker_name,
                        tasks = tasks,
                        migrate = options.db_migrate)
    print 'starting main worker loop...'
    scheduler.worker_loop(logger_level = options.logger_level,
                          heartbeat = options.heartbeat,
                          group_names = group_names)

if __name__=='__main__': main()


