#!/usr/bin/env python
# coding:utf-8
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import os, sys, time, csv, fnmatch

import pyinotify

#from plugin_stdout import pglog_saver
from plugin_redis import pglog_saver


csv.field_size_limit(20000000);

plugin_saver = pglog_saver()

oneminute_stat = []
# time, avg, max, min, cnt
data1min = [0,0,0,0,0]

class PatternNotFound(Exception):
    pass

class BreakParseCsv(Exception):
    pass

class MyEventHandler(pyinotify.ProcessEvent):
    where = 0
    work_filepattern = ''
    fpath = ''
    def my_init(self,f, w):
        self.work_filepattern = f
        self.where = w

    def process_IN_CLOSE_WRITE(self, event):
        if fnmatch.fnmatch(os.path.basename(event.pathname), os.path.basename(self.work_filepattern)):
            self.where = plugin_saver.save_pglog(event.pathname, self.where)
            self.where = 0

    def process_IN_MODIFY(self, event):
        if fnmatch.fnmatch(os.path.basename(event.pathname), os.path.basename(self.work_filepattern)):
            self.where = plugin_saver.save_pglog(event.pathname, self.where)


if __name__ == "__main__":
    if not sys.argv or len(sys.argv) != 2:
        if 'PGDATA' in os.environ:
            sys.stderr.write('Default file pattern("$PGDATA/pg_log/postgresql-*.csv") will be used!\n')
            work_filepattern = os.environ['PGDATA'] + "/pg_log/postgresql-*.csv"
        else:
            sys.stderr.write('Usage: %s "$PGDATA/pg_log/postgresql-*.csv"\n' % (sys.argv[0]))
            sys.exit(1)
    else:
        work_filepattern = sys.argv[1]

    work_dir = os.path.dirname(work_filepattern)
    if not os.path.isdir(work_dir):
        sys.stderr.write('cannot access ' + work_dir + ': No such file or directory\n')
        sys.stderr.write('HINT: set PGDATA os environment variable, ex) export PGDATA=/data\n')
        sys.exit(1)

    wm = pyinotify.WatchManager()
    wm.add_watch(work_dir, pyinotify.ALL_EVENTS, rec=True)
    sys.stderr.write('\n*** waiting file "' + os.path.basename(work_filepattern)  + '" in directory ' + work_dir + ' ***\n\n');
    sys.stderr.flush()
    eh = MyEventHandler(f=work_filepattern, w=0)
    notifier = pyinotify.Notifier(wm, eh)
    notifier.loop()
