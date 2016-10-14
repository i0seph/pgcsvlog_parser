# coding:utf-8
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import sys, json, datetime
import csvfile_common
import redis

logtype = ('panic', 'fatal', 'error', 'duration', 'slow', 'plan', 'vacuum', 'checkpoint', 'connect', 'parser_stat', 'planner_stat', 'executor_stat', 'statement_stat', 'misc')

def dump_pglog(csv_data, redis_dbs, redis_pipes, duration_arr):
    logname = None
    if csv_data['error_severity'] in ('PANIC', 'FATAL', 'ERROR'):
        logname = csv_data['error_severity'].lower()
    #checkpoint
    elif csv_data['error_severity'] == 'LOG' and 'message' in csv_data and (csv_data['message'][0:21] in ('checkpoint complete: ', 'checkpoint starting: ')):
        logname = 'checkpoint'
        k = str(redis_dbs[logname].incr('sequence'))
        redis_pipes[logname].set(k, {'log_time': csv_data['log_time']
            ,'message':  csv_data['message'][21:]
            ,'session_line_num': csv_data['session_line_num']
            ,'process_id': csv_data['process_id']
            ,'session_id': csv_data['session_id']
            ,'session_start_time': csv_data['session_start_time']})
    #vacuum
    elif csv_data['error_severity'] == 'LOG' and 'message' in csv_data and (csv_data['message'][0:10] == 'automatic '):
        logname = 'vacuum'
        k = str(redis_dbs[logname].incr('sequence'))
        redis_pipes[logname].set(k, {'log_time': csv_data['log_time']
            ,'message':  csv_data['message']
            ,'session_line_num': csv_data['session_line_num']
            ,'process_id': csv_data['process_id']
            ,'session_id': csv_data['session_id']
            ,'session_start_time': csv_data['session_start_time']});
    # except login info
    elif ('command_tag' in csv_data and csv_data['command_tag'] == 'authentication'):
        return logname;
    # except connect info
    elif ('message' in csv_data and csv_data['message'][0:21] == 'connection received: '):
        return logname;
    # disconnect (session duration)
    elif ('message' in csv_data and csv_data['message'][0:15] == 'disconnection: '):
        logname = 'connect'
        k = str(redis_dbs[logname].incr('sequence'))
        redis_pipes[logname].set(k, {'log_time': csv_data['log_time']
            ,'connection_from': csv_data['connection_from']
            , 'database_name': csv_data['database_name']
            , 'user_name': csv_data['user_name']
            , 'process_id': csv_data['process_id']
            , 'session_start_time': csv_data['session_start_time']
            , 'session_time': csv_data['message'].split(' ')[3]
            , 'session_id': csv_data['session_id']});
    elif not csv_data.get('message'):
        if 'query' in csv_data:
            del csv_data['location']
            del csv_data['error_severity']
            if 'virtual_transaction_id' in csv_data: del csv_data['virtual_transaction_id']
            logname = 'slow'
        else:
            if 'plan' in csv_data:
                logname = 'plan'
            else:
                k = None
                logname = 'duration'
                # 
                ctime = str(int(datetime.datetime.strptime(csv_data['log_time'], '%Y-%m-%d %H:%M:%S.%f %Z').timestamp()))
                csv_data['duration'] = int(csv_data['duration'])
                if not ctime in duration_arr:
                    duration_arr[ctime] = {csv_data['command_tag']: [1, csv_data['duration'], csv_data['duration'], csv_data['duration']]}
                else:
                    v = duration_arr[ctime]
                    if csv_data['command_tag'] in v:
                        cnts, vmin, vavg, vmax = v[csv_data['command_tag']]
                        vavg = round(((vavg * cnts) + csv_data['duration']) / (cnts + 1))
                        cnts += 1
                        vmin = vmin if vmin < csv_data['duration'] else csv_data['duration']
                        vmax = vmax if vmax > csv_data['duration'] else csv_data['duration']
                        duration_arr[ctime][csv_data['command_tag']] = [cnts, vmin, vavg, vmax]
                    else:
                        duration_arr[ctime][csv_data['command_tag']] =  [1, csv_data['duration'], csv_data['duration'], csv_data['duration']]
                    del v

                #update last key and set before 1 second data
                if duration_arr['lastctime'] > 0:
                    if duration_arr['lastctime'] < int(ctime):
                        if str(duration_arr['lastctime']) in duration_arr:
                            k = str(duration_arr['lastctime'])
                            redis_pipes[logname].set(k, duration_arr[str(duration_arr['lastctime'])])
                            redis_dbs[logname].set('lastkey' , str(duration_arr['lastctime']))
                            del duration_arr[str(duration_arr['lastctime'])]
                            duration_arr['lastctime'] = int(ctime)
                        else:
                            sys.stderr.write('miss data:')
                            sys.stderr.write( duration_arr['lastctime'] + '\n')
                            sys.stderr.flush()
                else:
                    duration_arr['lastctime'] = int(ctime)
        if logname != 'duration':
            k = str(redis_dbs[logname].incr('sequence'))
            redis_pipes[logname].set(k, csv_data)
    elif 'message' in csv_data and csv_data['message'] == 'PARSER STATISTICS':
        logname = 'parser_stat'
    elif 'message' in csv_data and csv_data['message'] == 'PARSE ANALYSIS STATISTICS':
        logname = 'parser_stat'
    elif 'message' in csv_data and csv_data['message'] == 'REWRITER STATISTICS':
        logname = 'parser_stat'
    elif 'message' in csv_data and csv_data['message'] == 'PLANNER STATISTICS':
        logname = 'planner_stat'
    elif 'message' in csv_data and csv_data['message'] == 'QUERY STATISTICS':
        logname = 'statement_stat'
    elif 'message' in csv_data and csv_data['message'] == 'BIND MESSAGE STATISTICS':
        logname = 'statement_stat'
    elif 'message' in csv_data and csv_data['message'] == 'EXECUTE MESSAGE STATISTICS':
        logname = 'statement_stat'
    elif 'message' in csv_data and csv_data['message'] == 'EXECUTOR STATISTICS':
        logname = 'executor_stat'
    else:
        logname = 'misc'

    if logname in ('panic', 'fatal', 'error', 'parser_stat', 'planner_stat', 'executor_stat', 'statement_stat', 'misc'):
        k = str(redis_dbs[logname].incr('sequence'))
        redis_pipes[logname].set(k, csv_data)

    if logname and k:
        tmptime = datetime.datetime.strptime(csv_data['log_time'], '%Y-%m-%d %H:%M:%S.%f %Z') + datetime.timedelta(hours=1)
        redis_pipes[logname].expireat(k,int(tmptime.timestamp()))
        
    return logname

class pglog_saver():
    global logtype
    redis_dbs = {}
    redis_pipes = {}
    # {'last':'lasttime', 'time':{'select':[0,0,0,0], 'update':[0,0,0,0], ...}}
    duration_arr = {'lastctime': 0}
    def  __init__(self):
        i = 0
        for logname in logtype:
            self.redis_dbs[logname] = redis.Redis(db=i)
            self.redis_pipes[logname] = self.redis_dbs[logname].pipeline(transaction=False)
            i += 1

    def save_pglog(self, pathname, where):
        current = where
        save_logtype = []
        for current, arr in csvfile_common.tail_f(pathname, where):
            if(len(arr) == 23):
                d, w = csvfile_common.makedict(arr,0)
                save_logname = dump_pglog(d, self.redis_dbs, self.redis_pipes, self.duration_arr)
                if (save_logname) and (not save_logname in save_logtype) : save_logtype.append(save_logname)
        if len(save_logtype) > 0:
            for logname in save_logtype:
                self.redis_pipes[logname].execute()
        return current
