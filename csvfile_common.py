# coding:utf-8
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import sys, csv
from io import StringIO

def tail_f(fn, nSeek):
    f = open(fn, 'r', encoding='latin1', newline='')
    f.seek(nSeek)
    startseek = nSeek;
    try:
        tempstr = StringIO(f.read());
    except UnicodeDecodeError as e:
        sys.stderr.write('** decode error' + str(e) + '\n')
        sys.stderr.flush()
        yield(startseek, [])
        return;
    r = csv.reader(tempstr)
    for arr in r:
        if len(arr) == 23:
            startseek = tempstr.tell() + nSeek;
            yield (startseek, arr)
        else:
            yield (startseek, arr)
            break;
    tempstr.close()
    del tempstr
    f.close()

def makedict(arr, where):
    aDicKey = [
        'log_time',
        'user_name',
        'database_name',
        'process_id',
        'connection_from',
        'session_id',
        'session_line_num',
        'command_tag',
        'session_start_time',
        'virtual_transaction_id',
        'transaction_id',
        'error_severity',
        'sql_state_code',
        'message',
        'detail',
        'hint',
        'internal_query',
        'internal_query_pos',
        'context',
        'query',
        'query_pos',
        'location',
        'application_name'];

    textcols = (
        'user_name',
        'database_name',
        'message',
        'detail',
        'hint',
        'internal_query',
        'context',
        'query',
    );

    aDict = {}
    val = None;
    for i in range(len(aDicKey)):
        if arr[i] == '': continue
        try:
            dummy  = int(arr[i])
            if dummy == 0: continue
        except ValueError as e:
            pass
        if aDicKey[i] in textcols:
            val = arr[i].encode('latin1').decode('utf8');
        else:
            val = arr[i]
        if aDicKey[i] == 'message' and val.find("duration: ") == 0:
            aDict['duration'] = str(int(round(float(val[10:10 + val[10:].find(' ')]))))
            if val.find(' ms  statement: ') > 0:
                aDict['query'] = val[val.find('statement: ') + 11:]
            elif val.find(' ms  plan:\n') > 0:
                aDict['plan'] = val[val.find('\n') + 1:]
        else:
            aDict[aDicKey[i]] = val
    del val
    return (aDict, where);
