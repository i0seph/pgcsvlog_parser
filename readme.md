pgcsvlog_parser
================


Real time parser for PostgreSQL csv format log to using Redis.

This code is python3.(I didn't check for python2)


Presetting
---

1. pre setting in postgresql.conf 

    log_destination = 'csvlog'
    (or)
    log_destination = 'stderr, csvlog'

2. Install Redis

Install
---

1. Make python virtualenv

    mkdir pgcsvlog_parser
    [python3/bin/]pyvenv pgcsvlog_parser
    cd pgcsvlog_parser
    git clone https://github.com/i0seph/pgcsvlog_parser app
    . bin/activate
    pip install -f app/pip_require
    cd app
    python pgcsvlog_parser.py

2. check Redis

    redis-cli info

and then, Do it yourself!
=========================
