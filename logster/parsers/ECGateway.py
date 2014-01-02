###  A sample logster parser file that can be used to count the number
###  of response codes found in an Apache access log.
###
###  For example:
###  sudo ./logster --dry-run --output=ganglia SampleLogster /var/log/httpd/access_log
###
###

import csv
import datetime
import json
import re
import time
import sys
from StringIO import StringIO

from logster.logster_helper import MetricObject, LogsterParser
from logster.logster_helper import LogsterParsingException

DATE, TIME, APP, MSG = 2,3,4,5

class AsyncQueue(LogsterParser):
    # TODO: change this to a super class

    def __init__(self, option_string=None):
        '''Initialize any data structures or variables needed for keeping track
        of the tasty bits we find in the log we are parsing.'''
        self.count = {}
        self.exec_time = {}

    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        try:
            for data in csv.reader(StringIO(line)):
                if data[APP] != "COMPASS_ECCONNECT":
                    continue

                s_logtime = '%s %s' % (fields[DATE],fields[TIME])
                dt_logtime = datetime.datetime.strptime(s_logtime,'%m/%d/%y %H:%M:%S')
                msg = json.loads(fields[MSG])
                #print msg

                # server, jobtype
                server = msg['SERVERNAME']
                job = msg['JOBTYPE'].split('.')[-1]

                # start, end, duration
                start = datetime.datetime.strptime('%s %s' % (fields[DATE], msg['START']), '%m/%d/%y %H:%M:%S')
                end = datetime.datetime.strptime('%s %s' % (fields[DATE], msg['END']), '%m/%d/%y %H:%M:%S')
                td=end-start
                duration=int(td.total_seconds())

                # add one to number of executions of job
                try:
                    self.count[job] += 1
                except KeyError:
                    self.count[job] = 1

                # record execution time of job
                try:
                    self.exec_time[job] += duration
                except KeyError:
                    self.exec_time[job] = duration

        except Exception, e:
            raise LogsterParsingException, ""


    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''
        self.duration = duration

        result = []

        # Return a list of metrics objects
        for job, count in self.count:
            result += MetricObject("job.%s.completed" % job, count, "Jobs completeted"),
        for job, exec_time in self.exec_time:
            result += MetricObject("job.%s.execution_time" % job, exec_time, "Job execution time"),
