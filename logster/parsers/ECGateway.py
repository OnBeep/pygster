###  A sample logster parser file that can be used to count the number
###  of response codes found in an Apache access log.
###
###  For example:
###  sudo ./logster --dry-run --output=ganglia SampleLogster /var/log/httpd/access_log
###
###

import csv
import datetime
import simplejson as json
import re
import time
import sys
from StringIO import StringIO

from logster.logster_helper import MetricObject, LogsterParser
from logster.logster_helper import LogsterParsingException

DATE, TIME, APP, MSG = 2,3,4,5

def total_seconds(td):
    # python2.4 backport: datetime.total_seconds()
    #  NB: need real/float division
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10.0**6

def strptime(date_string, format='%m/%d/%y %H:%M:%S'):
    # python2.4 backport: datetime.datetime.strptime(date_string,'%m/%d/%y %H:%M:%S')
    #  performing equiv from docs
    return datetime.datetime(*(time.strptime(date_string, format)[0:6]))

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

                s_logtime = '%s %s' % (data[DATE],data[TIME])
                dt_logtime = strptime(s_logtime)
                msg = json.loads(data[MSG])
                #print msg

                # server, jobtype
                server = msg['SERVERNAME']
                job = msg['JOBTYPE'].split('.')[-1]

                # start, end, duration
                start = strptime('%s %s' % (data[DATE], msg['START']))
                end = strptime('%s %s' % (data[DATE], msg['END']))
                td=end-start
                duration=int(total_seconds(td))

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
        for job, count in self.count.iteritems():
            result += MetricObject("%s.completed" % job, count, "Jobs completeted"),
        for job, exec_time in self.exec_time.iteritems():
            result += MetricObject("%s.execution_time" % job, exec_time, "Job execution time"),

        return result
