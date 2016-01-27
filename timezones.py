#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import datetime

class UTCTimeZone(datetime.tzinfo):
    """UTC Timezone"""
    def tzname(self, dt):
        return unicode('UTC')

    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def dst(self, dt):
        return datetime.timedelta(0)

# end UTCTimeZone

class AmsterdamTimeZone(datetime.tzinfo):
    """Timezone information for Amsterdam"""
    def __init__(self):
        # calculate for the current year:
        year = datetime.date.today().year
        d = datetime.datetime(year, 4, 1, 2, 0)  # Starts last Sunday in March 02:00:00
        self.dston = d - datetime.timedelta(days=d.weekday() + 1)
        d = datetime.datetime(year, 11, 1, 2, 0) # Ends last Sunday in October 02:00:00
        self.dstoff = d - datetime.timedelta(days=d.weekday() + 1)

    def tzname(self, dt):
        return unicode('CET_CEST')

    def utcoffset(self, dt):
        return datetime.timedelta(hours=1) + self.dst(dt)

    def dst(self, dt):

        if self.dston <=  dt.replace(tzinfo=None) < self.dstoff:
            return datetime.timedelta(hours=1)

        else:
            return datetime.timedelta(0)
# end AmsterdamTimeZone

