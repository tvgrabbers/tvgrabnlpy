#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Python 3 compatibility
from __future__ import unicode_literals
# from __future__ import print_function

# Modules we need
import sys, locale, traceback, json
import time, datetime, pytz
import tv_grab_config, tv_grab_fetch, sources
try:
    unichr(42)
except NameError:
    unichr = chr    # Python 3

# check Python version
if sys.version_info[:3] < (2,7,9):
    sys.stderr.write("tv_grab_nl_py requires Pyton 2.7.9 or higher\n")
    sys.exit(2)

if sys.version_info[:2] >= (3,0):
    sys.stderr.write("tv_grab_nl_py does not yet support Pyton 3 or higher.\nExpect errors while we proceed\n")

locale.setlocale(locale.LC_ALL, '')

if tv_grab_config.Configure().version()[1:4] < (1,0,0):
    sys.stderr.write("tv_grab_nl_py requires tv_grab_config 1.0.0 or higher\n")
    sys.exit(2)

class Configure(tv_grab_config.Configure):
    def __init__(self):
        self.name ='tv_grab_nl_py'
        self.datafile = 'tv_grab_nl.json'
        self.compat_text = '.tvgids.nl'
        tv_grab_config.Configure.__init__(self)
        # Version info as returned by the version function
        self.country = 'The Netherlands'
        self.description = 'Dutch/Flemish grabber combining multiple sources.'
        self.major = 3
        self.minor = 0
        self.patch = 0
        self.patchdate = u'20160208'
        self.alfa = True
        self.beta = True
        self.output_tz = pytz.timezone('Europe/Amsterdam')
        self.combined_channels_tz = pytz.timezone('Europe/Amsterdam')


# end Configure()
config = Configure()

def read_commandline(self):
    description = u"%s: %s\n" % (self.country, self.version(True)) + \
                    u"The Netherlands: %s\n" % self.version(True, True) + \
                    self.text('config', 29) + self.text('config', 30)

    parser = argparse.ArgumentParser(description = description, formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-V', '--version', action = 'store_true', default = False, dest = 'version',
                    help = self.text('config', 5, type='other'))

    parser.add_argument('-C', '--config-file', type = str, default = self.opt_dict['config_file'], dest = 'config_file',
                    metavar = '<file>', help =self.text('config', 23, (self.opt_dict['config_file'], ), type='other'))

def main():
    # We want to handle unexpected errors nicely. With a message to the log
    try:
        #~ site_tz = pytz.timezone('Europe/Amsterdam')
        #~ current_date = datetime.datetime.now(site_tz).toordinal()
        #~ for offset in range(14):
            #~ weekday = int(datetime.date.fromordinal(current_date + offset).strftime('%w'))
            #~ first_day = offset + 2 - weekday
            #~ if weekday < 2:
                #~ first_day -= 7
            #~ print weekday, first_day, datetime.date.fromordinal(current_date + first_day).strftime('%Y%m%d')


        channel =''
        #~ source = sources.tvgids_JSON(config, 0, 'source-tvgids.nl', True)
        #~ source = sources.horizon_JSON(config, 5, 'source-horizon.tv', True)
        #~ source = sources.tvgidstv_HTML(config, 1, 'source-tvgids.tv')
        #~ source = sources.npo_HTML(config, 4, 'source-npo.nl')
        #~ source = sources.vpro_HTML(config, 7, 'source-vpro.nl')
        #~ source = sources.nieuwsblad_HTML(config, 8, 'source-nieuwsblad.be')
        #~ source= sources.primo_HTML(config, 9, 'source-primo.eu')
        #~ source = sources.oorboekje_HTML(config, 12, 'source-oorboekje.nl')

        #~ source = tv_grab_fetch.FetchData(config, 0, 'source-tvgids.nl', True)
        #~ source = tv_grab_fetch.FetchData(config, 2, 'source-rtl.nl', True)
        #~ source = tv_grab_fetch.FetchData(config, 6, 'source-humo.be', True)
        #~ channel ='672816167173'
        #~ source = tv_grab_fetch.FetchData(config, 5, 'source-horizon.tv', True)
        #~ channel ='een'
        #~ source = tv_grab_fetch.FetchData(config, 10, 'source-vrt.be', True)
        #~ channel ='een'
        #~ source = tv_grab_fetch.FetchData(config, 8, 'source-nieuwsblad.be')
        #~ channel ='nederland-1'
        #~ source = tv_grab_fetch.FetchData(config, 1, 'source-tvgids.tv')
        #~ source = tv_grab_fetch.FetchData(config, 4, 'source-npo.nl')

        source = tv_grab_fetch.FetchData(config, 7, 'source-vpro.nl')
        #~ source= tv_grab_fetch.FetchData(config, 9, 'source-primo.eu')
        #~ source = tv_grab_fetch.FetchData(config, 12, 'source-oorboekje.nl')
        #~ source = tv_grab_fetch.FetchData(config, 11, 'source-virtual.nl')

        #~ source= tv_grab_fetch.FetchData(config, 9, 'source-primo.eu')

        config.validate_option('config_file')
        tz = source.site_tz
        start = int(time.mktime(datetime.datetime.now(tz).timetuple()))*1000
        end = start + (86400000 * 2)

        #~ source.print_tags = True
        #~ source.print_searchtree = True
        source.show_result = True
        sid = source.proc_id
        config.channelsource[sid] = source
        config.channelsource[sid].init_channel_source_ids()
        #~ config.channelsource[sid].get_channels()

        config.channelsource[sid].get_page_data('base',{'offset': 0, 'channel': channel, 'channelgrp': 'main', 'start':start, 'end':end})
        #~ # tvgids.tv, 1
        # nieuwsblad.be, 5
        # vrt.be, 9
        # horizon, 13
        # tvgids.nl, npo.nl, vpro.nl, primo.eu, oorboekje.nl, 2
        # rtl.nl, 6
        # humo, 3

        #~ print datetime.datetime.strptime('Mon, 22 Jun 2015 05:59:59 +0200', '%a, %d %b %Y %H:%M:%S %z')

    except:
        traceback.print_exc()
        #~ config.logging.log_queue.put({'fatal': [traceback.format_exc(), '\n'], 'name': None})
        return(99)

    # and return success
    return(0)
# end main()

# allow this to be a module
if __name__ == '__main__':
    x = main()
    config.close()
    sys.exit(x)
