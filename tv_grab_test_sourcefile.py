#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Python 3 compatibility
from __future__ import unicode_literals
# from __future__ import print_function

# Modules we need
import sys, locale, traceback, json
import time, datetime, pytz
import tvgrabpyAPI
import tvgrabpyAPI.tv_grab_fetch as tv_grab_fetch

try:
    unichr(42)
except NameError:
    unichr = chr    # Python 3

# check Python version
if sys.version_info[:3] < (2,7,9):
    sys.stderr.write("tv_grab_nl_API requires Pyton 2.7.9 or higher\n")
    sys.exit(2)

if sys.version_info[:2] >= (3,0):
    sys.stderr.write("tv_grab_nl_API does not yet support Pyton 3 or higher.\nExpect errors while we proceed\n")

locale.setlocale(locale.LC_ALL, '')

if tvgrabpyAPI.version()[1:4] < (1,0,0):
    sys.stderr.write("tv_grab_nl3_py requires tv_grab_nl_API 1.0.0 or higher\n")
    sys.exit(2)

class Configure(tvgrabpyAPI.Configure):
    def __init__(self):
        self.name ='tv_grab_nl3_py'
        self.datafile = 'tv_grab_nl'
        self.compat_text = '.tvgids.nl'
        tvgrabpyAPI.Configure.__init__(self)
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
        config.validate_option('config_file')
        config.get_sourcematching_file()

        #~ channel ='een'
        # virtual.nl
        #~ source = config.init_sources(11)

        # rtl.nl
        #~ source = config.init_sources(2)

        # npo.nl
        #~ source = config.init_sources(4)

        # humo.be
        #~ source = config.init_sources(6)

        # vpro.nl
        #~ source = config.init_sources(7)

        # oorboekje.nl
        #~ source = config.init_sources(12)

        # horizon.tv
        #~ channel ='24443943146'
        #~ source = config.init_sources(5)

        # nieuwsblad.be
        #~ channel ='een'
        #~ source = config.init_sources(8)

        # vrt.be
        #~ channel ='O8'
        #~ source = config.init_sources(10)

        # tvgids.nl
        channel = '5'
        source = config.init_sources(0)

        # tvgids.tv
        #~ channel ='nederland-1'
        #~ source = config.init_sources(1)

        # primo.eu
        #~ channel = 'npo1'
        #~ channel ='een'
        #~ source = config.init_sources(9)

        source.test_output = sys.stdout
        #~ source.print_tags = True
        source.print_roottree = True
        source.show_parsing = True
        source.print_searchtree = True
        source.show_result = True

        sid = source.proc_id
        config.channelsource[sid] = source
        config.channelsource[sid].init_channel_source_ids()
        #~ tdict = config.fetch_func.checkout_program_dict()
        tdict = {}
        tdict['detail_url'] = {}
        tdict['channelid'] = config.channelsource[sid].chanids[channel]

        #~ config.channelsource[sid].get_channels()

        #~ data = config.channelsource[sid].get_page_data('base',{'offset': 2, 'channel': channel, 'channelgrp': 'rest', 'cnt-offset': 0, 'start':0, 'days':4})
        #~ config.channelsource[sid].parse_basepage(data, {'offset': 1, 'channel': channel, 'channelgrp': 'main'})

        tdict['detail_url'][sid] = '20543224'
        #~ tdict['detail_url'][sid] = '20629464'
        #~ tdict['detail_url'][sid] = ''
        #~ tdict['detail_url'][sid] = ''
        #~ tdict['detail_url'][sid] = ''
        #~ tdict['detail_url'][sid] = ''
        config.channelsource[sid].load_detailpage('detail', tdict)

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
