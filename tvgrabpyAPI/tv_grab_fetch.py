#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Python 3 compatibility
from __future__ import unicode_literals
# from __future__ import print_function

import re, sys, traceback
import time, datetime, random, difflib
import httplib, socket, json, DataTreeGrab
import requests, pytz, tv_grab_IO
from threading import Thread, Lock, Semaphore, Event
from xml.sax import saxutils
from xml.etree import cElementTree as ET
from Queue import Queue, Empty
from copy import deepcopy
try:
    from html.entities import name2codepoint
except ImportError:
    from htmlentitydefs import name2codepoint

try:
    unichr(42)
except NameError:
    unichr = chr    # Python 3

class Functions():
    """Some general Fetch functions"""

    def __init__(self, config):
        self.config = config
        self.max_fetches = Semaphore(self.config.opt_dict['max_simultaneous_fetches'])
        self.count_lock = Lock()
        self.progress_counter = 0
        self.channel_counters = {}
        self.source_counters = {}
        self.source_counters['total'] = {}
        self.fetch_string_parts = re.compile("(.*?[.?!:]+ |.*?\Z)")
        self.raw_json = {}

    # end init()

    def update_counter(self, cnt_type, source_id=-1, chanid=None, cnt_add=True, cnt_change=1):
        #source_id: -1 = cache, -2 = ttvdb, -3 = jsondata
        if not isinstance(cnt_change, int) or cnt_change == 0:
            return

        if not cnt_type in ('base', 'detail', 'fail', 'lookup', 'lookup_fail', 'queue', 'jsondata', 'failjson'):
            return

        if not isinstance(cnt_change, int) or cnt_change == 0:
            return

        with self.count_lock:
            if not cnt_add:
                cnt_change = -cnt_change

            if chanid != None and isinstance(chanid, (str, unicode)):
                if not chanid in self.channel_counters.keys():
                    self.channel_counters[chanid] = {}

                if not cnt_type in self.channel_counters[chanid].keys():
                    self.channel_counters[chanid][cnt_type] = {}

                if not source_id in self.channel_counters[chanid][cnt_type].keys():
                    self.channel_counters[chanid][cnt_type][source_id] = 0

                self.channel_counters[chanid][cnt_type][source_id] += cnt_change

            if not source_id in self.source_counters.keys():
                self.source_counters[source_id] = {}

            if not cnt_type in self.source_counters[source_id].keys():
                self.source_counters[source_id][cnt_type] = 0

            self.source_counters[source_id][cnt_type] += cnt_change
            if isinstance(source_id, int) and (source_id >= 0 or source_id == -3):
                if cnt_type in self.source_counters['total'].keys():
                    self.source_counters['total'][cnt_type] += cnt_change

                else:
                    self.source_counters['total'][cnt_type] = cnt_change
    # end update_counter()

    def get_counter(self, cnt_type, source_id=-1, chanid=None):
        if chanid == None:
            if not source_id in self.source_counters.keys():
                return 0

            if not cnt_type in self.source_counters[source_id].keys():
                return 0

            return self.source_counters[source_id][cnt_type]

        elif not chanid in self.channel_counters.keys():
            return 0

        elif not cnt_type in self.channel_counters[chanid].keys():
            return 0

        elif not source_id in self.channel_counters[chanid][cnt_type].keys():
            return 0

        return self.channel_counters[chanid][cnt_type][source_id]
    # end get_counter()

    def get_page(self, url, encoding = None, accept_header = None, txtdata = None, counter = None, is_json = False):
        """
        Wrapper around get_page_internal to catch the
        timeout exception
        """
        try:
            if isinstance(url, (list, tuple)) and len(url) > 0:
                encoding = url[1] if len(url) > 1 else None
                accept_header = url[2] if len(url) > 2 else None
                txtdata = url[3] if len(url) > 3 else None
                counter = url[4] if len(url) > 4 else None
                is_json = url[5] if len(url) > 5 else False
                url = url[0]

            txtheaders = {'Keep-Alive' : '300',
                          'User-Agent' : self.config.user_agents[random.randint(0, len(self.config.user_agents)-1)] }

            if not accept_header in (None, ''):
                txtheaders['Accept'] = accept_header

            fu = FetchURL(self.config, url, txtdata, txtheaders, encoding, is_json)
            self.max_fetches.acquire()
            if isinstance(counter,(list, tuple)):
                if len(counter) == 2:
                    self.update_counter(counter[0], counter[1])

                if len(counter) >= 3:
                    self.update_counter(counter[0], counter[1], counter[2])

            fu.start()
            fu.join(self.config.opt_dict['global_timeout']+1)
            page = fu.result
            self.max_fetches.release()
            if (page == None) or (page =={}) or (isinstance(page, (str, unicode)) and ((re.sub('\n','', page) == '') or (re.sub('\n','', page) =='{}'))):
                if isinstance(counter,(list, tuple)):
                    if len(counter) == 2:
                        self.update_counter('fail', counter[1])

                    if len(counter) >= 3:
                        self.update_counter('fail', counter[1], counter[2])

                return None

            else:
                return page

        except(socket.timeout):
            self.config.log(self.config.text('fetch', 1, (self.config.opt_dict['global_timeout'], url)), 1, 1)
            if self.config.write_info_files:
                self.config.infofiles.add_url_failure('Fetch timeout: %s\n' % url)

            if isinstance(counter,(list, tuple)):
                if len(counter) == 2:
                    self.update_counter('fail', counter[1])

                if len(counter) >= 3:
                    self.update_counter('fail', counter[1], counter[2])

            self.max_fetches.release()
            return None
    # end get_page()

    def get_json_data(self, name, version = None, source = -3, url = None, fpath = None):
        self.raw_json[name] = ''
        local_name = '%s.json' % (name)
        # Try to find the source files locally
        if isinstance(version, int):
            local_name = '%s.%s.json' % (name, version)
            # First we try to get it in the supplied location
            try:
                if fpath != None:
                    fle = self.config.IO_func.open_file('%s/%s' % (fpath, local_name), 'r', 'utf-8')
                    if fle != None:
                        return json.load(fle)

            except:
                pass

            # And then in the library location if that is not the same
            try:
                if fpath != self.config.source_dir:
                    fle = self.config.IO_func.open_file('%s\%s' % (self.config.source_dir, local_name), 'r', 'utf-8')
                    if fle != None:
                        return json.load(fle)

            except:
                pass

        # Finaly we try to download unless the only_local_sourcefiles flag is set
        if not self.config.only_local_sourcefiles:
            try:
                txtheaders = {'Keep-Alive' : '300',
                              'User-Agent' : self.config.user_agents[random.randint(0, len(self.config.user_agents)-1)] }

                if url in (None, u''):
                    url = self.config.source_url

                url = '%s/%s.json' % (url, name)
                self.config.log(self.config.text('fetch', 1,(name, ), 'other'), 2)
                fu = FetchURL(self.config, url, None, txtheaders, 'utf-8', True)
                self.max_fetches.acquire()
                self.update_counter('jsondata', source)
                fu.start()
                fu.join(self.config.opt_dict['global_timeout']+1)
                page = fu.result
                self.max_fetches.release()
                if (page == None) or (page =={}) or (isinstance(page, (str, unicode)) and ((re.sub('\n','', page) == '') or (re.sub('\n','', page) =='{}'))):
                    self.update_counter('failjson', source)
                    if isinstance(version, int):
                        return None

                else:
                    self.raw_json[name] = fu.url_text
                    return page

            except:
                if isinstance(version, int):
                    return None

        # And for the two mainfiles we try to fall back to the library location
        if version == None:
            try:
                fle = self.config.IO_func.open_file('%s/%s' % (self.config.source_dir, local_name), 'r', 'utf-8')
                if fle != None:
                    return json.load(fle)

            except:
                return None

    # end get_json_data()

    def checkout_program_dict(self, tdict = None):
        """
        Checkout a given dict for invalid values or
        returnsa default empty dict for storing program info
        """
        self.text_values = ('channelid', 'source', 'channel', 'unixtime', 'prefered description', \
              'clumpidx', 'name', 'episode title', 'description', 'premiere year', \
              'originaltitle', 'subgenre', 'ID', 'merge-source', 'infourl', 'audio', 'star-rating', \
              'country', 'broadcaster')
        self.datetime_values = ('start-time', 'stop-time')
        self.timedelta_values = ('length',)
        self.date_values = ('airdate', )
        self.bool_values = ('tvgids-fetched', 'tvgidstv-fetched', 'primo-fetched', 'rerun', 'teletext', \
              'new', 'last-chance', 'premiere')
        self.num_values = ('season', 'episode', 'offset')
        self.dict_values = ('credits', 'video')
        self.source_values = ('prog_ID', 'detail_url')
        self.list_values = ('rating', )
        self.video_values = ('HD', 'widescreen', 'blackwhite')
        self.credit_values = ('director', 'actor', 'guest', 'writer', 'composer', 'presenter', 'reporter', 'commentator', 'adapter', 'producer', 'editor')

        if tdict == None:
            tdict = {}

        for key in self.text_values:
            if not key in tdict.keys() or tdict[key] == None:
                tdict[key] = u''

            try:
                if isinstance(tdict[key], str):
                    tdict[key] = unicode(tdict[key])

            except UnicodeError:
                tdict[key] = u''

        for key in self.date_values:
            if not key in tdict.keys() or tdict[key] == None:
                tdict[key] = u''

        for key in self.datetime_values:
            if not key in tdict.keys() or tdict[key] == None:
                tdict[key] = u''

        for key in self.timedelta_values:
            if not key in tdict.keys() or tdict[key] == None:
                tdict[key] = datetime.timedelta(0)

        if not 'genre' in tdict.keys() or tdict['genre'] == None or tdict['genre'] == '':
            tdict['genre'] = u'overige'

        for key in self.bool_values:
            if not key in tdict.keys() or tdict[key] != True:
                tdict[key] = False

        for key in self.num_values:
            if not key in tdict.keys() or tdict[key] == None or tdict[key] == '':
                tdict[key] = 0

        for key in self.dict_values:
            if not key in tdict.keys() or not isinstance(tdict[key], dict):
                tdict[key] = {}

        for key in self.source_values:
            if not key in tdict.keys() or not isinstance(tdict[key], dict):
                tdict[key] = {}
                for s in  self.config.source_order:
                    if not s in tdict[key] or tdict[key][s] == None:
                        tdict[key][s] = u''

                    try:
                        if not isinstance(tdict[key][s], unicode):
                            tdict[key][s] = unicode(tdict[key][s])

                    except UnicodeError:
                        tdict[key][s] = u''

        for key in self.list_values:
            if not key in tdict.keys() or tdict[key] in ('', None):
                tdict[key] = []

            #~ elif not isinstance(tdict[key], list):
                 #~ tdict[key] = [tdict[key]]

        for subkey in self.credit_values:
            if not subkey in tdict['credits'].keys() or  tdict['credits'][subkey] == None:
                tdict['credits'][subkey] = []

            for i in range(len(tdict['credits'][subkey])):
                item = tdict['credits'][subkey][i]
                if subkey in ('actor', 'guest'):
                    if not isinstance(item, dict):
                        tdict['credits'][subkey][i] = {'name': item}

                    for k, v in item.items():
                        try:
                            if not isinstance(v, unicode):
                                tdict['credits'][subkey][i][k] = unicode(v)

                        except UnicodeError:
                            tdict['credits'][subkey][i][k] = u''

                else:
                    try:
                        if not isinstance(item, unicode):
                            tdict['credits'][subkey][i] = unicode(item)

                    except UnicodeError:
                        tdict['credits'][subkey][i] = u''

        for subkey in self.video_values:
            if not subkey in tdict['video'].keys() or  tdict['video'][subkey] != True:
                tdict['video'][subkey] = False

        return tdict
    # end checkout_program_dict()

    def remove_accents(self, name):
        name = re.sub('á','a', name)
        name = re.sub('é','e', name)
        name = re.sub('í','i', name)
        name = re.sub('ó','o', name)
        name = re.sub('ú','u', name)
        name = re.sub('ý','y', name)
        name = re.sub('à','a', name)
        name = re.sub('è','e', name)
        name = re.sub('ì','i', name)
        name = re.sub('ò','o', name)
        name = re.sub('ù','u', name)
        name = re.sub('ä','a', name)
        name = re.sub('ë','e', name)
        name = re.sub('ï','i', name)
        name = re.sub('ö','o', name)
        name = re.sub('ü','u', name)
        name = re.sub('ÿ','y', name)
        name = re.sub('â','a', name)
        name = re.sub('ê','e', name)
        name = re.sub('î','i', name)
        name = re.sub('ô','o', name)
        name = re.sub('û','u', name)
        name = re.sub('ã','a', name)
        name = re.sub('õ','o', name)
        name = re.sub('@','a', name)
        return name
    # end remove_accents()

    def unescape(self, text):
        # Removes HTML or XML character references and entities from a text string.
        # source: http://effbot.org/zone/re-sub.htm#unescape-html
        #
        # @param text The HTML (or XML) source text.
        # @return The plain text, as a Unicode string

        def fixup(m):
            text = m.group(0)
            if text[:2] == "&#":
                # character reference
                try:
                    if text[:3] == "&#x":
                        return unichr(int(text[3:-1], 16))

                    else:
                        return unichr(int(text[2:-1]))

                except ValueError:
                    pass

            else:
                # named entity
                try:
                    text = unichr(name2codepoint[text[1:-1]])

                except KeyError:
                    pass

            return text # leave as is

        if not isinstance(text,(str, unicode)):
            return text

        text = re.sub("", "...", text)
        text = re.sub("", "'", text)
        text = re.sub("", "'", text)
        return unicode(re.sub("&#?\w+;", fixup, text))
    # end unescape()

    def clean_html(self, data):
        """Process characters that interfere with ElementTree processing"""
        if data == None:
            return

        data = re.sub('&quot;', ' emprsant quot;', data)
        data = re.sub('&lt;', ' emprsant lt;', data)
        data = re.sub('&gt;', ' emprsant gt;', data)
        data = self.unescape(data)
        data = re.sub('&raquo<', '<', data)
        data = re.sub('&', ' emprsant ', data)
        return data
    # end clean_html()

    def empersant(self, data):
        if data == None:
            return u''

        data = re.sub(' emprsant ', '&', data)
        data = re.sub('emprsant ', '&', data)
        data = re.sub(' emprsant', '&', data)
        data = re.sub('emprsant', '&', data)
        data = re.sub('&quot;', '"', data)
        data = re.sub('&lt;', '<', data)
        data = re.sub('&gt;', '>', data)
        if not isinstance(data, unicode):
            return unicode(data)

        return data
    # end empersant()

    def get_string_parts(self, sstring, header_items = None):
        if not isinstance(header_items, (list, tuple)):
            header_items = []

        test_items = []
        for hi in header_items:
            if isinstance(hi, (str, unicode)):
                test_items.append((hi.lower(), hi))

            elif isinstance(hi, (list, tuple)):
                if len(hi) > 0 and isinstance(hi[0], (str, unicode)):
                    hi0 = hi[0].lower()
                    if len(hi) > 1 and isinstance(hi[1], (str, unicode)):
                        hi1 = hi[1]

                    else:
                        hi1 = hi[0]

                    test_items.append((hi0, hi1))

        string_parts = self.fetch_string_parts.findall(sstring)
        string_items = {}
        act_item = 'start'
        string_items[act_item] = []
        for dp in string_parts:
            if dp.strip() == '':
                continue

            if dp.strip()[-1] == ':':
                act_item = dp.strip()[0:-1].lower()
                string_items[act_item] = []

            else:
                for ti in test_items:
                    if dp.strip().lower()[0:len(ti[0])] == ti[0]:
                        act_item = ti[1]
                        string_items[act_item] = []
                        string_items[act_item].append(dp[len(ti[0]):].strip())
                        break

                else:
                    string_items[act_item].append(dp.strip())

        return string_items
    # end get_string_parts()

    def get_offset(self, date):
        """Return the offset from today"""
        tz = self.config.fetch_timezone
        cd = tz.normalize(datetime.datetime.now(pytz.utc).astimezone(tz))
        rd = tz.normalize(date.astimezone(tz))

        return int(rd.toordinal() -  cd.toordinal())
    # end get_offset()

    def get_weekstart(self, current_date = None, offset = 0, sow = None):
        if sow == None:
            return offset

        if current_date == None:
            current_date = datetime.datetime.now(self.config.utc_tz).toordinal()

        weekday = int(datetime.date.fromordinal(current_date + offset).strftime('%w'))
        first_day = offset + sow - weekday
        if weekday < sow:
            first_day -= 7

        return first_day

    def get_datestamp(self, offset=0, tzinfo = None):
        if tzinfo == None:
            tzinfo = self.config.utc_tz

        tsnu = (int(time.time()/86400)) * 86400
        day =  datetime.datetime.fromtimestamp(tsnu)
        datenu = int(tsnu - tzinfo.utcoffset(day).total_seconds())
        if time.time() -  datenu > 86400:
            datenu += 86400

        return datenu + offset * 86400
    # end get_datestamp()

    #~ def get_timestamp(self, current_date, offset=0):
        #~ return = int(time.mktime(datetime.date.fromordinal(current_date + offset).timetuple()))

    # end get_timestamp()

    def get_datetime(self, date_string, match_string = '%Y-%m-%d %H:%M:%S', tzinfo = None, round_down = True):
        if tzinfo == None:
            tzinfo = self.config.utc_tz

        try:
            date = tzinfo.localize(datetime.datetime.strptime(date_string, match_string))
            seconds = date.second
            date = date.replace(second = 0)
            if seconds > 0 and not round_down:
                date = date + datetime.timedelta(minutes = 1)

            return self.config.utc_tz.normalize(date.astimezone(self.config.utc_tz))

        except:
            return None
    # end get_datetime()

    def merge_date_time(self, date_ordinal, date_time, tzinfo = None, as_utc = True):
        if tzinfo == None:
            tzinfo = self.config.utc_tz

        try:
            rtime = datetime.datetime.combine(datetime.date.fromordinal(date_ordinal), date_time)
            rtime = tzinfo.localize(rtime)
            if as_utc:
                rtime = self.config.utc_tz.normalize(rtime.astimezone(self.config.utc_tz))

            return rtime

        except:
            return None
    # end merge_date_time()

    def link_functions(self, fid, data=[], source = None, default = None):
        def split_kommastring(dstring):

            return re.sub('\) ([A-Z])', '), \g<1>', \
                re.sub(self.config.language_texts['and'], ', ', \
                re.sub(self.config.language_texts['and others'], '', dstring))).split(',')

        def add_person(prole, pname, palias = None):
            if not prole in credits:
                credits[prole] = []

            if prole in ('actor', 'guest'):
                #~ if isinstance(palias ,(str, unicode)):
                    #~ palias = palias.capitalize()

                p = {'name': pname, 'role': palias}
                credits[prole].append(p)

            else:
                credits[prole].append(pname)

        try:
            # strip data[1] from the end of data[0] if present and make sure it's unicode
            if fid == 0:
                if len(data) == 0:
                    if default != None:
                        return default

                    return u''

                if len(data) == 1:
                    return unicode(data[0]).strip()

                if data[0].strip().lower()[-len(data[1]):] == data[1].lower():
                    return unicode(data[0][:-len(data[1])]).strip()

                else:
                    return unicode(data[0]).strip()

            # split logo name and logo provider
            if fid == 1:
                if len(data)< 1 or data[0] == None:
                    return ('',-1)

                d = data[0].split('?')[0]
                for k, v in self.config.xml_output.logo_provider.items():
                    if d[0:len(v)] == v:
                        return (d[len(v):], k)

            # concatenate stringparts and make sure it's unicode
            if fid == 2:
                dd = u''
                for d in data:
                    if d != None:
                        try:
                            dd += unicode(d)

                        except:
                            continue

                return dd

            # Strip a channelid or prog_ID from a path
            if fid == 3:
                if len(data)< 2 or not isinstance(data[1], int) or data[0] in ('', None):
                    return default

                #~ for index in range(1, len(data)):
                return data[0].split('/')[data[1]]

            # Combine a date and time value
            if fid == 4:
                if len(data)< 3 or not isinstance (data[1], datetime.time) or not isinstance(data[2], int):
                    return default

                if not isinstance (data[0], datetime.date):
                    data[0] = datetime.date.fromordinal(source.fetch_date)

                if not isinstance (data[0], datetime.date):
                    return default

                dt = datetime.datetime.combine(data[0], data[1])
                dt = source.site_tz.localize(dt)
                dt = self.config.utc_tz.normalize(dt.astimezone(self.config.utc_tz))
                return dt.replace(second = 0, microsecond = 0)

            # Return True (or data[2]) if data[1] is present in data[0], else False (or data[3])
            if fid == 12:
                if len(data) < 2 or not isinstance(data[0], (str,unicode)) or not isinstance(data[1], (str,unicode)):
                    return False

                if data[1].lower() in data[0].lower():
                    if len(data) > 2:
                        return data[2]

                    else:
                        return True

                elif len(data) > 3:
                    return data[3]

                else:
                    return False
            # Compare the values 1 and 2 returning 3 (or True) if equal, 4 (or False) if unequal and 5 (or None) if one of them is None
            if fid == 15:
                if len(data) < 2:
                    return None

                if data[0] in (None, '') or data[1] in (None, ''):
                    if len(data) > 4:
                        rval = data[4]

                    else:
                        rval = None

                elif data[0] == data[1]:
                    if len(data) > 2:
                        rval = data[2]

                    else:
                        rval = True

                else:
                    if len(data) > 3:
                        rval = data[3]

                    else:
                        rval = False

                return rval

            # Return a string on value True
            if fid == 7:
                if len(data) < 2 or not isinstance(data[0], bool):
                    return default

                if data[0]:
                    return data[1]

                elif len(data) > 2:
                    return data[2]

                else:
                    return default

            # Return the longest not empty text value
            if fid == 8:
                if default == None:
                    text = u''
                else:
                    text = default

                if len(data) == 0:
                    return text

                for item in data:
                    if isinstance(item, (str, unicode)) and item != '':
                        if len(item) > len(text):
                            text = unicode(item.strip())

                    if isinstance(item, (list, tuple, dict)) and len(item) > 0:
                        if len(item) > len(text):
                            text = item

                return text

            # Return the first not empty value
            if fid == 13:
                if len(data) == 0:
                    return default

                for item in data:
                    if (isinstance(item, (str, unicode, list, tuple, dict)) and len(item) > 0) or \
                      (not isinstance(item, (str, unicode, list, tuple, dict)) and item != None):
                        return item

            # look for item 2 in list 0 and return the coresponding value in list1, If not found return item 3 (or None)
            if fid == 10:
                if len(data) < 3 :
                    return default

                if not isinstance(data[0], (list,tuple)):
                    data[0] = [data[0]]

                for index in range(len(data[0])):
                    data[0][index] = data[0][index].lower().strip()

                if not isinstance(data[1], (list,tuple)):
                    data[1] = [data[1]]

                if data[2].lower().strip() in data[0]:
                    index = data[0].index(data[2].lower().strip())
                    if index < len(data[1]):
                        return data[1][index]

                if len(data) > 3 :
                    return data[3]

                return default

            # look for item 1 in the keys from dict 0 and return the coresponding value
            if fid == 14:
                if len(data) < 2 or not isinstance(data[0], (list, tuple)):
                    return default

                if not isinstance(data[1], (list,tuple)):
                    data[1] = [data[1]]

                for item in data[1]:
                    for sitem in data[0]:
                        if item.lower() in sitem.keys():
                            if isinstance(sitem[item.lower()], (list, tuple)) and len(sitem[item.lower()]) == 0:
                                continue

                            if isinstance(sitem[item.lower()], (list, tuple)) and len(sitem[item.lower()]) == 1:
                                return sitem[item.lower()][0]

                            return sitem[item.lower()]

                return default

            # Extract roles from a set of lists or named dicts
            if fid == 5:
                credits = {}
                if len(data) == 0:
                    return default

                if len(data) == 1 and isinstance(data[0], (list,tuple)):
                    for item in data[0]:
                        if not isinstance(item, dict):
                            continue

                        for k, v in item.items():
                            if k.lower() in self.config.roletrans.keys():
                                role = self.config.roletrans[k.lower()]
                                for pp in v:
                                    pp = pp.split(',')
                                    for p in pp:
                                        cn = p.split('(')
                                        if len(cn) > 1:
                                            add_person(role, cn[0].strip(), cn[1].split(')')[0].strip())

                                        else:
                                            add_person(role, cn[0].strip())

                    return credits

                if len(data) < 2:
                    return default

                if isinstance(data[1], (list,tuple)):
                    for item in range(len(data[0])):
                        if item >= len(data[1]):
                            continue

                        if data[1][item].lower() in self.config.roletrans.keys():
                            role = self.config.roletrans[data[1][item].lower()]
                            if isinstance(data[0][item], (str, unicode)):
                                cast = split_kommastring(data[0][item])

                            else:
                                cast = data[0][item]

                            if isinstance(cast, (list, tuple)):
                                for person in cast:
                                    if len(data) > 2 and isinstance(data[2],(list, tuple)) and len(data[2]) > item:
                                        add_person(role, person.strip(), data[2][item])

                                    else:
                                        add_person(role, person.strip())

                elif isinstance(data[1], (str,unicode)) and data[1].lower() in self.config.roletrans.keys():
                    role = self.config.roletrans[data[1].lower()]

                    if isinstance(data[0], (str, unicode)):
                        cast = split_kommastring(data[0])

                    else:
                        cast = data[0]

                    if isinstance(cast, (list, tuple)):
                        for item in range(len(cast)):
                            if len(data) > 2 and isinstance(data[2],(list, tuple)) and len(data[2]) > item:
                                add_person(role, cast[item].strip(), data[2][item])

                            else:
                                add_person(role, cast[item].strip())

                return credits

            # Extract roles from a string
            if fid == 6:
                if len(data) == 0 or data[0] == None:
                    return {}

                if isinstance(data[0], (str, unicode)) and len(data[0]) > 0:
                    tstr = unicode(data[0])
                elif isinstance(data[0], list) and len(data[0]) > 0:
                    tstr = unicode(data[0][0])
                    for index in range(1, len(data[0])):
                        tstr = u'%s %s' % (tstr, unicode(data[0][index]))
                else:
                    return {}

                if len(data) == 1:
                    cast_items = self.get_string_parts(tstr)

                else:
                    cast_items = self.get_string_parts(tstr, data[1])

                credits = {}
                for crole, cast in cast_items.items():
                    if len(cast) == 0:
                        continue

                    elif crole.lower() in self.config.roletrans.keys():
                        role = self.config.roletrans[crole.lower()]
                        cast = split_kommastring(cast[0])

                        for cn in cast:
                            cn = cn.split('(')
                            if len(cn) > 1:
                                add_person(role, cn[0].strip(), cn[1].split(')')[0].strip())

                            else:
                                add_person(role, cn[0].strip())

                return credits

            # Process a rating item
            if fid == 9:
                rlist = []
                if len(data) == 0:
                    return rlist

                if isinstance(data[0], (str,unicode)):
                    if len(data) > 1 and data[1] == 'as_list':
                        item_length = source.data_value(2, int, data, 1)
                        unique_added = False
                        for index in range(len(data[0])):
                            code = None
                            for cl in range(item_length):
                                if index + cl >= len(data[0]):
                                    continue

                                tval = data[0][index: index + cl + 1]
                                if tval in source.rating.keys():
                                    code = source.rating[tval]
                                    break

                            if code != None:
                                if code in self.config.rating["unique_codes"].keys():
                                    if unique_added:
                                        continue

                                    rlist.append(code)
                                    unique_added = True

                                elif source.rating[code] in self.config.rating["addon_codes"].keys():
                                    rlist.append(code)

                            elif self.config.write_info_files:
                                self.config.infofiles.addto_detail_list(u'new %s rating => %s' % (source.source, code))

                    else:
                        if data[0].lower() in source.rating.keys():
                            v = source.rating[data[0].lower()]
                            if v in self.config.rating["unique_codes"].keys():
                                rlist.append(v)

                            elif v in self.config.rating["addon_codes"].keys():
                                rlist.append(v)

                        elif self.config.write_info_files:
                            self.config.infofiles.addto_detail_list(u'new %s rating => %s' % (source.source, data[0]))

                elif isinstance(data[0], (list,tuple)):
                    unique_added = False
                    for item in data[0]:
                        if item.lower() in source.rating.keys():
                            v = source.rating[item.lower()]
                            if v in self.config.rating["unique_codes"].keys():
                                if unique_added:
                                    continue

                                rlist.append(v)
                                unique_added = True

                            elif v in self.config.rating["addon_codes"].keys():
                                rlist.append(v)

                        elif self.config.write_info_files:
                            self.config.infofiles.addto_detail_list(u'new %s rating => %s' % (source.source, data[0]))

                return rlist

            # Check the text in data[1] for the presence of keywords to determine genre
            if fid == 17:
                if len(data) < 2 or not isinstance(data[0], dict):
                    return default

                for k, v in data[0].items():
                    for i in range(1, len(data)):
                        if isinstance(data[i], (str, unicode)) and k in data[i]:
                            return v

            # split a genre code in a geniric part of known length and a specific part
            if fid == 18:
                if len(data) == 0 or not isinstance(data[0],(str, unicode, list)):
                    return []

                if len(data) == 1:
                    if isinstance(data[0], list):
                        return data[0]

                    else:
                        return [data[0]]


                if isinstance(data[0], list):
                    if len(data[0]) == 0:
                        return []

                    data[0] = data[0][0]

                if not isinstance(data[1], int) or len(data[0]) <= data[1]:
                    return [data[0]]

                return [data[0][:data[1]], data[0][data[1]:]]

            # Return unlisted values to infofiles in a fid 14 dict
            if fid == 16:
                if len(data) < 2 or not isinstance(data[0], (list, tuple)):
                    return default

                if not isinstance(data[1], (list,tuple)):
                    data[1] = [data[1]]

                for index in range(len(data[1])):
                    data[1][index] = data[1][index].lower().strip()

                for sitem in data[0]:
                    for k, v in sitem.items():
                        if k.lower().strip() in data[1]:
                            continue

                        if k.lower().strip() in self.config.roletrans.keys():
                            continue

                        if self.config.write_info_files:
                            self.config.infofiles.addto_detail_list(u'new %s dataitem %s => %s' % (source.source, k, v))

            # Return unlisted values to infofiles in a fid 10 list set
            if fid == 11:
                if not self.config.write_info_files:
                    return

                if len(data) < 3 or not isinstance(data[0], (list,tuple)) or not isinstance(data[1], (list,tuple)) or not isinstance(data[2], (list,tuple)):
                    return

                for index in range(len(data[2])):
                    data[2][index] = data[2][index].lower().strip()

                for index in range(len(data[0])):
                    data[0][index] = data[0][index].lower().strip()

                for index in range(len(data[0])):
                    if data[0][index].lower() in data[2]:
                        continue

                    if data[0][index].lower() in self.config.roletrans.keys():
                        continue

                    if index >= len(data[1]):
                        self.config.infofiles.addto_detail_list(u'new %s dataitem %s' % (source.source, data[0][index]))

                    else:
                        self.config.infofiles.addto_detail_list(u'new %s dataitem %s => %s' % (source.source, data[0][index], data[1][index]))

        except:
            self.config.log([self.config.text('fetch', 69, ('link', fid, source.source)), traceback.format_exc()], 1)
            #~ self.config.log([self.config.text('fetch', 69, ('link', fid, source.source)), self.config.text('fetch', 70, (data,)), traceback.format_exc()], 1)
            return default

    # end link_functions()

    def url_functions(self, source, ptype, urlid, data={}):
        def get_dtstring(dtordinal):
            return datetime.date.fromordinal(dtordinal).strftime(udf)

        def get_timestamp(dtordinal):
            return int(time.mktime(datetime.date.fromordinal(dtordinal).timetuple())) * udm

        def get_weekday(dtordinal):
            wd = datetime.date.fromordinal(dtordinal).weekday()
            if len(wds) == 7:
                return unicode(wds[wd])

            return unicode(wd)

        try:
            udt = source.data_value([ptype, "url-date-type"], int, default=0)
            udm = source.data_value([ptype, "url-date-multiplier"], int, default=1)
            udf = source.data_value([ptype, "url-date-format"], str, default=None)
            wds = source.data_value([ptype, "weekdays"], list)
            offset = source.data_value('offset', int, data, default=0)
            start = source.data_value('start', int, data, default=self.config.opt_dict['offset'])
            days = source.data_value('days', int, data, default=self.config.opt_dict['days'])
            if urlid == 0:
                return source.data_value('detailid', unicode, data)

            elif urlid == 1:
                return source.data_value('channel', unicode, data)

            elif urlid == 2:
                cc = ''
                for c in source.data_value('channels', dict, data).values():
                    cc = '%s,%s'% (cc, c)

                return cc[1:]

            elif urlid == 3:
                return source.data_value('channelgrp', unicode, data)

            elif urlid == 4:
                cnt = source.data_value('count', int, data, default=source.item_count)
                cnt_offset = source.data_value('cnt-offset', int, data, default=0)
                cstep = cnt_offset * source.item_count
                splitter = source.data_value([ptype, "item-range-splitter"], str, default='-')
                return u'%s%s%s' % (cstep + 1, splitter, cstep  + cnt)

            elif urlid == 11:
                if udt == 0:
                    if udf not in (None, ''):
                        return get_dtstring(source.current_date + offset)

                    else:
                        return unicode(offset)

                elif udt == 1:
                    return get_timestamp(source.current_date + offset)

                elif udt == 2:
                    return get_weekday(source.current_date + offset)

            elif urlid == 12:
                if udt == 0:
                    if udf not in (None, ''):
                        return get_dtstring(source.current_date + start + days)

                    else:
                        return unicode(start + days - 1)

                elif udt == 1:
                    return get_timestamp(source.current_date + start + days)

                elif udt == 2:
                    return get_weekday(source.current_date + start + days)

            elif urlid == 13:
                if udt == 0:
                    if udf not in (None, ''):
                        return get_dtstring(source.current_date + start)

                    else:
                        return unicode(-start)

                elif udt == 1:
                    return get_timestamp(source.current_date + start)

                elif udt == 2:
                    return get_weekday(source.current_date + start)

            elif urlid == 14:
                if udt == 0:
                    if udf not in (None, ''):
                        st = get_dtstring(source.current_date + start)
                        end = get_dtstring(source.current_date + start + days)

                    else:
                        st = unicode(start)
                        end = unicode(start + days - 1)

                elif udt == 1:
                    st = get_timestamp(source.current_date + start)
                    end = get_timestamp(source.current_date + start + days)

                elif udt == 2:
                    st = get_weekday(source.current_date + start)
                    end = get_weekday(source.current_date + start + days)

                splitter = source.data_value([ptype, "date-range-splitter"], str, default='~')
                return '%s%s%s' % (st, splitter, end )

            else:
                return None

        except:
            self.config.log([self.config.text('fetch', 69, ('url', urlid, data['source'])), traceback.format_exc()], 1)
            return ''

    # end
# end Functions()

class FetchURL(Thread):
    """
    A simple thread to fetch a url with a timeout
    """
    def __init__ (self, config, url, txtdata = None, txtheaders = None, encoding = None, is_json = False):
        Thread.__init__(self)
        self.config = config
        self.url = url
        self.txtdata = txtdata
        self.txtheaders = txtheaders
        self.encoding = encoding
        self.is_json = is_json
        self.raw = ''
        self.result = None

    def run(self):
        try:
            self.result = self.get_page_internal()

        except:
            self.config.log(self.config.text('fetch', 2,  (sys.exc_info()[0], sys.exc_info()[1], self.url)), 0)
            if self.config.write_info_files:
                self.config.infofiles.add_url_failure('%s,%s:\n  %s\n' % (sys.exc_info()[0], sys.exc_info()[1], self.url))

            return None

    def find_html_encoding(self):
        # look for the text '<meta http-equiv="Content-Type" content="application/xhtml+xml; charset=UTF-8" />'
        # in the first 600 bytes of the HTTP page
        m = re.search(r'<meta[^>]+\bcharset=["\']?([A-Za-z0-9\-]+)\b', self.raw[:512].decode('ascii', 'ignore'))
        if m:
            return m.group(1)

    def get_page_internal(self):
        """
        Retrieves the url and returns a string with the contents.
        Optionally, returns None if processing takes longer than
        the specified number of timeout seconds.
        """
        try:
            url_request = requests.get(self.url, headers = self.txtheaders, params = self.txtdata, timeout=self.config.opt_dict['global_timeout']/2)
            self.raw = url_request.content
            encoding = self.find_html_encoding()
            if encoding != None:
                url_request.encoding = encoding

            elif self.encoding != None:
                url_request.encoding = self.encoding

            self.url_text = url_request.text

            if 'content-type' in url_request.headers and 'json' in url_request.headers['content-type'] or self.is_json:
                try:
                    return url_request.json()

                except:
                    return self.url_text

            else:
                return self.url_text

        except (requests.ConnectionError) as e:
            self.config.log(self.config.text('fetch', 3, (self.url, )), 1, 1)
            if self.config.write_info_files:
                self.config.infofiles.add_url_failure('URLError: %s\n' % self.url)

            return None

        except (requests.HTTPError) as e:
            self.config.log(self.config.text('fetch', 4, (self.url, e.code)), 1, 1)
            if self.config.write_info_files:
                self.config.infofiles.add_url_failure('HTTPError: %s\n' % self.url)

            return None

        except (requests.Timeout) as e:
            self.config.log(self.config.text('fetch', 5, (self.config.opt_dict['global_timeout'], self.url)), 1, 1)
            if self.config.write_info_files:
                self.config.infofiles.add_url_failure('Fetch timeout: %s\n' % self.url)

            return None

# end FetchURL

class theTVDB(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self.config = config
        self.functions = self.config.fetch_func
        self.thread_type = 'ttvdb'
        self.quit = False
        self.ready = False
        self.active = True
        self.api_key = "0629B785CE550C8D"
        self.detail_request = Queue()
        self.cache_return = Queue()
        self.source_lock = Lock()
        self.fetch_count = 0
        self.fail_count = 0
        self.config.queues['ttvdb'] = self.detail_request
        self.config.threads.append(self)

    def run(self):
        if self.config.opt_dict['disable_ttvdb']:
            return
        try:
            while True:
                if self.quit and self.detail_request.empty():
                    break

                try:
                    crequest = self.detail_request.get(True, 5)

                except Empty:
                    continue

                if (not isinstance(crequest, dict)) or (not 'task' in crequest):
                    continue

                if crequest['task'] == 'update_ep_info':
                    if not 'parent' in crequest:
                        continue

                    if 'tdict' in crequest:
                        qanswer = self.get_season_episode(crequest['parent'], crequest['tdict'])
                        if qanswer == -1:
                            self.quit = True
                            continue

                        qanswer = self.functions.checkout_program_dict(qanswer)
                        if qanswer['ID'] != '':
                            self.config.queues['cache'].put({'task':'add', 'program': qanswer})

                        with crequest['parent'].channel_lock:
                            crequest['parent'].detailed_programs.append(qanswer)

                    #~ crequest['parent'].update_counter('fetch', -1, False)
                    self.functions.update_counter('queue', -2,  crequest['parent'].chanid, False)
                    continue

                if crequest['task'] == 'last_one':
                    if not 'parent' in crequest:
                        continue

                    crequest['parent'].detail_data.set()

                if crequest['task'] == 'quit':
                    self.quit = True
                    continue

        except:
            self.config.queues['log'].put({'fatal': [traceback.format_exc(), '\n'], 'name': 'theTVDB'})
            self.ready = True
            return(98)

    def query_ttvdb(self, ftype='seriesid', title=None, lang='nl', chanid=None):
        if title == None:
            return

        base_url = "http://www.thetvdb.com"
        api_key = '0BB856A59C51D607'
        if isinstance(title, (int, str)):
            title = unicode(title)

        #~ title = urllib.quote(title.encode("utf-8"))
        if ftype == 'seriesid':
            if not lang in ('all', 'cs', 'da', 'de', 'el', 'en', 'es', 'fi', 'fr', 'he', 'hr', 'hu', 'it',
                                'ja', 'ko', 'nl', 'no', 'pl', 'pt', 'ru', 'sl', 'sv', 'tr', 'zh'):
                lang = 'en'

            #~ data = self.functions.get_page('%s/api/GetSeries.php?seriesname=%s&language=%s' % (base_url, title, lang), 'utf-8')
            txtdata = {'seriesname': title, 'language': lang}
            url = '%s/api/GetSeries.php' % base_url

        elif ftype == 'episodes':
            if not lang in ('cs', 'da', 'de', 'el', 'en', 'es', 'fi', 'fr', 'he', 'hr', 'hu', 'it',
                                'ja', 'ko', 'nl', 'no', 'pl', 'pt', 'ru', 'sl', 'sv', 'tr', 'zh'):
                lang = 'en'

            txtdata = None
            url = "%s/api/%s/series/%s/all/%s.xml" % (base_url, api_key, title, lang)

        elif ftype == 'seriesname':
            txtdata = None
            url = "%s/api/%s/series/%s/en.xml" % (base_url, api_key, title)

        else:
            return

        counter = ['detail', -2, chanid]
        data = self.functions.get_page(url, 'utf-8', None, txtdata, counter)
        # be nice to the source site
        time.sleep(random.randint(self.config.opt_dict['nice_time'][0], self.config.opt_dict['nice_time'][1]))
        if data != None:
            return ET.fromstring(data.encode('utf-8'))

    def get_all_episodes(self, tid, lang='nl', chanid=None):
        self.config.queues['cache'].put({'task':'query', 'parent': self, \
                'ep_by_id': {'tid': int(tid), 'sid': 0, 'eid': 0}})
        eps = self.cache_return.get(True)
        if eps == 'quit':
            self.ready = True
            return -1

        known_eps = {}
        for e in eps:
            if not (e['sid'],e['eid'],e['lang']) in known_eps.keys():
                known_eps[(e['sid'],e['eid'],e['lang'])] = []

            known_eps[(e['sid'],e['eid'],e['lang'])].append((e['title'],e['description']))

        try:
            eps = []
            langs = ('nl', 'en') if lang in ('nl', 'en') else (lang, 'nl', 'en')
            for l in langs:
                xmldata = self.query_ttvdb('episodes', tid, l, chanid)
                if xmldata == None:
                    # No data
                    continue

                for e in xmldata.findall('Episode'):
                    sid = e.findtext('SeasonNumber')
                    if sid == None or sid == '':
                        continue

                    eid = e.findtext('EpisodeNumber')
                    if eid == None or eid == '':
                        continue

                    title = e.findtext('EpisodeName')
                    if title == None or title == '':
                        title = 'Episode %s' % eid

                    airdate = e.findtext('FirstAired')

                    desc = e.findtext('Overview')
                    if desc == None:
                        desc == ''

                    if not (int(sid), int(eid), l) in known_eps.keys() or (title, desc) not in known_eps[(int(sid), int(eid), l)]:
                        eps.append({'tid': int(tid), 'sid': int(sid), 'eid': int(eid), 'title': title, 'airdate': airdate, 'lang': l, 'description': desc})

        except:
            self.config.log([self.config.text('fetch', 6), traceback.format_exc()])
            return

        self.config.queues['cache'].put({'task':'add', 'episode': eps})

    def get_ttvdb_id(self, title, lang='nl', search_db=True, chanid=None):
        get_id = False
        if search_db:
            self.config.queues['cache'].put({'task':'query_id', 'parent': self, 'ttvdb': {'title': title}})
            tid = self.cache_return.get(True)
            if tid == 'quit':
                self.ready = True
                return -1

            if tid != None:
                if ((datetime.date.today() - tid['tdate']).days > 30):
                    if (tid['tid'] == '' or int(tid['tid']) == 0):
                        # we try again to get an ID
                        get_id = True

                elif (tid['tid'] == '' or int(tid['tid']) == 0):
                    # Return failure
                    return 0

                else:
                    # We'll  use the episode info in the database
                    return tid

            else:
                # It's  not jet known
                get_id = True

        langs = ('nl', 'en') if lang in ('nl', 'en') else (lang, 'nl', 'en')
        if get_id or not search_db:
            # First we look for a known alias
            self.config.queues['cache'].put({'task':'query_id', 'parent': self, 'ttvdb_alias': {'alias': title}})
            alias = self.cache_return.get(True)
            if alias == 'quit':
                self.ready = True
                return -1

            series_name = title if alias == None else alias['title']
            try:
                xmldata = self.query_ttvdb('seriesid', series_name, lang, chanid)
                if xmldata == None:
                    # No data
                    self.config.queues['cache'].put({'task':'add', 'ttvdb': {'tid': 0, 'title': series_name, 'langs': langs}})
                    return 0

                tid = xmldata.findtext('Series/seriesid')
                if tid == None:
                    # No data
                    self.config.queues['cache'].put({'task':'add', 'ttvdb': {'tid': 0, 'title': series_name, 'langs': langs}})
                    return 0

                self.config.queues['cache'].put({'task':'add', 'ttvdb': {'tid': int(tid), 'title': series_name, 'langs': langs}})
                #We look for aliasses
                xmldata = self.query_ttvdb('seriesid', series_name, 'all', chanid)
                if xmldata!= None:
                    alias_list = []
                    for s in xmldata.findall('Series'):
                        t = s.findtext('SeriesName')
                        if s.findtext('seriesid') == tid and t.strip().lower()  != series_name.strip().lower() and t not in alias_list:
                            alias_list.append(s.findtext('SeriesName'))

                    if len(alias_list) > 1:
                        self.config.queues['cache'].put({'task':'add', 'ttvdb_alias': {'title':series_name, 'alias': alias_list}})

                    elif len(alias_list) == 1:
                        self.config.queues['cache'].put({'task':'add', 'ttvdb_alias': {'title':series_name, 'alias': alias_list[0]}})

            except:
                self.config.log([self.config.text('fetch', 7), traceback.format_exc()])
                return 0

        # And we retreive the episodes
        if self.get_all_episodes(tid, lang, chanid) == -1:
            return -1

        return {'tid': int(tid), 'tdate': datetime.date.today(), 'title': series_name}

    def get_season_episode(self, parent = None, data = None):
        if self.config.opt_dict['disable_ttvdb'] or parent.opt_dict['disable_ttvdb']:
            return data

        if data == None:
            return

        if data['episode title'][0:27].lower() == 'geen informatie beschikbaar':
            return data

        if parent != None and parent.group == 6:
            # We do not lookup for regional channels
            return data

        elif parent != None and parent.group == 4:
            tid = self.get_ttvdb_id(data['name'], 'de', chanid = parent.chanid)

        elif parent != None and parent.group == 5:
            tid = self.get_ttvdb_id(data['name'], 'fr', chanid = parent.chanid)

        else:
            tid = self.get_ttvdb_id(data['name'], chanid = parent.chanid)

        if tid == -1:
            return -1

        if tid == None or tid == 0:
            if parent != None:
                self.functions.update_counter('lookup_fail', -2, parent.chanid)

            self.config.log(self.config.text('fetch', 8, (data['name'], data['channel'])), 128)
            return data

        # First we just look for a matching subtitle
        tid = tid['tid']
        self.config.queues['cache'].put({'task':'query', 'parent': self, \
                'ep_by_title': {'tid': tid, 'title': data['episode title']}})
        eid = self.cache_return.get(True)
        if eid == 'quit':
            self.ready = True
            return -1

        if eid != None:
            if parent != None:
                self.functions.update_counter('lookup', -2, parent.chanid)

            data['season'] = eid['sid']
            data['episode'] = eid['eid']
            if isinstance(eid['airdate'], (datetime.date)):
                data['airdate'] = eid['airdate']

            self.config.log(self.config.text('fetch', 9, (data['name'], data['episode title'])), 24)
            return data

        # Now we get a list of episodes matching what we already know and compare with confusing characters removed
        self.config.queues['cache'].put({'task':'query', 'parent': self, \
                'ep_by_id': {'tid': tid, 'sid': data['season'], 'eid': data['episode']}})
        eps = self.cache_return.get(True)
        if eps == 'quit':
            self.ready = True
            return -1

        subt = re.sub('[-,. ]', '', self.functions.remove_accents(data['episode title']).lower())
        ep_dict = {}
        ep_list = []
        for ep in eps:
            s = re.sub('[-,. ]', '', self.functions.remove_accents(ep['title']).lower())
            ep_list.append(s)
            ep_dict[s] = {'sid': ep['sid'], 'eid': ep['eid'], 'airdate': ep['airdate'], 'title': ep['title']}
            if s == subt:
                if parent != None:
                    self.functions.update_counter('lookup', -2, parent.chanid)

                data['episode title'] = ep['title']
                data['season'] = ep['sid']
                data['episode'] = ep['eid']
                if isinstance(ep['airdate'], (datetime.date)):
                    data['airdate'] = ep['airdate']

                self.config.log(self.config.text('fetch', 9, (data['name'], data['episode title'])), 24)
                return data

        # And finally we try a difflib match
        match_list = difflib.get_close_matches(subt, ep_list, 1, 0.7)
        if len(match_list) > 0:
            if parent != None:
                self.functions.update_counter('lookup', -2, parent.chanid)

            ep = ep_dict[match_list[0]]
            data['episode title'] = ep['title']
            data['season'] = ep['sid']
            data['episode'] = ep['eid']
            if isinstance(ep['airdate'], (datetime.date)):
                data['airdate'] = ep['airdate']

            self.config.log(self.config.text('fetch', 9, (data['name'], data['episode title'])), 24)
            return data

        if parent != None:
            self.functions.update_counter('lookup_fail', -2, parent.chanid)

        self.config.log(self.config.text('fetch', 10, (data['name'], data['episode title'], data['channel'])), 128)
        return data

    def check_ttvdb_title(self, series_name, lang='nl'):
        if self.config.opt_dict['disable_ttvdb']:
            return(-1)

        langs = ['nl', 'en', 'de', 'fr']
        if lang in ('cs', 'da', 'el', 'es', 'fi', 'he', 'hr', 'hu', 'it',
                                'ja', 'ko', 'no', 'pl', 'pt', 'ru', 'sl', 'sv', 'tr', 'zh'):
            langs.append(lang)

        # Check if a record exists
        self.config.queues['cache'].put({'task':'query_id', 'parent': self, 'ttvdb': {'title': series_name}})
        tid = self.cache_return.get(True)
        if tid == 'quit':
            self.ready = True
            return(-1)

        if tid != None:
            print('The series "%s" is already saved under ttvdbID: %s -> %s' % (series_name,  tid['tid'], tid['title']))
            print('    for the languages: %s\n' % tid['langs'])
            old_tid = int(tid['tid'])
            for l in tid['langs']:
                if l not in langs:
                    langs.append(lang)

        else:
            print('The series "%s" is not jet known!\n' % (series_name))
            old_tid = -1

        try:
            xmldata = self.query_ttvdb('seriesid', series_name, lang)
            if xmldata == None or xmldata.find('Series') == None:
                print('No match for %s is found on theTVDB.com' % series_name)
                return(0)

            series_list = []
            for s in xmldata.findall('Series'):
                if not {'sid': s.findtext('seriesid'), 'name': s.findtext('SeriesName')} in series_list:
                    series_list.append({'sid': s.findtext('seriesid'), 'name': s.findtext('SeriesName')})

            print("theTVDB Search Results:")
            for index in range(len(series_list)):
                print("%3.0f -> %9.0f: %s" % (index+1, int(series_list[index]['sid']), series_list[index]['name']))

            # Ask to select the right one
            while True:
                try:
                    print("Enter choice (first number, q to abort):")
                    ans = raw_input()
                    selected_id = int(ans)-1
                    if 0 <= selected_id < len(series_list):
                        break

                except ValueError:
                    if ans.lower() == "q":
                        return(0)

            tid = series_list[selected_id]
            # Get the English name
            xmldata = self.query_ttvdb('seriesname', tid['sid'])
            ename = xmldata.findtext('Series/SeriesName')
            if ename == None:
                ename = tid['name']

            if old_tid != int(tid['sid']):
                print('Removing old instance')
                self.config.queues['cache'].put({'task':'delete', 'ttvdb': {'tid': old_tid}})

            self.config.queues['cache'].put({'task':'add', 'ttvdb': {'tid': int(tid['sid']), 'title': ename, 'langs': langs}})
            aliasses = []
            if ename.lower() != tid['name'].lower():
                aliasses.append(tid['name'])

            if ename.lower() != series_name.lower() and tid['name'].lower() != series_name.lower():
                aliasses.append(series_name)

            if len(aliasses) > 0:
                # Add an alias record
                self.config.queues['cache'].put({'task':'add', 'ttvdb_alias': {'tid': int(tid['sid']), 'title': ename, 'alias': aliasses}})
                if len(aliasses) == 2:
                    print('Adding "%s" under aliasses "%s" and "%s" as ttvdbID: %s to the database for lookups!' \
                                % (ename, aliasses[0], aliasses[1],  tid['sid']))

                else:
                    print('Adding "%s" under alias "%s" as ttvdbID: %s to the database for lookups!' \
                                % (ename, aliasses[0],  tid['sid']))

            else:
                print('Adding "%s" ttvdbID: %s to the database for lookups!' % (ename,  tid['sid']))

        except:
            traceback.print_exc()
            return(-1)

        if self.get_all_episodes(int(tid['sid']), langs) == -1:
            return(-1)

        return(0)

# end theTVDB

class FetchData(Thread):
    """
    Generic Class to fetch the data

    The output is a list of programming in order where each row
    contains a dictionary with program information.
    It runs as a separate thread for every source
    """
    def __init__(self, config, proc_id, source_data, cattrans_type = None):
        Thread.__init__(self)
        # Flag to stop the thread
        self.config = config
        self.functions = self.config.fetch_func
        self.thread_type = 'source'
        self.quit = False
        self.ready = False
        self.active = True
        # The ID of the source
        self.proc_id = proc_id
        self.detail_request = Queue()
        self.cache_return = Queue()
        self.source_lock = Lock()

        self.all_channels = {}
        self.channels = {}
        self.chanids = {}
        self.channel_loaded = {}
        self.day_loaded = {}
        self.program_data = {}
        self.chan_count = 0
        self.base_count = 0
        self.detail_count = 0
        self.fail_count = 0
        self.config.queues['source'][self.proc_id] = self.detail_request
        self.config.threads.append(self)
        self.fetch_date = None
        self.site_tz = self.config.utc_tz
        self.item_count = 0
        self.current_item_count = 0
        self.total_item_count = 0
        self.groupitems = {}

        self.test_output = sys.stdout
        self.print_tags = False
        self.print_roottree = False
        self.print_searchtree = False
        self.show_parsing = False
        self.show_result = False
        self.cattrans = {}
        self.new_cattrans = None
        self.cattrans_type = cattrans_type

        try:
            self.source_data = source_data
            self.source = self.data_value('name', str)
            self.config.sourceid_by_name[self.source] = self.proc_id
            self.detail_id = self.data_value('detail_id', str, default = '%s-ID' % self.source)
            self.detail_url = self.data_value('detail_url', str, default = '%s-url' % self.source)
            self.detail_processor = self.data_value('detail_processor', bool, default = False)
            self.detail_check = self.data_value('detail_check', str)
            self.without_full_timings = self.data_value('without-full-timings', bool, default = False)
            self.no_genric_matching = self.data_value('no_genric_matching', list)
            self.empty_channels = self.data_value('empty_channels', list)
            self.alt_channels = self.data_value('alt-channels', dict)
            cattrans = self.data_value('cattrans', dict)
            for k, v in cattrans.items():
                if isinstance(v, dict):
                    self.cattrans[k.lower().strip()] ={}
                    for k2, gg in v.items():
                        self.cattrans[k.lower().strip()][k2.lower().strip()] = gg

                else:
                    self.cattrans[k.lower().strip()] = v

            self.cattrans_keywords = self.data_value('cattrans_keywords', dict)
            self.rating = self.data_value('rating',dict)
            self.site_tz = pytz.timezone(self.data_value('site-timezone', str, default = 'utc'))
            self.night_date_switch = self.data_value('night-date-switch', int, default = 0)
            self.item_count = self.data_value(['base', 'default-item-count'], int, default=0)
            if self.detail_processor:
                if self.proc_id not in self.config.detail_sources:
                    self.detail_processor = False

                if self.is_data_value('detail', dict) or self.is_data_value('detail2', dict):
                    self.detail_keys = list(self.data_value(['detail', 'values'], dict).keys())
                    self.detail2_keys = list(self.data_value(['detail2', 'values'], dict).keys())

                else:
                    self.detail_processor = False

            elif self.proc_id in self.config.detail_sources:
                self.config.detail_sources.remove(self.proc_id)

        except:
            self.config.validate_option('disable_source', value = self.proc_id)
            traceback.print_exc()

    def run(self):
        """The grabing thread"""
        self.testlist = ((1, 0), (9, 0,), (1, 9))
        def check_queue():
            # If the queue is empty
            if self.detail_request.empty():
                time.sleep(random.randint(self.config.opt_dict['nice_time'][0], self.config.opt_dict['nice_time'][1]))
                # and if we are not tvgids.nl we wait for followup requests from other failures failures
                for q_no in self.testlist:
                    if (self.proc_id == q_no[0]) and self.config.channelsource[q_no[1]].is_alive():
                        return 0

                # Check if all channels are ready
                for channel in self.config.channels.values():
                    if channel.is_alive() and not channel.detail_data.is_set():
                        break

                # All channels are ready, so if there is nothing in the queue
                else:
                    self.ready = True
                    return -1

                # OK we have been sitting idle for 30 minutes, So we tell all channels they won get anything more!
                if (datetime.datetime.now() - self.lastrequest).total_seconds() > idle_timeout:
                    if self.proc_id == 1:
                        for chanid, channel in self.config.channels.items():
                            if channel.is_alive() and not channel.detail_data.is_set():
                                #~ print channel.statetext
                                d = 0
                                for s in self.config.detail_sources:
                                    d += self.functions.get_counter('queue', s, chanid)

                                channel.detail_data.set()
                                self.config.log([self.config.text('fetch', 11, (channel.chan_name, d, self.source)), self.config.text('fetch', 12)])

                    self.ready = True
                    return -1

                else:
                    return 0

            self.lastrequest = datetime.datetime.now()
            try:
                return self.detail_request.get()

            except Empty:
                return 0

        def check_ttvdb(tdict, parent):
            if not (self.config.opt_dict['disable_ttvdb'] or parent.opt_dict['disable_ttvdb']) and \
              tdict['genre'].lower() == u'serie/soap' and tdict['episode title'] != '' and tdict['season'] == 0:
                # We do a ttvdb lookup
                #~ parent.update_counter('fetch', -1)
                self.functions.update_counter('queue', -2,  parent.chanid, False)
                self.config.queues['ttvdb'].put({'tdict':tdict, 'parent': parent, 'task': 'update_ep_info'})

            else:
                with parent.channel_lock:
                    parent.detailed_programs.append(tdict)

        def check_other_sources(tdict, cache_id, logstring, parent):
            cached_program = None
            if (self.proc_id in (0, 9)) and (cache_id != None):
                # Check the cache again
                self.config.queues['cache'].put({'task':'query', 'parent': self, 'pid': cache_id})
                cached_program = self.cache_return.get(True)
                if cached_program == 'quit':
                    self.ready = True
                    return -1

            for q_no in self.testlist:
                if cached_program != None and self.proc_id == q_no[1] and \
                  cached_program[self.config.channelsource[q_no[0]].detail_check]:
                    self.config.log(self.config.text('fetch', 18, (parent.chan_name, parent.get_counter(), logstring)), 8, 1)
                    tdict= parent.use_cache(tdict, cached_program)
                    #~ parent.update_counter('fetch', self.proc_id, False)
                    self.functions.update_counter('detail', -1, parent.chanid)
                    self.functions.update_counter('queue', self.proc_id,  parent.chanid, False)
                    check_ttvdb(tdict, parent)
                    return 0

                # If there is an url we'll try tvgids.tv
                elif self.proc_id == q_no[1] and self.config.channelsource[q_no[0]].detail_processor and \
                  q_no[0] not in parent.opt_dict['disable_detail_source'] and \
                  tdict['detail_url'][q_no[0]] != '':
                    self.config.queues['source'][q_no[0]].put({'tdict':tdict, 'cache_id': cache_id, 'logstring': logstring, 'parent': parent, 'last_one': False})
                    #~ parent.update_counter('fetch', q_no[0])
                    #~ parent.update_counter('fetch', self.proc_id, False)
                    self.functions.update_counter('queue', q_no[0],  parent.chanid)
                    self.functions.update_counter('queue', self.proc_id,  parent.chanid, False)
                    return 0

        # First some generic initiation that couldn't be done earlier in __init__
        # Specifics can be done in init_channels and init_json which are called here
        #~ tdict = self.functions.checkout_program_dict()
        tdict = {}
        idle_timeout = 1800
        try:
            # Check if the source is not deactivated and if so set them all loaded
            if self.proc_id in self.config.opt_dict['disable_source']:
                for chanid in self.channels.keys():
                    self.channel_loaded[chanid] = True
                    self.config.channels[chanid].source_ready(self.proc_id).set()

                self.ready = True

            else:
                self.day_loaded[0] = {}
                for day in range( self.config.opt_dict['offset'], (self.config.opt_dict['offset'] + self.config.opt_dict['days'])):
                    self.day_loaded[0][day] = False

                for chanid in self.config.channels.keys():
                    self.channel_loaded[chanid] = False
                    self.day_loaded[chanid] ={}
                    for day in range( self.config.opt_dict['offset'], (self.config.opt_dict['offset'] + self.config.opt_dict['days'])):
                        self.day_loaded[chanid][day] = False

                    self.program_data[chanid] = []

                self.init_channel_source_ids()
                # Load and proccess al the program pages
                self.load_pages()

                # if this is the prefered description source set the value
                with self.source_lock:
                    for chanid in self.channels.keys():
                        if self.config.channels[chanid].opt_dict['prefered_description'] == self.proc_id:
                            for i in range(len(self.program_data[chanid])):
                                self.program_data[chanid][i]['prefered description'] = self.program_data[chanid][i]['description']

            if self.config.write_info_files:
                self.config.infofiles.check_new_channels(self, self.config.source_channels)


        except:
            self.config.queues['log'].put({'fatal': ['While fetching the base pages\n', \
                traceback.format_exc(), '\n'], 'name': self.source})

            self.ready = True
            return(98)

        try:
            if self.detail_processor and  not self.proc_id in self.config.opt_dict['disable_detail_source']:
                # We process detail requests, so we loop till we are finished
                self.cookyblock = False
                self.lastrequest = datetime.datetime.now()
                while True:
                    if self.quit:
                        self.ready = True
                        break

                    queue_val = check_queue()
                    if queue_val == -1:
                        break

                    if queue_val == 0 or not isinstance(queue_val, dict):
                        continue

                    tdict = queue_val
                    parent = tdict['parent']
                    # Is this the closing item for the channel?
                    if ('last_one' in tdict) and tdict['last_one']:
                        if self.proc_id == 0 and self.functions.get_counter('queue', 9, parent.chanid) > 0:
                            self.config.queues['source'][1].put(tdict)

                        elif self.proc_id == 9 and self.functions.get_counter('queue', 1, parent.chanid) > 0:
                            self.config.queues['source'][1].put(tdict)

                        elif self.functions.get_counter('queue', -2, parent.chanid) > 0 and not (self.config.opt_dict['disable_ttvdb'] or parent.opt_dict['disable_ttvdb']):
                            self.config.queues['ttvdb'].put({'task': 'last_one', 'parent': parent})

                        else:
                            parent.detail_data.set()

                        continue

                    cache_id = tdict['cache_id']
                    logstring = tdict['logstring']
                    tdict = tdict['tdict']
                    chanid = tdict['channelid']
                    # be nice to the source site
                    time.sleep(random.randint(self.config.opt_dict['nice_time'][0], self.config.opt_dict['nice_time'][1]))
                    # First if the cookyblock is not encountered try the html detail page (only tvgids.nl, the others only have html)
                    if not self.cookyblock:
                        try:
                            detailed_program = self.load_detailpage(tdict)
                            if detailed_program == None:
                                self.fail_count += 1

                        except:
                            detailed_program = None
                            self.fail_count += 1
                            self.config.log([self.config.text('fetch', 15, (tdict['detail_url'][self.proc_id], )), traceback.format_exc()], 1)

                    else:
                        detailed_program = None

                    # It failed! If this is tvgids.nl we check the json page
                    if detailed_program == None and (self.proc_id == 0):
                        try:
                            detailed_program = self.load_json_detailpage(tdict)
                            if detailed_program == None:
                                self.fail_count += 1

                        except:
                            detailed_program = None
                            self.fail_count += 1
                            self.config.log([self.config.text('fetch', 16, (tdict['prog_ID'][self.proc_id][3:], )), traceback.format_exc()], 1)

                    # It failed!
                    if detailed_program == None:
                        # If this is tvgids.nl and there is an url we'll try tvgids.tv, but first check the cache again
                        if self.proc_id == 1:
                            self.config.log(self.config.text('fetch', 17, (parent.chan_name, parent.get_counter(), logstring)), 8, 1)
                            #~ self.functions.update_counter('fail', self.proc_id, parent.chanid)
                            #~ parent.update_counter('fetch', self.proc_id, False)
                            self.functions.update_counter('queue', self.proc_id,  parent.chanid, False)
                            check_ttvdb(tdict, parent)
                            continue

                        else:
                            ret_val = check_other_sources(tdict, cache_id, logstring, parent)
                            if ret_val == -1:
                                break

                            else:
                                continue

                    # Success
                    else:
                        # If this is the prefered description source for this channel, set its value
                        if self.config.channels[detailed_program['channelid']].opt_dict['prefered_description'] == self.proc_id:
                            detailed_program['prefered description'] = detailed_program['description']

                        detailed_program[self.config.channelsource[self.proc_id].detail_check] = True
                        detailed_program['ID'] = detailed_program['prog_ID'][self.proc_id]
                        check_ttvdb(detailed_program, parent)
                        self.config.log(self.config.text('fetch', 19, (self.source, parent.chan_name, parent.get_counter(), logstring)), 8, 1)
                        #~ self.functions.update_counter('detail', self.proc_id, parent.chanid)
                        #~ parent.update_counter('fetch', self.proc_id, False)
                        self.functions.update_counter('queue', self.proc_id,  parent.chanid, False)
                        self.detail_count += 1

                        # do not cache programming that is unknown at the time of fetching.
                        if tdict['name'].lower() != 'onbekend':
                            #~ self.config.queues['cache'].put({'task':'add', 'program': self.functions.checkout_program_dict(detailed_program)})
                            self.config.queues['cache'].put({'task':'add', 'program': detailed_program})

            else:
                self.ready = True

        except:
            if 'detail_url' in tdict and self.proc_id in tdict['detail_url']:
                self.config.queues['log'].put({'fatal': ['The current detail url is: %s\n' \
                    % (tdict['detail_url'][self.proc_id]), \
                    traceback.format_exc(), '\n'], 'name': self.source})

            else:
                self.config.queues['log'].put({'fatal': ['While fetching the detail pages\n', \
                    traceback.format_exc(), '\n'], 'name': self.source})

            self.ready = True
            return(98)

    # The fetching functions
    def init_channel_source_ids(self):
        self.current_date = self.site_tz.normalize(datetime.datetime.now(pytz.utc).astimezone(self.site_tz)).toordinal()
        self.current_hour = self.site_tz.normalize(datetime.datetime.now(pytz.utc).astimezone(self.site_tz)).hour
        for chanid, channel in self.config.channels.iteritems():
            self.groupitems[chanid] = 0
            self.program_data[chanid] = []
            # Is the channel active and this source for the channel not disabled
            if channel.active and not self.proc_id in channel.opt_dict['disable_source']:
                # Is there a sourceid for this channel
                if channel.get_source_id(self.proc_id) != '':
                    # Unless it is in empty channels we add it else set it ready
                    #~ if channel.get_source_id(self.proc_id) in self.config.empty_channels[self.proc_id]:
                    if channel.get_source_id(self.proc_id) in self.config.channelsource[self.proc_id].empty_channels:
                        self.channel_loaded[chanid] = True
                        self.config.channels[chanid].source_ready(self.proc_id).set()

                    else:
                        self.channels[chanid] = channel.get_source_id(self.proc_id)

                # Does the channel have child channels
                if chanid in self.config.combined_channels.keys():
                    # Then see if any of the childs has a sourceid for this source and does not have this source disabled
                    for c in self.config.combined_channels[chanid]:
                        if c['chanid'] in self.config.channels.keys() and self.config.channels[c['chanid']].get_source_id(self.proc_id) != '' \
                          and not self.proc_id in self.config.channels[c['chanid']].opt_dict['disable_source']:
                            # Unless it is in empty channels we add and mark it as a child else set it ready
                            #~ if self.config.channels[c['chanid']].get_source_id(self.proc_id) in self.config.empty_channels[self.proc_id]:
                            if self.config.channels[c['chanid']].get_source_id(self.proc_id) in self.config.channelsource[self.proc_id].empty_channels:
                                self.channel_loaded[c['chanid']] = True
                                self.config.channels[c['chanid']].source_ready(self.proc_id).set()

                            else:
                                self.channels[c['chanid']] = self.config.channels[c['chanid']].get_source_id(self.proc_id)
                                self.config.channels[c['chanid']].is_child = True

        for chanid, channelid in self.channels.items():
            self.chanids[channelid] = chanid

    def get_url(self, ptype, udata):
        """return the several url's for ordinairy, detail and channel info"""
        udata['source'] = self.source
        udata['channels'] = self.channels
        if not self.is_data_value([ptype, "url"]):
            self.config.log([self.config.text('fetch', 68, (ptype, self.source))], 1)
            return None

        if self.is_data_value([ptype, "url"], list):
            url = ''
            for u_part in self.data_value([ptype, "url"], list):
                if isinstance(u_part, (str, unicode)):
                    url += u_part

                elif isinstance(u_part, int):
                    # get a variable
                    uval = self.functions.url_functions(self, ptype, u_part, udata)
                    if uval == None:
                        self.config.log([self.config.text('fetch', 68, (ptype, self.source))], 1)
                        return None

                    else:
                        url += unicode(uval)

        else:
            url = self.data_value([ptype, "url"])

        is_json = bool('json' in self.data_value([ptype, "data-format"], str))
        encoding = self.data_value([ptype, "encoding"])
        accept_header = self.data_value([ptype, "accept-header"])
        url_data = {}
        for k, v in self.data_value([ptype, "url-data"], dict).items():
            if isinstance(v, (str, unicode)):
                url_data[k] = v

            elif isinstance(v, int):
                # get a variable
                uval = self.functions.url_functions(self, ptype, v, udata)
                if uval == None:
                    self.config.log([self.config.text('fetch', 68, (ptype, self.source))], 1)
                    return None

                else:
                    url_data[k] = uval

        if ptype in ('detail', 'detail2'):
            counter = ['detail', self.proc_id, udata['channel']]

        else:
            counter = ['base', self.proc_id]

        return (url, encoding, accept_header, url_data, counter, is_json)

    def get_channels(self):
        """The code for the retreiving a list of supported channels"""
        self.all_channels ={}
        ptype = "channels"
        if not self.is_data_value([ptype], dict):
            ptype = "base-channels"
            if not self.is_data_value([ptype], dict):
                return

        if not self.is_data_value([ptype, "data"]):
            return

        if not self.is_data_value([ptype, "url"]):
            # The channels are defined in the datafile
            self.all_channels = self.data_value([ptype, "data"], dict)
            return

        #extract the data
        channel_list = self.get_page_data(ptype)
        if channel_list == None:
            self.config.log(self.config.text('sources', 1, (self.source, )))
            return 69

        if isinstance(channel_list, list):
            for channel in channel_list:
                # link the data to the right variable, doing any defined adjustments
                values = self.link_values(ptype, channel)
                if "inactive_channel" in values.keys() and values["inactive_channel"]:
                    continue

                if "channelid" in values.keys():
                    channelid = unicode(values["channelid"])
                    if channelid in self.alt_channels.keys():
                        values['channelid'] = self.alt_channels[channelid][0]
                        values['name'] = self.alt_channels[channelid][1]
                        channelid = unicode(values['channelid'])
                    #~ if channelid in self.empty_channels:
                        #~ continue

                    self.all_channels[channelid] = values

        else:
            self.config.log(self.config.text('sources', 1, (self.source, )))
            return 69

    def load_pages(self):
        """The code for the actual Grabbing and dataprocessing"""
        def do_final_processing(chanid):
            self.program_data[chanid].sort(key=lambda program: (program['start-time']))
            #~ print chanid, self.groupitems[chanid]
            if self.groupitems[chanid] > 0:
                group_start = False
                for p in self.program_data[chanid][:]:
                    if 'group' in p.keys():
                        # Collecting the group
                        if not group_start:
                            group = []
                            start = p['start-time']
                            group_start = True

                        group.append(p.copy())
                        group_duur = p['stop-time'] - start

                    elif group_start:
                        # Repeating the group
                        group_start = False
                        group_eind = p['start-time']
                        group_length = group_eind - start
                        if group_length > datetime.timedelta(days = 1):
                            # Probably a week was not grabbed
                            group_eind -= datetime.timedelta(days = int(group_length.days))

                        repeat = 0
                        while True:
                            repeat+= 1
                            for g in group[:]:
                                gdict = g.copy()
                                gdict['prog_ID'] = ''
                                gdict['rerun'] = True
                                gdict['start-time'] += repeat*group_duur
                                gdict['stop-time'] += repeat*group_duur
                                if gdict['start-time'] < group_eind:
                                    if gdict['stop-time'] > group_eind:
                                        gdict['stop-time'] = group_eind

                                    self.program_data[chanid].append(gdict)

                                else:
                                    break

                            else:
                                continue

                            break

            self.parse_programs(chanid, 0, 'None')
            try:
                if self.config.write_info_files:
                    self.config.infofiles.write_fetch_list(self.program_data[chanid], chanid, self.source, self.config.channels[chanid].chan_name, self.proc_id)

            except:
                traceback.print_exc()
                pass

            self.config.channels[chanid].source_ready(self.proc_id).set()
            self.channel_loaded[chanid] = True
            for day in range( self.config.opt_dict['offset'], (self.config.opt_dict['offset'] + self.config.opt_dict['days'])):
                self.day_loaded[chanid][day] = True

        if len(self.channels) == 0  or not self.is_data_value(["base", "url"]):
            return

        self.day_loaded = {}
        self.day_loaded[0] = {}
        day_channels = {}
        for day in range( self.config.opt_dict['offset'], (self.config.opt_dict['offset'] + self.config.opt_dict['days'])):
            day_channels[day] = []
            self.day_loaded[0][day] = False

        self.page_loaded = {}
        self.channel_loaded = {}
        for chanid in self.config.channels.keys():
            self.channel_loaded[chanid] = False
            self.day_loaded[chanid] ={}
            self.page_loaded[chanid] = {}
            for day in range( self.config.opt_dict['offset'], (self.config.opt_dict['offset'] + self.config.opt_dict['days'])):
                self.day_loaded[chanid][day] = False

            self.program_data[chanid] = []

        try:
            append_source = None
            first_fetch = True
            max_days = self.data_value(["base", "max days"], int, default = 14)
            url_type = self.data_value(["base", "url-type"], int, default = 2)
            if self.config.opt_dict['offset'] > max_days:
                for chanid in self.channels.keys():
                    self.channel_loaded[chanid] = True
                    self.config.channels[chanid].source_ready(self.proc_id).set()

                return

            if (url_type & 12) == 8:
                # We fetch a set number of  days in one
                if not self.is_data_value(["base", "url-date-range"]):
                    return

                if self.data_value(["base", "url-date-range"]) == 'week':
                    sow = self.data_value(["base", "url-date-week-start"], int, default = 1)
                    first_day = self.functions.get_weekstart(self.current_date, self.config.opt_dict['offset'], sow)
                    offset_step = 7

                elif self.is_data_value(["base", "url-date-range"], int):
                    first_day = self.config.opt_dict['offset']
                    offset_step = self.data_value(["base", "url-date-range"])

                else:
                    return

                fetch_range = range(first_day, (self.config.opt_dict['offset'] + self.config.opt_dict['days']), offset_step)
                for chanid in self.channels.keys():
                    for r in range(len(fetch_range)):
                        self.page_loaded[chanid][r] = False

            elif (url_type & 12) == 12:
                udt = self.data_value(["base", "url-date-type"], int, default=0)
                udd = self.data_value(['base', 'url-date-multiplier'], int, default=0)
                fs = self.config.opt_dict['offset']
                fe = min((self.config.opt_dict['offset'] + self.config.opt_dict['days']), max_days)

            for retry in (0, 1):
                # We fetch every channel separate
                if (url_type & 3) == 1:
                    channel_cnt = 0
                    for chanid in self.channels.keys():
                        channel_cnt += 1
                        failure_count = 0
                        if self.quit:
                            return

                        if self.config.channels[chanid].source_ready(self.proc_id).is_set():
                            continue

                        channel = self.channels[chanid]
                        # We fetch every day separate
                        # tvgids.tv
                        if (url_type & 12) == 0:
                            ats = self.data_value(["base", "append_to_source"], unicode)
                            if ats in self.config.sourceid_by_name.keys() and self.config.channels[chanid].opt_dict['append_tvgidstv']:
                                # Start from the offset but skip the days allready fetched by tvgids.nl
                                # Except when append_tvgidstv is False
                                append_source = self.config.sourceid_by_name[ats]
                                fetch_range = []
                                for i in range(self.config.opt_dict['offset'], min((self.config.opt_dict['offset'] + self.config.opt_dict['days']), max_days)):
                                    if not chanid in self.config.channelsource[append_source].day_loaded \
                                      or not self.config.channelsource[append_source].day_loaded[chanid][i]:
                                        fetch_range.append(i)

                            else:
                                fetch_range = range(self.config.opt_dict['offset'], min((self.config.opt_dict['offset'] + self.config.opt_dict['days']), max_days))

                            if len(fetch_range) == 0:
                                self.channel_loaded[chanid] = True
                                self.config.channels[chanid].source_ready(self.proc_id).set()
                                continue

                            for offset in fetch_range:
                                # Check if it is allready loaded
                                if self.quit:
                                    return

                                if self.day_loaded[chanid][offset] or \
                                  (self.config.channels[chanid].opt_dict['append_tvgidstv'] and \
                                  append_source != None and \
                                  chanid in self.config.channelsource[append_source].day_loaded and \
                                  self.config.channelsource[append_source].day_loaded[chanid][offset]):
                                    self.day_loaded[chanid][offset] = True
                                    continue

                                self.config.log(['\n', self.config.text('sources', 13, \
                                    (self.config.channels[chanid].chan_name, self.config.channels[chanid].xmltvid , \
                                    (self.config.channels[chanid].opt_dict['compat'] and self.config.compat_text or ''), self.source)), \
                                    self.config.text('sources', 23, (channel_cnt, len(self.channels), offset, self.config.opt_dict['days']))], 2)

                                if not first_fetch:
                                    # be nice to the source
                                    time.sleep(random.randint(self.config.opt_dict['nice_time'][0], self.config.opt_dict['nice_time'][1]))

                                first_fetch = False
                                strdata = self.get_page_data('base',{'channel': channel, 'offset': offset})
                                if strdata == None:
                                    if retry == 1:
                                        self.config.log(self.config.text('sources', 20, (self.config.channels[chanid].chan_name, self.source, offset)))
                                    failure_count += 1
                                    self.fail_count += 1
                                    continue

                                self.day_loaded[chanid][offset] = True
                                self.parse_basepage(strdata, {'offset': offset, 'channelid': channel})

                        # We fetch all days in one
                        elif (url_type & 12) == 4:
                            if self.day_loaded[chanid][self.config.opt_dict['offset']]:
                                continue

                            self.config.log(['\n', self.config.text('sources', 13, \
                                (self.config.channels[chanid].chan_name, self.config.channels[chanid].xmltvid , \
                                (self.config.channels[chanid].opt_dict['compat'] and self.config.compat_text or ''), self.source)), \
                                self.config.text('sources', 34, (channel_cnt, len(self.channels), '6'))], 2)

                            if not first_fetch:
                                # be nice to the source
                                time.sleep(random.randint(self.config.opt_dict['nice_time'][0], self.config.opt_dict['nice_time'][1]))

                            first_fetch = False
                            strdata = self.get_page_data('base',{'channel': channel,
                                                                                    'start': self.config.opt_dict['offset'],
                                                                                    'days': self.config.opt_dict['days']})
                            if strdata == None:
                                if retry == 1:
                                    self.config.log(self.config.text('sources', 40, (self.config.channels[chanid].chan_name, self.source)))
                                failure_count += 1
                                self.fail_count += 1
                                continue

                            self.day_loaded[chanid][self.config.opt_dict['offset']]
                            self.parse_basepage(strdata, {'channelid': channel})

                        # We fetch a set number of  days in one
                        # vrt.be, nieuwsblad.be
                        elif (url_type & 12) == 8:
                            for offset in range(len(fetch_range)):
                                if self.quit:
                                    return

                                # Check if it is already loaded
                                if self.page_loaded[chanid][offset]:
                                    continue

                                self.config.log(['\n', self.config.text('sources', 13, \
                                    (self.config.channels[chanid].chan_name, self.config.channels[chanid].xmltvid , \
                                    (self.config.channels[chanid].opt_dict['compat'] and self.config.compat_text or ''), self.source)), \
                                    self.config.text('sources', 17, (channel_cnt, len(self.channels), offset, len(fetch_range)))], 2)

                                if not first_fetch:
                                    # be nice to the source
                                    time.sleep(random.randint(self.config.opt_dict['nice_time'][0], self.config.opt_dict['nice_time'][1]))

                                first_fetch = False
                                strdata = self.get_page_data('base',{'channel': channel, 'offset': fetch_range[offset]})
                                if strdata == None:
                                    if retry == 1:
                                        self.config.log(self.config.text('sources', 41, (self.config.channels[chanid].chan_name, self.source, offset)))
                                    failure_count += 1
                                    self.fail_count += 1
                                    continue

                                self.parse_basepage(strdata, {'offset': offset, 'channelid': channel})
                                self.page_loaded[chanid][offset] = True

                        # We fetch a set number of  records in one
                        # horizon.nl
                        elif (url_type & 12) == 12:
                            if self.item_count == 0:
                                return

                            self.current_item_count = self.item_count
                            page_count = 0
                            while self.current_item_count == self.item_count:
                                if not page_count in self.page_loaded[chanid]:
                                    self.page_loaded[chanid][page_count] = False

                                if self.quit:
                                    return

                                # Check if it is already loaded
                                if self.page_loaded[chanid][page_count]:
                                    page_count += 1
                                    continue

                                self.config.log(['\n', self.config.text('sources', 13, \
                                    (self.config.channels[chanid].chan_name, self.config.channels[chanid].xmltvid, \
                                    (self.config.channels[chanid].opt_dict['compat'] and self.config.compat_text or ''), self.source)), \
                                    self.config.text('sources', 14, \
                                    ( channel_cnt, len(self.channels), self.config.opt_dict['days'], page_count))], 2)

                                if not first_fetch:
                                    # be nice to the source
                                    time.sleep(random.randint(self.config.opt_dict['nice_time'][0], self.config.opt_dict['nice_time'][1]))

                                first_fetch = False
                                strdata = self.get_page_data('base',{'channel': channel, 'cnt-offset': page_count})
                                if strdata == None:
                                    if retry == 1:
                                        self.config.log(self.config.text('sources', 20, (self.config.channels[chanid].chan_name, self.source, page_count)))
                                    failure_count += 1
                                    self.fail_count += 1
                                    page_count += 1
                                    if failure_count > 10:
                                        break

                                    continue

                                self.parse_basepage(strdata, {'channelid': channel})
                                self.page_loaded[chanid][page_count] = True
                                page_count += 1

                        if failure_count == 0 or retry == 1:
                            do_final_processing(chanid)

                # We fetch all channels in one
                if (url_type & 3) == 2:
                    failure_count = 0
                    if self.quit:
                        return

                    if len(self.channels) == 0 :
                        return

                    # We fetch every day separate
                    # tvgids.nl, npo.nl, vpro.nl, primo.eu, oorboekje.nl
                    if (url_type & 12) == 0:
                        for offset in range(self.config.opt_dict['offset'], min((self.config.opt_dict['offset'] + self.config.opt_dict['days']), max_days)):
                            if self.quit:
                                return

                            # Check if it is already loaded
                            if self.day_loaded[0][offset]:
                                continue

                            self.config.log(['\n', self.config.text('sources', 2, (len(self.channels), self.source)), \
                                self.config.text('sources', 3, (offset, self.config.opt_dict['days']))], 2)

                            if not first_fetch:
                                # be nice to the source
                                time.sleep(random.randint(self.config.opt_dict['nice_time'][0], self.config.opt_dict['nice_time'][1]))

                            first_fetch = False
                            strdata = self.get_page_data('base',{'offset': offset})
                            if strdata == None:
                                if retry == 1:
                                    self.config.log(self.config.text('sources', 4, (offset, self.source)))
                                failure_count += 1
                                self.fail_count += 1
                                continue

                            self.day_loaded[0][offset] = True
                            self.parse_basepage(strdata, {'offset':offset})

                    # We fetch all days in one
                    # rtl.nl
                    elif (url_type & 12) == 4:
                        #~ self.print_searchtree = True
                        #~ self.show_parsing = True
                        #~ self.show_result = True
                        self.config.log(['\n', self.config.text('sources', 2, (len(self.channels), self.source)), \
                            self.config.text('sources', 11, (self.config.opt_dict['days']))], 2)
                        # be nice to the source
                        time.sleep(random.randint(self.config.opt_dict['nice_time'][0], self.config.opt_dict['nice_time'][1]))
                        strdata = self.get_page_data('base')
                        if strdata == None:
                            if retry == 1:
                                self.config.log(self.config.text('sources', 12, (self.source, )))
                            failure_count += 1
                            self.fail_count += 1
                            continue

                        self.parse_basepage(strdata)

                    elif (url_type & 12) == 8:
                        # We fetch a set number of  days in one
                        pass

                    elif (url_type & 12) == 12:
                        # We fetch a set number of  records in one
                        pass

                    if failure_count == 0 or retry == 1:
                        for chanid in self.channels.keys():
                            do_final_processing(chanid)

                        break

                # We fetch the channels in two or more groups
                if (url_type & 3) == 3:
                    if not self.is_data_value(["base", "url-channel-groups"], list):
                        return

                    for channelgrp in self.data_value(["base", "url-channel-groups"], list):
                        failure_count = 0
                        if self.quit:
                            return

                        # We fetch every day separate
                        #humo.be
                        if (url_type & 12) == 0:
                            for offset in range(self.config.opt_dict['offset'], min((self.config.opt_dict['offset'] + self.config.opt_dict['days']), max_days)):
                                if self.quit:
                                    return

                                # Check if all channels for the day are already loaded
                                if len(day_channels[offset]) == len(self.channels):
                                    continue

                                self.config.log(['\n', self.config.text('sources', 47, (channelgrp, self.source)), \
                                    self.config.text('sources', 3, (offset, self.config.opt_dict['days']))], 2)

                                if not first_fetch:
                                    # be nice to the source
                                    time.sleep(random.randint(self.config.opt_dict['nice_time'][0], self.config.opt_dict['nice_time'][1]))

                                first_fetch = False
                                strdata = self.get_page_data('base',{'channelgrp': channelgrp, 'offset': offset})
                                if strdata == None:
                                    if retry == 1:
                                        self.config.log(self.config.text('sources', 4, (self.source, offset)))
                                    failure_count += 1
                                    self.fail_count += 1
                                    continue

                                chanids = self.parse_basepage(strdata, {'channelgrp': channelgrp, 'offset':offset})
                                if isinstance(chanids, list):
                                    for chanid in chanids:
                                        self.day_loaded[chanid][offset] = True
                                        if not chanid in day_channels[offset]:
                                            day_channels[offset].append(chanid)


                        elif (url_type & 12) == 4:
                            # We fetch all days in one
                            pass

                        elif (url_type & 12) == 8:
                            # We fetch a set number of  days in one
                            pass

                        elif (url_type & 12) == 12:
                            # We fetch a set number of  records in one
                            pass


                    if failure_count == 0 or retry == 1:
                        for chanid in self.channels.keys():
                            do_final_processing(chanid)

                        break

        except:
            self.config.log([self.config.text('fetch', 13, (self.source,)), self.config.text('fetch', 14), traceback.format_exc()], 0)
            for chanid in self.channels.keys():
                self.channel_loaded[chanid] = True
                self.config.channels[chanid].source_ready(self.proc_id).set()

            return None

    def parse_basepage(self, fdata, subset = {}):
        chanids = []
        last_start = {}
        tdd = datetime.timedelta(days=1)
        tdh = datetime.timedelta(hours=1)
        if isinstance(fdata, list):
            last_stop = None
            for program in fdata:
                # link the data to the right variable, doing any defined adjustments
                values = self.link_values("base", program)
                if 'channelid' in values.keys():
                    channelid = unicode(values['channelid'])
                    if channelid in self.alt_channels.keys():
                        values['channelid'] = self.alt_channels[channelid][0]
                        channelid = unicode(values['channelid'])

                elif 'channelid' in subset.keys():
                    channelid = subset['channelid']

                else:
                    continue

                # it's not requested
                if not channelid in self.chanids.keys():
                    continue

                # A list of processed channels to send back
                if not self.chanids[channelid] in chanids:
                    chanids.append(self.chanids[channelid])

                if not channelid in last_start.keys():
                    last_start[channelid] = None

                chanid = self.chanids[channelid]
                if not 'prog_ID' in values.keys():
                    values['prog_ID'] = ''
                if  not 'name' in values.keys() or values['name'] == None or values['name'] == '':
                    # Give it the Unknown Program Title Name, to mark it as a groupslot.
                    values['name'] = self.config.unknown_program_title
                    #~ self.config.log(self.config.text('sources', 6, (values['prog_ID'], self.config.channels[chanid].chan_name, self.source)))
                    #~ continue

                tdict = {}
                tdict['source'] = self.source
                tdict['channelid'] = chanid
                tdict['channel']  = self.config.channels[chanid].chan_name

                if 'stop-time' in values.keys() and isinstance(values['stop-time'], datetime.datetime):
                    tdict['stop-time'] = values['stop-time']
                elif "alt-stop-time" in values and isinstance(values["alt-stop-time"], datetime.datetime):
                    tdict['stop-time'] = values["alt-stop-time"]

                if 'start-time' in values.keys() and isinstance(values['start-time'], datetime.datetime):
                    tdict['start-time'] = values['start-time']
                elif "alt-start-time" in values and isinstance(values["alt-start-time"], datetime.datetime):
                    tdict['start-time'] = values["alt-start-time"]
                elif "length" in values and isinstance(values['length'], datetime.timedelta) and 'stop-time' in tdict.keys():
                    tdict['start-time'] = tdict['stop-time'] - values['length']
                elif "previous-start-time" in values and isinstance(values["previous-start-time"], datetime.datetime) \
                  and "previous-length" in values and isinstance(values["previous-length"], datetime.timedelta):
                    tdict['start-time'] = values['previous-start-time'] + values['previous-length']
                elif self.data_value(["base", "data-format"], str) == "text/html" and isinstance(last_stop, datetime.datetime):
                    tdict['start-time'] = last_stop
                else:
                    # Unable to determin a Start Time
                    self.config.log(self.config.text('sources', 7, (values['name'], tdict['channel'], self.source)))
                    continue

                if not 'stop-time' in tdict.keys() and "length" in values and isinstance(values['length'], datetime.timedelta):
                    tdict['stop-time'] = tdict['start-time'] + values['length']

                if self.without_full_timings and self.data_value(["base", "data-format"], str) == "text/html":
                    # This is to catch the midnight date change for HTML pages with just start(stop) times without date
                    # don't enable it on json pages where the programs are in a dict as they will not be in chronological order!!!
                    if last_start[channelid] == None:
                        last_start[channelid] = tdict['start-time']

                    while tdict['start-time'] < last_start[channelid] - tdh:
                        tdict['start-time'] += tdd

                    last_start[channelid] = tdict['start-time']
                    if 'stop-time' in tdict.keys():
                        while tdict['stop-time'] < tdict['start-time']:
                            tdict['stop-time'] += tdd

                tdict['offset'] = self.functions.get_offset(tdict['start-time'])
                if self.data_value(["base", "data-format"], str) == "text/html":
                    if 'stop-time' in tdict.keys():
                        last_stop = tdict['stop-time']
                    else:
                        last_stop = None

                # Add any known value that does not need further processing
                for k, v in values.items():
                    if k in ('channelid', 'video', 'genre', 'subgenre'):
                        continue

                    if k in self.config.key_values['text'] and not v in (None, ''):
                        tdict[k] = v

                    elif (k in self.config.key_values['bool'] or k in self.config.key_values['video']) and  isinstance(v, bool):
                        tdict[k] = v

                    elif k in self.config.key_values['int'] and isinstance(v, int):
                        if k == 'episode' and v > 1000:
                            continue

                        tdict[k] = v

                    elif k in self.config.key_values['list'] and isinstance(v, list) and len(v) > 0:
                        tdict[k] = v

                    elif k in self.config.key_values['timedelta'] and isinstance(v, datetime.timedelta):
                        tdict[k] = v

                    elif k in self.config.key_values['date'] and isinstance(v, datetime.date):
                        tdict[k] = v

                # The credits
                if 'credits' in values.keys() and isinstance(values['credits'], dict):
                    for k, v in values['credits'].items():
                        if k in self.config.roletrans.keys() and isinstance(v, (list, tuple)):
                            if not self.config.roletrans[k] in tdict or len(tdict[self.config.roletrans[k]]) == 0:
                                tdict[self.config.roletrans[k]] = v

                            for item in v:
                                if not item in tdict[self.config.roletrans[k]]:
                                    tdict[self.config.roletrans[k]].append(item)

                for k in self.config.roletrans.keys():
                    if k in values.keys() and isinstance(values[k], (list, tuple)):
                        if not self.config.roletrans[k] in tdict or len(tdict[self.config.roletrans[k]]) == 0:
                            tdict[self.config.roletrans[k]] = values[k]

                        for item in values[k]:
                            if not item in tdict[self.config.roletrans[k]]:
                                tdict[self.config.roletrans[k]].append(item)

                gg = self.get_genre(values)
                tdict['genre'] = gg[0]
                tdict['subgenre'] = gg[1]
                if 'group' in values.keys() and not values['group'] in (None, ''):
                    self.groupitems[chanid] += 1
                    tdict['group'] = values['group']
                    #~ if not values['group'] in self.groupitems[chanid].keys():
                        #~ self.groupitems[chanid][values['group']] = []

                    #~ self.groupitems[chanid][values['group']].append({'channel': channelid, 'start-time': tdict['start-time'], 'stop-time': tdict['stop-time'], 'program': dict})

                #~ tdict = self.functions.checkout_program_dict(tdict)
                tdict = self.check_title_name(tdict)
                with self.source_lock:
                    self.program_data[chanid].append(tdict)

                #~ self.config.genre_list.append((tdict['genre'].lower(), tdict['subgenre'].lower()))

                if self.show_result:
                    print '    ', channelid, tdict['start-time']
                    for k, v in tdict.items():
                        if isinstance(v, (str, unicode)):
                            vv = ': "%s"' % v
                            print '        ', k, vv.encode('utf-8', 'replace')
                        else:
                            print '        ', k, v

        #~ if self.show_result:
            #~ for name, item in self.groupitems[chanid].items():
                #~ print name
                #~ for p in item:
                    #~ print '  ',p['channel'] , p['start-time'], p['stop-time']

        return chanids

    def load_detailpage(self, ptype, tdict):
        """The code for retreiving and processing a detail page"""
        ddata = {'channel': tdict['channelid'], 'detailid': tdict['detail_url'][self.proc_id]}
        strdata = self.get_page_data(ptype, ddata)
        if not isinstance(strdata, (list,tuple)) or len(strdata) == 0:
            self.config.log(self.config.text('sources', 8, (tdict['detail_url'][self.proc_id], )), 1)
            return

        values = self.link_values(ptype, strdata[0])
        if self.show_result:
            print
            print 'Resulting values'
            for k, v in values.items():
                if isinstance(v, (str, unicode)):
                    vv = ': "%s"' % v
                    print '        ', k, vv.encode('utf-8', 'replace')
                else:
                    print '        ', k, ': ', v

        return tdict

    def get_page_data(self, ptype, pdata = None):
        try:
            if pdata == None:
                pdata = {}

            if ptype in ('channels', 'base-channels'):
                pdata['start'] = 0
                pdata['days'] = 0
                pdata[ 'offset'] = 0

            url = self. get_url(ptype, pdata)
            if url == None:
                return

            is_json = url[5]
            if self.print_searchtree:
                #~ self.test_output.write(url)
                #~ self.test_output.write('\n')
                print url
            page = self.functions.get_page(url)
            if page == None:
                self.config.log([self.config.text('fetch', 71, (ptype, self.source))], 1)
                if self.print_searchtree:
                    print 'No Data'
                return None

            #~ if ptype in ('detail', 'detail2') and self.proc_id in (0, 1, 9):
                #~ return page

            if is_json:
                searchtree = DataTreeGrab.JSONtree(page, self.test_output)

            else:
                autoclose_tags = self.data_value([ptype, "autoclose-tags"], list)
                if self.data_value([ptype, "enclose-with-html-tag"], bool, default=False):
                    page = u'<html>%s</html>' % page

                searchtree = DataTreeGrab.HTMLtree(page, autoclose_tags, self.print_tags, self.test_output)

            self.source_data[ptype]['timezone'] = self.data_value('site-timezone', str, default = 'utc')
            searchtree.check_data_def(self.data_value(ptype, dict))
            if ptype in ('base', 'detail', 'detail2'):
                # We load some values from the definition file into the tree
                self.fetch_date = self.current_date + self.data_value('offset', int, pdata, default=0)
                if not "channelid" in searchtree.value_filters.keys() or not isinstance(searchtree.value_filters["channelid"], list) :
                    searchtree.value_filters["channelid"] = []

                searchtree.value_filters["channelid"].extend(list(self.chanids.keys()))
                searchtree.value_filters["channelid"].extend(list(self.alt_channels.keys()))

            if self.is_data_value([ptype, "total-item-count"],list):
                self.total_item_count = searchtree.find_data_value(self.data_value([ptype, "total-item-count"],list))

            if self.is_data_value([ptype, "page-item-count"],list):
                self.current_item_count = searchtree.find_data_value(self.data_value([ptype, "page-item-count"],list))

            searchtree.show_result = self.show_parsing
            searchtree.print_searchtree = self.print_roottree
            searchtree.find_start_node()
            if ptype == 'base' and self.is_data_value([ptype,'data',"today"],list):
                # We check on the right offset
                url_type = self.data_value([ptype, "url-type"], int, default = 2)
                cd = searchtree.find_data_value(self.data_value([ptype,'data',"today"],list))
                if not isinstance(cd, datetime.date):
                    self.config.log(self.config.text('sources', 22))
                    return None

                elif self.night_date_switch > 0 and self.current_hour < self.night_date_switch and (self.current_date - cd.toordinal()) == 1:
                    # This page switches date at a later time so we allow
                    pass

                elif cd.toordinal() != self.current_date:
                    url_type = self.data_value(["base", "url-type"], int, default = 2)
                    if url_type == 1:
                        self.config.log(self.config.text('sources', 21, (pdata['channel'], self.source, pdata['offset'])))
                    elif (url_type & 3) == 1:
                        # chanid
                        pass
                    elif (url_type & 12) in (0, 8):
                        # offset
                        pass
                    return None

            searchtree.print_searchtree = self.print_searchtree
            searchtree.extract_datalist()
            if self.show_result:
                #~ self.test_output.write(searchtree.result)
                #~ self.test_output.write('\n')
                for p in searchtree.result:
                    if isinstance(p[0], (str, unicode)):
                        print p[0].encode('utf-8', 'replace')
                    else:
                        print p[0]
                    for v in range(1,len(p)):
                        if isinstance(p[v], (str, unicode)):
                            print '    ', p[v].encode('utf-8', 'replace')
                        else:
                            print '    ', p[v]

            return searchtree.result

        except:
            self.config.log([self.config.text('fetch', 71, (ptype, self.source)), traceback.format_exc()], 1)
            return None

    def link_values(self, ptype, linkdata):
        def get_variable(vdef):
            max_length = self.data_value('max length', int, vdef, 0)
            min_length = self.data_value('min length', int, vdef, 0)
            varid = self.data_value("varid", int, vdef)
            if not ((isinstance(linkdata, list) and (0 <= varid < len(linkdata))) \
              or (isinstance(linkdata, dict) and varid in linkdata.keys())):
                return

            d = linkdata[varid] if (not  isinstance(linkdata[varid], (unicode, str))) else unicode(linkdata[varid]).strip()
            if min_length > 0 and len(d) < min_length:
                return

            if max_length > 0 and len(d) > max_length:
                return

            if self.is_data_value('regex', str, vdef):
                search_regex = self.data_value('regex', str, vdef, None)
                try:
                    dd = re.search(search_regex, d, re.DOTALL)
                    if dd.group(1) not in ('', None):
                        d = dd.group(1)

                    else:
                        return

                except:
                    return

            if self.is_data_value('type', str, vdef):
                d = check_type(v, d)

            return d

        def process_link_function(vdef):
            funcid = self.data_value("funcid", int, vdef)
            default = self.data_value("default", None, vdef)
            if funcid != None:
                funcdata = self.data_value("data", list, vdef)
                data = []
                for fd in funcdata:
                    if self.is_data_value("varid", int, fd):
                        dvar = get_variable(fd)
                        if dvar == None:
                            data.append('')

                        else:
                            data.append(dvar)

                    elif self.is_data_value("funcid", int, fd):
                        data.append(process_link_function(fd))

                    else:
                        data.append(fd)

                return self.functions.link_functions(funcid, data, self, default)

        def check_type(vdef, value):
            dtype = self.data_value('type', str, vdef)
            try:
                if dtype == 'string':
                    return unicode(value)

                elif dtype == 'lower':
                    return unicode(value).lower()

                elif dtype == 'upper':
                    return unicode(value).upper()

                elif dtype == 'capitalize':
                    return unicode(value).capitalize()

                elif dtype == 'int':
                    return int(value)

                elif dtype == 'float':
                    return float(value)

                elif dtype == 'bool':
                    return bool(value)

                else:
                    return value

            except:
                return None

        def calc_value(vdef, value):
            if self.is_data_value('multiplier', float, vdef):
                try:
                    if not isinstance(value, (int, float)):
                        value = float(value)
                    value = value * vdef['multiplier']

                except:
                    #~ traceback.print_exc()
                    pass

            if self.is_data_value('devider', float, vdef):
                try:
                    if not isinstance(value, (int, float)):
                        value = float(value)
                    value = value / vdef['devider']

                except:
                    #~ traceback.print_exc()
                    pass

            return value

        values = {}
        if isinstance(linkdata, (list, tuple, dict)):
            for k, v in self.data_value([ptype,"values"], dict).items():
                if self.is_data_value("varid", int, v):
                    vv = get_variable(v)
                    if vv not in (None, '', '-'):
                        values[k] = vv
                        continue

                elif self.is_data_value("funcid", int, v):
                    cval = process_link_function(v)
                    if cval not in (None, '', '-'):
                        if self.is_data_value('type', str, v):
                            cval = check_type(v, cval)

                        if self.is_data_value('calc', dict, v):
                            cval = calc_value(v['calc'], cval)

                        if v["funcid"] == 1:
                            if len(cval) > 1:
                                values[k] = cval[0]
                                values['icongrp'] = cval[1]

                        else:
                            values[k] = cval

                        continue

                elif self.is_data_value("value", None, v):
                    values[k] = self.data_value("value", None, v)
                    continue

                if self.is_data_value('default', None, v):
                    values[k] = v['default']

        return values

    def get_genre(self, values):
        genre = ''
        subgenre = ''
        if 'genres'in values:
            # It is return as a set of genre/subgenre so we split them
            if isinstance(values['genres'], (str,unicode)):
                values['genre'] = values['genres']

            if isinstance(values['genres'], (list,tuple)):
                if len(values['genres'])> 0:
                    values['genre'] = values['genres'][0]

                if len(values['genres'])> 1:
                    values['subgenre'] = values['genres'][1]

        if self.cattrans_type == 1:
            if self.new_cattrans == None:
                self.new_cattrans = {}

            if 'genre' in values:
                # Just in case it is a comma seperated list
                if isinstance(values['genre'], (str, unicode)):
                    gg = values['genre'].split(',')

                elif isinstance(values['genre'], list):
                    gg = values['genre']

                gg0 = gg[0].lower().strip()
                gg1 = u''
                if gg0 in self.cattrans.keys():
                    if len(gg) > 1:
                        gg1 = gg[1].lower().strip()
                        if gg1 in self.cattrans[gg0].keys():
                            genre = self.cattrans[gg0][gg1][0]
                            subgenre = self.cattrans[gg0][gg1][1]

                    elif 'subgenre' in values and values['subgenre'] not in (None, ''):
                        if values['subgenre'].lower().strip() in self.cattrans[gg0].keys():
                            genre = self.cattrans[gg0][values['subgenre'].lower().strip()][0]
                            subgenre = self.cattrans[gg0][values['subgenre'].lower().strip()][1]

                        else:
                            genre = self.cattrans[gg0]['default'][0]
                            subgenre = values['subgenre']
                            self.new_cattrans[(gg0, values['subgenre'].strip().lower())] = (self.cattrans[gg0]['default'][0].strip().lower(), values['subgenre'].strip().lower())

                    else:
                        genre = self.cattrans[gg0]['default'][0]
                        subgenre = self.cattrans[gg0]['default'][1]
                        if len(gg) > 1:
                            self.new_cattrans[(gg0,gg1)] = (self.cattrans[gg0]['default'][0].strip().lower(), self.cattrans[gg0]['default'][1].strip().lower())

                elif gg0 not in (None, ''):
                    if len(gg) > 1 and gg1 not in (None, ''):
                        self.new_cattrans[(gg0,gg1)] = [self.config.cattrans_unknown.lower().strip(),'']

                    elif 'subgenre' in values and values['subgenre'] not in (None, ''):
                        self.new_cattrans[(gg0,values['subgenre'].strip().lower())] = [self.config.cattrans_unknown.lower().strip(),'']

                    else:
                        self.new_cattrans[(gg0,'')] = [self.config.cattrans_unknown.lower().strip(),'']

                    if self.config.write_info_files:
                        if 'subgenre' in values and values['subgenre'] not in (None, ''):
                            self.config.infofiles.addto_detail_list(u'unknown %s genre/subgenre => ["%s", "%s"]' % (self.source, values['genre'], values['subgenre']))

                        else:
                            self.config.infofiles.addto_detail_list(u'unknown %s genre => %s' % (self.source, values['genre']))

        elif self.cattrans_type == 2:
            if self.new_cattrans == None:
                self.new_cattrans = []
            if 'subgenre' in values and values['subgenre'] not in (None, ''):
                if values['subgenre'].lower().strip() in self.cattrans.keys():
                    genre = self.cattrans[values['subgenre'].lower().strip()]
                    subgenre = values['subgenre'].strip()

                else:
                    for k, v in self.cattrans_keywords.items():
                        if k.lower() in values['subgenre'].lower():
                            genre = v
                            subgenre = values['subgenre'].strip()
                            self.new_cattrans.append((values['subgenre'].lower().strip(), tdict['genre'].strip().lower()))
                            break

                    else:
                        self.new_cattrans.append((values['subgenre'].lower().strip(), self.config.cattrans_unknown.lower().strip()))

                    if self.config.write_info_files:
                        self.config.infofiles.addto_detail_list(u'unknown %s subgenre => "%s"' % (self.source, values['subgenre']))

        else:
            if 'genre' in values.keys():
                genre = values['genre']

            if 'subgenre' in values.keys():
                subgenre = values['subgenre']

        return (genre.strip(), subgenre.strip())

    # Helper functions
    def is_data_value(self, dpath, dtype = None, subpath = None):
        pval = (dpath, dtype, subpath)
        if isinstance(dpath, (str, unicode, int)):
            dpath = [dpath]

        if not isinstance(dpath, (list, tuple)):
            return False

        if subpath == None:
            subpath = self.source_data

        for d in dpath:
            #~ if not isinstance(subpath, dict):
                #~ return False

            #~ if not d in subpath.keys():
                #~ return False

            if isinstance(subpath, dict):
                if not d in subpath.keys():
                    return False

            elif isinstance(subpath, list):
                if (not isinstance(d, int) or d >= len(subpath)):
                    return False

            else:
                return False

            subpath = subpath[d]

        if subpath in (None, "", {}, []):
            return False

        if dtype == None:
            return True

        if dtype == float:
            return bool(isinstance(subpath, (float, int)))

        if dtype in (str, unicode):
            return bool(isinstance(subpath, (str, unicode)))

        if dtype in (list, tuple):
            return bool(isinstance(subpath, (list, tuple)))

        return bool(isinstance(subpath, dtype))

    def data_value(self, dpath, dtype = None, subpath = None, default = None):
        if self.is_data_value(dpath, dtype, subpath):
            if isinstance(dpath, (str, unicode, int)):
                dpath = [dpath]

            if subpath == None:
                subpath = self.source_data

            for d in dpath:
                subpath = subpath[d]

        else:
            subpath = None

        if subpath == None:
            if default != None:
                return default

            elif dtype in (str, unicode):
                return ""

            elif dtype == dict:
                return {}

            elif dtype in (list, tuple):
                return []

        return subpath

    def check_title_name(self, program):
        """
        Process Title names on Grouping issues and apply the rename table
        Return the updated Progam dict
        """
        ptitle = program['name']
        psubtitle = '' if not 'episode title'in program.keys() else program['episode title']
        if  ptitle == None or ptitle == '':
            return program

        if re.sub('[-,. ]', '', ptitle) == re.sub('[-,. ]', '', psubtitle):
            program['episode title'] = ''
            psubtitle = ''

        # Remove a groupname if in the list
        for group in self.config.groupnameremove:
            if (len(ptitle) > len(group) + 3) and (ptitle[0:len(group)].lower() == group):
                p = ptitle.split(':')
                if len(p) >1:
                    self.config.log(self.config.text('fetch', 20,  (group, ptitle)), 64)
                    if self.config.write_info_files:
                        self.config.infofiles.addto_detail_list(unicode('Group removing = \"%s\" from \"%s\"' %  (group, ptitle)))

                    ptitle = "".join(p[1:]).strip()

        # Fixing subtitle both named and added to the title
        if ptitle.lower() == psubtitle.lower() and program['genre'] != 'serie/soap':
            psubtitle = ''
        if  (psubtitle != '') and (len(ptitle) > len(psubtitle)):
            lentitle = len(ptitle) - len(psubtitle)
            if psubtitle.lower().strip() == ptitle[lentitle:].lower().strip():
                ptitle = ptitle[0:lentitle].strip()
                if (ptitle[-1] == ':') or (ptitle[-1] == '-'):
                    ptitle = ptitle[0:(len(ptitle) - 1)].strip()

        # And the other way around
        elif  (psubtitle != '') and (len(ptitle) < len(psubtitle)):
            lentitle = len(ptitle.strip())
            if ptitle.lower().strip() == psubtitle[0:lentitle].lower().strip():
                psubtitle = psubtitle[lentitle:].strip()
                if (psubtitle[0:1] == ':') or (psubtitle[0:1] == '-'):
                    psubtitle = psubtitle[1:].strip()

        # Check the Title rename list
        if ptitle.lower() in self.config.titlerename:
            self.config.log(self.config.text('fetch', 21, (ptitle, self.config.titlerename[ptitle.lower()])), 64)
            if self.config.write_info_files:
                self.config.infofiles.addto_detail_list(unicode('Title renaming %s to %s\n' % (ptitle, self.config.titlerename[ptitle.lower()])))

            ptitle = self.config.titlerename[ptitle.lower()]

        program['name'] = ptitle
        if psubtitle != '':
            program['episode title'] = psubtitle

        return program

    def filter_description(self,ETitem, ETfind, tdict):
        """
        Filter the description as found on the detailpages for relevant info
        and return the adapted program dict
        """
        alinea = []
        atype = []
        aheader = []

        def format_text(text):
            newtext = self.functions.empersant(text.strip())
            newtext = re.sub('\n','', newtext)
            newtext = re.sub(' +?',' ', newtext)
            return newtext

        pcount = 0
        # We scan every alinea of the description
        for p in ETitem.findall(ETfind):
            aheader.append('')
            atype.append('')
            # Check if it has a class like 'summary'
            if p.get('class') == None:
                atype[pcount] = u''

            else:
                atype[pcount] = self.functions.empersant(p.get('class')).strip()
                if self.config.write_info_files:
                    self.config.infofiles.addto_detail_list(u'%s descriptionattribute => class: %s' % (self.source, p.get('class').strip()))

            content = ''
            # Add the alinea text
            if (p.text != None) and (p.text != ''):
                content = format_text(p.text) + u' '

            # Check for further tags like <i>talic and their following text
            for d in list(p.iter()):
                if d.tag == 'span' and atype[pcount] == 'summary':
                    # On tvgids.nl, this is the genre
                    pass

                elif d.tag in ('br', 'img'):
                    # Linebreaks don't contain text and images we ignore and don't count
                    # But we want the tail text
                    pass

                elif (d.tag == 'p') or (d.text != None and 'gesponsorde link' in d.text.lower()):
                    # We don't want those
                    continue

                elif (d.text != None) and (d.text != ''):
                    if d.tag == 'strong':
                        # The first is an alineaheader
                        # or if it's the first alinea the subgenre or something like it
                        if content.strip() == '':
                            aheader[pcount] = format_text(d.text)
                        else:
                            aheader[pcount] = u''
                            content = content + format_text(d.text) + u' '

                    elif d.tag in ('i', 'em', 'a', 'b'):
                        content = content + format_text(d.text) + u' '

                    else:
                        # Unknown tag we just check for text
                        content = content + format_text(d.text) + u' '
                        if self.config.write_info_files:
                            self.config.infofiles.addto_detail_list(unicode('new '+ self.source+' descriptiontag => ' + \
                                                    unicode(d.tag.strip()) + ': ' + unicode(d.text.strip())))

                # and we add the text inbetween the tags
                if (d.tail != None) and d.tail != '' :
                    content = content + format_text(d.tail) + u' '

            content = content.strip()

            if re.search('geen detailgegevens be(?:kend|schikbaar)', content.lower()) \
              or (content.lower() == '') or (content.lower() == 'none'):
                # No text so unless it's the first alinea, we ignore it
                if pcount == 0:
                    alinea.append('')
                    pcount +=1
                else:
                    continue

            else:
                alinea.append(content)
                pcount +=1

        # Now we decide what to return
        if len(alinea) > 0:
            for i, v in atype.items():
                if v == 'summary' and alinea[i] != '':
                    # We just go for the summary
                    description = alinea[i]
                    break

            else:
                if len(alinea) ==1:
                    # Only ony alinea
                    description = alinea[0]

                elif len(alinea) == 2 and alinea[0] == '':
                    # we go for the second alinea
                    description = alinea[1]

                # Now it gets tricky for most of the time one is general and the other is specific
                # We check if one is contained in the other
                elif len(alinea) == 2 and alinea[1] in alinea[0] :
                     description = alinea[0]

                elif len(alinea) == 2 and alinea[0] in alinea[1] :
                     description = alinea[1]

                # So we return everything
                else:
                    content = ''
                    for p in alinea:
                        if p != '':
                            content = '%s%s ' % (content, p)
                    description = content.strip()

                    if self.config.write_info_files:
                        strdesc = ''
                        for p in alinea:
                            strdesc = strdesc + '    <p>%s</p>\n' % p

                        strdesc = '  <div start="' + tdict['start-time'].strftime('%d %b %H:%M') + \
                                                    '" name="' + tdict['name'] + '">\n' + strdesc + '  </div>'
                        if self.config.write_info_files:
                            self.config.infofiles.addto_raw_string(strdesc)

            # We check to not ovrwrite an already present longer description
            if description > tdict['description']:
                tdict['description'] = description

            # use the first header as subgenre, if not already present
            if tdict['subgenre'] == '' and aheader[0] != '':
                tdict['subgenre']  = aheader[0]

        return tdict

    # Selectie functions
    def get_programcount(self, chanid = 0, offset = None):
        """Return the programcount for given channel id and Offset"""
        if not chanid in self.channels.keys():
            return 0

        if not self.channel_loaded[chanid]:
            return 0

        if offset == None:
            if chanid == 0:
                count = 0

            else:
                return len(self.program_data[chanid])

        if not self.day_loaded[chanid][offset]:
            return 0

        pcount = 0
        for tdict in self.program_data[chanid]:
            if tdict['offset'] == offset:
                pcount += 1

        return pcount

    def get_channel(self, chanid):
        """Return program_data for given channel"""
        if not chanid in self.channels.keys():
            return []

        if not self.channel_loaded[chanid]:
            return []

        return self.program_data[chanid]

    def get_program_data(self, id, item):
        """Return value of given program id and dict key"""
        tdict = get_program(id, item)

        if item in tdict.keys():
            return tdict[item]

        else:
            return None

    # Filter/merge processes
    def parse_programs(self, chanid, mode = 0, overlap_strategy = None):
        """
        Parse a list of programs as generated by parser and
        adjust begin and end times to avoid gaps and overlap.
        Depending on the mode either:
        it's own data 'self.program_data[chanid]' (mode = 0) or
        the finally joined data 'self.config.channels[chanid].all_programs' (mode = 1) is parsed.
        Not setting the overlap_strategy will use the configured default.
        For inbetween parsing you best set it to 'None'
        """

        if mode == 0:
            with self.source_lock:
                programs = self.program_data[chanid][:]

        elif mode == 1:
            programs = self.config.channels[chanid].all_programs[:]

        else:
            return

        for item in programs[:]:
            if item == None:
                programs.remove(item)

        if len(programs) == 0:
            return

        # good programs
        good_programs = []
        fill_programs = []

        # sort all programs by startdate, enddate
        programs.sort(key=lambda program: (program['start-time']))
        if overlap_strategy == None:
            overlap_strategy = self.config.channels[chanid].opt_dict['overlap_strategy']

        # next, correct for missing end time and copy over all good programming to the
        # good_programs list
        for i in range(len(programs)):

            # Try to correct missing end time by taking start time from next program on schedule
            if (not 'stop-time' in programs[i] or not isinstance(programs[i]['stop-time'], datetime.datetime)):
                if i < len(programs)-1:
                    self.config.log(self.config.text('fetch', 22, (programs[i]['name'], )), 64)
                    programs[i]['stop-time'] = programs[i+1]['start-time']

                else:
                    continue

            # The common case: start and end times are present and are not
            # equal to each other (yes, this can happen)
            if isinstance(programs[i]['start-time'], datetime.datetime) \
                and isinstance(programs[i]['stop-time'], datetime.datetime) \
                and programs[i]['start-time'] < programs[i]['stop-time'] \
                and (self.config.opt_dict["offset"] <= programs[i]['offset'] < (self.config.opt_dict["offset"] + self.config.opt_dict["days"])):
                    programs[i]['start-time'].replace(second = 0, microsecond = 0)
                    programs[i]['stop-time'].replace(second = 0, microsecond = 0)
                    programs[i]['length'] = programs[i]['stop-time'] - programs[i]['start-time']
                    good_programs.append(programs[i])
                    if self.show_result:
                        print  'outside timerange', programs[i]['offset'], programs[i]['start-time'], programs[i]['stop-time']

        # Try to exclude programs that only identify a group or broadcaster and have overlapping start/end times with
        # the actual programs
        for i in range(len(good_programs)-2,-1,-1):

            if good_programs[i]['start-time'] == good_programs[i+1]['start-time'] \
                and good_programs[i]['stop-time']  == good_programs[i+1]['stop-time'] \
                and good_programs[i]['name']  == good_programs[i+1]['name']:
                    self.config.log(self.config.text('fetch', 24, (good_programs[i]['name'], )), 64)
                    del good_programs[i]
                    continue

            if good_programs[i]['start-time'] <= good_programs[i+1]['start-time'] \
                and good_programs[i]['stop-time']  >= good_programs[i+1]['stop-time']:
                    self.config.log(self.config.text('fetch', 25, (good_programs[i]['name'], )), 64)
                    del good_programs[i]

        # Fix overlaps/gaps
        if overlap_strategy in ['average', 'stop', 'start', 'fill']:
            for i in range(len(good_programs)-1):
                if i > len(good_programs)-2:
                    break

                # PdB: Fix tvgids start-before-end x minute interval overlap.  An overlap (positive or
                # negative) is halved and each half is assigned to the adjacent programmes. The maximum
                # overlap length between programming is set by the global variable 'max_overlap' and is
                # default 10 minutes. Examples:
                #
                # Positive overlap (= overlap in programming):
                #   10:55 - 12:00 Lala
                #   11:55 - 12:20 Wawa
                # is transformed in:
                #   10:55 - 11.57 Lala
                #   11:57 - 12:20 Wawa
                #
                # Negative overlap (= gap in programming):
                #   10:55 - 11:50 Lala
                #   12:00 - 12:20 Wawa
                # is transformed in:
                #   10:55 - 11.55 Lala
                #   11:55 - 12:20 Wawa

                stop  = good_programs[i]['stop-time']
                start = good_programs[i+1]['start-time']
                dt    = stop-start
                avg   = start + dt // 2
                #~ overlap = 24*60*60*dt.days + dt.seconds
                overlap = dt.total_seconds()

                # check for the size of the overlap
                if 0 < abs(overlap) <= self.config.channels[chanid].opt_dict['max_overlap']*60:
                    if overlap > 0:
                        self.config.log(self.config.text('fetch', 26, (good_programs[i]['name'],good_programs[i+1]['name'],overlap // 60)), 64)
                    else:
                        self.config.log(self.config.text('fetch', 27, (good_programs[i]['name'],good_programs[i+1]['name'],abs(overlap) // 60)), 64)

                    # stop-time of previous program wins
                    if overlap_strategy == 'stop':
                       good_programs[i+1]['start-time'] = good_programs[i]['stop-time']

                    # start-time of next program wins
                    elif overlap_strategy == 'start':
                       good_programs[i]['stop-time'] = good_programs[i+1]['start-time']

                    # average the difference
                    elif overlap_strategy == 'average':
                       good_programs[i]['stop-time']    = avg
                       good_programs[i+1]['start-time'] = avg

                    # We fill it with a programinfo/commercial block
                    elif overlap_strategy == 'fill' and overlap < 0:
                        #~ tdict = self.functions.checkout_program_dict()
                        tdict = {}
                        tdict['source'] = good_programs[i]['source']
                        tdict['channelid'] = good_programs[i]['channelid']
                        tdict['channel'] = good_programs[i]['channel']
                        tdict['name'] = self.config.language_texts['gap text']
                        tdict['start-time'] = good_programs[i]['stop-time']
                        tdict['stop-time'] = good_programs[i+1]['start-time']
                        tdict['offset'] = good_programs[i+1]['offset']
                        tdict['genre'] = u'overige'
                        fill_programs.append(tdict)

                    # leave as is
                    else:
                       pass

                programs[i]['length'] = programs[i]['stop-time'] -  programs[i]['start-time']

                if mode != 0:
                    continue

                # We fill a night gap if start and end program are the same
                elif (0 < good_programs[i]['stop-time'].hour < 6) and (1 < programs[i]['length'].hours < 7):
                    if good_programs[i]['name'] == good_programs[i+1]['name']:
                        good_programs[i]['stop-time'] = good_programs[i+1]['stop-time']
                        good_programs.pop(i+1)

                    else:
                        #~ tdict = self.functions.checkout_program_dict()
                        tdict = {}
                        tdict['source'] = good_programs[i]['source']
                        tdict['channelid'] = good_programs[i]['channelid']
                        tdict['channel'] = good_programs[i]['channel']
                        tdict['name'] = 'Night gap/programs'
                        tdict['start-time'] = good_programs[i]['stop-time']
                        tdict['stop-time'] = good_programs[i+1]['start-time']
                        tdict['offset'] = good_programs[i+1]['offset']
                        tdict['genre'] = u'nieuws/actualiteiten'
                        fill_programs.append(tdict)

        # Experimental strategy to make sure programming does not disappear. All programs that overlap more
        # than the maximum overlap length, but less than the shortest length of the two programs are
        # clumped.
        if self.config.do_clump:
            for i in range(len(good_programs)-1):

                stop  = good_programs[i]['stop-time']
                start = good_programs[i+1]['start-time']
                dt    = stop-start
                overlap = 24*60*60*dt.days + dt.seconds

                length0 = good_programs[i]['stop-time']   - good_programs[i]['start-time']
                length1 = good_programs[i+1]['stop-time'] - good_programs[i+1]['start-time']

                l0 = length0.days*24*60*60 + length0.seconds
                l1 = length1.days*24*60*60 + length0.seconds

                if abs(overlap) >= self.config.channels[chanid].opt_dict['max_overlap']*60 <= min(l0,l1)*60 and \
                    'clumpidx' not in good_programs[i]   and \
                    'clumpidx' not in good_programs[i+1]:
                    good_programs[i]['clumpidx']   = '0/2'
                    good_programs[i+1]['clumpidx'] = '1/2'
                    good_programs[i]['stop-time'] = good_programs[i+1]['stop-time']
                    good_programs[i+1]['start-time'] = good_programs[i]['start-time']


        # done, nothing to see here, please move on
        if len(fill_programs) > 0:
            good_programs.extend(fill_programs)

        if mode == 0:
            with self.source_lock:
                self.program_data[chanid] = good_programs

        elif mode == 1:
            self.config.channels[chanid].all_programs = good_programs

    def merge_sources(self, chanid, prime_source, counter = 0, merge_channel = None):
        """
        Try to match the channel info from the sources into the prime source.  If No prime_source is set
        If available: rtl.nl is used for the rtl channels, npo.nl for the npo and regional channels and teveblad.be
        for the flemmish channels.
        Else the first available is used as set in config.source_order
        """

        no_genric_matching = False
        if merge_channel == None:
            cur_source_id = self.config.channels[chanid].get_source_id(self.proc_id)
            #~ if cur_source_id != '' and cur_source_id in self.config.no_genric_matching[self.proc_id]:
            if cur_source_id != '' and cur_source_id in self.no_genric_matching:
                no_genric_matching = True

            source_merge = True
            prime_source_name = self.config.channelsource[prime_source].source
            other_source_name = self.source
            with self.source_lock:
                if not chanid in self.program_data:
                    self.program_data[chanid] = []

                if len(self.program_data[chanid]) == 0:
                    return

                if len(self.config.channels[chanid].all_programs) == 0:
                    self.config.channels[chanid].all_programs = self.program_data[chanid][:]
                    return

                # This is the by this source collected data
                programs = self.program_data[chanid][:]
                # This is the already collected data to start with the prime source
                info = self.config.channels[chanid].all_programs[:]

        else:
            # This is a channel merge
            source_merge = False
            prime_source_name = self.config.channels[chanid].chan_name
            other_source_name = self.config.channels[merge_channel['chanid']].chan_name
            if len(self.config.channels[merge_channel['chanid']].child_programs) == 0:
                return

            programs = []
            # This channel is limited to a timeslot
            if 'start' in merge_channel and 'end' in merge_channel:
                no_genric_matching = True
                for tdict in self.config.channels[merge_channel['chanid']].child_programs[:]:
                    pstart = tdict['start-time']
                    pstop = tdict['stop-time']
                    tstart = datetime.datetime.combine(pstart.date(), merge_channel['start'])
                    tstart = self.config.combined_channels_tz.localize(tstart)
                    tstart = self.config.utc_tz.normalize(tstart.astimezone(self.config.utc_tz))
                    tstop = datetime.datetime.combine(pstop.date(), merge_channel['end'])
                    tstop = self.config.combined_channels_tz.localize(tstop)
                    tstop = self.config.utc_tz.normalize(tstop.astimezone(self.config.utc_tz))
                    if pstart.date() != pstop.date() and tstop - tstart > datetime.timedelta(days=1):
                        tstart = datetime.datetime.combine(pstop.date(), merge_channel['start'])
                        tstart = self.config.combined_channels_tz.localize(tstart)
                        tstart = self.config.utc_tz.normalize(tstart.astimezone(self.config.utc_tz))
                        tstop = datetime.datetime.combine(pstart.date(), merge_channel['end'])
                        tstop = self.config.combined_channels_tz.localize(tstop)
                        tstop = self.config.utc_tz.normalize(tstop.astimezone(self.config.utc_tz))

                    if (tstart > tstop and tstop <= pstart <= tstart and tstop <= pstop <= tstart) or \
                        (tstart < tstop and ((pstart <= tstart and pstop <= tstart) or (pstart >= tstop and pstop >= tstop))):
                            continue

                    if pstart < tstart and pstop >= tstart:
                        tdict['start-time'] = tstart

                    if pstart <= tstop and pstop > tstop:
                        tdict['stop-time'] = tstop

                    programs.append(tdict)

                self.config.channels[merge_channel['chanid']].child_programs = programs

            else:
                # This is the by this source collected data
                programs = self.config.channels[merge_channel['chanid']].child_programs

            if len(self.config.channels[chanid].all_programs) == 0:
                self.config.channels[chanid].all_programs = self.config.channels[merge_channel['chanid']].child_programs
                return

            # This is the already collected data to start with the prime source
            info = self.config.channels[chanid].all_programs[:]

        match_array = [self.config.text('fetch', 28)]
        def matchlog(matchstr, other_prog, tvgids_prog = None, mode = 1):
            if not (mode & self.config.opt_dict['match_log_level']):
                return

            if mode == 4:
                match_array.extend([u'%s: %s - %s: %s.\n' % \
                        ((matchstr+other_source_name).rjust(25),  other_prog['start-time'].strftime('%d %b %H:%M'),  other_prog['stop-time'].strftime('%H:%M'), other_prog['name']), \
                        '%s: %s - %s: %s.\n' % \
                        (('to '+ prime_source_name).rjust(25), tvgids_prog['start-time'].strftime('%d %b %H:%M'), tvgids_prog['stop-time'].strftime('%H:%M'), tvgids_prog['name'])])
            elif tvgids_prog == None:
                match_array.append(u'%s: %s - %s: %s Genre: %s.\n' % \
                        ((matchstr+other_source_name).rjust(25), other_prog['start-time'].strftime('%d %b %H:%M'),  other_prog['stop-time'].strftime('%H:%M'), \
                        other_prog['name'], other_prog['genre']))
            elif other_prog == None:
                match_array.append(u'%s: %s - %s: %s Genre: %s.\n' % \
                        (matchstr.rjust(25), tvgids_prog['start-time'].strftime('%d %b %H:%M'), tvgids_prog['stop-time'].strftime('%H:%M'), \
                        tvgids_prog['name'], tvgids_prog['genre']))
        # end matchlog()

        def general_renames(name):
            # Some renaming to cover diferences between the sources
            mname = name.lower()
            if chanid in ('0-1', '0-2', '0-3'):
                if mname == 'journaal':
                    return 'NOS Journaal'

                if mname in ('tekst-tv', 'nos tekst tv', 'nos tekst-tv'):
                    return 'Tekst TV'

            if chanid in ('0-1', '0-2'):
                if mname == 'nieuws':
                    return 'NOS Journaal'

            if chanid == '0-3':
                if mname == 'nieuws':
                    return 'NOS op 3'

            if chanid == '0-5':
                if mname == 'herhalingen':
                    return 'Journaallus'

            if chanid == '0-6':
                if mname == 'herhalingen':
                    return 'Canvaslus'

            if chanid in ('0-7', '0-8'):
                if mname == 'nieuws':
                    return 'BBC News'

                if mname == 'het weer':
                    return 'Regional News and Weather'

            if chanid == '0-9':
                if mname == 'nieuws':
                    return 'Tagesschau'

            if chanid == '0-10':
                if mname == 'nieuws':
                    return 'Heute'

            if self.source == 'horizon.tv':
                if chanid in ('0-1', '0-2', '0-3'):
                    if  'nos journaal' in mname:
                        return 'NOS Journaal'

                    if  'nos jeugdjournaal' in mname:
                        return 'Jeugdjournaal'

                    if  'studio sport' in mname:
                        return 'Studio sport'

                    if  'sportjournaal' in mname:
                        return 'Sportjournaal'

                    if mname == 'z@ppbios':
                        return 'Zappbios'

                    if mname == 'z@ppsport':
                        return 'ZappSport'

                if chanid in ('0-5', '0-6'):
                    if  'het journaal' in mname:
                        return 'Journaal'

                if chanid in ('0-4', '0-31', '0-46', '0-92'):
                    if 'rtl nieuws' in mname:
                        return 'Nieuws'

            name = re.sub(' / ',' - ', name)
            return name
        # end general_renames()

        def checkrange(crange = 0):
            checktimes = []
            if crange == 0:
                checktimes.append(0)

            for i in range(1, 6):
                checktimes.append(crange + i)
                checktimes.append(-(crange + i))

            return checktimes
        # end checkrange()

        def match_name(other_title, tvgids_name, other_subtitle = ''):
            """
            Main process for name matching
            Returns 0 if matched on name = name
            Returns 1 if matched on name:episode = name
            Returns None if no match
            """
            def compare(nother, ntvgids, nsub = ''):
                if nother == ntvgids:
                    return 0

                if re.sub('[-,. ]', '', nother) == re.sub('[-,. ]', '', ntvgids):
                    return 0

                if len(ntvgids.split(':')) > 1 and nsub != '':
                    ntvsplit = ntvgids.split(':')[0]
                    if nother == ntvsplit:
                        return 1

                    if len(nother) < len(ntvsplit):
                        if nother == ntvsplit[len(ntvsplit) - len(nother):]:
                            return 1

                        if nother == ntvsplit[0:len(nother)]:
                            return 1

                    if len(nother) > len(ntvsplit):
                        if nother[len(nother) - len(ntvsplit):] == ntvsplit:
                            return 1

                        elif nother[0:len(ntvsplit)] == ntvsplit:
                            return 1

                if len(nother) < len(ntvgids):
                    if nother == ntvgids[len(ntvgids) - len(nother):]:
                        return 0

                    if nother == ntvgids[0:len(nother)]:
                        return 0

                if len(nother) > len(ntvgids):
                    if nother[len(nother) - len(ntvgids):] == ntvgids:
                        return 0

                    elif nother[0:len(ntvgids)] == ntvgids:
                        return 0

                return None
            # end compare()

            other_name = other_title.lower().strip()
            other_subname = other_subtitle.lower().strip()
            tvgids_name = tvgids_name.lower().strip()
            x = compare(self.functions.remove_accents(other_name), self.functions.remove_accents(tvgids_name), self.functions.remove_accents(other_subname))
            if x != None:
                return x

            matchobject = difflib.SequenceMatcher(isjunk=lambda x: x in " '\",.-/", autojunk=False)
            matchobject.set_seqs(self.functions.remove_accents(other_name), self.functions.remove_accents(tvgids_name))
            if matchobject.ratio() > .8:
                return 0

            name_split = False
            lother_name = other_name
            rother_name = other_name
            if other_name.find(':') != -1:
                name_split = True
                lother_name = other_name.split(':')[0].strip()
                rother_name = other_name.split(':')[1].strip()

            ltvgids_name = tvgids_name
            rtvgids_name = tvgids_name
            if tvgids_name.find(':') != -1:
                name_split = True
                ltvgids_name = tvgids_name.split(':')[0].strip()
                rtvgids_name = tvgids_name.split(':')[1].strip()

            if name_split:
                x = compare(self.functions.remove_accents(rother_name), self.functions.remove_accents(rtvgids_name))
                if x != None:
                    return x

                matchobject.set_seqs(self.functions.remove_accents(rother_name), self.functions.remove_accents(rtvgids_name))
                if matchobject.ratio() > .8:
                    return 0

                x = compare(self.functions.remove_accents(lother_name), self.functions.remove_accents(ltvgids_name))
                if x != None:
                    return x

                matchobject.set_seqs(self.functions.remove_accents(lother_name), self.functions.remove_accents(ltvgids_name))
                if matchobject.ratio() > .8:
                    return 0

            return None
        # end match_name()

        def match_genre(other_genre, tvgids_genre, tvgids_subgenre):
            """
            Process for Genre matching
            Returns True or False
            """
            tvgids_genre = tvgids_genre.lower().strip()
            tvgids_subgenre = tvgids_subgenre.lower().strip()
            other_genre = other_genre.lower().strip()
            if  (tvgids_genre == 'overige') or (other_genre == 'overige'):
                return False

            elif  (tvgids_genre != '') and (other_genre == tvgids_genre):
                return True

            elif (other_genre == 'amusement'):
                if (tvgids_genre == 'amusement') or (tvgids_genre == 'amusment') \
                  or (tvgids_genre == 'kunst en cultuur'):
                    return True

            elif (other_genre == 'kinderen') and (tvgids_genre == 'jeugd'):
                return True

            elif (other_genre == 'magazine') and (tvgids_genre == 'informatief, amusement'):
                return True

            elif (other_genre == 'nieuws') and (tvgids_genre == 'nieuws/actualiteiten'):
                return True

            elif (other_genre == 'serie') and (tvgids_genre == 'serie/soap'):
                return True

            elif (other_genre == 'serie') and (tvgids_genre == 'film'):
                return True

            elif (other_genre == 'reality'):
                if (tvgids_genre == 'informatief'):
                    if (tvgids_subgenre == 'realityprogramma') or (tvgids_subgenre == 'realityserie'):
                        return True

            elif (other_genre == 'documentaire'):
                if (tvgids_genre == 'informatief') and (tvgids_subgenre == 'documentaire'):
                    return True

                elif (tvgids_genre == 'info') and (tvgids_subgenre == 'documentary'):
                    return True

                elif (tvgids_genre == 'natuur') and (tvgids_subgenre == 'natuurdocumentaire, natuurprogramma'):
                    return True

            return False
        # end match_genre()

        def set_main_id(tdict):

            for s in self.config.sourceid_order:
                if tdict['prog_ID'][s] != '':
                    tdict['ID'] = tdict['prog_ID'][s]
                    break

            return tdict
        # end set_main_id()

        def merge_programs(tdict, tvdict, reverse_match=False, use_other_title = 0, copy_ids = True):
            if use_other_title != 0:
                tdict['name']  = tvdict['name']

            if tdict['premiere year'] == '':
                tdict['premiere year'] = tvdict['premiere year']

            if tdict['airdate'] == '':
                tdict['airdate'] = tvdict['airdate']

            if tvdict['rerun']:
                tdict['rerun'] = True

            if tdict['country'] == '':
                tdict['country'] = tvdict['country']

            if tdict['originaltitle'] == '':
                tdict['originaltitle'] = tvdict['originaltitle']

            if len(tvdict['description']) > len(tdict['description']):
                tdict['description']  = tvdict['description']

            if tdict['prefered description'] == '':
                tdict['prefered description'] = tvdict['prefered description']

            if tdict['broadcaster'] == '':
                tdict['broadcaster'] = tvdict['broadcaster']

            if tdict['star-rating'] == '':
                tdict['star-rating'] = tvdict['star-rating']

            if len(tvdict['rating']) > 0:
                for item in tvdict['rating']:
                    tdict['rating'].append(item)

            if tvdict['video']['HD']:
                tdict['video']['HD']  = True

            if tvdict['video']['widescreen']:
                tdict['video']['widescreen']  = True

            if tvdict['video']['blackwhite']:
                tdict['video']['blackwhite']  = True

            if tvdict['teletext']:
                tdict['teletext']  = True

            if tdict['audio'] == '':
                tdict['audio'] = tvdict['audio']

            for role in tvdict['credits']:
                if not role in tdict['credits']:
                    tdict['credits'][role] = []

                for rp in tvdict['credits'][role]:
                    if not rp in tdict['credits'][role]:
                        tdict['credits'][role].append(rp)

            if copy_ids:
                for source in self.config.source_order:
                    if tvdict['prog_ID'][source] != u'':
                        tdict['prog_ID'][source]  = tvdict['prog_ID'][source]

                    if tvdict['detail_url'][source] != u'':
                        tdict['detail_url'][source]  = tvdict['detail_url'][source]

            tdict = set_main_id(tdict)
            if reverse_match:
                if not self.proc_id in (2, 6, 5) and tdict['episode title'] == '':
                    tdict['episode title'] = tvdict['episode title']

                if self.proc_id != 1:
                    tdict['genre'] = tvdict['genre']
                    tdict['subgenre'] = tvdict['subgenre']

                elif tdict['genre'] in ('', 'overige'):
                    tdict['genre'] = tvdict['genre']
                    if tdict['subgenre'] == '':
                        tdict['subgenre'] = tvdict['subgenre']

                tdict['merge-source'] = other_source_name
                matched_programs.append(tdict)
                if tdict in programs: programs.remove(tdict)
                if tdict['start-time'] in prog_starttimes: del prog_starttimes[tdict['start-time']]

            else:
                # We try to fill gaps in the prime source that are defined in the other
                for item in info_gaps:
                    if tdict['stop-time'] == item['start-time'] and item['start-time'] < tvdict['stop-time'] <= item['stop-time']:
                            tdict['stop-time'] = tvdict['stop-time']
                            break

                    if tdict['start-time'] == item['stop-time'] and item['start-time'] < tvdict['start-time'] <= item['stop-time']:
                            tdict['start-time'] = tvdict['start-time']
                            break

                if self.proc_id in (2, 6, 5) and (tvdict['episode title'] != '' or tdict['episode title'] == ''):
                    tdict['episode title'] = tvdict['episode title']

                if tdict['season'] == 0:
                    tdict['season'] = tvdict['season']

                if tdict['episode'] == 0:
                    tdict['episode'] = tvdict['episode']

                if self.proc_id == 1:
                    tdict['genre'] = tvdict['genre']
                    tdict['subgenre'] = tvdict['subgenre']

                elif tdict['genre'] in ('', 'overige'):
                    tdict['genre'] = tvdict['genre']
                    if tdict['subgenre'] == '':
                        tdict['subgenre'] = tvdict['subgenre']

                if tdict['merge-source'] == '':
                    tdict['merge-source'] = prime_source_name

                matched_programs.append(tdict)
                if tdict in info: info.remove(tdict)

        # merge_programs()

        # tdict is from info
        def check_match_to_info(tdict, pi, mstart, check_overlap = True, check_genre = True, auto_merge = True):
            if no_genric_matching:
                check_genre = False

            x = match_name(pi['name'], tdict['name'], pi['episode title'])
            if x != None:
                matchlog(self.config.text('fetch', 29), pi, tdict, 4)
                retval = 1

            elif check_genre and match_genre(pi['genre'], tdict['genre'], pi['subgenre']):
                matchlog(self.config.text('fetch', 30), pi, tdict, 4)
                x = 0
                retval = 2

            else:
                return 0

            if check_overlap and not no_genric_matching:
                try:
                    mduur = (tdict['stop-time'] - tdict['start-time']).total_seconds()
                    pduur = (pi['stop-time'] - pi['start-time']).total_seconds()
                    if pduur * 1.1 > mduur:
                        # We check for program merging in info
                        merge_match.append({'type': 1, 'tdict': tdict, 'prog': pi, 'match': x})
                        if tdict in info: info.remove(tdict)

                    elif mduur * 1.1 > pduur:
                        # We check for program merging in programs
                        merge_match.append({'type': 2, 'tdict': tdict, 'prog': pi, 'match': x})
                        if tdict in info: info.remove(tdict)

                    elif auto_merge:
                        merge_programs(tdict, pi, reverse_match=False, use_other_title = x)

                except:
                    if auto_merge:
                        merge_programs(tdict, pi, reverse_match=False, use_other_title = x)

            elif auto_merge:
                merge_programs(tdict, pi, reverse_match=False, use_other_title = x)

            if pi in programs: programs.remove(pi)
            if mstart in prog_starttimes: del prog_starttimes[mstart]
            return retval

        # end check_match_to_info()

        log_array =['\n']
        if merge_channel == None:
            log_array.append(self.config.text('fetch', 33, \
                (self.config.channels[chanid].chan_name , counter, self.config.chan_count, other_source_name, prime_source_name)))
            self.config.log(['\n', self.config.text('fetch', 31, (self.config.channels[chanid].chan_name , counter, self.config.chan_count)), \
                self.config.text('fetch', 32, (len(programs), other_source_name, len(info), prime_source_name))], 2)

        else:
            log_array.append(self.config.text('fetch', 36, (prime_source_name , counter, self.config.chan_count, other_source_name)))
            self.config.log(['\n', self.config.text('fetch', 34, (len(programs), other_source_name, len(info), prime_source_name)), \
                self.config.text('fetch', 35, (counter, self.config.chan_count))], 2)

        # Do some general renaming to match tvgids.nl naming
        for i in range(0, len(programs)):
            programs[i]['name'] = general_renames(programs[i]['name'])

        for i in range(0, len(info)):
            info[i]['name'] = general_renames(info[i]['name'])

        # Sort both lists on starttime and get their ranges
        info.sort(key=lambda program: (program['start-time']))
        infostarttime = info[0]['start-time'] + datetime.timedelta(seconds = 5)
        infoendtime = info[-1]['stop-time'] - datetime.timedelta(seconds = 5)

        programs.sort(key=lambda program: (program['start-time']))
        progstarttime = programs[0]['start-time'] + datetime.timedelta(seconds = 5)
        progendtime = programs[-1]['stop-time'] - datetime.timedelta(seconds = 5)

        log_array.append(self.config.text('fetch', 37, \
            (len(info), prime_source_name.ljust(11), infostarttime.strftime('%d-%b %H:%M'), infoendtime.strftime('%d-%b %H:%M'))))
        log_array.append(self.config.text('fetch', 38, \
            (len(programs), other_source_name.ljust(11), progstarttime.strftime('%d-%b %H:%M'), progendtime.strftime('%d-%b %H:%M'))))
        log_array.append('\n')

        # move all programs outside the range of programs to matched_programs
        # count the info names, changing them to lowercase for matching
        # and organise them by name and start-time
        matched_programs = []
        info_gaps = []
        generic_match = []
        info_groups = []
        info_starttimes = {}
        info_names = {}
        prog_groups = []
        prog_names = {}
        prog_starttimes ={}
        ocount = 0

        # Get existing gaps in info larger then 'max_overlap'
        for index in range(1, len(info)):
            if (info[index]['start-time'] -  info[index -1]['stop-time']).total_seconds()  > self.config.channels[chanid].opt_dict['max_overlap']*60:
                info_gaps.append({'start-time': info[index -1]['stop-time'] - datetime.timedelta(seconds = 5 ),
                                                'stop-time': info[index]['start-time'] + datetime.timedelta(seconds = 5 )})

        # And we create a list of starttimes and of names for matching
        for tdict in info[:]:
            if (tdict['name'].lower() in self.config.groupslot_names) \
              or (chanid in ('0-1', '0-2', '0-3') and  tdict['name'].lower() == 'kro kindertijd') \
              or (chanid in ('0-34','1-veronica', "0-311") and \
              (tdict['name'].lower() == 'disney xd' or tdict['name'].lower() == 'disney')):
                # These are group names. We move them aside to not get hit by merge_match
                info_groups.append(tdict)
                if tdict in info: info.remove(tdict)
                continue

            info_starttimes[tdict['start-time']] = tdict
            iname = tdict['name'].lower().strip()
            if not iname in info_names or (info_names[iname]['genre'] in ('', 'overige')):
                info_names[iname] = tdict

            # These do not overlap in time so they cannot be matched
            if (tdict['start-time'] >= progendtime) or (tdict['stop-time'] <= progstarttime):
                ocount += 1
                tdict = set_main_id(tdict)
                if tdict['merge-source'] == '':
                    tdict['merge-source'] = prime_source_name

                if tdict['genre'] in ('', 'overige'):
                    # We later try to match them generic to get a genre
                    generic_match.append(tdict)

                else:
                    matched_programs.append(tdict)

                matchlog(self.config.text('fetch', 46), None, tdict, 1)
                if tdict in info: info.remove(tdict)

        # count the occurense of the rest and organise by name/start-time and stop-time
        for tdict in programs[:]:
            if (tdict['name'].lower() in self.config.groupslot_names) \
              or (chanid in ('0-1', '0-2', '0-3') and  tdict['name'].lower() == 'kro kindertijd') \
              or (chanid in ('0-34','1-veronica', "0-311") and \
              (tdict['name'].lower() == 'disney xd' or tdict['name'].lower() == 'disney')):
                # These are group names. We move them aside to not get hit by merge_match
                prog_groups.append(tdict)
                if tdict in programs: programs.remove(tdict)
                continue

            prog_starttimes[tdict['start-time']] = tdict
            prog_starttimes[tdict['start-time']]['matched'] = False
            rname = tdict['name'].lower().strip()
            if not (rname in prog_names):
                prog_names[rname] = {}
                prog_names[rname]['count'] = 0
                prog_names[rname]['genre'] = tdict['genre']
                prog_names[rname]['subgenre'] = tdict['subgenre']

            elif prog_names[rname]['genre'] in ('', 'overige'):
                prog_names[rname]['genre'] = tdict['genre']
                prog_names[rname]['subgenre'] = tdict['subgenre']

            prog_names[rname]['count'] += 1
            # These do not overlap in time so they cannot be matched
            if (tdict['start-time'] >= infoendtime) or (tdict['stop-time'] <= infostarttime):
                ocount += 1
                tdict = set_main_id(tdict)
                tdict['merge-source'] = other_source_name
                if tdict['genre'] in ('', 'overige'):
                    # We later try to match them generic to get a genre
                    generic_match.append(tdict)

                else:
                    matched_programs.append(tdict)

                matchlog(self.config.text('fetch', 48), tdict, None, 1)
                if tdict in programs: programs.remove(tdict)
                if tdict['start-time'] in prog_starttimes: del prog_starttimes[tdict['start-time']]
                continue

            # These are missing in info so they cannot be matched
            for pgap in info_gaps[:]:
                if (tdict['start-time'] >= pgap['start-time']) and (tdict['stop-time'] <= pgap['stop-time']):
                    ocount += 1
                    tdict = set_main_id(tdict)
                    tdict['merge-source'] = other_source_name
                    if tdict['genre'] in ('', 'overige'):
                        # We later try to match them generic to get a genre
                        generic_match.append(tdict)

                    else:
                        matched_programs.append(tdict)

                    matchlog(self.config.text('fetch', 48), tdict, None, 1)
                    if tdict in programs: programs.remove(tdict)
                    if tdict['start-time'] in prog_starttimes: del prog_starttimes[tdict['start-time']]
                    break

        log_array.append(self.config.text('fetch', 39, (ocount, )))
        log_array.append(self.config.text('fetch', 40, (len(info), prime_source_name)))
        log_array.append(self.config.text('fetch', 40, (len(programs), other_source_name)))
        log_array.append('\n')

        ncount = 0
        gcount = 0
        rcount = 0
        scount = 0
        # Try to match programs without genre to get genre
        for tdict in generic_match[:]:
            rname = tdict['name'].lower().strip()
            match_list = difflib.get_close_matches(rname, info_names.iterkeys(), 1, 0.9)
            if len(match_list) > 0 and not info_names[match_list[0]]['genre'] in ('', 'overige'):
                tdict['genre'] = info_names[match_list[0]]['genre']
                tdict['subgenre'] = info_names[match_list[0]]['subgenre']
                rcount += 1

            else:
                match_list = difflib.get_close_matches(rname, prog_names.iterkeys(), 1, 0.9)
                if len(match_list) > 0 and not prog_names[match_list[0]]['genre'] in ('', 'overige'):
                    tdict['genre'] = prog_names[match_list[0]]['genre']
                    tdict['subgenre'] = prog_names[match_list[0]]['subgenre']
                    rcount += 1

            tdict = set_main_id(tdict)
            matched_programs.append(tdict)
            if tdict in generic_match: generic_match.remove(tdict)

        # Parse twice to recheck after generic name matching
        for checkrun in (0, 1):
            # first look on matching starttime (+/- 5 min) and similar names or matching genre
            # extending the range by 5 min to 30
            merge_match =[]
            for check in range(0, 30, 5):
                if len(info) == 0:
                    break

                for tdict in info[:]:
                    for i in checkrange(check):
                        mstart = tdict['start-time'] + datetime.timedelta(0, 0, 0, 0, i)
                        if mstart in prog_starttimes:
                            pi = prog_starttimes[mstart]
                            x = check_match_to_info(tdict, pi, mstart, check_genre = (source_merge and (checkrun==1)))
                            if x == 1:
                                ncount += 1
                                break

                            if x == 2:
                                gcount += 1
                                break

            # Check for following twins that were merged in the other (teveblad shows following parts often separate)
            for item in merge_match:
                tdict = item['tdict']
                pi = item['prog']
                pset = []
                # pi (from programs) is the longer one (by 10%+)
                if item['type'] == 1:
                    pset.append(tdict)
                    for pp in info:
                        pduur = (pp['stop-time'] - pp['start-time']).total_seconds()
                        if (pi['start-time'] <= pp['start-time'] <= pi['stop-time']) \
                          and (pi['start-time'] <= pp['start-time'] <= pi['stop-time']):
                            # Full overlap
                            pset.append(pp)

                        elif (pi['start-time'] <= pp['start-time'] <= pi['stop-time']):
                            # Starttime overlap more than 50%
                            if (pi['stop-time'] - pp['start-time']).total_seconds() > (0.5 * pduur):
                                pset.append(pp)

                        elif (pi['start-time'] <= pp['stop-time'] <= pi['stop-time']):
                            # Stoptime overlap more than 50%
                            if (pp['stop-time'] - pi['start-time']).total_seconds() > (0.5 * pduur):
                                pset.append(pp)

                    if len(pset) > 1:
                        twin_ncount = 0
                        twin_gcount = 0
                        for pp in pset[:]:
                            if pp != tdict:
                                x = check_match_to_info(pp, pi, None, False, check_genre = source_merge, auto_merge = False)
                                if x == 0:
                                    # No match. Remove it
                                    pset.remove(pp)

                                elif x == 1:
                                    # It matches on name
                                    twin_ncount += 1

                                elif x == 2:
                                    # It matches on genre
                                    twin_gcount += 1

                    if len(pset) > 1:
                        if self.config.channels[chanid].opt_dict['use_split_episodes']:
                            ncount += twin_ncount
                            gcount += twin_gcount
                            for pp in pset:
                                if pp == tdict:
                                    # The original match
                                    merge_programs(pp, pi)

                                else:
                                    merge_programs(pp, pi, copy_ids = False)

                        else:
                            # So we have to use the timings from programs
                            merge_programs(pi, tdict, reverse_match = True, use_other_title = item['match'])

                    else:
                        merge_programs(tdict, pi, use_other_title = item['match'])

                # tdict (from info) is the longer one (by 10%+)
                elif item['type'] == 2:
                    pset.append(pi)
                    for pp in prog_starttimes.values():
                        pduur = (pp['stop-time'] - pp['start-time']).total_seconds()
                        if (tdict['start-time'] <= pp['start-time'] <= tdict['stop-time']) \
                          and (tdict['start-time'] <= pp['start-time'] <= tdict['stop-time']):
                            # Full overlap
                            pset.append(pp)

                        elif (tdict['start-time'] <= pp['start-time'] <= tdict['stop-time']) and \
                          (tdict['stop-time'] - pp['start-time']).total_seconds() > (0.5 * pduur):
                            # Starttime overlap more than 50%
                                pset.append(pp)

                        elif (tdict['start-time'] <= pp['stop-time'] <= tdict['stop-time']) and \
                          (pp['stop-time'] - tdict['start-time']).total_seconds() > (0.5 * pduur):
                            # Stoptime overlap more than 50%
                                pset.append(pp)

                    if len(pset) > 1:
                        twin_ncount = 0
                        twin_gcount = 0
                        for pp in pset[:]:
                            if pp != pi:
                                x = check_match_to_info(tdict, pp, None, False, check_genre = source_merge, auto_merge = False)
                                if x == 0:
                                    # No match. Remove it
                                    pset.remove(pp)

                                elif x == 1:
                                    # It matches on name
                                    twin_ncount += 1

                                elif x == 2:
                                    # It matches on genre
                                    twin_gcount += 1

                    if len(pset) > 1 and self.config.channels[chanid].opt_dict['use_split_episodes']:
                        ncount += twin_ncount
                        gcount += twin_gcount
                        # So we have to use the timings from programs
                        for pp in pset:
                            if pp == pi:
                                # The original match
                                merge_programs(pp, tdict, reverse_match = True)

                            else:
                                merge_programs(pp, tdict, reverse_match = True, copy_ids = False)

                    else:
                        merge_programs(tdict, pi)

            # next mainly for rtl match generic on name to get genre. But only the first run
            if checkrun > 0:
                break

            for tdict in info[:]:
                rname = tdict['name'].lower().strip()
                match_list = difflib.get_close_matches(rname, info_names.iterkeys(), 1, 0.9)
                if len(match_list) > 0 and not info_names[match_list[0]]['genre'] in ('', 'overige'):
                    tdict['genre'] = info_names[match_list[0]]['genre']
                    tdict['subgenre'] = info_names[match_list[0]]['subgenre']
                    rcount += 1

                else:
                    match_list = difflib.get_close_matches(rname, prog_names.iterkeys(), 1, 0.9)
                    if len(match_list) > 0 and not prog_names[match_list[0]]['genre'] in ('', 'overige'):
                        tdict['genre'] = prog_names[match_list[0]]['genre']
                        tdict['subgenre'] = prog_names[match_list[0]]['subgenre']
                        rcount += 1

            log_array.append(self.config.text('fetch', 41, (rcount, )))
            if rcount == 0 or no_genric_matching:
                break

        # Passing over generic timeslots that maybe detailed in the other
        delta_10 =  datetime.timedelta(minutes = 10)
        info.extend(info_groups)
        for tdict in prog_groups[:]:
            pcount = 0
            for tvdict in info[:]:
                if (tvdict['start-time'] >= (tdict['start-time'] - delta_10)) and (tvdict['stop-time'] <= (tdict['stop-time'] + delta_10)):
                    scount += 1
                    pcount += 1
                    tvdict = set_main_id(tvdict)
                    if tvdict['merge-source'] == '':
                        tvdict['merge-source'] = prime_source_name

                    matched_programs.append(tvdict)
                    if pcount == 1:
                        matchlog(self.config.text('fetch', 47), tdict, None, 8)

                    matchlog('', None, tvdict, 8)
                    if tvdict in info: info.remove(tvdict)
                    if tvdict in info_groups: info_groups.remove(tvdict)

            if pcount == 0:
                programs.append(tdict)

            if tdict['start-time'] in prog_starttimes: del prog_starttimes[tdict['start-time']]

        for tdict in info_groups[:]:
            pcount = 0
            for tvdict in programs[:]:
                if (tvdict['start-time'] >= (tdict['start-time'] - delta_10)) and (tvdict['stop-time'] <= (tdict['stop-time'] + delta_10)):
                    scount += 1
                    pcount += 1
                    tvdict = set_main_id(tvdict)
                    tvdict['merge-source'] = other_source_name
                    matched_programs.append(tvdict)
                    if pcount == 1:
                        matchlog(self.config.text('fetch', 49), None, tdict, 8)

                    matchlog('', tvdict, None, 8)
                    if tvdict in programs: programs.remove(tvdict)
                    if tvdict['start-time'] in prog_starttimes: del prog_starttimes[tvdict['start-time']]

            if pcount == 0:
                tdict = set_main_id(tdict)
                if tdict['merge-source'] == '':
                    tdict['merge-source'] = prime_source_name

                matchlog(self.config.text('fetch', 46), None, tdict, 1)
                matched_programs.append(tdict)

            if tdict in info: info.remove(tdict)

        log_array.append(self.config.text('fetch', 42, (ncount, )))
        log_array.append(self.config.text('fetch', 43, (gcount, )))
        log_array.append(self.config.text('fetch', 44, (scount, )))
        log_array.append(self.config.text('fetch', 45, (len(info), )))

        # List unmatched items to the log
        for tdict in info[:]:
            matchlog(self.config.text('fetch', 46), None, tdict, 1)
            tdict = set_main_id(tdict)
            if tdict['merge-source'] == '':
                tdict['merge-source'] = prime_source_name

            matched_programs.append(tdict)

        p = []
        for tdict in prog_starttimes.itervalues():
            if infostarttime < tdict['start-time'] < infoendtime:
                p.append(tdict)

        p.sort(key=lambda program: (program['start-time'],program['stop-time']))
        for tdict in p:
            matchlog(self.config.text('fetch', 50), tdict, None , 2)

        log_array.append('\n')
        self.config.log(log_array, 4, 3)
        self.config.log(match_array, 32, 3)

        self.config.channels[chanid].all_programs = matched_programs
        try:
            if self.config.write_info_files:
                self.config.infofiles.write_fetch_list(matched_programs, chanid, other_source_name, self.config.channels[chanid].chan_name, None, True)

        except:
            pass

# end FetchData()

class Channel_Config(Thread):
    """
    Class that holds the Channel definitions and manages the data retrieval and processing
    """
    def __init__(self, config, chanid = 0, name = '', group = 99):
        Thread.__init__(self)
        # Flag to stop the thread
        self.config = config
        self.functions = self.config.fetch_func
        self.channel_node = None
        self.quit = False
        self.state = None
        self.statetext = ''
        self.thread_type = 'channel'

        # Flags to indicate the data is in
        self.source_data = {}
        self.detail_data = Event()
        self.child_data = Event()
        self.cache_return = Queue()
        self.channel_lock = Lock()

        # Flag to indicate all data is processed
        self.ready = False

        self.active = False
        self.is_child = False
        self.child_programs = []
        self.counter = 0
        self.chanid = chanid
        self.xmltvid = self.chanid
        self.chan_name = name
        self.group = group
        self.source_id = {}
        self.icon_source = -1
        self.icon = ''

        # This will contain the final fetcheddata
        self.all_programs = []
        self.current_prime = ''

        self.opt_dict = {}
        self.prevalidate_opt = {}
        self.opt_dict['xmltvid_alias'] = None
        self.opt_dict['disable_source'] = []
        self.opt_dict['disable_detail_source'] = []
        self.opt_dict['disable_ttvdb'] = False
        self.opt_dict['prime_source'] = -1
        self.prevalidate_opt['prime_source'] = -1
        self.opt_dict['prefered_description'] = -1
        self.opt_dict['append_tvgidstv'] = True
        self.opt_dict['fast'] = self.config.opt_dict['fast']
        self.opt_dict['slowdays'] = self.config.opt_dict['slowdays']
        self.opt_dict['compat'] = self.config.opt_dict['compat']
        self.opt_dict['legacy_xmltvids'] = self.config.opt_dict['legacy_xmltvids']
        self.opt_dict['max_overlap'] = self.config.opt_dict['max_overlap']
        self.opt_dict['overlap_strategy'] = self.config.opt_dict['overlap_strategy']
        self.opt_dict['logos'] = self.config.opt_dict['logos']
        self.opt_dict['desc_length'] = self.config.opt_dict['desc_length']
        self.opt_dict['use_split_episodes'] = self.config.opt_dict['use_split_episodes']
        self.opt_dict['cattrans'] = self.config.opt_dict['cattrans']
        self.opt_dict['mark_hd'] = self.config.opt_dict['mark_hd']
        self.opt_dict['add_hd_id'] = False
        self.config.threads.append(self)

    def validate_settings(self):

        if not self.active and not self.is_child:
            return

        if self.prevalidate_opt['prime_source'] == -1:
            self.config.validate_option('prime_source', self)

        else:
            self.config.validate_option('prime_source', self, self.prevalidate_opt['prime_source'])

        self.config.validate_option('prefered_description', self)
        self.config.validate_option('overlap_strategy', self)
        self.config.validate_option('max_overlap', self)
        self.config.validate_option('desc_length', self)
        self.config.validate_option('slowdays', self)
        if self.group in self.config.ttvdb_disabled_groups:
            self.opt_dict['disable_ttvdb'] = True

        if self.opt_dict['xmltvid_alias'] != None:
            self.xmltvid = self.opt_dict['xmltvid_alias']

        elif (self.config.configversion < 2.208 or self.opt_dict['legacy_xmltvids'] == True):
            xmltvid = self.chanid.split('-',1)
            self.xmltvid = xmltvid[1] if int(xmltvid[0]) < 4 else self.chanid

    def run(self):

        if not self.active and not self.is_child:
            self.ready = True
            self.state = None
            for index in self.config.source_order:
                self.source_ready(index).set()

            self.detail_data.set()
            return

        if not self.is_child:
            self.state = None
            self.child_data.set()

        try:
            # Create the merge order
            self.statetext = 'preparing'
            self.state = 1
            self.merge_order = []
            last_merge = []
            if (self.get_source_id(self.opt_dict['prime_source']) != '') \
              and not (self.opt_dict['prime_source'] in self.opt_dict['disable_source']) \
              and not (self.opt_dict['prime_source'] in self.config.opt_dict['disable_source']):
                if self.get_source_id(self.opt_dict['prime_source']) in self.config.channelsource[self.opt_dict['prime_source']].no_genric_matching:
                    last_merge.append(self.opt_dict['prime_source'])

                else:
                    self.merge_order.append(self.opt_dict['prime_source'])

            for index in self.config.source_order:
                if (self.get_source_id(index) != '') \
                  and index != self.opt_dict['prime_source'] \
                  and not (index in self.opt_dict['disable_source']) \
                  and not (index in self.config.opt_dict['disable_source']):
                    if self.get_source_id(index) in self.config.channelsource[index].no_genric_matching:
                        last_merge.append(index)

                    else:
                        self.merge_order.append(index)

                elif index != self.opt_dict['prime_source']:
                    self.source_ready(index).set()

            self.merge_order.extend(last_merge)
            xml_data = False
            # Retrieve and merge the data from the available sources.
            self.statetext = 'waiting for basepages'
            self.state = 2
            for index in self.merge_order:
                while not self.source_ready(index).is_set():
                    # Wait till the event is set by the source, but check every 5 seconds for an unexpected break or wether the source is still alive
                    self.source_ready(index).wait(5)
                    if self.quit:
                        self.ready = True
                        return

                    # Check if the source is still alive
                    if not self.config.channelsource[index].is_alive():
                        self.source_ready(index).set()
                        break

                if self.source_ready(index).is_set():
                    if len(self.config.channelsource[index].program_data[self.chanid]) == 0:
                        # Nothing was returned
                        if not (index == 1 and 0 in self.merge_order):
                            self.config.log(self.config.text('fetch', 51, (self.config.channelsource[index].source, self.chan_name)))

                    elif self.channel_node == None:
                        # This is the first source with data, so we just take in the data creating the channel Node
                        xml_data = True
                        #~ prime_source = self.config.channelsource[index].proc_id
                        prime_source_name = self.config.channelsource[index].source
                        with self.config.channelsource[index].source_lock:
                            self.channel_node = tv_grab_IO.ChannelNode(self.config, self, self.config.channelsource[index].program_data[self.chanid][:], index)
                            #~ self.all_programs = self.config.channelsource[index].program_data[self.chanid][:]

                    else:
                        # There is already data, so we merge the incomming data into that
                        xml_data = True
                        with self.config.channelsource[index].source_lock:
                            self.channel_node.merge_source(self.config.channelsource[index].program_data[self.chanid][:], index)
                            #~ self.config.channelsource[index].merge_sources(self.chanid,  prime_source, self.counter)
                            #~ self.config.channelsource[index].parse_programs(self.chanid, 1, 'None')
                            #~ for i in range(0, len(self.all_programs)):
                                #~ self.all_programs[i] = self.config.fetch_func.checkout_program_dict(self.all_programs[i])

            if self.chanid in self.config.combined_channels.keys():
                self.statetext = 'waiting for children'
                self.state = 3
                for c in self.config.combined_channels[self.chanid][:]:
                    if c['chanid'] in self.config.channels:
                        while not self.config.channels[c['chanid']].child_data.is_set():
                            # Wait till the event is set by the child, but check every 5 seconds for an unexpected break or wether the child is still alive
                            #~ print 'Waiting for', c['chanid']
                            self.config.channels[c['chanid']].child_data.wait(5)
                            if self.quit:
                                self.ready = True
                                return

                            # Check if the child is still alive
                            if not self.config.channels[c['chanid']].is_alive():
                                break

                        if not isinstance(self.config.channels[c['chanid']].channel_node, tv_grab_IO.ChannelNode) \
                          or self.config.channels[c['chanid']].channel_node.program_count() == 0:
                            self.config.log(self.config.text('fetch', 51, (self.config.channels[c['chanid']].chan_name, self.chan_name)))

                        elif self.child_data.is_set():
                            # We always merge as there might be restrictions
                            xml_data = True
                            if self.channel_node == None:
                                self.channel_node = tv_grab_IO.ChannelNode(self.config, self)

                            self.channel_node.merge_channel(self.config.channels[c['chanid']].channel_node)
                            #~ self.config.channelsource[0].merge_sources(self.chanid,  None, self.counter, c)
                            #~ self.config.channelsource[0].parse_programs(self.chanid, 1, 'None')
                            #~ for i in range(0, len(self.all_programs)):
                                #~ self.all_programs[i] = self.config.fetch_func.checkout_program_dict(self.all_programs[i])

                if isinstance(self.channel_node, tv_grab_IO.ChannelNode):
                     #~ and self.channel_node.program_count() > 0
                    #~ print 'checking lineup'
                    self.channel_node.check_lineup()

            if self.is_child and not self.active:
                #~ self.child_programs = deepcopy(self.all_programs) if self.active else self.all_programs
                #~ print 'setting child_data for', self.chanid
                self.child_data.set()
                self.statetext = ''
                self.state = None
                #~ self.ready = True

            # And get the detailpages
            #~ elif not isinstance(self.channel_node, tv_grab_IO.ChannelNode) or self.channel_node.program_count() == 0:
            elif len(self.all_programs) == 0:
                self.statetext = ''
                self.state = None
                self.detail_data.set()

            else:
                self.statetext = 'processing details'
                #~ print 'processing details for ', self.chanid
                self.state = 4
                self.get_details()
                self.statetext = 'waiting for details'
                self.state = 5
                while not self.detail_data.is_set():
                    #~ print 'waiting for details for ', self.chanid
                    self.detail_data.wait(5)
                    if self.quit:
                        self.ready = True
                        return

                    # Check if the sources are still alive
                    s_cnt = 0
                    for s in self.config.detail_sources:
                        s_cnt += 1
                        if self.config.channelsource[s].is_alive():
                            break

                        if s_cnt == 1:
                            log_string = self.config.channelsource[s].source

                        elif s_cnt == len(self.config.detail_sources):
                            log_string += u' and %s' % self.config.channelsource[s].source

                        else:
                            log_string += u', %s' % self.config.channelsource[s].source

                    else:
                        self.detail_data.set()
                        self.config.log([self.config.text('fetch', 52, (log_string, )), self.config.text('fetch', 53, (self.chan_name,))])

                self.all_programs = self.detailed_programs

            if self.is_child:
                #~ print 'setting active child_data for', self.chanid
                self.child_data.set()

            # And log the results
            with self.functions.count_lock:
                self.functions.progress_counter+= 1
                counter = self.functions.progress_counter

            log_array = ['\n', self.config.text('fetch', 54, (self.chan_name, counter, self.config.chan_count))]
            log_array.append( self.config.text('fetch',55, (self.functions.get_counter('detail', -1, self.chanid), )))
            if self.opt_dict['fast']:
                log_array.append(self.config.text('fetch', 56, (self.functions.get_counter('fail', -1, self.chanid), )))
                log_array.append('\n')
                log_array.append(self.config.text('fetch', 57, (self.functions.get_counter('detail', -2, self.chanid), )))
                log_array.append(self.config.text('fetch', 58, (self.functions.get_counter('fail', -2, self.chanid), )))

            else:
                fail = 0
                for source in self.config.detail_sources:
                    fail += self.functions.get_counter('fail', source, self.chanid)
                    log_array.append(self.config.text('fetch', 59, \
                        (self.functions.get_counter('detail', source, self.chanid), self.config.channelsource[source].source)))

                log_array.append(self.config.text('fetch', 60, (fail,)))
                log_array.append(self.config.text('fetch', 61, (self.functions.get_counter('fail', -1, self.chanid), )))
                log_array.append('\n')
                log_array.append(self.config.text('fetch', 57, (self.functions.get_counter('lookup', -2, self.chanid), )))
                log_array.append(self.config.text('fetch', 58, (self.functions.get_counter('lookup_fail', -2, self.chanid), )))
                log_array.append('\n')
                for source in self.config.detail_sources:
                    log_array.append(self.config.text('fetch', 62, \
                        (self.config.channelsource[source].detail_request.qsize(), self.config.channelsource[source].source)))

            log_array.append('\n')
            self.config.log(log_array, 4, 3)

            # a final check on the sanity of the data
            #~ self.config.channelsource[0].parse_programs(self.chanid, 1)

            # Split titles with colon in it
            # Note: this only takes place if all days retrieved are also grabbed with details (slowdays=days)
            # otherwise this function might change some titles after a few grabs and thus may result in
            # loss of programmed recordings for these programs.
            # Also check if a genric genre does aply
            for g, chlist in self.config.generic_channel_genres.items():
                if self.chanid in chlist:
                    gen_genre = g
                    break

            else:
                gen_genre = None

            for i, v in enumerate(self.all_programs):
                self.all_programs[i] = self.title_split(v)
                if gen_genre != None and self.all_programs[i]['genre'] in (u'overige', u''):
                    self.all_programs[i]['genre'] = gen_genre

            if self.opt_dict['add_hd_id']:
                self.opt_dict['mark_hd'] = False
                self.config.xml_output.create_channel_strings(self.chanid, False)
                self.config.xml_output.create_program_string(self.chanid, False)
                self.config.xml_output.create_channel_strings(self.chanid, True)
                self.config.xml_output.create_program_string(self.chanid, True)

            else:
                self.config.xml_output.create_channel_strings(self.chanid)
                self.config.xml_output.create_program_string(self.chanid)

            if self.config.write_info_files:
                self.config.infofiles.write_raw_list()

            self.statetext = ''
            self.state = None
            self.ready = True

        except:
            self.config.logging.log_queue.put({'fatal': [traceback.format_exc(), '\n'], 'name': self.chan_name})
            self.ready = True
            return(97)

    def use_cache(self, tdict, cached):
        # copy the cached information, except the start/end times, rating and clumping,
        # these may have changed.
        # But first checkout the dict
        #~ cached = self.config.fetch_func.checkout_program_dict(cached)
        try:
            clump  = tdict['clumpidx']

        except LookupError:
            clump = False

        cached['start-time'] = tdict['start-time']
        cached['stop-time']  = tdict['stop-time']
        if clump:
            cached['clumpidx'] = clump

        # Make sure we do not overwrite fresh info with cashed info
        if tdict['description'] > cached['description']:
            cached['description'] = tdict['description']

        if not 'prefered description' in cached.keys():
            cached['prefered description'] = tdict['prefered description']

        elif tdict['prefered description'] > cached['prefered description']:
            cached['prefered description'] = tdict['prefered description']

        for fld in ('name', 'episode title', 'originaltitle', 'premiere year', 'airdate', 'country', 'star-rating', 'broadcaster'):
            if tdict[fld] != '':
                cached[fld] = tdict[fld]

        if re.sub('[-,. ]', '', cached['name']) == re.sub('[-,. ]', '', cached['episode title']):
            cached['episode title'] = ''

        for fld in ('season', 'episode'):
            if tdict[fld] != 0:
                cached[fld] = int(tdict[fld])

        if tdict['rerun'] == True:
            cached['rerun'] = True

        if len(tdict['rating']) > 0:
            for item in tdict['rating']:
                if not item in cached['rating']:
                    cached['rating'].append(item)

        return cached

    def get_counter(self):
        with self.channel_lock:
            self.fetch_counter += 1
            return 100*float(self.fetch_counter)/float(self.nprograms)

    def get_source_id(self, source):
        if source in self.source_id.keys():
            return self.source_id[source]

        return ''

    def source_ready(self, source):
        if not source in self.source_data.keys():
            self.source_data[source] = Event()

        return self.source_data[source]

    def get_details(self, ):
        """
        Given a list of programs, from the several sources, retrieve program details
        """
        # Check if there is data
        self.detailed_programs = []
        if len(self.all_programs) == 0:
            return

        programs = self.all_programs[:]

        if self.opt_dict['fast']:
            self.config.log(['\n', self.config.text('fetch', 63, \
                (len(programs), self.chan_name, self.xmltvid, (self.opt_dict['compat'] and self.config.compat_text or ''))), \
                self.config.text('fetch', 64, (self.counter, self.config.chan_count, self.config.opt_dict['days']))], 2)

        else:
            self.config.log(['\n', self.config.text('fetch', 65, \
                (len(programs), self.chan_name, self.xmltvid, (self.opt_dict['compat'] and self.config.compat_text or ''))), \
                self.config.text('fetch', 64, (self.counter, self.config.chan_count, self.config.opt_dict['days']))], 2)

        # randomize detail requests
        self.fetch_counter = 0
        self.nprograms = len(programs)
        fetch_order = list(range(0,self.nprograms))
        random.shuffle(fetch_order)

        for i in fetch_order:
            if self.quit:
                self.ready = True
                return

            try:
                if programs[i] == None:
                    continue

            except:
                self.config.log(traceback.format_exc())
                if self.config.write_info_files:
                    self.config.infofiles.write_raw_string('Error: %s with index %s\n' % (sys.exc_info()[1], i))

                continue

            p = programs[i]
            logstring = u'%s-%s: %s' % \
                                (p['start-time'].strftime('%d %b %H:%M'), \
                                p['stop-time'].strftime('%H:%M'), \
                                p['name'])

            # We only fetch when we are in slow mode and slowdays is not set to tight
            no_fetch = (self.opt_dict['fast'] or p['offset'] >= (self.config.opt_dict['offset'] + self.opt_dict['slowdays']))
            no_fetch = True

            # check the cache for this program's ID
            # If not found, check the various ID's and (if found) make it the prime one
            self.config.program_cache.cache_request.put({'task':'query_id', 'parent': self, 'program': p})
            cache_id = self.cache_return.get(True)
            if cache_id =='quit':
                self.ready = True
                return

            if cache_id != None:
                self.config.program_cache.cache_request.put({'task':'query', 'parent': self, 'pid': cache_id})
                cached_program = self.cache_return.get(True)
                if cached_program =='quit':
                    self.ready = True
                    return

                # check if it contains detail info from tvgids.nl or (if no nl-url known, or in no_fetch mode) tvgids.tv
                if cached_program != None and \
                    (no_fetch or \
                        cached_program[self.config.channelsource[0].detail_check] or \
                        (p['detail_url'][0] == '' and \
                            (cached_program[self.config.channelsource[9].detail_check] or \
                                (p['detail_url'][9] == '' and \
                                cached_program[self.config.channelsource[1].detail_check])))):
                        self.config.log(self.config.text('fetch', 18, (self.chan_name, self.get_counter(), logstring)), 8, 1)
                        self.functions.update_counter('detail', -1, self.chanid)
                        p = self.use_cache(p, cached_program)
                        if not (self.config.opt_dict['disable_ttvdb'] or self.opt_dict['disable_ttvdb']):
                            if p['genre'].lower() == u'serie/soap' and p['episode title'] != '' and p['season'] == 0:
                                #~ self.update_counter('fetch', -1)
                                self.functions.update_counter('queue', -2, self.chanid)
                                self.config.ttvdb.detail_request.put({'tdict':p, 'parent': self, 'task': 'update_ep_info'})
                                continue

                        self.detailed_programs.append(p)
                        continue

            # Either we are fast-mode, outside slowdays or there is no url. So we continue
            no_detail_fetch = (no_fetch or ((p['detail_url'][0] == '') and \
                                                                (p['detail_url'][9] == '') and \
                                                                (p['detail_url'][1] == '')))

            if no_detail_fetch:
                self.config.log(self.config.text('fetch', 66, (self.chan_name, self.get_counter(), logstring)), 8, 1)
                self.functions.update_counter('fail', -1, self.chanid)
                if not (self.config.opt_dict['disable_ttvdb'] or self.opt_dict['disable_ttvdb']):
                    if p['genre'].lower() == u'serie/soap' and p['episode title'] != '' and p['season'] == 0:
                        #~ self.update_counter('fetch', -1)
                        self.functions.update_counter('queue', -2, self.chanid)
                        self.config.ttvdb.detail_request.put({'tdict':p, 'parent': self, 'task': 'update_ep_info'})
                        continue

                self.detailed_programs.append(p)

                continue

            for src_id in self.config.detail_sources:
                if src_id not in self.config.opt_dict['disable_detail_source'] and \
                  src_id not in self.opt_dict['disable_detail_source'] and \
                  p['detail_url'][src_id] != '':
                    #~ self.update_counter('fetch', src_id)
                    self.functions.update_counter('queue',src_id, self.chanid)
                    self.config.channelsource[src_id].detail_request.put({'tdict':p, 'cache_id': cache_id, 'logstring': logstring, 'parent': self})
                    break

        # Place terminator items in the queue
        for src_id in self.config.detail_sources:
            if self.functions.get_counter('queue', src_id, self.chanid) > 0:
                self.config.channelsource[src_id].detail_request.put({'last_one': True, 'parent': self})
                break

        else:
            if not (self.config.opt_dict['disable_ttvdb'] or self.opt_dict['disable_ttvdb']):
                self.config.ttvdb.detail_request.put({'task': 'last_one', 'parent': self})

            else:
                self.detail_data.set()

    def title_split(self,program):
        """
        Some channels have the annoying habit of adding the subtitle to the title of a program.
        This function attempts to fix this, by splitting the name at a ': '.
        """
        # Some programs (BBC3 when this happened) have no genre. If none, then set to a default
        if program['genre'] is None:
            program['genre'] = 'overige';

        ptitle = program['name']
        psubtitle = program['episode title']
        if  ptitle == None or ptitle == '':
            return program

        # exclude certain programs
        if  ('episode title' in program and psubtitle != '')  \
          or ('genre' in program and program['genre'].lower() in ['movies','film']) \
          or (ptitle.lower() in self.config.notitlesplit):
            return program

        # and do the title split test
        p = ptitle.split(':')
        if len(p) >1:
            self.config.log(self.config.text('fetch', 67, (ptitle, )), 64)
            program['name'] = p[0].strip()
            program['episode title'] = "".join(p[1:]).strip()
            if self.config.write_info_files:
                self.config.infofiles.addto_detail_list(unicode('Name split = %s + %s' % (program['name'] , program['episode title'])))

        return program

# end Channel_Config

