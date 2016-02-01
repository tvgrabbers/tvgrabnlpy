#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import re, sys, traceback
import time, datetime, random, difflib
import requests
import httplib, json, socket
import timezones
try:
    import urllib.request as urllib
except ImportError:
    import urllib2 as urllib
try:
    from html.entities import name2codepoint
except ImportError:
    from htmlentitydefs import name2codepoint
from threading import Thread, Lock, Semaphore
from xml.sax import saxutils
from xml.etree import cElementTree as ET
from Queue import Queue, Empty
try:
    unichr(42)
except NameError:
    unichr = chr    # Python 3

CET_CEST = timezones.AmsterdamTimeZone()
UTC  = timezones.UTCTimeZone()

class Functions():
    """Some general Fetch functions"""

    def __init__(self, config):
        # Version info as returned by the version function
        self.name ='tv_grab_fetch_py'
        self.major = 1
        self.minor = 0
        self.patch = 0
        self.patchdate = u'20160201'
        self.alfa = True
        self.beta = True

        self.config = config
        self.max_fetches = Semaphore(self.config.opt_dict['max_simultaneous_fetches'])
        self.count_lock = Lock()
        self.counters = {}

    # end init()

    def version(self, as_string = False):
        """
        return tuple or string with version info
        """
        if as_string and self.alfa:
            return u'%s (Version: %s.%s.%s-p%s-alpha)' % (self.name, self.major, self.minor, '{:0>2}'.format(self.patch), self.patchdate)

        if as_string and self.beta:
            return u'%s (Version: %s.%s.%s-p%s-beta)' % (self.name, self.major, self.minor, '{:0>2}'.format(self.patch), self.patchdate)

        if as_string and not self.beta:
            return u'%s (Version: %s.%s.%s-p%s)' % (self.name, self.major, self.minor, '{:0>2}'.format(self.patch), self.patchdate)

        else:
            return (self.name, self.major, self.minor, self.patch, self.patchdate, self.beta)

    # end version()

    def update_counter(self, source_id, cnt_type, chanid=-1, cnt_add=True, cnt_change=1):
        if not isinstance(cnt_change, int) or cnt_change == 0:
            return

        with self.count_lock:
            if not source_id in self.counters:
                self.counters[source_id] = {}

            if not cnt_type in self.counters[source_id]:
                self.counters[source_id][cnt_type] = {}
                self.counters[source_id][cnt_type]['total'] = 0

            if chanid == -1:
                if cnt_add:
                    self.counters[source_id][cnt_type]['total'] += cnt_change

                else:
                    self.counters[source_id][cnt_type]['total'] -= cnt_change

            else:
                if not chanid in self.counters[source_id][cnt_type]:
                    self.counters[source_id][cnt_type][chanid] = 0

                if cnt_add:
                    self.counters[source_id][cnt_type][chanid] += cnt_change
                    self.counters[source_id][cnt_type]['total'] += cnt_change

                else:
                    self.counters[source_id][cnt_type][chanid] -= cnt_change
                    self.counters[source_id][cnt_type]['total'] -= cnt_change

    def get_counter(self, source_id, cnt_type, chanid=-1):
            if not source_id in self.counters:
                return 0

            if not cnt_type in self.counters[source_id]:
                return 0

            if chanid == -1:
                return self.counters[source_id][cnt_type]['total']

            if not chanid in self.counters[source_id][cnt_type]:
                return 0

            return self.counters[source_id][cnt_type][chanid]

    def get_page(self, url, encoding = "default encoding", accept_header = None):
        """
        Wrapper around get_page_internal to catch the
        timeout exception
        """
        try:
            txtdata = None
            txtheaders = {'Keep-Alive' : '300',
                          'User-Agent' : self.config.user_agents[random.randint(0, len(self.config.user_agents)-1)] }

            if accept_header != None:
                txtheaders['Accept'] = accept_header

            fu = FetchURL(self.config, url, txtdata, txtheaders, encoding)
            self.max_fetches.acquire()
            fu.start()
            fu.join(self.config.opt_dict['global_timeout'])
            page = fu.result
            self.max_fetches.release()
            if (page == None) or (page.replace('\n','') == '') or (page.replace('\n','') =='{}'):
                #~ with xml_output.output_lock:
                    #~ xml_output.fail_count += 1

                return None

            else:
                return page

        except(urllib.URLError, socket.timeout):
            self.config.log('get_page timed out on (>%s s): %s\n' % (self.config.opt_dict['global_timeout'], url), 1, 1)
            if self.config.infofiles != None:
                self.config.infofiles.add_url_failure('Fetch timeout: %s\n' % url)

            #~ with xml_output.output_lock:
                #~ xml_output.fail_count += 1

            self.max_fetches.release()
            return None
    # end get_page()

    def checkout_program_dict(self, tdict = None):
        """
        Checkout a given dict for invalid values or
        returnsa default empty dict for storing program info
        """
        self.text_values = ('channelid', 'source', 'channel', 'unixtime', 'prefered description', \
              'clumpidx', 'name', 'titel aflevering', 'description', 'jaar van premiere', \
              'originaltitle', 'subgenre', 'ID', 'merge-source', 'infourl', 'audio', 'star-rating', \
              'country', 'omroep')
        self.datetime_values = ('start-time', 'stop-time')
        self.date_values = ('airdate', )
        self.bool_values = ('tvgids-fetched', 'tvgidstv-fetched', 'primo-fetched', 'rerun', 'teletekst', \
              'new', 'last-chance', 'premiere')
        self.num_values = ('season', 'episode', 'offset')
        self.dict_values = ('credits', 'video')
        self.source_values = ('prog_ID', 'detail_url')
        self.list_values = ('kijkwijzer', )
        self.video_values = ('HD', 'breedbeeld', 'blackwhite')

        if tdict == None:
            tdict = {}

        for key in self.text_values:
            if not key in tdict.keys() or tdict[key] == None:
                tdict[key] = u''

            try:
                if type(tdict[key]) != unicode:
                    tdict[key] = unicode(tdict[key])

            except UnicodeError:
                tdict[key] = u''

        for key in self.date_values:
            if not key in tdict.keys() or tdict[key] == None:
                tdict[key] = u''

        for key in self.datetime_values:
            if not key in tdict.keys() or tdict[key] == None:
                tdict[key] = u''

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
                        if type(tdict[key][s]) != unicode:
                            tdict[key][s] = unicode(tdict[key][s])

                    except UnicodeError:
                        tdict[key][s] = u''

        for key in self.list_values:
            if not key in tdict.keys() or not isinstance(tdict[key], list):
                tdict[key] = []

        for subkey in tdict['credits'].keys():
            if  tdict['credits'][subkey] == None:
                tdict['credits'][subkey] = []

            for i, item in enumerate(tdict['credits'][subkey]):
                try:
                    if type(item) != unicode:
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

        text = re.sub("", "...", text)
        text = re.sub("", "'", text)
        text = re.sub("", "'", text)
        return re.sub("&#?\w+;", fixup, text)
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
        if type(data) != unicode:
            return unicode(data)

        return data
    # end empersant()

    def get_datestamp(self, offset=0):
        tsnu = (int(time.time()/86400)) * 86400
        day =  datetime.datetime.fromtimestamp(tsnu)
        datenu = int(tsnu - CET_CEST.utcoffset(day).total_seconds())
        if time.time() -  datenu > 86400:
            datenu += 86400

        return datenu + offset * 86400
    # end get_datestamp()

    def get_offset(self, date):
        """Return the offset from today"""
        return int(date.toordinal() -  self.current_date)
    # end get_offset()

# end Functions()

class FetchURL(Thread):
    """
    A simple thread to fetch a url with a timeout
    """
    def __init__ (self, config, url, txtdata = None, txtheaders = None, encoding = "default encoding"):
        Thread.__init__(self)
        self.config = config
        self.url = url
        self.txtdata = txtdata
        self.txtheaders = txtheaders
        self.encoding = encoding
        self.result = None

    def run(self):
        #~ with xml_output.output_lock:
            #~ xml_output.fetch_count += 1

        try:
            self.result = self.get_page_internal()

        except:
            self.config.log('An unexpected error "%s:%s" has occured while fetching page: %s\n' %  (sys.exc_info()[0], sys.exc_info()[1], self.url), 0)
            if self.config.infofiles != None:
                self.config.infofiles.add_url_failure('%s,%s:\n  %s\n' % (sys.exc_info()[0], sys.exc_info()[1], self.url))

            return None

    def find_html_encoding(self, httphead, htmlhead, default_encoding="default encoding"):
        # look for the text '<meta http-equiv="Content-Type" content="application/xhtml+xml; charset=UTF-8" />'
        # in the first 600 bytes of the HTTP page
        m = re.search(r'<meta[^>]+\bcharset=["\']?([A-Za-z0-9\-]+)\b', htmlhead[:512].decode('ascii', 'ignore'))
        if m:
            return m.group(1)

        # Find a HTTP header: Content-Type: text/html; charset=UTF-8
        m = re.search(r'\bcharset=([A-Za-z0-9\-]+)\b', httphead.info().getheader('Content-Type'))
        if m:
            return m.group(1)

        elif default_encoding == "default encoding":
            return self.config.httpencoding

        else:
            return default_encoding # the default HTTP encoding.

    def get_page_internal(self):
        """
        Retrieves the url and returns a string with the contents.
        Optionally, returns None if processing takes longer than
        the specified number of timeout seconds.
        """
        try:
            rurl = urllib.Request(self.url, self.txtdata, self.txtheaders)
            fp = urllib.urlopen(rurl)
            bytes = fp.read()
            page = None

            encoding = self.find_html_encoding(fp, bytes, self.encoding)

            try:
                page = bytes.decode(encoding, 'replace')

            except:
                self.config.log('Cannot decode url %s as %s, trying Windows-1252\n' % (self.url, encoding))
                # 'Windows-1252'
                page = bytes.decode('Windows-1252', 'ignore') # At least gets it somewhat correct

            return page

        except (urllib.URLError) as e:
            self.config.log('Cannot open url %s: %s\n' % (self.url, e.reason), 1, 1)
            if self.config.infofiles != None:
                self.config.infofiles.add_url_failure('URLError: %s\n' % self.url)

            return None

        except (urllib.HTTPError) as e:
            self.config.log('Cannot parse url %s: code=%s\n' % (self.url, e.code), 1, 1)
            if self.config.infofiles != None:
                self.config.infofiles.add_url_failure('HTTPError: %s\n' % self.url)

            return None

        except (httplib.IncompleteRead):
            self.config.log('Cannot retrieve full url %s: %s\n' % (self.url, sys.exc_info()[1]), 1, 1)
            if self.config.infofiles != None:
                self.config.infofiles.add_url_failure('IncompleteRead: %s\n' % self.url)

            return None

# end FetchURL

class theTVDB(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self.config = config
        self.functions = self.config.IO_func
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


                    crequest['parent'].update_counter('fetch', -1, False)
                    continue

                if crequest['task'] == 'last_one':
                    if not 'parent' in crequest:
                        continue

                    crequest['parent'].detail_data.set()

                if crequest['task'] == 'quit':
                    self.quit = True
                    continue

        except:
            self.config.queues['log'].put({'fatal': [traceback.print_exc(), '\n'], 'name': 'theTVDB'})
            self.ready = True
            return(98)

    def query_ttvdb(self, type='seriesid', title=None, lang='nl'):
        base_url = "http://www.thetvdb.com"
        api_key = '0BB856A59C51D607'
        if isinstance(title, (int, str)):
            title = unicode(title)

        title = urllib.quote(title.encode("utf-8"))
        if type == 'seriesid':
            if not lang in ('all', 'cs', 'da', 'de', 'el', 'en', 'es', 'fi', 'fr', 'he', 'hr', 'hu', 'it',
                                'ja', 'ko', 'nl', 'no', 'pl', 'pt', 'ru', 'sl', 'sv', 'tr', 'zh'):
                lang = 'en'

            if title != None:
                data = self.functions.get_page('%s/api/GetSeries.php?seriesname=%s&language=%s' % (base_url, title, lang), 'utf-8')

        elif type == 'episodes':
            if not lang in ('cs', 'da', 'de', 'el', 'en', 'es', 'fi', 'fr', 'he', 'hr', 'hu', 'it',
                                'ja', 'ko', 'nl', 'no', 'pl', 'pt', 'ru', 'sl', 'sv', 'tr', 'zh'):
                lang = 'en'

            if title != None:
                data= self.functions.get_page("%s/api/%s/series/%s/all/%s.xml" % (base_url, api_key, title, lang), 'utf-8')

        elif type == 'seriesname':
            if title != None:
                data= self.functions.get_page("%s/api/%s/series/%s/en.xml" % (base_url, api_key, title), 'utf-8')

        else:
            data = None

        # be nice to the source site
        time.sleep(random.randint(self.config.opt_dict['nice_time'][0], self.config.opt_dict['nice_time'][1]))
        if data != None:
            return ET.fromstring(data.encode('utf-8'))

    def get_all_episodes(self, tid, lang='nl'):
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
                xmldata = self.query_ttvdb('episodes', tid, l)
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
            self.config.log(['Error retreiving episodes from theTVDB.com\n', traceback.print_exc()])
            return

        self.config.queues['cache'].put({'task':'add', 'episode': eps})

    def get_ttvdb_id(self, title, lang='nl', search_db=True):
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
                xmldata = self.query_ttvdb('seriesid', series_name, lang)
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
                xmldata = self.query_ttvdb('seriesid', series_name, 'all')
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
                self.config.log(['Error retreiving an ID from theTVdb.com', traceback.print_exc()])
                return 0

        # And we retreive the episodes
        if self.get_all_episodes(tid, lang) == -1:
            return -1

        return {'tid': int(tid), 'tdate': datetime.date.today(), 'title': series_name}

    def get_season_episode(self, parent = None, data = None):
        if self.config.opt_dict['disable_ttvdb'] or parent.opt_dict['disable_ttvdb']:
            return data

        if data == None:
            return

        if data['titel aflevering'][0:27].lower() == 'geen informatie beschikbaar':
            return data

        if parent != None and parent.group == 6:
            # We do not lookup for regional channels
            return data

        elif parent != None and parent.group == 4:
            tid = self.get_ttvdb_id(data['name'], 'de')

        elif parent != None and parent.group == 5:
            tid = self.get_ttvdb_id(data['name'], 'fr')

        else:
            tid = self.get_ttvdb_id(data['name'])

        if tid == -1:
            return -1

        if tid == None or tid == 0:
            if parent != None:
                parent.update_counter('ttvdb_fail')

            self.config.log("  No ttvdb id for '%s' on channel %s\n" % (data['name'], data['channel']), 128)
            return data

        # First we just look for a matching subtitle
        tid = tid['tid']
        self.config.queues['cache'].put({'task':'query', 'parent': self, \
                'ep_by_title': {'tid': tid, 'title': data['titel aflevering']}})
        eid = self.cache_return.get(True)
        if eid == 'quit':
            self.ready = True
            return -1

        if eid != None:
            if parent != None:
                parent.update_counter('ttvdb')

            data['season'] = eid['sid']
            data['episode'] = eid['eid']
            if isinstance(eid['airdate'], (datetime.date)):
                data['airdate'] = eid['airdate']

            self.config.log('ttvdb  lookup for %s: %s\n' % (data['name'], data['titel aflevering']), 24)
            return data

        # Now we get a list of episodes matching what we already know and compare with confusing characters removed
        self.config.queues['cache'].put({'task':'query', 'parent': self, \
                'ep_by_id': {'tid': tid, 'sid': data['season'], 'eid': data['episode']}})
        eps = self.cache_return.get(True)
        if eps == 'quit':
            self.ready = True
            return -1

        subt = re.sub('[-,. ]', '', self.functions.remove_accents(data['titel aflevering']).lower())
        ep_dict = {}
        ep_list = []
        for ep in eps:
            s = re.sub('[-,. ]', '', self.functions.remove_accents(ep['title']).lower())
            ep_list.append(s)
            ep_dict[s] = {'sid': ep['sid'], 'eid': ep['eid'], 'airdate': ep['airdate'], 'title': ep['title']}
            if s == subt:
                if parent != None:
                    parent.update_counter('ttvdb')

                data['titel aflevering'] = ep['title']
                data['season'] = ep['sid']
                data['episode'] = ep['eid']
                if isinstance(ep['airdate'], (datetime.date)):
                    data['airdate'] = ep['airdate']

                self.config.log('ttvdb  lookup for %s: %s\n' % (data['name'], data['titel aflevering']), 24)
                return data

        # And finally we try a difflib match
        match_list = difflib.get_close_matches(subt, ep_list, 1, 0.7)
        if len(match_list) > 0:
            if parent != None:
                parent.update_counter('ttvdb')

            ep = ep_dict[match_list[0]]
            data['titel aflevering'] = ep['title']
            data['season'] = ep['sid']
            data['episode'] = ep['eid']
            if isinstance(ep['airdate'], (datetime.date)):
                data['airdate'] = ep['airdate']

            self.config.log('ttvdb  lookup for %s: %s\n' % (data['name'], data['titel aflevering']), 24)
            return data

        if parent != None:
            parent.update_counter('ttvdb_fail')

        self.config.log("ttvdb failure for '%s': '%s' on channel %s\n" % (data['name'], data['titel aflevering'], data['channel']), 128)
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
    #~ current_date = datetime.date.today().toordinal()
    current_date = datetime.datetime.now(CET_CEST).toordinal()

    def __init__(self, config, proc_id, source, detail_id, detail_url = '', isjson = False, detail_check = '', detail_processor = False):
        Thread.__init__(self)
        # Flag to stop the thread
        self.config = config
        self.functions = self.config.fetch_func
        self.thread_type = 'source'
        self.quit = False
        self.ready = False
        self.active = True
        self.isjson = isjson
        # The ID of the source
        self.proc_id = proc_id
        # The Name of the source
        self.source = source
        # The dict name of the details etc.
        self.detail_id = detail_id
        self.detail_url = detail_url
        self.detail_check = detail_check
        self.detail_processor = detail_processor
        self.detail_request = Queue()
        self.cache_return = Queue()
        self.source_lock = Lock()

        self.all_channels = {}
        self.channels = {}
        self.channel_loaded = {}
        self.day_loaded = {}
        self.program_data = {}
        self.program_by_id = {}
        self.chan_count = 0
        self.base_count = 0
        self.detail_count = 0
        self.fail_count = 0
        self.fetch_string_parts = re.compile("(.*?[.?!:]+ |.*?\Z)")
        self.config.queues['source'][self.proc_id] = self.detail_request
        self.config.threads.append(self)

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
                        for channel in self.config.channels.values():
                            if channel.is_alive() and not channel.detail_data.is_set():
                                channel.detail_data.set()
                                self.config.log('Channel %s seems to be waiting for %s lost detail requests from %s.\nSetting it to ready\n' % \
                                    (channel.chan_name, channel.counters['fetch'][1], self.source))

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
              tdict['genre'].lower() == u'serie/soap' and tdict['titel aflevering'] != '' and tdict['season'] == 0:
                # We do a ttvdb lookup
                parent.update_counter('fetch', -1)
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
                    self.config.log(u'      [cached] %s:(%3.0f%%) %s\n' % (parent.chan_name, parent.get_counter(), logstring), 8, 1)
                    tdict= parent.use_cache(tdict, cached_program)
                    parent.update_counter('cache')
                    parent.update_counter('fetch', self.proc_id, False)
                    check_ttvdb(tdict, parent)
                    return 0

                # If there is an url we'll try tvgids.tv
                elif self.proc_id == q_no[1] and self.config.channelsource[q_no[0]].detail_processor and \
                  q_no[0] not in parent.opt_dict['disable_detail_source'] and \
                  tdict['detail_url'][q_no[0]] != '':
                    self.config.queues['source'][q_no[0]].put({'tdict':tdict, 'cache_id': cache_id, 'logstring': logstring, 'parent': parent, 'last_one': False})
                    parent.update_counter('fetch', q_no[0])
                    parent.update_counter('fetch', self.proc_id, False)
                    return 0

        # First some generic initiation that couldn't be done earlier in __init__
        # Specifics can be done in init_channels and init_json which are called here
        tdict = self.functions.checkout_program_dict()
        idle_timeout = 1800
        try:
            # Check if the source is not deactivated and if so set them all loaded
            if self.proc_id in self.config.opt_dict['disable_source']:
                for chanid in self.channels.keys():
                    self.channel_loaded[chanid] = True
                    self.config.channels[chanid].source_data[self.proc_id] = True

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

                self.init_channels()
                self.init_json()
                # Load and proccess al the program pages
                try:
                    self.load_pages()

                except:
                    self.fail_count += 1
                    self.config.log(['Fatal Error processing the basepages from %s\n' % (self.source), \
                        'Setting them all to being loaded, to let the other sources finish the job\n', traceback.print_exc()], 0)
                    for chanid in self.channels.keys():
                        self.channel_loaded[chanid] = True
                        self.config.channels[chanid].source_data[self.proc_id].set()

                # if this is the prefered description source set the value
                with self.source_lock:
                    for chanid in self.channels.keys():
                        if self.config.channels[chanid].opt_dict['prefered_description'] == self.proc_id:
                            for i in range(len(self.program_data[chanid])):
                                self.program_data[chanid][i]['prefered description'] = self.program_data[chanid][i]['description']

            if self.config.infofiles != None:
                self.config.infofiles.check_new_channels(self, self.config.source_channels, self.config.empty_channels)

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
                        if self.proc_id == 0 and parent.counters['fetch'][9] > 0:
                            self.config.queues['source'][1].put(tdict)

                        elif self.proc_id == 9 and parent.counters['fetch'][1] > 0:
                            self.config.queues['source'][1].put(tdict)

                        elif parent.counters['fetch'][-1] > 0 and not (self.config.opt_dict['disable_ttvdb'] or parent.opt_dict['disable_ttvdb']):
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
                            self.config.log(['Error processing the detailpage: %s\n' % (tdict['detail_url'][self.proc_id]), traceback.print_exc()], 1)

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
                            self.config.log(['Error processing the json detailpage: http://www.tvgids.nl/json/lists/program.php?id=%s\n' \
                                % tdict['prog_ID'][self.proc_id][3:], traceback.print_exc()], 1)

                    # It failed!
                    if detailed_program == None:
                        # If this is tvgids.nl and there is an url we'll try tvgids.tv, but first check the cache again
                        if self.proc_id == 1:
                            self.config.log(u'[fetch failed or timed out] %s:(%3.0f%%) %s\n' % (parent.chan_name, parent.get_counter(), logstring), 8, 1)
                            parent.update_counter('fail')
                            parent.update_counter('fetch', self.proc_id, False)
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
                        if self.proc_id == 0:
                            self.config.log(u'[normal fetch] %s:(%3.0f%%) %s\n' % (parent.chan_name, parent.get_counter(), logstring), 8, 1)

                        elif self.proc_id == 1:
                            self.config.log(u'   [.tv fetch] %s:(%3.0f%%) %s\n' % (parent.chan_name, parent.get_counter(), logstring), 8, 1)

                        elif self.proc_id == 9:
                            self.config.log(u' [primo fetch] %s:(%3.0f%%) %s\n' % (parent.chan_name, parent.get_counter(), logstring), 8, 1)

                        parent.update_counter('fetched', self.proc_id)
                        parent.update_counter('fetch', self.proc_id, False)
                        self.detail_count += 1

                        # do not cache programming that is unknown at the time of fetching.
                        if tdict['name'].lower() != 'onbekend':
                            self.config.queues['cache'].put({'task':'add', 'program': self.functions.checkout_program_dict(detailed_program)})

            else:
                self.ready = True

        except:
            if tdict['detail_url'][self.proc_id] == '':
                self.config.queues['log'].put({'fatal': ['While fetching the base pages\n', \
                    traceback.print_exc(), '\n'], 'name': self.source})

            else:
                self.config.queues['log'].put({'fatal': ['The current detail url is: %s\n' \
                    % (tdict['detail_url'][self.proc_id]), \
                    traceback.print_exc(), '\n'], 'name': self.source})

            self.ready = True
            return(98)

    # Dummys to be filled in by the sub-Classes
    def init_channels(self):
        """The specific initiation code before starting with grabbing"""
        self.init_channel_source_ids()

    def init_json(self):
        """The specific initiation code if the source is json before starting with grabbing"""
        if not self.isjson:
            return

        # Define here the json structure if it's not a flat list of program dicts
        # self.jsondata = {<name>: ['listname':<list>,'keyname':<key>,'valuename':<vname>}
        # self.jsondict[<list>][<key-in-json_by_id[id][tdict['keyname']] >][<vname>] = value
        self.json_by_id = {}
        self.jsondata = {}
        self.jsondict = {}

    def get_url(self):
        """return the several url's for ordinairy, detail and channel info"""
        pass

    def get_channels(self):
        """The code for the retreiving a list of suppoted channels"""
        pass

    def load_pages(self):
        """The code for the actual Grabbing and dataprocessing"""
        if len(self.channels) == 0 :
            return

        else:
            for chanid in self.channels.keys():
                self.channel_loaded[chanid] = True
                self.config.channels[chanid].source_data[self.proc_id].set()

    def load_detailpage(self, tdict):
        """The code for retreiving and processing a detail page"""
        return tdict

    # Helper functions
    def init_channel_source_ids(self):
        for chanid, channel in self.config.channels.iteritems():
            self.program_data[chanid] = []
            # Is the channel active and this source for the channel not disabled
            if channel.active and not self.proc_id in channel.opt_dict['disable_source']:
                # Is there a sourceid for this channel
                if channel.get_source_id(self.proc_id) != '':
                    # Unless it is in empty channels we add it else set it ready
                    if channel.get_source_id(self.proc_id) in self.config.empty_channels[self.proc_id]:
                        self.channel_loaded[chanid] = True
                        self.config.channels[chanid].source_data[self.proc_id].set()

                    else:
                        self.channels[chanid] = channel.get_source_id(self.proc_id)

                # Does the channel have child channels
                if chanid in self.config.combined_channels.keys():
                    # Then see if any of the childs has a sourceid for this source and does not have this source disabled
                    for c in self.config.combined_channels[chanid]:
                        if c['chanid'] in self.config.channels.keys() and self.config.channels[c['chanid']].get_source_id(self.proc_id) != '' \
                          and not self.proc_id in self.config.channels[c['chanid']].opt_dict['disable_source']:
                            # Unless it is in empty channels we add and mark it as a child else set it ready
                            if self.config.channels[c['chanid']].get_source_id(self.proc_id) in self.config.empty_channels[self.proc_id]:
                                self.channel_loaded[c['chanid']] = True
                                self.config.channels[c['chanid']].source_data[self.proc_id].set()

                            else:
                                self.channels[c['chanid']] = self.config.channels[c['chanid']].get_source_id(self.proc_id)
                                self.config.channels[c['chanid']].is_child = True

    def add_endtimes(self, chanid, date_switch = 6):
        """
        For the sites that only give start times, add the next starttime as endtime
        date_switch is the time we asume the last program will end if started before that time
        else  we assume next midnight
        """
        if len(self.program_data[chanid]) > 0:
            for i, tdict in enumerate(self.program_data[chanid]):
                if i > 0 and type(tdict['start-time']) == datetime.datetime:
                    try:
                        if not type(self.program_data[chanid][i-1]['stop-time']) == datetime.datetime:
                            self.program_data[chanid][i-1]['stop-time'] =  tdict['start-time']

                    except:
                        pass

            # And one for the last program
            prog_date = datetime.date.fromordinal(self.current_date + self.program_data[chanid][-1]['offset'])
            if not type(self.program_data[chanid][-1]['stop-time']) == datetime.datetime:
                if int(self.program_data[chanid][-1]['start-time'].strftime('%H')) < date_switch:
                    self.program_data[chanid][-1]['stop-time'] = datetime.datetime.combine(prog_date, datetime.time(date_switch, 0,0 ,0 ,CET_CEST))

                else:
                    self.program_data[chanid][-1]['stop-time'] = datetime.datetime.combine(prog_date, datetime.time(23, 59,0 ,0 ,CET_CEST))

            # remove programs that end when they start
            for tdict in self.program_data[chanid][:]:
                if tdict['start-time'] == tdict['stop-time']:
                    self.program_data[chanid].remove(tdict)

    def check_title_name(self, program):
        """
        Process Title names on Grouping issues and apply the rename table
        Return the updated Progam dict
        """
        ptitle = program['name']
        psubtitle = program['titel aflevering']
        if  ptitle == None or ptitle == '':
            return program

        if re.sub('[-,. ]', '', ptitle) == re.sub('[-,. ]', '', psubtitle):
            program['titel aflevering'] = ''
            psubtitle = ''

        # Remove a groupname if in the list
        for group in self.config.groupnameremove:
            if (len(ptitle) > len(group) + 3) and (ptitle[0:len(group)].lower() == group):
                p = ptitle.split(':')
                if len(p) >1:
                    self.config.log('Removing \"%s\" from \"%s\"\n' %  (group, ptitle), 64)
                    if self.config.infofiles != None:
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
            self.config.log('Renaming %s to %s\n' % (ptitle, self.config.titlerename[ptitle.lower()]), 64)
            if self.config.infofiles != None:
                self.config.infofiles.addto_detail_list(unicode('Title renaming %s to %s\n' % (ptitle, self.config.titlerename[ptitle.lower()])))

            ptitle = self.config.titlerename[ptitle.lower()]

        program['name'] = ptitle
        program['titel aflevering'] = psubtitle
        return program

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
                if self.config.infofiles != None:
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
                        if self.config.infofiles != None:
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
            for i, v in enumerate(atype):
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

                    if self.config.infofiles != None:
                        strdesc = ''
                        for p in alinea:
                            strdesc = strdesc + '    <p>%s</p>\n' % p

                        strdesc = '  <div start="' + tdict['start-time'].strftime('%d %b %H:%M') + \
                                                    '" name="' + tdict['name'] + '">\n' + strdesc + '  </div>'
                        if self.config.infofiles != None:
                            self.config.infofiles.addto_raw_string(strdesc)

            # We check to not ovrwrite an already present longer description
            if description > tdict['description']:
                tdict['description'] = description

            # use the first header as subgenre, if not already present
            if tdict['subgenre'] == '' and aheader[0] != '':
                tdict['subgenre']  = aheader[0]

        return tdict

    # Selectie functions
    def get_json_data(self, id, item):
        """Return the requested json item or None if not found"""
        if not self.isjson:
            return None

        if not id in self.json_by_id.keys():
            return None

        if item in self.json_by_id[id].keys():
            return self.functions.unescape(self.json_by_id[id][item])

        if item in self.jsondata.keys():
            tdict = self.jsondata[item]
            if  tdict['keyname'] in self.json_by_id[id]:
                key =self.json_by_id[id][tdict['keyname']]
                if key in self.jsondict[tdict['listname']] and \
                  tdict['valuename'] in self.jsondict[tdict['listname']][key]:
                    return self.functions.unescape(self.jsondict[tdict['listname']][key][tdict['valuename']])

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

    def get_program(self, id):
        """Return program data for given program id"""
        if not id in self.program_by_id.keys():
            return self.functions.checkout_program_dict()

        return self.program_by_id[id]

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
        programs.sort(key=lambda program: (program['start-time'],program['stop-time']))
        if overlap_strategy == None:
            overlap_strategy = self.config.channels[chanid].opt_dict['overlap_strategy']

        # next, correct for missing end time and copy over all good programming to the
        # good_programs list
        for i in range(len(programs)):

            # Try to correct missing end time by taking start time from next program on schedule
            if (programs[i]['stop-time'] == None and i < len(programs)-1):
                self.config.log('Oops, "%s" has no end time. Trying to fix...\n' % programs[i]['name'], 64)
                programs[i]['stop-time'] = programs[i+1]['start-time']

            # The common case: start and end times are present and are not
            # equal to each other (yes, this can happen)
            if programs[i]['start-time'] != None \
                and programs[i]['stop-time']  != None \
                and programs[i]['start-time'] != programs[i]['stop-time']:
                    good_programs.append(programs[i])

        # Han Holl: try to exclude programs that stop before they begin
        for i in range(len(good_programs)-1,-1,-1):
            if good_programs[i]['stop-time'] <= good_programs[i]['start-time']:
                self.config.log('Deleting invalid stop/start time: %s\n' % good_programs[i]['name'], 64)

        # Try to exclude programs that only identify a group or broadcaster and have overlapping start/end times with
        # the actual programs
        for i in range(len(good_programs)-2,-1,-1):

            if good_programs[i]['start-time'] == good_programs[i+1]['start-time'] \
                and good_programs[i]['stop-time']  == good_programs[i+1]['stop-time'] \
                and good_programs[i]['name']  == good_programs[i+1]['name']:
                    self.config.log('Deleting duplicate: %s\n' % good_programs[i]['name'], 64)
                    del good_programs[i]
                    continue

            if good_programs[i]['start-time'] <= good_programs[i+1]['start-time'] \
                and good_programs[i]['stop-time']  >= good_programs[i+1]['stop-time']:
                    self.config.log('Deleting grouping/broadcaster: %s\n' % good_programs[i]['name'], 64)
                    del good_programs[i]

        # Fix overlaps/gaps
        if overlap_strategy in ['average', 'stop', 'start', 'fill']:
            for i in range(len(good_programs)-1):

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
                        self.config.log('"%s" and "%s" overlap %s minutes. Adjusting times.\n' % \
                            (good_programs[i]['name'],good_programs[i+1]['name'],overlap // 60), 64)
                    else:
                        self.config.log('"%s" and "%s" have gap of %s minutes. Adjusting times.\n' % \
                            (good_programs[i]['name'],good_programs[i+1]['name'],abs(overlap) // 60), 64)

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
                        tdict = self.functions.checkout_program_dict()
                        tdict['source'] = good_programs[i]['source']
                        tdict['channelid'] = good_programs[i]['channelid']
                        tdict['channel'] = good_programs[i]['channel']
                        tdict['name'] = self.config.npo_fill
                        tdict['start-time'] = good_programs[i]['stop-time']
                        tdict['stop-time'] = good_programs[i+1]['start-time']
                        tdict['offset'] = good_programs[i+1]['offset']
                        tdict['genre'] = u'overige'
                        fill_programs.append(tdict)

                    # leave as is
                    else:
                       pass

                # For NPO we fill the night gap
                elif good_programs[i]['source'] == u'npo' and overlap_strategy == 'fill' and (0 < good_programs[i]['stop-time'].hour < 6):
                    if good_programs[i]['name'] == 'Tekst-TV':
                        good_programs[i]['stop-time'] = good_programs[i+1]['start-time']

                    elif good_programs[i+1]['name'] == 'Tekst-TV':
                        good_programs[i+1]['start-time'] = good_programs[i]['stop-time']

                    else:
                        tdict = self.functions.checkout_program_dict()
                        tdict['source'] = good_programs[i]['source']
                        tdict['channelid'] = good_programs[i]['channelid']
                        tdict['channel'] = good_programs[i]['channel']
                        tdict['name'] = 'Tekst-TV'
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
            if cur_source_id != '' and cur_source_id in self.config.no_genric_matching[self.proc_id]:
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
                    tstop = datetime.datetime.combine(pstop.date(), merge_channel['end'])
                    if pstart.date() != pstop.date() and tstop - tstart > datetime.timedelta(days=1):
                        tstart = datetime.datetime.combine(pstop.date(), merge_channel['start'])
                        tstop = datetime.datetime.combine(pstart.date(), merge_channel['end'])

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

        match_array = [   'Match details:\n']
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

            if tdict['jaar van premiere'] == '':
                tdict['jaar van premiere'] = tvdict['jaar van premiere']

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

            if tdict['omroep'] == '':
                tdict['omroep'] = tvdict['omroep']

            if tdict['star-rating'] == '':
                tdict['star-rating'] = tvdict['star-rating']

            if len(tvdict['kijkwijzer']) > 0:
                for item in tvdict['kijkwijzer']:
                    tdict['kijkwijzer'].append(item)

            if tvdict['video']['HD']:
                tdict['video']['HD']  = True

            if tvdict['video']['breedbeeld']:
                tdict['video']['breedbeeld']  = True

            if tvdict['video']['blackwhite']:
                tdict['video']['blackwhite']  = True

            if tvdict['teletekst']:
                tdict['teletekst']  = True

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
                if not self.proc_id in (2, 6, 5) and tdict['titel aflevering'] == '':
                    tdict['titel aflevering'] = tvdict['titel aflevering']

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

                if self.proc_id in (2, 6, 5) and (tvdict['titel aflevering'] != '' or tdict['titel aflevering'] == ''):
                    tdict['titel aflevering'] = tvdict['titel aflevering']

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

            x = match_name(pi['name'], tdict['name'], pi['titel aflevering'])
            if x != None:
                matchlog('title match: ', pi, tdict, 4)
                retval = 1

            elif check_genre and match_genre(pi['genre'], tdict['genre'], pi['subgenre']):
                matchlog('genre match: ', pi, tdict, 4)
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

        if merge_channel == None:
            self.config.log(['\n', 'Now merging %s (channel %s of %s):\n' % (self.config.channels[chanid].chan_name , counter, self.config.chan_count), \
                '  %s programs from %s into %s programs from %s\n' % \
                (len(programs), other_source_name, len(info), prime_source_name)], 2)
            log_array =['\n']
            log_array.append('Merg statistics for %s (channel %s of %s) from %s into %s\n' % \
                (self.config.channels[chanid].chan_name , counter, self.config.chan_count, other_source_name, prime_source_name))

        else:
            self.config.log(['\n', 'Now merging %s programs from %s into %s programs from %s\n' % \
                    (len(programs), other_source_name, len(info), prime_source_name), \
                    '    (channel %s of %s)' % (counter, self.config.chan_count)], 2)
            log_array =['\n']
            log_array.append('Merg statistics for %s (channel %s of %s) from %s\n' % \
                (prime_source_name , counter, self.config.chan_count, other_source_name))

        # Do some general renaming to match tvgids.nl naming
        for i in range(0, len(programs)):
            programs[i]['name'] = general_renames(programs[i]['name'])

        for i in range(0, len(info)):
            info[i]['name'] = general_renames(info[i]['name'])

        # Sort both lists on starttime and get their ranges
        info.sort(key=lambda program: (program['start-time'],program['stop-time']))
        infostarttime = info[0]['start-time'] + datetime.timedelta(seconds = 5)
        infoendtime = info[-1]['stop-time'] - datetime.timedelta(seconds = 5)

        programs.sort(key=lambda program: (program['start-time'],program['stop-time']))
        progstarttime = programs[0]['start-time'] + datetime.timedelta(seconds = 5)
        progendtime = programs[-1]['stop-time'] - datetime.timedelta(seconds = 5)

        log_array.append('%6.0f programs in %s for range: %s - %s, \n' % \
            (len(info), prime_source_name.ljust(11), infostarttime.strftime('%d-%b %H:%M'), infoendtime.strftime('%d-%b %H:%M')))
        log_array.append('%6.0f programs in %s for range: %s - %s\n' % \
            (len(programs), other_source_name.ljust(11), progstarttime.strftime('%d-%b %H:%M'), progendtime.strftime('%d-%b %H:%M')))
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

                matchlog('added from info', None, tdict, 1)
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

                matchlog('added from ', tdict, None, 1)
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

                    matchlog('added from ', tdict, None, 1)
                    if tdict in programs: programs.remove(tdict)
                    if tdict['start-time'] in prog_starttimes: del prog_starttimes[tdict['start-time']]
                    break

        log_array.append('%6.0f programs added outside common timerange\n' % ocount)
        log_array.append('%6.0f programs left in %s to match\n' % (len(info), prime_source_name))
        log_array.append('%6.0f programs left in %s to match\n' % (len(programs), other_source_name))
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

            log_array.append('%6.0f programs generically matched on name to get genre\n' % rcount)
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
                        matchlog('groupslot in ', tdict, None, 8)

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
                        matchlog('groupslot in info', None, tdict, 8)

                    matchlog('', tvdict, None, 8)
                    if tvdict in programs: programs.remove(tvdict)
                    if tvdict['start-time'] in prog_starttimes: del prog_starttimes[tvdict['start-time']]

            if pcount == 0:
                tdict = set_main_id(tdict)
                if tdict['merge-source'] == '':
                    tdict['merge-source'] = prime_source_name

                matchlog('added from info', None, tdict, 1)
                matched_programs.append(tdict)

            if tdict in info: info.remove(tdict)

        log_array.append('%6.0f programs matched on time and name\n' % ncount)
        log_array.append('%6.0f programs matched on time and genre\n' % gcount)
        log_array.append('%6.0f details  added from group slots\n' % scount)
        log_array.append('%6.0f programs added unmatched from info\n' % len(info))

        # List unmatched items to the log
        for tdict in info[:]:
            matchlog('added from info', None, tdict, 1)
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
            matchlog('left over in ', tdict, None , 2)

        log_array.append('\n')
        self.config.log(log_array, 4, 3)
        self.config.log(match_array, 32, 3)

        self.config.channels[chanid].all_programs = matched_programs
        try:
            self.config.infofiles.write_fetch_list(matched_programs, chanid, other_source_name, self.config.channels[chanid].chan_name, None, True)

        except:
            pass

# end FetchData()

