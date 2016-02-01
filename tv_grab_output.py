#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Python 3 compatibility
from __future__ import unicode_literals
# from __future__ import print_function

import re, sys
import traceback, datetime, random
from threading import Thread, Lock, Event
from Queue import Queue, Empty
from copy import deepcopy
from xml.sax import saxutils


class Channel_Config(Thread):
    """
    Class that holds the Channel definitions and manages the data retrieval and processing
    """
    def __init__(self, config, chanid = 0, name = '', group = 99):
        Thread.__init__(self)
        # Flag to stop the thread
        self.config = config
        self.quit = False
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

        for index in range(self.config.source_count):
            self.source_id[index] = ''
            self.source_data[index] = Event()

        self.counters = {}
        self.counters['none'] = 0
        self.counters['cache'] = 0
        self.counters['fail'] = 0
        self.counters['ttvdb'] = 0
        self.counters['ttvdb_fail'] = 0
        self.counters['fetch'] = {}
        self.counters['fetched'] = {}
        self.counters['fetch'][-1] = 0
        #~ self.counters['fetched'][-1] = 0
        for index in self.config.detail_sources:
            self.counters['fetch'][index] = 0
            self.counters['fetched'][index] = 0

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
            for index in self.config.source_order:
                self.source_data[index].set()

            self.detail_data.set()
            return

        if not self.is_child:
            self.child_data.set()

        try:
            # Create the merge order
            self.merge_order = []
            last_merge = []
            if (self.get_source_id(self.opt_dict['prime_source']) != '') \
              and not (self.opt_dict['prime_source'] in self.opt_dict['disable_source']) \
              and not (self.opt_dict['prime_source'] in self.config.opt_dict['disable_source']):
                if self.get_source_id(self.opt_dict['prime_source']) in self.config.no_genric_matching[self.opt_dict['prime_source']]:
                    last_merge.append(self.opt_dict['prime_source'])

                else:
                    self.merge_order.append(self.opt_dict['prime_source'])

            for index in self.config.source_order:
                if (self.get_source_id(index) != '') \
                  and index != self.opt_dict['prime_source'] \
                  and not (index in self.opt_dict['disable_source']) \
                  and not (index in self.config.opt_dict['disable_source']):
                    if self.get_source_id(index) in self.config.no_genric_matching[index]:
                        last_merge.append(index)

                    else:
                        self.merge_order.append(index)

                elif index != self.opt_dict['prime_source']:
                    self.source_data[index].set()

            self.merge_order.extend(last_merge)
            xml_data = False
            # Retrieve and merge the data from the available sources.
            for index in self.merge_order:
                while not self.source_data[index].is_set():
                    # Wait till the event is set by the source, but check every 5 seconds for an unexpected break or wether the source is still alive
                    self.source_data[index].wait(5)
                    if self.quit:
                        self.ready = True
                        return

                    # Check if the source is still alive
                    if not self.config.channelsource[index].is_alive():
                        self.source_data[index].set()
                        break

                if self.source_data[index].is_set():
                    if len(self.config.channelsource[index].program_data[self.chanid]) == 0:
                        if not (index == 1 and 0 in self.merge_order):
                            self.config.log('No Data from %s for channel: %s\n'% (self.config.channelsource[index].source, self.chan_name))

                    elif xml_data == False:
                        # This is the first source with data, so we just take in the data
                        xml_data = True
                        prime_source = self.config.channelsource[index].proc_id
                        with self.config.channelsource[index].source_lock:
                            self.all_programs = self.config.channelsource[index].program_data[self.chanid][:]

                    else:
                        # There is already data, so we merge the incomming data into that
                        xml_data = True
                        self.config.channelsource[index].merge_sources(self.chanid,  prime_source, self.counter)
                        self.config.channelsource[index].parse_programs(self.chanid, 1, 'None')
                        for i in range(0, len(self.all_programs)):
                            self.all_programs[i] = self.config.fetch_func.checkout_program_dict(self.all_programs[i])

            if self.chanid in self.config.combined_channels.keys():
                for c in self.config.combined_channels[self.chanid]:
                    if c['chanid'] in self.config.channels:
                        while not self.config.channels[c['chanid']].child_data.is_set():
                            # Wait till the event is set by the child, but check every 5 seconds for an unexpected break or wether the child is still alive
                            self.config.channels[c['chanid']].child_data.wait(5)
                            if self.quit:
                                self.ready = True
                                return

                            # Check if the child is still alive
                            if not self.config.channels[c['chanid']].is_alive():
                                break

                        if len(self.config.channels[c['chanid']].child_programs) == 0:
                            self.config.log('No Data from %s for channel: %s\n'% (self.config.channels[c['chanid']].chan_name, self.chan_name))

                        elif self.child_data.is_set():
                            # We always merge as there might be restrictions
                            xml_data = True
                            self.config.channelsource[0].merge_sources(self.chanid,  None, self.counter, c)
                            self.config.channelsource[0].parse_programs(self.chanid, 1, 'None')
                            for i in range(0, len(self.all_programs)):
                                self.all_programs[i] = self.config.fetch_func.checkout_program_dict(self.all_programs[i])

            if self.is_child:
                self.child_programs = deepcopy(self.all_programs) if self.active else self.all_programs
                self.child_data.set()
                if not self.active:
                    self.ready = True
                    return

            # And get the detailpages
            if len(self.all_programs) == 0:
                self.detail_data.set()

            else:
                self.get_details()
                while not self.detail_data.is_set():
                    self.detail_data.wait(5)
                    if self.quit:
                        self.ready = True
                        return

                    # Check if the sources are still alive
                    for s in self.config.detail_sources:
                        if self.config.channelsource[s].is_alive():
                            break

                    else:
                        self.detail_data.set()
                        self.config.log('Detail sources: %s, %s and %s died.\n So we stop waiting for the pending details for channel %s\n' \
                            % (self.config.channelsource[0].source, self.config.channelsource[1].source, self.config.channelsource[9].source, self.chan_name))

                self.all_programs = self.detailed_programs

            # And log the results
            #~ with xml_output.output_lock:
                #~ xml_output.cache_count += self.counters['cache']
                #~ xml_output.ttvdb_count += self.counters['ttvdb']
                #~ xml_output.ttvdb_fail_count += self.counters['ttvdb_fail']
                #~ xml_output.progress_counter+= 1
                #~ counter = xml_output.progress_counter

            #~ log_array = ['\n', 'Detail statistics for %s (channel %s of %s)\n' % (self.chan_name, counter, self.config.chan_count)]
            #~ log_array.append( '%6.0f cache hit(s)\n' % (self.counters['cache']))
            #~ if self.opt_dict['fast']:
                #~ log_array.append('%6.0f without details in cache\n' % self.counters['none'])
                #~ log_array.append('\n')
                #~ log_array.append('%6.0f succesful ttvdb lookups\n' % self.counters['ttvdb'])
                #~ log_array.append('%6.0f failed ttvdb lookups\n' % self.counters['ttvdb_fail'])

            #~ else:
                #~ log_array.append('%6.0f detail fetch(es) from tvgids.nl\n' % self.counters['fetched'][0])
                #~ log_array.append('%6.0f detail fetch(es) from tvgids.tv\n' % self.counters['fetched'][1])
                #~ log_array.append('%6.0f detail fetch(es) from primo.eu\n' % self.counters['fetched'][9])
                #~ log_array.append('%6.0f failure(s)\n' % self.counters['fail'])
                #~ log_array.append('%6.0f without detail info\n' % self.counters['none'])
                #~ log_array.append('\n')
                #~ log_array.append('%6.0f succesful ttvdb lookups\n' % self.counters['ttvdb'])
                #~ log_array.append('%6.0f    failed ttvdb lookups\n' % self.counters['ttvdb_fail'])
                #~ log_array.append('\n')
                #~ log_array.append('%6.0f left in the tvgids.nl queue to process\n' % (xml_output.channelsource[0].detail_request.qsize()))
                #~ log_array.append('%6.0f left in the tvgids.tv queue to process\n' % (xml_output.channelsource[1].detail_request.qsize()))
                #~ log_array.append('%6.0f left in the primo.eu queue to process\n' % (xml_output.channelsource[9].detail_request.qsize()))

            #~ log_array.append('\n')
            #~ self.config.log(log_array, 4, 3)

            # a final check on the sanity of the data
            self.config.channelsource[0].parse_programs(self.chanid, 1)

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

            self.ready = True

        except:
            self.config.logging.log_queue.put({'fatal': [traceback.print_exc(), '\n'], 'name': self.chan_name})
            self.ready = True
            return(97)

    def use_cache(self, tdict, cached):
        # copy the cached information, except the start/end times, rating and clumping,
        # these may have changed.
        # But first checkout the dict
        cached = self.config.fetch_func.checkout_program_dict(cached)
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

        for fld in ('name', 'titel aflevering', 'originaltitle', 'jaar van premiere', 'airdate', 'country', 'star-rating', 'omroep'):
            if tdict[fld] != '':
                cached[fld] = tdict[fld]

        if re.sub('[-,. ]', '', cached['name']) == re.sub('[-,. ]', '', cached['titel aflevering']):
            cached['titel aflevering'] = ''

        for fld in ('season', 'episode'):
            if tdict[fld] != 0:
                cached[fld] = int(tdict[fld])

        if tdict['rerun'] == True:
            cached['rerun'] = True

        if len(tdict['kijkwijzer']) > 0:
            for item in tdict['kijkwijzer']:
                if not item in cached['kijkwijzer']:
                    cached['kijkwijzer'].append(item)

        return cached

    def update_counter(self, cnt_type, source_id=None, cnt_add=True, cnt_change=1):
        if not isinstance(cnt_change, int) or cnt_change == 0:
            return

        with self.channel_lock:
            if not cnt_type in self.counters:
                if source_id == None:
                    self.counters[cnt_type] = 0

                else:
                    self.counters[cnt_type] = {}
                    self.counters[cnt_type][source_id] = 0

            if isinstance(self.counters[cnt_type], int):
                if cnt_add:
                    self.counters[cnt_type] += cnt_change

                else:
                    self.counters[cnt_type] -= cnt_change

            elif isinstance(self.counters[cnt_type], dict):
                if source_id == None:
                    source_id = 0

                if isinstance(self.counters[cnt_type][source_id], int):
                    if cnt_add:
                        self.counters[cnt_type][source_id] += cnt_change

                    else:
                        self.counters[cnt_type][source_id] -= cnt_change

    def get_counter(self):
        with self.channel_lock:
            self.fetch_counter += 1
            return 100*float(self.fetch_counter)/float(self.nprograms)

    def get_source_id(self, source):
        if source in self.source_id.keys():
            return self.source_id[source]

        return ''

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
            self.config.log(['\n', 'Now Checking cache for %s programs on %s(xmltvid=%s%s)\n' % \
                (len(programs), self.chan_name, self.xmltvid, (self.opt_dict['compat'] and '.tvgids.nl' or '')), \
                '    (channel %s of %s) for %s days.\n' % (self.counter, self.config.chan_count, self.config.opt_dict['days'])], 2)

        else:
            self.config.log(['\n', 'Now fetching details for %s programs on %s(xmltvid=%s%s)\n' % \
                (len(programs), self.chan_name, self.xmltvid, (self.opt_dict['compat'] and '.tvgids.nl' or '')), \
                '    (channel %s of %s) for %s days.\n' % (self.counter, self.config.chan_count, self.config.opt_dict['days'])], 2)

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
                        self.config.log(u'      [cached] %s:(%3.0f%%) %s\n' % (self.chan_name, self.get_counter(), logstring), 8, 1)
                        self.update_counter('cache')
                        p = self.use_cache(p, cached_program)
                        if not (self.config.opt_dict['disable_ttvdb'] or self.opt_dict['disable_ttvdb']):
                            if p['genre'].lower() == u'serie/soap' and p['titel aflevering'] != '' and p['season'] == 0:
                                self.update_counter('fetch', -1)
                                self.config.ttvdb.detail_request.put({'tdict':p, 'parent': self, 'task': 'update_ep_info'})
                                continue

                        self.detailed_programs.append(p)
                        continue

            # Either we are fast-mode, outside slowdays or there is no url. So we continue
            no_detail_fetch = (no_fetch or ((p['detail_url'][0] == '') and \
                                                                (p['detail_url'][9] == '') and \
                                                                (p['detail_url'][1] == '')))

            if no_detail_fetch:
                self.config.log(u'    [no fetch] %s:(%3.0f%%) %s\n' % (self.chan_name, self.get_counter(), logstring), 8, 1)
                self.update_counter('none')
                if not (self.config.opt_dict['disable_ttvdb'] or self.opt_dict['disable_ttvdb']):
                    if p['genre'].lower() == u'serie/soap' and p['titel aflevering'] != '' and p['season'] == 0:
                        self.update_counter('fetch', -1)
                        self.config.ttvdb.detail_request.put({'tdict':p, 'parent': self, 'task': 'update_ep_info'})
                        continue

                self.detailed_programs.append(p)

                continue

            for src_id in self.config.detail_sources:
                if src_id not in self.config.opt_dict['disable_detail_source'] and \
                  src_id not in self.opt_dict['disable_detail_source'] and \
                  p['detail_url'][src_id] != '':
                    self.update_counter('fetch', src_id)
                    self.config.channelsource[src_id].detail_request.put({'tdict':p, 'cache_id': cache_id, 'logstring': logstring, 'parent': self})
                    break

        # Place terminator items in the queue
        for src_id in self.config.detail_sources:
            if self.counters['fetch'][src_id] > 0:
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
        psubtitle = program['titel aflevering']
        if  ptitle == None or ptitle == '':
            return program

        # exclude certain programs
        if  ('titel aflevering' in program and psubtitle != '')  \
          or ('genre' in program and program['genre'].lower() in ['movies','film']) \
          or (ptitle.lower() in self.config.notitlesplit):
            return program

        # and do the title split test
        p = ptitle.split(':')
        if len(p) >1:
            self.config.log('Splitting title \"%s\"\n' %  ptitle, 64)
            program['name'] = p[0].strip()
            program['titel aflevering'] = "".join(p[1:]).strip()
            if self.config.write_info_files:
                self.config.infofiles.addto_detail_list(unicode('Name split = %s + %s' % (program['name'] , program['titel aflevering'])))

        return program

# end Channel_Config

class XMLoutput:
    '''
    This class collects the data and creates the output
    '''
    def __init__(self, config):

        self.config = config
        self.xmlencoding = 'UTF-8'
        # Thes will contain the seperate XML strings
        self.xml_channels = {}
        self.xml_programs = {}
        self.progress_counter = 0

        # We have several sources of logos, the first provides the nice ones, but is not
        # complete. We use the tvgids logos to fill the missing bits.
        self.logo_source_preference = []
        self.logo_provider = []

        self.output_lock = Lock()
        self.cache_return = Queue()

        self.cache_count = 0
        self.fetch_count = 0
        self.fail_count = 0
        self.ttvdb_count = 0
        self.ttvdb_fail_count = 0
        self.program_count = 0

    def xmlescape(self, s):
        """Escape <, > and & characters for use in XML"""
        return saxutils.escape(s)

    def format_timezone(self, td, use_utc=False, only_date=False ):
        """
        Given a datetime object, returns a string in XMLTV format
        """
        if use_utc:
            td = td.astimezone(UTC)

        if only_date:
            return td.strftime('%Y%m%d')

        else:
            return td.strftime('%Y%m%d%H%M%S %z')

    def add_starttag(self, tag, ident = 0, attribs = '', text = '', close = False):
        '''
        Add a starttag with optional attributestring, textstring and optionally close it.
        Give it the proper ident.
        '''
        if attribs != '':
            attribs = ' %s' % attribs

        if close and text == '':
            return u'%s<%s%s/>\n' % (''.rjust(ident), self.xmlescape(tag), self.xmlescape(attribs))

        if close and text != '':
            return u'%s<%s%s>%s</%s>\n' % (''.rjust(ident), self.xmlescape(tag), self.xmlescape(attribs), self.xmlescape(text), self.xmlescape(tag))

        else:
            return u'%s<%s%s>%s\n' % (''.rjust(ident), self.xmlescape(tag), self.xmlescape(attribs), self.xmlescape(text))

    def add_endtag(self, tag, ident = 0):
        '''
        Return a proper idented closing tag
        '''
        return u'%s</%s>\n' % (''.rjust(ident), self.xmlescape(tag))

    def create_channel_strings(self, chanid, add_HD = None):
        '''
        Create the strings for the channels we fetched info about
        '''
        if add_HD == True:
            xmltvid = '%s-hd' % self.config.channels[chanid].xmltvid

        else:
            xmltvid = self.config.channels[chanid].xmltvid

        self.xml_channels[xmltvid] = []
        self.xml_channels[xmltvid].append(self.add_starttag('channel', 2, 'id="%s%s"' % \
            (xmltvid, self.config.channels[chanid].opt_dict['compat'] and '.tvgids.nl' or '')))
        self.xml_channels[xmltvid].append(self.add_starttag('display-name', 4, 'lang="nl"', \
            self.config.channels[chanid].chan_name, True))
        if (self.config.channels[chanid].opt_dict['logos']):
            if self.config.channels[chanid].icon_source in range(len(self.logo_provider)):
                lpath = self.logo_provider[self.config.channels[chanid].icon_source]
                lname = self.config.channels[chanid].icon
                if self.config.channels[chanid].icon_source == 5 and lpath[-16:] == 'ChannelLogos/02/':
                    if len(lname) > 16 and  lname[0:16] == 'ChannelLogos/02/':
                        lname = lname[16:].split('?')[0]

                    else:
                        lname = lname.split('?')[0]

                elif self.config.channels[chanid].icon_source == 5 and lpath[-16:] != 'ChannelLogos/02/':
                    if len(lname) > 16 and  lname[0:16] == 'ChannelLogos/02/':
                        lname = lname.split('?')[0]

                    else:
                        lpath = lpath + 'ChannelLogos/02/'
                        lname = lname.split('?')[0]

                full_logo_url = lpath + lname
                self.xml_channels[xmltvid].append(self.add_starttag('icon', 4, 'src="%s"' % full_logo_url, '', True))

            elif self.config.channels[chanid].icon_source == 99:
                self.xml_channels[xmltvid].append(self.add_starttag('icon', 4, 'src="%s"' % self.config.channels[chanid].icon, '', True))

        self.xml_channels[xmltvid].append(self.add_endtag('channel', 2))

    def create_program_string(self, chanid, add_HD = None):
        '''
        Create all the program strings
        '''
        if add_HD == True:
            xmltvid = '%s-hd' % self.config.channels[chanid].xmltvid

        else:
            xmltvid = self.config.channels[chanid].xmltvid
            with self.output_lock:
                self.program_count += len(self.config.channels[chanid].all_programs)

        self.xml_programs[xmltvid] = []
        self.config.channels[chanid].all_programs.sort(key=lambda program: (program['start-time'],program['stop-time']))
        for program in self.config.channels[chanid].all_programs[:]:
            xml = []

            # Start/Stop
            attribs = 'start="%s" stop="%s" channel="%s%s"' % \
                (self.format_timezone(program['start-time'], self.config.opt_dict['use_utc']), \
                self.format_timezone(program['stop-time'], self.config.opt_dict['use_utc']), \
                xmltvid, self.config.channels[chanid].opt_dict['compat'] and '.tvgids.nl' or '')

            if 'clumpidx' in program and program['clumpidx'] != '':
                attribs += 'clumpidx="%s"' % program['clumpidx']

            xml.append(self.add_starttag('programme', 2, attribs))

            # Title
            xml.append(self.add_starttag('title', 4, 'lang="nl"', program['name'], True))
            if program['originaltitle'] != '' and program['country'] != '' and program['country'].lower() != 'nl' and program['country'].lower() != 'be':
                xml.append(self.add_starttag('title', 4, 'lang="%s"' % (program['country'].lower()), program['originaltitle'], True))

            # Subtitle
            if 'titel aflevering' in program and program['titel aflevering'] != '':
                xml.append(self.add_starttag('sub-title', 4, 'lang="nl"', program['titel aflevering'] ,True))

            # Add an available subgenre in front off the description or give it as description

            # A prefered description was set and found
            if len(program['prefered description']) > 100:
                program['description'] = program['prefered description']

            desc_line = u''
            if program['subgenre'] != '':
                 desc_line = u'%s: ' % (program['subgenre'])

            if program['omroep'] != ''and re.search('(\([A-Za-z \-]*?\))', program['omroep']):
                desc_line = u'%s%s ' % (desc_line, re.search('(\([A-Za-z \-]*?\))', program['omroep']).group(1))

            if program['description'] != '':
                desc_line = u'%s%s ' % (desc_line, program['description'])

            # Limit the length of the description
            if desc_line != '':
                desc_line = re.sub('\n', ' ', desc_line)
                if len(desc_line) > self.config.channels[chanid].opt_dict['desc_length']:
                    spacepos = desc_line[0:self.config.channels[chanid].opt_dict['desc_length']-3].rfind(' ')
                    desc_line = desc_line[0:spacepos] + '...'

                xml.append(self.add_starttag('desc', 4, 'lang="nl"', desc_line.strip(),True))

            # Process credits section if present.
            # This will generate director/actor/presenter info.
            if program['credits'] != {}:
                xml.append(self.add_starttag('credits', 4))
                for role in ('director', 'actor', 'writer', 'adapter', 'producer', 'composer', 'editor', 'presenter', 'commentator', 'guest'):
                    if role in program['credits']:
                        for name in program['credits'][role]:
                            if name != '':
                                xml.append(self.add_starttag((role), 6, '', self.xmlescape(name),True))

                xml.append(self.add_endtag('credits', 4))

            # Original Air-Date
            if isinstance(program['airdate'], datetime.date):
                xml.append(self.add_starttag('date', 4, '',  \
                    self.format_timezone(program['airdate'], self.config.opt_dict['use_utc'],True), True))

            elif program['jaar van premiere'] != '':
                xml.append(self.add_starttag('date', 4, '', program['jaar van premiere'],True))

            # Genre
            if self.config.channels[chanid].opt_dict['cattrans']:
                cat0 = ('', '')
                cat1 = (program['genre'].lower(), '')
                cat2 = (program['genre'].lower(), program['subgenre'].lower())
                if cat2 in self.config.cattrans.keys() and self.config.cattrans[cat2] != '':
                    cat = self.config.cattrans[cat2].capitalize()

                elif cat1 in self.config.cattrans.keys() and self.config.cattrans[cat1] != '':
                    cat = self.config.cattrans[cat1].capitalize()

                elif cat0 in self.config.cattrans.keys() and self.config.cattrans[cat0] != '':
                   cat = self.config.cattrans[cat0].capitalize()

                else:
                    cat = 'Unknown'

                xml.append(self.add_starttag('category', 4 , '', cat, True))

            else:
                cat = program['genre']
                if program['genre'] != '':
                    xml.append(self.add_starttag('category', 4, 'lang="nl', program['genre'], True))

                else:
                    xml.append(self.add_starttag('category', 4 , '', 'Overige', True))

            # An available url
            if program['infourl'] != '':
                xml.append(self.add_starttag('url', 4, '', program['infourl'],True))

            if program['country'] != '':
                xml.append(self.add_starttag('country', 4, '', program['country'],True))

            # Only add season/episode if relevant. i.e. Season can be 0 if it is a pilot season, but episode never.
            # Also exclude Sports for MythTV will make it into a Series
            if cat.lower() != 'sports' and cat.lower() != 'sport':
                if program['season'] != 0 and program['episode'] != 0:
                    if program['season'] == 0:
                        text = ' . %d . '  % (int(program['episode']) - 1)

                    else:
                        text = '%d . %d . '  % (int(program['season']) - 1, int(program['episode']) - 1)

                    xml.append(self.add_starttag('episode-num', 4, 'system="xmltv_ns"', text,True))

            # Process video/audio/teletext sections if present
            if (program['video']['breedbeeld'] or program['video']['blackwhite'] \
              or (self.config.channels[chanid].opt_dict['mark_hd'] \
              or add_HD == True) and (program['video']['HD'])):
                xml.append(self.add_starttag('video', 4))

                if program['video']['breedbeeld']:
                    xml.append(self.add_starttag('aspect', 6, '', '16:9',True))

                if program['video']['blackwhite']:
                    xml.append(self.add_starttag('colour', 6, '', 'no',True))

                if (self.config.channels[chanid].opt_dict['mark_hd'] \
                  or add_HD == True) and (program['video']['HD']):
                    xml.append(self.add_starttag('quality', 6, '', 'HDTV',True))

                xml.append(self.add_endtag('video', 4))

            if program['audio'] != '':
                xml.append(self.add_starttag('audio', 4))
                xml.append(self.add_starttag('stereo', 6, '',program['audio'] ,True))
                xml.append(self.add_endtag('audio', 4))

            # It's been shown before
            if program['rerun']:
                xml.append(self.add_starttag('previously-shown', 4, '', '',True))

            # It's a first
            if program['premiere']:
                xml.append(self.add_starttag('premiere', 4, '', '',True))

            # It's the last showing
            if program['last-chance']:
                xml.append(self.add_starttag('last-chance', 4, '', '',True))

            # It's new
            if program['new']:
                xml.append(self.add_starttag('new', 4, '', '',True))

            # There are teletext subtitles
            if program['teletekst']:
                xml.append(self.add_starttag('subtitles', 4, 'type="teletext"', '',True))

            # Add any Kijkwijzer items
            if self.config.opt_dict['kijkwijzerstijl'] in ('long', 'short', 'single'):
                kstring = ''
                # First only one age limit from high to low
                for k in ('4', '3', '9', '2', '1'):
                    if k in program['kijkwijzer']:
                        if self.config.opt_dict['kijkwijzerstijl'] == 'single':
                            kstring += (self.config.kijkwijzer[k]['code'] + ': ')

                        else:
                            xml.append(self.add_starttag('rating', 4, 'system="kijkwijzer"'))
                            if self.config.opt_dict['kijkwijzerstijl'] == 'long':
                                xml.append(self.add_starttag('value', 6, '', self.config.kijkwijzer[k]['text'], True))

                            else:
                                xml.append(self.add_starttag('value', 6, '', self.config.kijkwijzer[k]['code'], True))

                            xml.append(self.add_starttag('icon', 6, 'src="%s"' % self.config.kijkwijzer[k]['icon'], '', True))
                            xml.append(self.add_endtag('rating', 4))
                        break

                # And only one of any of the others
                for k in ('g', 'a', 's', 't', 'h', 'd'):
                    if k in program['kijkwijzer']:
                        if self.config.opt_dict['kijkwijzerstijl'] == 'single':
                            kstring += k.upper()

                        else:
                            xml.append(self.add_starttag('rating', 4, 'system="kijkwijzer"'))
                            if self.config.opt_dict['kijkwijzerstijl'] == 'long':
                                xml.append(self.add_starttag('value', 6, '', self.config.kijkwijzer[k]['text'], True))

                            else:
                                xml.append(self.add_starttag('value', 6, '', self.config.kijkwijzer[k]['code'], True))

                            xml.append(self.add_starttag('icon', 6, 'src="%s"' % self.config.kijkwijzer[k]['icon'], '', True))
                            xml.append(self.add_endtag('rating', 4))

                if self.config.opt_dict['kijkwijzerstijl'] == 'single' and kstring != '':
                    xml.append(self.add_starttag('rating', 4, 'system="kijkwijzer"'))
                    xml.append(self.add_starttag('value', 6, '', kstring, True))
                    xml.append(self.add_endtag('rating', 4))

            # Set star-rating if applicable
            if program['star-rating'] != '':
                xml.append(self.add_starttag('star-rating', 4))
                xml.append(self.add_starttag('value', 6, '',('%s/10' % (program['star-rating'])).strip(),True))
                xml.append(self.add_endtag('star-rating', 4))

            xml.append(self.add_endtag('programme', 2))
            self.xml_programs[xmltvid].append(xml)

    def get_xmlstring(self):
        '''
        Compound the compleet XML output and return it
        '''
        if self.config.output == None:
            startstring =[u'<?xml version="1.0" encoding="%s"?>\n' % logging.local_encoding]

        else:
            startstring =[u'<?xml version="1.0" encoding="%s"?>\n' % self.xmlencoding]

        startstring.append(u'<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')
        startstring.append(u'<tv generator-info-name="%s" generator-info-url="https://github.com/tvgrabbers/tvgrabnlpy">\n' % self.config.version(True))
        closestring = u'</tv>\n'

        xml = []
        xml.append(u"".join(startstring))

        for channel in self.config.channels.values():
            if channel.active and channel.xmltvid in self.xml_channels:
                xml.append(u"".join(self.xml_channels[channel.xmltvid]))
                if channel.opt_dict['add_hd_id'] and '%s-hd' % (channel.xmltvid) in self.xml_channels:
                    xml.append(u"".join(self.xml_channels['%s-hd' % channel.xmltvid]))

        for channel in self.config.channels.values():
            if channel.active and channel.xmltvid in self.xml_programs:
                for program in self.xml_programs[channel.xmltvid]:
                    xml.append(u"".join(program))

                if channel.opt_dict['add_hd_id'] and '%s-hd' % (channel.xmltvid) in self.xml_channels:
                    for program in self.xml_programs['%s-hd' % channel.xmltvid]:
                        xml.append(u"".join(program))

        xml.append(closestring)

        return u"".join(xml)

    def print_string(self):
        '''
        Print the compleet XML string to stdout or selected file
        '''
        xml = self.get_xmlstring()

        if xml != None:
            if self.config.output == None:
                sys.stdout.write(xml.encode(logging.local_encoding, 'replace'))

            else:
                self.config.output.write(xml)

            if self.config.write_info_files:
                self.config.infofiles.write_xmloutput(xml)

# end XMLoutput
