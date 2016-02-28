#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Python 3 compatibility
from __future__ import unicode_literals
# from __future__ import print_function

import re, sys, traceback, codecs, json
import time, datetime, random
import tv_grab_fetch, pytz
from xml.etree import cElementTree as ET
try:
    unichr(42)
except NameError:
    unichr = chr    # Python 3

class tvgids_JSON(tv_grab_fetch.FetchData):
    """
    Get all available days of programming for the requested channels
    from the tvgids.nl json pages. Based on FetchData
    """
    def init_channels(self):
        """ Detail Site layout oud
            <head>
            <body>
                <div id="container">
                    <div id="header">
                    <div id="content">
                        <div id="content-header">Title</div>
                        <div id="content-col-left">
                            <div id="prog-content">Description</div>
                        <div id="content-col-right">
                            <div id="prog-info">
                                <div id="prog-info-content">
                                    <ul id="prog-info-content-colleft">
                                        <li><strong>Titel:</strong>Nederland Waterland</li>
                                            ...
                                    <ul id="prog-info-content-colright">
                                        <li><strong>Jaar van premiere:</strong>2014</li>
                                            ...
                                        <li><strong>Bijzonderheden:</strong>Teletekst ondertiteld, Herhaling, HD 1080i</li>
                                <div id="prog-info-footer"></div>
                            </div>
                        </div>
                    </div>
                    <div class="clearer"></div>
                </div>
                <div id="footer-container">
            </body>
            Nieuw
            <head>
            <body>
                <input type="hidden" id="categoryClass" value="">
                    <input type="hidden" id="notAllowedClass" value="">
                        <input type="hidden" id="notAllowedTitles" value="">
                            <div class="container pagecontainer">
                                <div class="row">
                                    <div class="col-md-8">
                                        <div id="prog-content">
                                            <div id="prog-video">
                                            ...
                                            </div>
                                            <div class="programmering">
                                                <h1>Harry Potter and the Goblet of Fire<span><sup>(2005)</sup></span></h1>
                                                <div class="clear:both;"></div>
                                                <script type="text/javascript" src="http://tvgidsassets.nl/v43/js/nlziet.js"></script>
                                                <div class="programmering_details">
                                                    <ul>
                                                        <li class="datum_tijd"> 1 mei 2015, 22:45 - 23:55 uur</li>
                                                        <li class="zender"><img src="http://tvgidsassets.nl/img/channels/53x27/36.png">SBS 6</li>
                                                    </ul>
                                                </div>
                                                <div style="clear:both"></div>
                                            </div>
                                            <div class="clear"></div>
                                                ...
                                            <div class="clear"></div>
                                            <p class="summary">
                                                <span class="articleblock articleblock_color_fantasy">
                                            FANTASY
                                                </span>
                                                                    Harry Potter gaat zijn vierde schooljaar in op de magische school Zweinstein, waar dit jaar het belangrijke internationale Triwizard Tournament wordt gehouden. Deze competitie is alleen voor de oudere en ervaren tovenaarsstudenten, maar toch komt Harry's naam boven als een van de deelnemers. Harry weet niet hoe dit mogelijk is, maar wordt toch gedwongen om mee te doen. Terwijl Harry zich voorbereidt op de gevaarlijke wedstrijd, wordt duidelijk dat de boosaardige Voldemort en zijn aanhangers steeds sterker worden en het nog altijd op zijn leven hebben gemunt. Dit nieuws is niet het enige wat Harry de rillingen bezorgt, hij heeft ook nog geen afspraakje voor het gala.
                                            </p>
                                            <p></p>
                                            <br class="brclear" />
                                            <div class="programmering_info_socials">
                                                ...
                                            </div>
                                            <br class="clear" />
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </body>
        """

        # These regexes fetch the relevant data out of thetvgids.nl pages, which then will be parsed to the ElementTree
        self.tvgidsnlprog = re.compile('<div id="prog-content">(.*?)<div id="prog-banner-content"',re.DOTALL)
        self.tvgidsnltitle = re.compile('<div class="programmering">(.*?)</h1>',re.DOTALL)
        self.tvgidsnldesc = re.compile('<p(.*?)</p>',re.DOTALL)
        self.tvgidsnldesc2 = re.compile('<div class="tekst col-sm-12">(.*?)</div>',re.DOTALL)
        self.tvgidsnldetails = re.compile('<div class="programmering_info_detail">(.*?)</div>',re.DOTALL)
        self.aflevering = re.compile('(\d*)/?\d*(.*)')

        self.url_channels = ''
        self.cooky_cnt = 0
        self.init_channel_source_ids()
        for channel in self.channels.values():
            if self.url_channels == '':
                self.url_channels = channel

            else:
                self.url_channels  = '%s,%s' % (self.url_channels, channel)

    def get_url(self, type = 'channels', offset = 0, id = None):

        tvgids = 'http://www.tvgids.nl/'
        tvgids_json = tvgids + 'json/lists/'

        if type == 'channels':
            return  u'%schannels.php' % (tvgids_json)

        elif type == 'day':
            return ['%sprograms.php' % (tvgids_json), {'channels': self.url_channels, 'day': offset}]

        elif (id == None) or id == '':
            return ''

        elif type == 'detail':
            return u'%sprogramma/%s/' % (tvgids, id)

        elif type == 'json_detail':
            return [u'%sprogram.php' % (tvgids_json), {'id':id}]

    #~ def get_channels(self):
        #~ """
        #~ Get a list of all available channels and store these
        #~ in all_channels.
        #~ """

        #~ channel_list = self.config.fetch_func.get_page(self.get_url(), 'utf-8', counter = ['base', self.proc_id], is_json = True)
        #~ if channel_list == None:
            #~ self.config.log(self.config.text('sources', 1, (self.source, )))
            #~ return 69  # EX_UNAVAILABLE

        #~ # and create a file with the channels
        #~ self.all_channels ={}
        #~ for channel in channel_list:
            #~ # the json data has the channel names in XML entities.
            #~ chanid = channel['id']
            #~ self.all_channels[chanid] = {}
            #~ self.all_channels[chanid]['name'] = self.functions.unescape(channel['name']).strip()

    #~ def load_pages(self):

        #~ if self.config.opt_dict['offset'] > 4:
            #~ for chanid in self.channels.keys():
                #~ self.channel_loaded[chanid] = True
                #~ self.config.channels[chanid].source_data[self.proc_id].set()

            #~ return

        #~ if len(self.channels) == 0 :
            #~ return

        #~ self.dl = {}
        #~ self.dd = {}
        #~ for chanid in self.channels.values():
            #~ self.dl[chanid] =[]
            #~ self.dd[chanid] =[]

        #~ first_fetch = True

        #~ for retry in (0, 1):
            #~ for offset in range(self.config.opt_dict['offset'], min((self.config.opt_dict['offset'] + self.config.opt_dict['days']), 4)):
                #~ if self.quit:
                    #~ return

                #~ # Check if it is already loaded
                #~ if self.day_loaded[0][offset]:
                    #~ continue

                #~ self.config.log(['\n', self.config.text('sources', 2, (len(self.channels), self.source)), \
                    #~ self.config.text('sources', 3, (offset, self.config.opt_dict['days']))], 2)

                #~ channel_url = self.get_url('day', offset)

                #~ if not first_fetch:
                    #~ # be nice to tvgids.nl
                    #~ time.sleep(random.randint(self.config.opt_dict['nice_time'][0], self.config.opt_dict['nice_time'][1]))
                    #~ first_fetch = false

                #~ # get the raw programming for the day
                #~ strdata = self.config.fetch_func.get_page(channel_url[0], 'utf-8', None, channel_url[1], ['base', self.proc_id], True)
                #~ if strdata == None or strdata == {}:
                    #~ self.config.log(self.config.text('sources', 4, (self.source, offset)))
                    #~ self.fail_count += 1
                    #~ continue

                #~ self.parse_basepage(strdata)

        #~ self.parse_basepage2()
    #~ def parse_basepage(self,strdata, offset):
        #~ offset = offset['offset']
        #~ # Just let the json library parse it.
        #~ self.base_count += 1
        #~ for chanid, v in strdata.iteritems():
            #~ # Most channels provide a list of program dicts, some a numbered dict
            #~ try:
                #~ if isinstance(v, dict):
                    #~ v=list(v.values())

                #~ elif not isinstance(v, (list,tuple)):
                    #~ raise TypeError

            #~ except (TypeError, LookupError):
                #~ self.config.log(self.config.text('sources', 5, (channel_url, )))
                #~ continue
            #~ # remove the overlap at daychange and seperate the channels
            #~ for p in v:
                #~ if not p in self.dl[chanid]:
                    #~ self.dd[chanid].append(p)

        #~ self.day_loaded[0][offset] = True
        #~ for chanid, chan_scid in self.channels.items():
            #~ if len(self.dd) > 0:
                #~ self.day_loaded[chanid][offset] = True
                #~ self.dl[chan_scid].extend(self.dd[chan_scid])
                #~ self.dd[chan_scid] =[]

    #~ def parse_basepage2(self):
        #~ for chanid, chan_scid in self.channels.items():
            #~ if len(self.dl[chan_scid]) == 0:
                #~ self.config.channels[chanid].source_data[self.proc_id].set()
                #~ continue

            #~ # parse the list to adjust to what we want
            #~ for item in self.dl[chan_scid]:
                #~ tdict = self.functions.checkout_program_dict()
                #~ if (item['db_id'] != '') and (item['db_id'] != None):
                    #~ tdict['prog_ID'][self.proc_id] = u'nl-%s' % (item['db_id'])
                    #~ self.json_by_id[tdict['prog_ID'][self.proc_id]] = item
                    #~ tdict['ID'] = tdict['prog_ID'][self.proc_id]

                #~ tdict['source'] = self.source
                #~ tdict['channelid'] = chanid
                #~ tdict['channel']  = self.config.channels[chanid].chan_name
                #~ tdict['detail_url'][self.proc_id] = self.get_url(type= 'detail', id = item['db_id'])

                #~ # The Title
                #~ tdict['name'] = self.functions.unescape(item['titel'])
                #~ tdict = self.check_title_name(tdict)
                #~ if  tdict['name'] == None or tdict['name'] == '':
                    #~ self.config.log(self.config.text('sources', 6, (tdict['detail_url'][self.proc_id], tdict['channel'], self.source)))
                    #~ continue

                #~ # The timing
                #~ tdict['start-time'] = self.functions.get_datetime(item['datum_start'], tzinfo = self.site_tz)
                #~ tdict['stop-time']  = self.functions.get_datetime(item['datum_end'], tzinfo = self.site_tz)
                #~ if tdict['start-time'] == None or tdict['stop-time'] == None:
                    #~ self.config.log(self.config.text('sources', 7, (tdict['name'], tdict['channel'], self.source)))
                    #~ continue

                #~ tdict['offset'] = self.functions.get_offset(tdict['start-time'], self.current_date)

                #~ tdict['genre'] = self.functions.unescape(item['genre']) if ('genre' in item and item['genre'] != None) else ''
                #~ tdict['subgenre'] = self.functions.unescape(item['soort']) if ('soort' in item and item['soort'] != None) else ''
                #~ if  ('kijkwijzer' in item and not (item['kijkwijzer'] == None or item['kijkwijzer'] == '')):
                    #~ for k in item['kijkwijzer']:
                        #~ if k in self.config.kijkwijzer.keys() and k not in tdict['kijkwijzer']:
                            #~ tdict['kijkwijzer'].append(k)

                #~ self.program_by_id[tdict['prog_ID'][self.proc_id]] = tdict
                #~ with self.source_lock:
                    #~ self.program_data[chanid].append(tdict)

                #~ self.config.genre_list.append((tdict['genre'].lower(), tdict['subgenre'].lower()))

            #~ self.program_data[chanid].sort(key=lambda program: (program['start-time'],program['stop-time']))
            #~ self.parse_programs(chanid, 0, 'None')
            #~ self.channel_loaded[chanid] = True
            #~ self.config.channels[chanid].source_data[self.proc_id].set()
            #~ try:
                #~ self.config.infofiles.write_fetch_list(self.program_data[chanid], chanid, self.source, self.config.channels[chanid].chan_name, self.proc_id)

            #~ except:
                #~ pass

    def load_detailpage(self, tdict):

        try:
            strdata = self.config.fetch_func.get_page(self.get_url('detail', id = tdict['detail_url'][self.proc_id]),
                                                                                txtdata = {'cookieoptin': 'true'},
                                                                                counter = ['detail', self.proc_id, tdict['channelid']])
            if strdata == None:
                self.config.log(self.config.text('sources', 8, (tdict['detail_url'][self.proc_id], )), 1)
                return

            if re.search('<div class="cookie-backdrop">', strdata):
                self.cooky_cnt += 1
                if self.cooky_cnt > 2:
                    self.cookyblock = True
                    self.config.log(self.config.text('sources', 1, type = self.source), 1)

                else:
                    self.cooky_cnt = 0

                return

            strdata = self.tvgidsnlprog.search(strdata)
            if strdata == None:
                self.config.log(self.config.text('sources', 8, (tdict['detail_url'][self.proc_id], )), 1)
                return

            strdata = '<div>\n' +  strdata.group(1)
            if re.search('[Gg]een detailgegevens be(?:kend|schikbaar)', strdata):
                strtitle = ''
                strdesc = ''

            else:
                # They sometimes forget to close a <p> tag
                strdata = re.sub('<p>', '</p>xxx<p>', strdata, flags = re.DOTALL)
                strtitle = self.tvgidsnltitle.search(strdata)
                if strtitle == None:
                    strtitle = ''

                else:
                    # There are titles containing '<' (eg. MTV<3) which interfere. Since whe don't need it we remove the title
                    strtitle = re.sub('<h1>.*?<span>', '<h1><span>', strtitle.group(0), flags = re.DOTALL)
                    strtitle = strtitle + '\n</div>\n'

                strdesc = ''
                for d in self.tvgidsnldesc.findall(strdata):
                    strdesc += '<p%s</p>\n' % d

                strdesc = '<div>\n' + strdesc + '\n</div>\n'

                d = self.tvgidsnldesc2.search(strdata)
                if d != None:
                    d = re.sub('</p>xxx<p>', '<p>', d.group(0), flags = re.DOTALL)
                    strdesc += d + '\n'

            strdetails = self.tvgidsnldetails.search(strdata)
            if strdetails == None:
                strdetails = ''

            else:
                strdetails = strdetails.group(0)

            strdata = (self.functions.clean_html('<root>\n' + strtitle + strdesc + strdetails + '\n</root>\n')).strip().encode('utf-8')
            htmldata = ET.fromstring(strdata)

        except:
            self.config.log([self.config.text('sources', 9, (tdict['detail_url'][self.proc_id],)), traceback.format_exc()])
            if self.config.write_info_files:
                self.config.infofiles.write_raw_string('Error: %s at line %s\n\n' % (sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
                self.config.infofiles.write_raw_string('<root>\n' + strtitle + strdesc + strdetails + '\n</root>\n')

            # if we cannot find the description page,
            # go to next in the loop
            return None

        # We scan every alinea of the description
        try:
            tdict = self.filter_description(htmldata, 'div/p', tdict)
            if self.config.channels[tdict['channelid']].opt_dict['prefered_description'] == self.proc_id:
                tdict['prefered description'] = tdict['description']

        except:
            self.config.log([self.config.text('sources', 10, (tdict['detail_url'][self.proc_id], )), traceback.format_exc()])
            if self.config.write_info_files:
                self.config.infofiles.write_raw_string('Error: %s at line %s\n\n' % (sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
                self.config.infofiles.write_raw_string('<root>\n' + strdesc + '\n</root>\n')

        try:
            if htmldata.find('div/h1/span/sup') != None:
                tmp = htmldata.find('div/h1/span/sup').text
                if tmp != None:
                    tmp = re.sub('\(', '', tmp)
                    tdict['jaar van premiere'] = re.sub('\)', '', tmp).strip()

        except:
            self.config.log(traceback.format_exc())
            if self.config.write_info_files:
                self.config.infofiles.write_raw_string('Error: %s at line %s\n\n' % (sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
                self.config.infofiles.write_raw_string(strdata)

        # We scan all the details
        for d in htmldata.findall('div/ul/li'):
            try:
                ctype = self.functions.empersant(d.find('span[@class="col-lg-3"]').text).strip().lower()
                if ctype[-1] == ':':
                    ctype = ctype[0:len(ctype)-1]

                if ctype == 'kijkwijzer':
                    content = ''
                    for k in d.find('span[@class="col-lg-9 programma_detail_info kijkwijzer_img"]'):
                        item = {'text':k.get('alt', '') ,'icon':k.get('src', '')}
                        if item['text'] != '' or item['icon'] != '':
                            for kk, kw in self.config.kijkwijzer.items():
                                if (kw['text'] == item['text'] or kw['icon'] == item['icon']) and kk not in tdict['kijkwijzer']:
                                    tdict['kijkwijzer'].append(kk)
                                    break

                else:
                    content = self.functions.empersant(d.find('span[@class="col-lg-9 programma_detail_info"]').text).strip()

            except:
                self.config.log(traceback.format_exc())
                if self.config.write_info_files:
                    self.config.infofiles.write_raw_string('Error: %s at line %s\n%s\n' % (sys.exc_info()[1], sys.exc_info()[2].tb_lineno, d))
                    self.config.infofiles.write_raw_string(strdata)

                continue

            try:
                if content == '':
                    continue

                elif ctype == 'aflevering':
                    # This contains a subtitle, optionally preseded by an episode number and an episode count
                    txt = self.aflevering.search(content)
                    if txt != None:
                        tdict['episode'] = 0 if txt.group(1) in ('', None) else int(txt.group(1))
                        tdict['titel aflevering'] = '' if txt.group(2) in ('', None) else txt.group(2).strip()

                elif ctype == 'seizoen':
                    try:
                        tdict['season'] = int(content)

                    except:
                        pass

                elif ctype == 'genre':
                    tdict['genre'] = content.title()

                # Parse persons and their roles for credit info
                elif ctype in self.config.roletrans:
                    if not self.config.roletrans[ctype] in tdict['credits']:
                        tdict['credits'][self.config.roletrans[ctype]] = []

                    content = re.sub(' en ', ' , ', content)
                    persons = content.split(',');
                    for name in persons:
                        if name.find(':') != -1:
                            name = name.split(':')[1]

                        if name.find('-') != -1:
                            name = name.split('-')[0]

                        if name.find('e.a') != -1:
                            name = name.split('e.a')[0]

                        if not self.functions.unescape(name.strip()) in tdict['credits'][self.config.roletrans[ctype]]:
                            tdict['credits'][self.config.roletrans[ctype]].append(self.functions.unescape(name.strip()))

                # Add extra properties, while at the same time checking if we do not uncheck already set properties
                elif ctype == 'kleur':
                    tdict['video']['blackwhite'] = (content.find('zwart/wit') != -1)

                elif ctype == 'bijzonderheden':
                    if self.config.write_info_files:
                        self.config.infofiles.addto_detail_list(unicode(ctype + ' = ' + content))

                    content = content.lower()
                    if tdict['video']['breedbeeld'] == False:
                        tdict['video']['breedbeeld'] = (content.find('breedbeeld') != -1)
                    if tdict['video']['HD'] == False:
                        tdict['video']['HD'] = (content.find('hd 1080i') != -1)
                    if tdict['video']['blackwhite'] == False:
                        tdict['video']['blackwhite'] = (content.find('zwart/wit') != -1)
                    if tdict['teletekst'] == False:
                        tdict['teletekst'] = (content.find('teletekst') != -1)
                    if content.find('stereo') != -1: tdict['audio'] = 'stereo'
                    if tdict['rerun'] == False:
                        tdict['rerun'] = (content.find('herhaling') != -1)

                elif ctype == 'nl-url':
                    tdict['infourl'] = content

                elif (ctype not in tdict) and (ctype.lower() not in ('zender', 'datum', 'uitzendtijd', 'titel', 'prijzen')):
                    # In unmatched cases, we still add the parsed type and content to the program details.
                    # Some of these will lead to xmltv output during the xmlefy_programs step
                    if self.config.write_info_files:
                        self.config.infofiles.addto_detail_list(unicode('new tvgids.nl detail => ' + ctype + ': ' + content))

                    tdict[ctype] = content

            except:
                self.config.log(traceback.format_exc())
                if self.config.write_info_files:
                    self.config.infofiles.write_raw_string('Error: %s at line %s\n\n' % (sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
                    self.config.infofiles.write_raw_string(strdata)

        tdict['ID'] = tdict['prog_ID'][self.proc_id]
        tdict[self.detail_check] = True
        return tdict

    def load_json_detailpage(self, tdict):
        try:
            # We first get the json url
            url = self.get_url('json_detail', id = tdict['prog_ID'][self.proc_id][3:])
            detail_data = self.config.fetch_func.get_page(url[0], 'utf-8', None, url[1],
                                                                                    ['detail', self.proc_id, tdict['channelid']], True)
            if detail_data == None or detail_data == {}:
                return None

        except:
            # if we cannot find the description page,
            # go to next in the loop
            return None

        for ctype, content in detail_data.items():
            if ctype in ('db_id', 'titel', 'datum', 'btijd', 'etijd', 'zender_id'):
                # We allready have these or we don use them
                continue

            if content == '':
                continue

            if ctype == 'genre':
                tdict['genre'] = content

            elif  ctype == 'kijkwijzer':
                for k in content:
                    if k in self.config.kijkwijzer.keys() and k not in tdict['kijkwijzer']:
                        tdict['kijkwijzer'].append(k)

            elif ctype == 'synop':
                content = re.sub('<p>', '', content)
                content = re.sub('</p>', '', content)
                content = re.sub('<br/>', '', content)
                content = re.sub('<strong>.*?</strong>', '', content)
                content = re.sub('<.*?>', '', content)
                content = re.sub('\\r\\n', '\\n', content)
                content = re.sub('\\n\\n\\n', '\\n', content)
                content = re.sub('\\n\\n', '\\n', content)
                if tdict['subgenre'].lower().strip() == content[0:len(tdict['subgenre'])].lower().strip():
                    content = content[len(tdict['subgenre'])+1:]

                if content > tdict['description']:
                    tdict['description'] = self.functions.unescape(content)

                if self.config.channels[tdict['channelid']].opt_dict['prefered_description'] == self.proc_id:
                    tdict['prefered description'] = tdict['description']

            # Parse persons and their roles for credit info
            elif ctype in self.config.roletrans:
                if not self.config.roletrans[ctype] in tdict['credits']:
                    tdict['credits'][self.config.roletrans[ctype]] = []
                persons = content.split(',');
                for name in persons:
                    if name.find(':') != -1:
                        name = name.split(':')[1]

                    if name.find('-') != -1:
                        name = name.split('-')[0]

                    if name.find('e.a') != -1:
                        name = name.split('e.a')[0]

                    if not self.functions.unescape(name.strip()) in tdict['credits'][self.config.roletrans[ctype]]:
                        tdict['credits'][self.config.roletrans[ctype]].append(self.functions.unescape(name.strip()))

            else:
                if self.config.write_info_files:
                    self.config.infofiles.addto_detail_list(unicode('new tvgids.nl json detail => ' + ctype + ': ' + content))

        tdict['ID'] = tdict['prog_ID'][self.proc_id]
        tdict[self.detail_check] = True
        return tdict

# end tvgids_JSON

class tvgidstv_HTML(tv_grab_fetch.FetchData):
    """
    Get all available days of programming for the requested channels
    from the tvgids.tv page. Based on FetchData Class
    """
    def init_channels(self):
        """ General Site layout
            <head>
            <body><div id="wrap"><div class="container"><div class="row">
                            <div class="span16">
                            <div class="span47 offset1">
                                een of meer
                                <div class="section">
                                    ...
                            <div class="span30 offset1">
                <div id="footer">

        Channel listing:
            <div class="section-title">
                contains the grouping name (Nederlands, Vlaams, ...)
            </div>
            <div class="section-content"><div class="section-item channels"><div class="section-item-content">
                        each contain groupings of up to four channels
                        <a href="/zenders/nederland-1" title="TV Gids NPO 1" class="">
                            <div class="channel-icon sprite-channel-1"></div><br />
                           <div class="channel-name ellipsis">NPO 1</div>
                        </a>
            </div></div></div>

        Program listing:
            <div class="section-content">
                contains for each program
                <a href="/tv/hart-van-nederland" title="Hart van Nederland"></a>
                <a href="/tv/hart-van-nederland/12568262" title="Hart van Nederland" class="section-item posible-progress-bar" rel="nofollow">
                    <div class="content">
                        <div class="channel-icon sprite-channel-8"></div>
                        <span class="section-item-title">
                                                                05:25
                                                                Hart van Nederland
                        </span>
                        <div class="clearfix"></div>
                    </div>
                </a>
            </div>

        Detail layout
            <div class="section-title">
                <h1>Navy NCIS</h1>
                <a class="channel-icon sprite-channel-8 pull-right" href="/zenders/net-5" title="TV Gids NET 5"></a>
            </div>
            <div class="section-content">
                <div class="section-item gray">
                    <img class="pull-right large" src="http://images.cdn.tvgids.tv/programma/square_iphone_hd_TVGiDStv_navy-ncis.jpg" alt="Navy NCIS" title="Navy NCIS" />
                    <dl class="dl-horizontal program-details">
                        <dt>Datum</dt><dd>Ma 29 december 2014 </dd>
                        <dt>Tijd</dt><dd>19:35 tot 20:30</dd>
                        <dt>    Name    </dt><dd>    Content    </dd>
                                   ...
                    </dl>
                    <div class="program-details-social">
                        ...
                    </div>
                    <p>                description                     </p>
                </div>
            </div>
        """

        # These regexes are used to get the time offset (whiche day they see as today)
        self.fetch_datecontent = re.compile('<div class="section-title select-scope">(.*?)<div class="section-content">',re.DOTALL)
        # These regexes fetch the relevant data out of thetvgids.tv pages, which then will be parsed to the ElementTree
        self.getcontent = re.compile('<div class="span47 offset1">(.*?)<div class="span30 offset1">',re.DOTALL)
        self.daydata = re.compile('<div class="section-content">(.*?)<div class="advertisement">',re.DOTALL)
        self.detaildata = re.compile('<div class="section-title">(.*?)<div class="advertisement">',re.DOTALL)

        self.init_channel_source_ids()

    def get_url(self, channel = None, offset = 0, href = None):

        tvgidstv_url = 'http://www.tvgids.tv'

        if href == None and channel == None:
            return u'%s/zenders/' % tvgidstv_url

        if href == None:
            return u'%s/zenders/%s/%s' % (tvgidstv_url, channel, offset)

        if href == '':
            return ''

        else:
            return u'%s%s' % (tvgidstv_url, self.functions.unescape(href))

    def check_date(self, page_data, channel, offset):

        # Check on the right offset for appending the date to the time. Their date switch is aroud 6:00
        dnow = datetime.datetime.now(self.site_tz).strftime('%d %b').split()
        dlast = datetime.date.fromordinal(self.current_date - 1).strftime('%d %b').split()

        if page_data == None:
            self.config.log(self.config.text('sources', 20, (channel, self.source, offset)))
            return None

        d = self.fetch_datecontent.search(page_data)
        if d == None:
            self.config.log(self.config.text('sources', 22) )
            return None

        try:
            d = d.group(1)
            d = self.functions.clean_html(d)
            htmldata = ET.fromstring( ('<div>' + d).encode('utf-8'))

        except:
            self.config.log(self.config.text('sources', 22) )
            return None

        dd = htmldata.find('div/a[@class="today "]/br')
        if dd == None:
            dd = htmldata.find('div/a[@class="today"]/br')

        if dd == None:
            dd = htmldata.find('div/a[@class="today active"]/br')

        if dd.tail == None:
            self.config.log(self.config.text('sources', 22) )
            return None

        d = dd.tail.strip().split()
        if int(dnow[0]) == int(d[0]):
            return offset

        elif int(dlast[0]) == int(d[0]):
            return offset - 1

        else:
            self.config.log(self.config.text('sources', 21, (channel, self.source, offset)))
            return None

    #~ def get_channels(self):
        #~ """
        #~ Get a list of all available channels and store these
        #~ in all_channels.
        #~ """

        #~ try:
            #~ strdata = self.config.fetch_func.get_page(self.get_url(), counter = ['base', self.proc_id])
            #~ if strdata == None:
                #~ self.fail_count += 1
                #~ return

            #~ strdata = self.functions.clean_html('<div>' + self.getcontent.search(strdata).group(1)).encode('utf-8')
            #~ htmldata = ET.fromstring(strdata)

        #~ except:
            #~ self.fail_count += 1
            #~ self.config.log([self.config.text('sources', 1, (self.source, )), traceback.format_exc()])
            #~ return 69  # EX_UNAVAILABLE

        #~ self.all_channels ={}
        #~ for changroup in htmldata.findall('div[@class="section"]'):
            #~ group_name = self.functions.empersant(changroup.findtext('div[@class="section-title"]')).strip()
            #~ for chan in changroup.findall('div[@class="section-content"]/div[@class="section-item channels"]/div[@class="section-item-content"]/a'):
                #~ chanid = chan.get('href')
                #~ if chanid == None:
                    #~ continue

                #~ chanid = re.split('/', chanid)[2]
                #~ name = self.functions.empersant(chan.findtext('div[@class="channel-name ellipsis"]'))
                #~ self.all_channels[chanid] = {}
                #~ self.all_channels[chanid]['name'] = name
                #~ self.all_channels[chanid]['group'] = 99
                #~ for id in self.config.group_order:
                    #~ if group_name == self.config.chan_groups[id]:
                        #~ self.all_channels[chanid]['group'] = id
                        #~ break

    def match_genre(self, dtext, tdict):
        if len(dtext) > 20:
            tdict['genre'] = u'overige'
            return tdict

        if dtext.lower() in self.config.source_cattrans[self.proc_id].keys():
            tdict['genre'] = self.config.source_cattrans[self.proc_id][dtext.lower()].capitalize()
            tdict['subgenre'] = dtext

        # Now we try to match the genres not found in source_cattrans[self.proc_id]
        else:
            if 'jeugd' in dtext.lower():
                tdict['genre'] = u'Jeugd'

            elif 'muziek' in dtext.lower():
                tdict['genre'] = u'Muziek'

            elif 'sport' in dtext.lower():
                tdict['genre'] = u'Sport'

            elif 'nieuws' in dtext.lower():
                tdict['genre'] = u'Nieuws/Actualiteiten'

            elif 'natuur' in dtext.lower():
                tdict['genre'] = u'Natuur'

            elif 'cultuur' in dtext.lower():
                tdict['genre'] = u'Kunst en Cultuur'

            elif 'kunst' in dtext.lower():
                tdict['genre'] = u'Kunst en Cultuur'

            elif 'wetenschap' in dtext.lower():
                tdict['genre'] = u'Wetenschap'

            elif 'medisch' in dtext.lower():
                tdict['genre'] = u'Wetenschap'

            elif 'film' in dtext.lower():
                tdict['genre'] = u'Film'

            elif 'spel' in dtext.lower():
                tdict['genre'] = u'Amusement'

            elif 'show' in dtext.lower():
                tdict['genre'] = u'Amusement'

            elif 'quiz' in dtext.lower():
                tdict['genre'] = u'Amusement'

            elif 'praatprogramma' in dtext.lower():
                tdict['genre'] = u'Magazine'

            elif 'magazine' in dtext.lower():
                tdict['genre'] = u'Magazine'

            elif 'documentair' in dtext.lower():
                tdict['genre'] = u'Informatief'

            elif 'serie' in dtext.lower():
                tdict['genre'] = u'Serie/Soap'

            elif 'soap' in dtext.lower():
                tdict['genre'] = u'Serie/Soap'

            elif 'drama' in dtext.lower():
                tdict['genre'] = u'Serie/Soap'

            elif 'thriller' in dtext.lower():
                tdict['genre'] = u'Serie/Soap'

            elif 'komedie' in dtext.lower():
                tdict['genre'] = u'Serie/Soap'

            elif 'western' in dtext.lower():
                tdict['genre'] = u'Serie/Soap'

            else:
                tdict['genre'] = u'overige'
                if self.config.write_info_files and not tdict['channelid'] in ('29', '438',):
                    self.config.infofiles.addto_detail_list(unicode('unknown tvgids.tv genre => ' + dtext + ' on ' + tdict['channel']))

            if not tdict['channelid'] in ('29', '438',):
                tdict['subgenre'] = dtext
                # And add them to source_cattrans[self.proc_id] (and tv_grab_nl_py.set for later reference
                # But not for Discovery Channel or TLC as that is garbage
                if not tdict['genre'] == u'overige':
                    self.config.new_cattrans[self.proc_id].append((dtext.lower().strip(), tdict['genre']))

        return tdict

    def load_detailpage(self, tdict):

        try:
            strdata = self.config.fetch_func.get_page(tdict['detail_url'][self.proc_id], counter = ['detail', self.proc_id, tdict['channelid']])
            if strdata == None:
                return

            strdata = self.functions.clean_html('<root><div><div class="section-title">' + self.detaildata.search(strdata).group(1) + '</root>').encode('utf-8')
        except:
            self.config.log([self.config.text('sources', 28, (tdict['detail_url'][self.proc_id], )), traceback.format_exc()])
            return None

        try:
            htmldata = ET.fromstring(strdata)

        except:
            self.config.log(self.config.text('sources', 29, (tdict['detail_url'][self.proc_id], self.source)))
            if self.config.write_info_files:
                self.config.infofiles.write_raw_string('Error: %s at line %s\n\n' % (sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
                self.config.infofiles.write_raw_string(strdata + u'\n')

            return None

        # We scan every alinea of the description
        try:
            tdict = self.filter_description(htmldata, 'div/div/div/p', tdict)
            if self.config.channels[tdict['channelid']].opt_dict['prefered_description'] == self.proc_id:
                tdict['prefered description'] = tdict['description']

        except:
            self.config.log([self.config.text('sources', 10, (tdict['detail_url'][self.proc_id], )), traceback.format_exc()])

        data = htmldata.find('div/div[@class="section-content"]')
        datatype = u''
        try:
            for d in data.find('div/dl'):
                if d.tag == 'dt':
                    datatype = self.functions.empersant(d.text.lower())

                elif d.tag == 'dd':
                    dtext = self.functions.empersant(d.text).strip() if (d.text != None) else ''
                    if datatype in ('datum', 'tijd', 'uitzending gemist', 'officiële twitter', 'twitter hashtag', 'deel-url'):
                        continue

                    elif datatype == 'genre':
                        if dtext == '':
                            continue

                        tdict = self.match_genre(dtext, tdict)

                    elif datatype == 'jaar':
                        tdict['jaar van premiere'] = dtext

                    elif datatype in self.config.roletrans:
                        tdict['credits'][self.config.roletrans[datatype]] = []
                        persons = dtext.split(',');
                        for name in persons:
                            if name.find(':') != -1:
                                name = name.split(':')[1]

                            if name.find('-') != -1:
                                name = name.split('-')[0]

                            if name.find('e.a') != -1:
                                name = name.split('e.a')[0]

                            tdict['credits'][self.config.roletrans[datatype]].append(name.strip())

                    elif datatype == 'imdb':
                        dd = d.find('a')
                        if dd == None:
                            continue

                        durl = self.functions.empersant(dd.get('href', ''))
                        if durl != '':
                            tdict['infourl'] = durl

                        stars = unicode(dd.text.strip())
                        if stars != '' and tdict['star-rating'] == '':
                            tdict['star-rating'] = stars

                    elif datatype== 'officiële website':
                        if d.find('a') == None:
                            continue

                        durl = self.functions.empersant(d.find('a').get('href', ''))
                        if durl != '':
                            tdict['infourl'] = durl

                    elif datatype== 'kijkwijzer':
                        kw_val = d.find('div')
                        if kw_val != None:
                            kw_val = kw_val.get('class').strip()

                        if kw_val != None and len(kw_val) > 27:
                            kw_val = kw_val[27:]
                            if kw_val in self.config.tvkijkwijzer.keys():
                                if self.config.tvkijkwijzer[kw_val] not in tdict['kijkwijzer']:
                                    tdict['kijkwijzer'].append(self.config.tvkijkwijzer[kw_val])

                            elif self.config.write_info_files:
                                self.config.infofiles.addto_detail_list(unicode('new tvgids.tv kijkwijzer detail => ' + datatype + '=' + kw_val))

                    else:
                        if dtext != '':
                            if self.config.write_info_files:
                                self.config.infofiles.addto_detail_list(unicode('new tvgids.tv text detail => ' + datatype + '=' + dtext))

                            tdict[datatype] = dtext

                        elif d.find('div') != None and d.find('div').get('class') != None:
                            if self.config.write_info_files:
                                self.config.infofiles.addto_detail_list(unicode('new tvgids.tv div-class detail => ' + datatype + '=' + d.find('div').get('class')))

                            tdict[datatype] = unicode(d.find('div').get('class'))

                        elif d.find('a') != None and d.find('a').get('href') != None:
                            if self.config.write_info_files:
                                self.config.infofiles.addto_detail_list(unicode('new tvgids.tv a-href detail => ' + datatype + '=' + d.find('a').get('href')))

                            tdict[datatype] = unicode(d.find('a').get('href'))

                        elif self.config.write_info_files:
                            self.config.infofiles.addto_detail_list(unicode('new tvgids.tv empty detail => ' + datatype))

                elif self.config.write_info_files:
                    self.config.infofiles.addto_detail_list(unicode('new tvgids.d-tag => ' + d.tag))

        except:
            self.config.log([self.config.text('sources', 30, (self.source, tdict['detail_url'][self.proc_id])), traceback.format_exc()])
            return

        tdict['ID'] = tdict['prog_ID'][self.proc_id]
        tdict[self.detail_check] = True

        return tdict

# end tvgidstv_HTML

class primo_HTML(tv_grab_fetch.FetchData):
    """
    Get all available days of programming for the requested channels
    from the primo.eu page. Based on FetchData Class
    """
    def init_channels(self):

        # These regexes fetch the relevant data out of the nieuwsblad.be pages, which then will be parsed to the ElementTree
        self.getmain = re.compile('<!--- HEADER SECTION -->(.*?)<!-- USER PROFILE-->',re.DOTALL)
        self.getchannelstring = re.compile('(.*?) channel channel-(.*?) channel-.*?')
        self.getprogduur = re.compile('width:(\d+)px;')

        self.init_channel_source_ids()

    def get_url(self, offset = 0, detail = None):
        base_url = 'http://www.primo.eu'
        if offset == 'channels':
            return base_url + "/Tv%20programma's%20in%20volledig%20scherm%20bekijken"

        elif detail == None and isinstance(offset, int):
            date = self.functions.get_datestamp(offset, self.site_tz)
            return '%s/tv-programs-full-view/%s/all/all' % (base_url, date)

        else:
            return u'%s/tvprograms/ajaxcallback/%s' % (base_url,  detail)

    #~ def get_channels(self):
        #~ """
        #~ Get a list of all available channels and store these
        #~ in all_channels.
        #~ """

        #~ try:
            #~ strdata = self.config.fetch_func.get_page(self.get_url('channels'), counter = ['base', self.proc_id])
            #~ if self.get_channel_lineup(strdata) == 69:
                #~ self.config.log([self.config.text('sources', 1, (self.source, ))])
                #~ return 69  # EX_UNAVAILABLE

        #~ except:
            #~ self.fail_count += 1
            #~ self.config.log([self.config.text('sources', 1, (self.source, )),traceback.format_exc()])
            #~ return 69  # EX_UNAVAILABLE

    def get_channel_lineup(self, chandata):

        try:
            if not isinstance(chandata, (str, unicode)):
                chandata = self.config.fetch_func.get_page(self.get_url(0), counter = ['base', self.proc_id])

            strdata = self.getmain.search(chandata).group(1)
            strdata = self.functions.clean_html(strdata).encode('utf-8')
            htmldata = ET.fromstring(strdata)
            htmldata = htmldata.find('div/div[@id="tvprograms-main"]/div[@id="tvprograms"]')
            for item in htmldata.findall('div[@id="program-channel-programs"]/div/div/div'):
                if item.get("style") != None:
                    continue

                chan_string = self.getchannelstring.search(item.get("class"))
                chanid = chan_string.group(1)
                cname = chan_string.group(2)
                icon_search = 'div[@id="program-channels-list-main"]/div/ul/li/div/a/img[@class="%s"]' % chanid
                icon = htmldata.find(icon_search)
                if icon == None:
                    icon = ''

                else:
                    icon = re.split('/',icon.get("src"))[-1]

                if not chanid in self.all_channels.keys():
                    self.all_channels[chanid] = {}
                    self.all_channels[chanid]['name'] = cname
                    self.all_channels[chanid]['icon'] = icon
                    self.all_channels[chanid]['icongrp'] = 9

        except:
            self.fail_count += 1
            self.config.log(traceback.format_exc())
            return 69

    def load_detailpage(self, tdict):
        try:
            strdata = self.config.fetch_func.get_page(tdict['detail_url'][self.proc_id], 'utf-8',
                                                                                counter = ['detail', self.proc_id, tdict['channelid']])
            if strdata == None:
                return

            strdata = self.functions.clean_html('<root>' + strdata + '</root>').encode('utf-8')
        except:
            self.config.log([self.config.text('sources', 28, (tdict['detail_url'][self.proc_id], )), traceback.format_exc()])
            return None

        try:
            htmldata = ET.fromstring(strdata)

        except:
            self.config.log(self.config.text('sources', 29, (tdict['detail_url'][self.proc_id], self.source)))
            if self.config.write_info_files:
                self.config.infofiles.write_raw_string('Error: %s at line %s\n\n' % (sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
                self.config.infofiles.write_raw_string(strdata + u'\n')

            return None

        try:
            genre = ''
            subgenre = ''
            for d in htmldata.findall('div/div[@class="details"]/div'):
                dlabel = d.findtext('label')[:-1].lower().strip()
                ddata = self.functions.empersant(d.findtext('span')).strip()
                if ddata in (None, '-'):
                    ddata = ''

                try:
                    if dlabel in ("programmanaam", "datum en tijd", "zender"):
                        continue

                    elif dlabel == "synopsis":
                        tdict['description'] = ddata

                    elif dlabel == "titel aflevering":
                        tdict['titel aflevering'] = ddata if ((ddata != tdict['name'])) else ''
                        tdict = self.check_title_name(tdict)

                    elif dlabel == "nr. aflevering":
                        tdict['episode'] = 0 if (ddata  == '') else int(ddata)

                    elif dlabel == "seizoen":
                        tdict['season'] = 0 if (ddata == '') else int(ddata)


                    elif dlabel in  self.config.roletrans.keys():
                        if not self.config.roletrans[dlabel] in tdict['credits']:
                            tdict['credits'][self.config.roletrans[dlabel]] = []

                        for p in d.findall('span'):
                            name = self.functions.empersant(p.text).split('(')[0].strip()
                            if not name in tdict['credits'][self.config.roletrans[dlabel]]:
                                tdict['credits'][self.config.roletrans[dlabel]].append(name)

                    elif dlabel == "jaar":
                        tdict['jaar van premiere'] = ddata

                    elif dlabel == "land":
                        #~ tdict['country']
                        ddata = re.sub('.', '', ddata).upper()
                        ddata = re.split(',', ddata)
                        for c in ddata:
                            if c in self.config.coutrytrans.values():
                                tdict['country'] = cstr
                                break

                            elif c in self.config.coutrytrans.keys():
                                tdict['country'] = self.config.coutrytrans[cstr]
                                break

                            elif self.config.write_info_files:
                                self.config.infofiles.addto_detail_list(u'new country => %s' % (c))

                    elif dlabel == "genre":
                        genre = ddata if len(ddata) > 2 else ''

                    elif dlabel == "samenvatting":
                        subgenre = ddata if len(ddata) <= 25 else ''

                    #~ elif dlabel == "rating":
                        #~ pass

                    #~ elif dlabel == "minimumleeftijd":
                        #~ pass

                    #~ elif dlabel == "":
                        #~ pass

                    elif self.config.write_info_files:
                        self.config.infofiles.addto_detail_list(u'new primo-tag => %s: %s' % (dlabel, ddata))

                except:
                    continue

            if (genre, subgenre) in self.config.source_cattrans[self.proc_id].keys():
                tdict['genre'] = self.config.source_cattrans[self.proc_id][(genre, subgenre)][0]
                if self.config.source_cattrans[self.proc_id][(genre, subgenre)][1] == '':
                    tdict['subgenre'] = subgenre

                else:
                    tdict['subgenre'] = self.config.source_cattrans[self.proc_id][(genre, subgenre)][1]

            elif genre in self.config.source_cattrans[self.proc_id].keys():
                tdict['genre'] = self.config.source_cattrans[self.proc_id][genre][0]
                if self.config.source_cattrans[self.proc_id][genre][1] == '':
                    tdict['subgenre'] = subgenre
                    if subgenre != '':
                        self.config.new_cattrans[self.proc_id][(genre, subgenre)] = (self.config.source_cattrans[self.proc_id][genre][0], subgenre)

                else:
                    tdict['subgenre'] = self.config.source_cattrans[self.proc_id][genre][1]

                if self.config.write_info_files and subgenre != '':
                    self.config.infofiles.addto_detail_list(u'new primo-subgenre => %s: %s' % (genre, subgenre))

            elif genre != '':
                tdict['genre'] = genre
                tdict['subgenre'] = subgenre
                self.config.new_cattrans[self.proc_id][(genre, subgenre)] = (genre, subgenre)
                if self.config.write_info_files and subgenre != '':
                    self.config.infofiles.addto_detail_list(u'new primo-genre => %s: %s' % (genre, subgenre))

            else:
                tdict['genre'] = 'overige'
                tdict['subgenre'] = ''

        except:
            self.config.log([self.config.text('sources', 30, (self.source, tdict['detail_url'][self.proc_id])), traceback.format_exc()])
            return

        tdict['ID'] = tdict['prog_ID'][self.proc_id]
        tdict[self.detail_check] = True

        return tdict

# end primo_HTML

