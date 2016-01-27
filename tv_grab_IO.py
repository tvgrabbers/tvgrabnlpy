#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import codecs, locale, re, os, sys, io
import traceback, datetime, smtplib
from threading import Thread
from Queue import Queue, Empty
from email.mime.text import MIMEText
from copy import deepcopy

class IO_functions():

    def __init__(self, logging = None):
        self.default_file_encoding = 'utf-8'
        self.encoding = None
        self.configversion = None
        self.logging = logging

    # end init()

    def log(self, message, log_level = 1, log_target = 3):
        if self.logging == None:
            return

        # If logging not (jet) available, make sure important messages go to the screen
        if (self.logging.log_output == None) and (log_level < 2) and (log_target & 1):
            if isinstance(message, (str, unicode)):
                sys.stderr.write(message.encode(self.logging.local_encoding, 'replace'))

            elif isinstance(message, (list ,tuple)):
                for m in message:
                    sys.stderr.write(m.encode(self.logging.local_encoding, 'replace'))

            if log_target & 2:
                self.logging.log_queue.put([message, log_level, 2])

        else:
            self.logging.log_queue.put([message, log_level, log_target])

    # end log()

    def save_oldfile(self, fle):
        """ save the old file to .old if it exists """
        if os.path.isfile(fle + '.old'):
            os.remove(fle + '.old')

        if os.path.isfile(fle):
            os.rename(fle, fle + '.old')

    # end save_oldfile()

    def open_file(self, file_name, mode = 'rb', encoding = None):
        """ Open a file and return a file handler if success """
        if encoding == None:
            encoding = self.default_file_encoding

        try:
            if 'b' in mode:
                file_handler =  io.open(file_name, mode = mode)
            else:
                file_handler =  io.open(file_name, mode = mode, encoding = encoding)

        except IOError as e:
            if e.errno == 2:
                self.log('File: "%s" not found.\n' % file_name)
            else:
                self.log('File: "%s": %s.\n' % (file_name, e.strerror))
            return None

        return file_handler

    # end open_file ()

    def get_line(self, fle, byteline, isremark = False, encoding = None):
        """
        Check line encoding and if valid return the line
        If isremark is True or False only remarks or non-remarks are returned.
        If None all are returned
        """
        if encoding == None:
            encoding = self.default_file_encoding

        try:
            line = byteline.decode(encoding)
            line = line.lstrip()
            line = line.replace('\n','')
            if isremark == None:
                return line

            if len(line) == 0:
                return False

            if isremark and line[0:1] == '#':
                return line

            if not isremark and not line[0:1] == '#':
                return line

        except UnicodeError:
            self.log('%s is not encoded in %s.\n' % (fle.name, encoding))

        return False

    # end get_line()

    def check_encoding(self, fle, encoding = None, check_version = False):
        """
        Check file encoding. Return True or False
        Encoding is stored in self.encoding
        Optionally check for a version string
        and store it in self.configversion
        """
        # regex to get the encoding string
        reconfigline = re.compile(r'#\s*(\w+):\s*(.+)')

        self.encoding = None
        self.configversion = None

        if encoding == None:
            encoding = self.default_file_encoding

        for byteline in fle.readlines():
            line = self.get_line(fle, byteline, True, self.encoding)
            if not line:
                continue

            else:
                match = reconfigline.match(line)
                if match is not None and match.group(1) == "encoding":
                    encoding = match.group(2)

                    try:
                        codecs.getencoder(encoding)
                        self.encoding = encoding

                    except LookupError:
                        self.log('%s has invalid encoding %s.\n' % (fle.name, encoding))
                        return False

                    if (not check_version) or self.configversion != None:
                        return True

                    continue

                elif match is not None and match.group(1) == "configversion":
                    self.configversion = float(match.group(2))
                    if self.encoding != None:
                        return True

                continue

        if check_version and self.configversion == None:
            fle.seek(0,0)
            for byteline in fle.readlines():
                line = self.get_line(fle, byteline, False, self.encoding)
                if not line:
                    continue

                else:
                    config_title = re.search('[(.*?)]', line)
                    if config_title != None:
                        self.configversion = float(2.0)
                        break

            else:
                self.configversion = float(1.0)

        if self.encoding == None:
            return False

        else:
            return True

    # end check_encoding()


# end IO_functions()

class Logging(Thread):
    """The tread that manages all logging.
    You put the messages in a queue that is sampled.
    So logging can start after the queue is opend when this class is called"""
    def __init__(self):
        Thread.__init__(self)
        # Version info as returned by the version function
        self.name ='tv_grab_IO_py'
        self.major = 1
        self.minor = 0
        self.patch = 0
        self.patchdate = u'20160124'
        self.alfa = True
        self.beta = True

        self.quit = False
        self.log_dict = {}
        self.log_dict['log_level'] = 175
        self.log_dict['quiet'] = False
        self.log_dict['graphic_frontend'] = False
        self.log_queue = Queue()
        self.log_output = None
        self.log_string = []
        try:
            codecs.lookup(locale.getpreferredencoding())
            self.local_encoding = locale.getpreferredencoding()

        except LookupError:
            if os.name == 'nt':
                self.local_encoding = 'windows-1252'

            else:
                self.local_encoding = 'utf-8'

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

    def init_run(self, output = None, log_dict = {}):
        self.log_output = output
        if isinstance(log_dict, dict):
            for k, v in self.log_dict.items():
                if not k in log_dict.keys():
                    log_dict[k] = v

            self.log_dict = log_dict

    # end init_run()

    def run(self):
        while True:
            try:
                if self.quit and self.log_queue.empty():
                    if self.log_dict['mail_log']:
                        self.send_mail(self.log_string, self.log_dict['mail_log_address'])

                    return(0)

                try:
                    message = self.log_queue.get(True, 5)

                except Empty:
                    continue

                if message == None:
                    continue

                if isinstance(message, (str, unicode)):
                    if message == 'Closing down\n':
                        self.quit=True

                    self.writelog(message)
                    continue

                elif isinstance(message, (list ,tuple)):
                    llevel = message[1] if len(message) > 1 else 1
                    ltarget = message[2] if len(message) > 2 else 3
                    if message[0] == None:
                        continue

                    if message[0] == 'Closing down\n':
                        self.quit = True

                    if isinstance(message[0], (str, unicode)):
                        self.writelog(message[0], llevel, ltarget)
                        continue

                    elif isinstance(message[0], (list, tuple)):
                        for m in message[0]:
                            if isinstance(m, (str, unicode)):
                                self.writelog(m, llevel, ltarget)

                        continue

                self.writelog('Unrecognized log-message: %s of type %s\n' % (message, type(message)))

            except:
                sys.stderr.write((self.now() + 'An error ocured while logging!\n').encode(self.local_encoding, 'replace'))
                traceback.print_exc()

    # end run()

    def now(self):
         return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z') + ': '

    # end now()

    def writelog(self, message, log_level = 1, log_target = 3):
        try:
            if message == None:
                return

            # If output is not yet available
            if (self.log_output == None) and (log_target & 1):
                sys.stderr.write(('Error writing to log. Not (yet) available?\n').encode(self.local_encoding, 'replace'))
                sys.stderr.write(message.encode(self.local_encoding, 'replace'))
                return

            # Log to the Frontend. To set-up later.
            if self.log_dict['graphic_frontend']:
                pass

            # Log to the screen
            elif log_level == 0 or ((not self.log_dict['quiet']) and (log_level & self.log_dict['log_level']) and (log_target & 1)):
                sys.stderr.write(message.encode(self.local_encoding, 'replace'))

            # Log to the log-file
            if (log_level == 0 or ((log_level & self.log_dict['log_level']) and (log_target & 2))) and self.log_output != None:
                if '\n' in message:
                    message = re.split('\n', message)

                    for i in range(len(message)):
                        if message[i] != '':
                            self.log_output.write(self.now() + message[i] + '\n')
                            if self.log_dict['mail_log']:
                                self.log_string.append(self.now() + message[i] + '\n')

                else:
                    self.log_output.write(self.now() + message + '\n')
                    if self.log_dict['mail_log']:
                        self.log_string.append(self.now() + message + '\n')

                self.log_output.flush()

        except:
            sys.stderr.write((self.now() + 'An error ocured while logging!\n').encode(self.local_encoding, 'replace'))
            traceback.print_exc()

    # end writelog()

    def send_mail(self, message, mail_address, subject=None):
        try:
            if isinstance(message, (list,tuple)):
                msg = u''.join(message)

            elif isinstance(message, (str,unicode)):
                msg = unicode(message)

            else:
                return

            if subject == None:
                subject = 'Tv_grab_nl_py %s' % datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

            msg = MIMEText(msg, _charset='utf-8')
            msg['Subject'] = subject
            msg['From'] = mail_address
            msg['To'] = mail_address
            try:
                mail = smtplib.SMTP(self.log_dict['mailserver'], self.log_dict['mailport'])

            except:
                sys.stderr.write(('Error mailing message: %s\n' % sys.exc_info()[1]).encode(logging.local_encoding, 'replace'))
                return

            mail.sendmail(mail_address, mail_address, msg.as_string())

        except:
            sys.stderr.write('Error mailing message\n'.encode(logging.local_encoding, 'replace'))
            sys.stderr.write(traceback.format_exc())

        mail.quit()

    # send_mail()

# end Logging

# used for gathering extra info to better the code
class InfoFiles:
    """used for gathering extra info to better the code"""
    def __init__(self, logging, opt_dict, xmltv_dir, write_info_files = True):

        self.logging = logging
        self.IO_func = IO_functions(logging)
        self.write_info_files = write_info_files
        self.opt_dict = opt_dict
        self.xmltv_dir = xmltv_dir
        self.info_lock = Lock()
        self.cache_return = Queue()
        self.detail_list = []
        self.raw_list = []
        self.raw_string = ''
        self.fetch_strings = {}
        self.lineup_changes = []
        self.url_failure = []
        if self.write_info_files:
            self.fetch_list = self.IO_func.open_file(self.xmltv_dir + '/fetched-programs','w')
            self.raw_output =  self.IO_func.open_file(self.xmltv_dir+'/raw_output', 'w')

    def check_new_channels(self, source, source_channels, empty_channels):
        if not self.write_info_files:
            return

        if source.all_channels == {}:
            source.get_channels()

        for chan_scid, channel in source.all_channels.items():
            if not (chan_scid in source_channels[source.proc_id].values() or chan_scid in empty_channels[source.proc_id]):
                self.lineup_changes.append( u'New channel on %s => %s (%s)\n' % (source.source, chan_scid, channel['name']))

        for chanid, chan_scid in source_channels[source.proc_id].items():
            if not (chan_scid in source.all_channels.keys() or chan_scid in empty_channels[source.proc_id]):
                self.lineup_changes.append( u'Removed channel on %s => %s (%s)\n' % (source.source, chan_scid, chanid))

        for chan_scid in empty_channels[source.proc_id]:
            if not chan_scid in source.all_channels.keys():
                self.lineup_changes.append( u"Empty channelID %s on %s doesn't exist\n" % (chan_scid, source.source))

    def add_url_failure(self, string):
        self.url_failure.append(string)

    def addto_raw_string(self, string):
        if self.write_info_files:
            with self.info_lock:
                self.raw_string = unicode(self.raw_string + string)

    def write_raw_string(self, string):
        if self.write_info_files:
            with self.info_lock:
                self.raw_string = unicode(self.raw_string + string)
                self.raw_output.write(self.raw_string + u'\n')
                self.raw_string = ''

    def addto_raw_list(self, raw_data = None):

        if self.write_info_files:
            with self.info_lock:
                if raw_data == None:
                    self.raw_list.append(self.raw_string)
                    self.raw_string = ''
                else:
                    self.raw_list.append(raw_data)

    def write_raw_list(self, raw_data = None):

        if (not self.write_info_files) or (self.raw_output == None):
            return

        with self.info_lock:
            if raw_data != None:
                self.raw_list.append(raw_data)

            self.raw_list.sort()
            for i in self.raw_list:
                i = re.sub('\n +?\n', '\n', i)
                i = re.sub('\n+?', '\n', i)
                if i.strip() == '\n':
                    continue

                self.raw_output.write(i + u'\n')

            self.raw_list = []
            self.raw_string = ''

    def addto_detail_list(self, detail_data):

        if self.write_info_files:
            with self.info_lock:
                self.detail_list.append(detail_data)

    def write_fetch_list(self, programs, chanid, source, chan_name = '', sid = None, ismerge = False):

        if (not self.write_info_files) or (self.fetch_list == None):
            return

        with self.info_lock:
            plist = deepcopy(programs)
            if not chanid in  self.fetch_strings:
                 self.fetch_strings[chanid] = {}

            if not source in  self.fetch_strings[chanid]:
                self.fetch_strings[chanid][source] = ''

            if ismerge:
                self.fetch_strings[chanid][source] += u'(%3.0f) merging channel: %s from: %s\n' % \
                    (len(plist), chan_name, source)

            else:
                self.fetch_strings[chanid][source] += u'(%3.0f) channel: %s from: %s\n' % \
                    (len(plist), chan_name, source)

            plist.sort(key=lambda program: (program['start-time']))

            for tdict in plist:
                if sid == None:
                    sid = tdict['ID']

                elif sid in tdict['prog_ID']:
                    sid = tdict['prog_ID'][sid]

                self.fetch_strings[chanid][source] += u'  %s-%s: [%s][%s] %s: %s [%s/%s]\n' % (\
                                tdict['start-time'].strftime('%d %b %H:%M'), \
                                tdict['stop-time'].strftime('%H:%M'), \
                                sid.rjust(15), tdict['genre'][0:10].rjust(10), \
                                tdict['name'], tdict['titel aflevering'], \
                                tdict['season'], tdict['episode'])

            if ismerge: self.fetch_strings[chanid][source] += u'#\n'

    def write_xmloutput(self, xml):

        if self.write_info_files:
            xml_output =self.IO_func.open_file(self.xmltv_dir+'/xml_output', 'w')
            if xml_output == None:
                return

            xml_output.write(xml)
            xml_output.close()

    def close(self, channels, combined_channels, sources):
        if not self.write_info_files:
            return

        if self.opt_dict['mail_info_address'] == None:
            self.opt_dict['mail_info_address'] = self.opt_dict['mail_log_address']

        if self.opt_dict['mail_log'] and len(self.lineup_changes) > 0:
            self.logging.send_mail(self.lineup_changes, self.opt_dict['mail_info_address'], 'Tv_grab_nl_py lineup changes')

        if self.opt_dict['mail_log'] and len(self.url_failure) > 0:
            self.logging.send_mail(self.url_failure, self.opt_dict['mail_info_address'], 'Tv_grab_nl_py url failures')

        if self.fetch_list != None:
            for chanid in channels.keys():
                if (channels[chanid].active or channels[chanid].is_child) and chanid in self.fetch_strings:
                    for s in channels[chanid].merge_order:
                        if sources[s].source in self.fetch_strings[chanid].keys():
                            self.fetch_list.write(self.fetch_strings[chanid][sources[s].source])

                    if chanid in combined_channels.keys():
                        for c in combined_channels[chanid]:
                            if c['chanid'] in channels and channels[c['chanid']].chan_name in self.fetch_strings[chanid]:
                                self.fetch_list.write(self.fetch_strings[chanid][channels[c['chanid']].chan_name])


            self.fetch_list.close()

        if self.raw_output != None:
            self.raw_output.close()

        if len(self.detail_list) > 0:
            f = self.IO_func.open_file(self.xmltv_dir+'/detail_output')
            if (f != None):
                f.seek(0,0)
                for byteline in f.readlines():
                    line = self.IO_func.get_line(f, byteline, False)
                    if line:
                        self.detail_list.append(line)

                f.close()

            f = self.IO_func.open_file(self.xmltv_dir+'/detail_output', 'w')
            if (f != None):
                ds = set(self.detail_list)
                ds = set(self.detail_list)
                tmp_list = []
                tmp_list.extend(ds)
                tmp_list.sort()
                for i in tmp_list:
                    f.write(u'%s\n' % i)

                f.close()

# end InfoFiles

