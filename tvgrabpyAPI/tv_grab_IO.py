#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Python 3 compatibility
from __future__ import unicode_literals
# from __future__ import print_function

import codecs, locale, re, os, sys, io, shutil, difflib
import traceback, smtplib, sqlite3
import datetime, time, pytz, copy
import tv_grab_channel
from threading import Thread, Lock, RLock
from Queue import Queue, Empty
from email.mime.text import MIMEText
from xml.sax import saxutils


class Functions():
    """Some general IO functions"""

    def __init__(self, config):
        self.default_file_encoding = 'utf-8'
        self.encoding = None
        self.configversion = None
        self.config = config
        self.logging = config.logging

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

    def save_oldfile(self, fle, save_ext='old'):
        """ save the old file to .old if it exists """
        save_fle = '%s.%s' % (fle, save_ext)
        if os.path.isfile(save_fle):
            os.remove(save_fle)

        if os.path.isfile(fle):
            os.rename(fle, save_fle)

    # end save_oldfile()

    def restore_oldfile(self, fle, save_ext='old'):
        """ restore the old file from .old if it exists """
        save_fle = '%s.%s' % (fle, save_ext)
        if os.path.isfile(fle):
            os.remove(fle)

        if os.path.isfile(save_fle):
            os.rename(save_fle, fle)

    # end save_oldfile()

    def open_file(self, file_name, mode = 'rb', encoding = None):
        """ Open a file and return a file handler if success """
        if encoding == None:
            encoding = self.default_file_encoding

        if 'r' in mode and not (os.path.isfile(file_name) and os.access(file_name, os.R_OK)):
            self.log(self.config.text('IO', 1, (file_name, )))
            return None

        if ('a' in mode or 'w' in mode):
            if os.path.isfile(file_name) and not os.access(file_name, os.W_OK):
                self.log(self.config.text('IO', 1, (file_name, )))
                return None

        try:
            if 'b' in mode:
                file_handler =  io.open(file_name, mode = mode)
            else:
                file_handler =  io.open(file_name, mode = mode, encoding = encoding)

        except IOError as e:
            if e.errno == 2:
                self.log(self.config.text('IO', 1, (file_name, )))
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
            self.log(self.config.text('IO', 2, (fle.name, encoding)))

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
                        self.log(self.config.text('IO', 3, (fle.name, encoding)))
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


# end Functions()

class Logging(Thread):
    """
    The tread that manages all logging.
    You put the messages in a queue that is sampled.
    So logging can start after the queue is opend when this class is called
    Before the fle to log to is known
    """
    def __init__(self, config):
        Thread.__init__(self)
        self.quit = False
        self.config = config
        self.functions = Functions(config)
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

    def run(self):
        self.log_output = self.config.log_output
        self.fatal_error = [self.config.text('IO', 4), \
                '     %s\n' % (self.config.opt_dict['config_file']), \
                '     %s\n' % (self.config.opt_dict['log_file'])]

        while True:
            try:
                if self.quit and self.log_queue.empty():
                    # We close down after mailing the log
                    if self.config.opt_dict['mail_log']:
                        self.send_mail(self.log_string, self.config.opt_dict['mail_log_address'])

                    return(0)

                try:
                    message = self.log_queue.get(True, 5)

                except Empty:
                    continue

                if message == None:
                    continue

                elif isinstance(message, dict) and 'fatal' in message:
                    # A fatal Error has been received, after logging we send all threads the quit signal
                    if 'name'in message and message['name'] != None:
                        mm =  ['\n', self.config.text('IO', 21, (message['name'], ))]

                    else:
                        mm = ['\n', self.config.text('IO', 22)]

                    if isinstance(message['fatal'], (str, unicode)):
                        mm.append(message['fatal'])

                    elif isinstance(message['fatal'], (list, tuple)):
                        mm.extend(list(message['fatal']))

                    mm.extend(self.fatal_error)
                    for m in mm:
                        if isinstance(m, (str, unicode)):
                            self.writelog(m, 0)

                    for t in self.config.threads:
                        if t.is_alive():
                            if t.thread_type in ('ttvdb', 'source'):
                                t.detail_request.put({'task': 'quit'})

                            if t.thread_type == 'cache':
                                t.cache_request.put({'task': 'quit'})

                            if t.thread_type in ('source', 'channel'):
                                t.cache_return.put('quit')

                            t.quit = True

                    self.log_queue.put('Closing down\n')
                    continue

                elif isinstance(message, (str, unicode)):
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

                self.writelog(self.config.text('IO', 5, (message, type(message))))

            except:
                sys.stderr.write((self.now() + u'An error ocured while logging!\n').encode(self.local_encoding, 'replace'))
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
            if self.config.opt_dict['graphic_frontend']:
                pass

            # Log to the screen
            elif log_level == 0 or ((not self.config.opt_dict['quiet']) and (log_level & self.config.opt_dict['log_level']) and (log_target & 1)):
                sys.stderr.write(message.encode(self.local_encoding, 'replace'))

            # Log to the log-file
            if (log_level == 0 or ((log_level & self.config.opt_dict['log_level']) and (log_target & 2))) and self.log_output != None:
                if '\n' in message:
                    message = re.split('\n', message)

                    for i in range(len(message)):
                        if message[i] != '':
                            self.log_output.write(self.now() + message[i] + u'\n')
                            if self.config.opt_dict['mail_log']:
                                self.log_string.append(self.now() + message[i] + u'\n')

                else:
                    self.log_output.write(self.now() + message + u'\n')
                    if self.config.opt_dict['mail_log']:
                        self.log_string.append(self.now() + message + u'\n')

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
                mail = smtplib.SMTP(self.config.opt_dict['mailserver'], self.config.opt_dict['mailport'])

            except:
                sys.stderr.write(('Error mailing message: %s\n' % sys.exc_info()[1]).encode(self.local_encoding, 'replace'))
                return

            mail.sendmail(mail_address, mail_address, msg.as_string())

        except smtplib.SMTPRecipientsRefused:
            sys.stderr.write(('The mailserver at %s refused the message\n' % self.config.opt_dict['mailserver']).encode(self.local_encoding, 'replace'))

        except:
            sys.stderr.write('Error mailing message\n'.encode(self.local_encoding, 'replace'))
            sys.stderr.write(traceback.format_exc())

        mail.quit()

    # send_mail()

# end Logging

class ProgramCache(Thread):
    """
    A cache to hold program name and category info.
    TVgids and others stores the detail for each program on a separate
    URL with an (apparently unique) ID. This cache stores the fetched info
    with the ID. New fetches will use the cached info instead of doing an
    (expensive) page fetch.
    """
    def __init__(self, config, filename=None):
        Thread.__init__(self)
        """
        Create a new ProgramCache object, optionally from file
        """
        self.config = config
        self.functions = self.config.IO_func
        self.ID_list = {}
        self.url_list = {}
        for key, s in self.config.channelsource.items():
            self.ID_list[s.detail_id] = key
            self.url_list[s.detail_url] = key

        self.config.fetch_func.checkout_program_dict()
        self.field_list = ['genre', 'rating']
        self.field_list.extend(self.config.fetch_func.text_values)
        self.field_list.extend(self.config.fetch_func.date_values)
        self.field_list.extend(self.config.fetch_func.datetime_values)
        self.field_list.extend(self.config.fetch_func.bool_values)
        self.field_list.extend(self.config.fetch_func.num_values)
        self.field_list.extend(self.config.fetch_func.video_values)
        sqlite3.register_adapter(list, self.adapt_kw)
        sqlite3.register_converter(str('rating'), self.convert_kw)
        sqlite3.register_adapter(list, self.adapt_list)
        sqlite3.register_converter(str('listing'), self.convert_list)
        sqlite3.register_adapter(bool, self.adapt_bool)
        sqlite3.register_converter(str('boolean'), self.convert_bool)
        sqlite3.register_adapter(datetime.datetime, self.adapt_datetime)
        sqlite3.register_converter(str('datetime'), self.convert_datetime)
        sqlite3.register_adapter(datetime.date, self.adapt_date)
        sqlite3.register_converter(str('date'), self.convert_date)

        # where we store our info
        self.filename  = filename
        self.quit = False
        self.thread_type = 'cache'
        self.cache_request = Queue()
        self.config.threads.append(self)
        self.config.queues['cache'] = self.cache_request

    def adapt_kw(self, val):
        ret_val = ''
        for k in val:
            ret_val += k

        return ret_val

    def convert_kw(self, val):
        ret_val = []
        for k in val:
            ret_val.append(k)

        return ret_val

    def adapt_list(self, val):
        if isinstance(val, (str, unicode)):
            return val

        if not isinstance(val, (list, tuple, set)) or len(val) == 0:
            return ''

        ret_val = ''
        for k in val:
            ret_val += ';%s' % k

        return ret_val[1:]

    def convert_list(self, val):
        ret_val = []
        val = val.split(';')
        for k in val:
            ret_val.append(k)

        return ret_val

    def adapt_bool(self, val):
        if val:
            return 'True'

        elif val == None:
            return 'None'

        else:
            return 'False'

    def convert_bool(self, val):
        if val == 'True':
            return True

        elif val == 'False':
            return False

        else:
            return None

    def adapt_datetime(self, val):
        if isinstance(val, (datetime.datetime)):
            if val.tzinfo == self.config.utc_tz:
                return time.mktime(val.timetuple())*1000

            else:
                return time.mktime(val.astimezone(self.config.utc_tz).timetuple())*1000

        else:
            return 0

    def convert_datetime(self, val):
        try:
            if int(val) == 0 or val == '':
                return None

            if len(val) < 10:
                return datetime.date.fromordinal(int(val))

            return datetime.datetime.fromtimestamp(int(val)/1000, self.config.utc_tz)

        except:
            return None

    def adapt_date(self, val):
        if isinstance(val, (datetime.date)):
            return val.toordinal()

        return 0

    def convert_date(self, val):
        try:
            if int(val) == 0 or val == '':
                return None

            return datetime.date.fromordinal(int(val))

        except:
            return None

    def run(self):
        self.open_db()
        try:
            while True:
                if self.quit and self.cache_request.empty():
                    self.pconn.close()
                    break

                try:
                    crequest = self.cache_request.get(True, 5)

                except Empty:
                    continue

                if (not isinstance(crequest, dict)) or (not 'task' in crequest):
                    continue

                if crequest['task'] == 'query_id':
                    if not 'parent' in crequest:
                        continue

                    if self.filename == None:
                        qanswer = None

                    else:
                        for t in ('program', 'ttvdb', 'ttvdb_alias', 'tdate'):
                            if t in crequest:
                                qanswer = self.query_id(t, crequest[t])
                                break

                            else:
                                qanswer = None

                    crequest['parent'].cache_return.put(qanswer)
                    continue

                if crequest['task'] == 'query':
                    if not 'parent' in crequest:
                        continue

                    if self.filename == None:
                        qanswer = None

                    else:
                        for t in ('pid', 'ttvdb', 'ttvdb_aliasses', 'ttvdb_langs', 'ep_by_id', 'ep_by_title', 'icon', 'chan_group', 'chan_scid'):
                            if t in crequest:
                                qanswer = self.query(t, crequest[t])
                                break

                            else:
                                qanswer = None

                    crequest['parent'].cache_return.put(qanswer)
                    continue

                if self.filename == None:
                    continue

                if crequest['task'] == 'add':
                    for t in ('program', 'channelsource', 'channel', 'icon', 'ttvdb', 'ttvdb_alias', 'ttvdb_lang', 'episode'):
                        if t in crequest:
                            self.add(t, crequest[t])
                            continue

                if crequest['task'] == 'delete':
                    for t in ('ttvdb', ):
                        if t in crequest:
                            self.delete(t, crequest[t])
                            continue

                if crequest['task'] == 'clear':
                    if 'table' in crequest:
                        for t in crequest['table']:
                            self.clear(t)

                    else:
                        self.clear('programs')
                        self.clear('credits')

                    continue

                if crequest['task'] == 'clean':
                    self.clean()
                    continue

                if crequest['task'] == 'quit':
                    self.quit = True
                    continue

        except:
            self.config.queues['log'].put({'fatal': [traceback.format_exc(), '\n'], 'name': 'ProgramCache'})
            self.ready = True
            return(98)

    def open_db(self):
        if self.filename == None:
            self.functions.log(self.config.text('IO', 6))
            return

        if os.path.isfile(self.filename) and \
          (datetime.date.today() - datetime.date.fromtimestamp(os.stat(self.filename).st_mtime)).days > 14:
            os.remove(self.filename)

        if os.path.isfile(self.filename +'.db'):
            # There is already a db file
            self.load_db()
            return

        # Check the directory
        if not os.path.exists(os.path.dirname(self.filename)):
            try:
                os.makedirs(os.path.dirname(self.filename), 0755)
                self.load_db
                return

            except:
                self.functions.log(self.config.text('IO', 7))
                self.filename = None
                return

        self.load_db()
        # Check for an old cache file to convert
        if os.path.isfile(self.filename +'.tmp'):
            # Trying to recover a backup cache file
            if not os.path.isfile(self.filename) or os.stat(self.filename +'.tmp').st_size > os.stat(self.filename).st_size:
                try:
                    self.functions.restore_oldfile(self.filename, 'tmp')

                except:
                    pass

            else:
                try:
                    os.remove(self.filename + '.tmp')

                except:
                    pass

        if os.path.isfile(self.filename) and \
          (datetime.date.today() - datetime.date.fromtimestamp(os.stat(self.filename).st_mtime)).days < 14:
            self.load_old()

    def load_db(self):
        """
        Opens a sqlite cache db
        """
        for try_loading in (0,1):
            try:
                self.pconn = sqlite3.connect(database=self.filename + '.db', isolation_level=None, detect_types=sqlite3.PARSE_DECLTYPES)
                self.pconn.row_factory = sqlite3.Row
                pcursor = self.pconn.cursor()
                self.functions.log(self.config.text('IO', 8))
                pcursor.execute("PRAGMA main.integrity_check")
                if pcursor.fetchone()[0] == 'ok':
                    # Making a backup copy
                    self.pconn.close()
                    if os.path.isfile(self.filename +'.db.bak'):
                        os.remove(self.filename + '.db.bak')

                    shutil.copy(self.filename + '.db', self.filename + '.db.bak')
                    self.pconn = sqlite3.connect(database=self.filename + '.db', isolation_level=None, detect_types=sqlite3.PARSE_DECLTYPES)
                    self.pconn.row_factory = sqlite3.Row
                    pcursor = self.pconn.cursor()
                    break

                if try_loading == 0:
                    self.functions.log([self.config.text('IO', 9, (self.filename, )), self.config.text('IO', 10)])

            except:
                if try_loading == 0:
                    self.functions.log([self.config.text('IO', 9, (self.filename, )), self.config.text('IO', 10), traceback.format_exc()])

            try:
                self.pconn.close()

            except:
                pass

            try:
                if os.path.isfile(self.filename +'.db'):
                    os.remove(self.filename + '.db')

                if os.path.isfile(self.filename +'.db.bak'):
                    if try_loading == 0:
                        shutil.copy(self.filename + '.db.bak', self.filename + '.db')

                    else:
                        os.remove(self.filename + '.db.bak')

            except:
                self.functions.log([self.config.text('IO', 11, (self.filename, )), traceback.format_exc(), self.config.text('IO', 12)])
                self.filename = None
                self.config.opt_dict['disable_ttvdb'] = True
                return

        try:
            pcursor.execute("PRAGMA main.synchronous = OFF")
            pcursor.execute("PRAGMA main.temp_store = MEMORY")
            for t in ( 'programs',  'credits', 'channels', 'channelsource', 'iconsource', 'ttvdb', 'ttvdb_alias', 'episodes'):
                # (cid, Name, Type, Nullable = 0, Default, Pri_key index)
                pcursor.execute("PRAGMA main.table_info('%s')" % (t,))
                trows = pcursor.fetchall()
                if len(trows) == 0:
                    # Table does not exist
                    self.create_table(t)
                    continue

                else:
                    clist = {}
                    for r in trows:
                        clist[r[1].lower()] = r

                    self.check_collumns(t, clist)

                self.check_indexes(t)

            for a, t in self.config.ttvdb_aliasses.items():
                if not self.query_id('ttvdb_alias', {'title': t, 'alias': a}):
                    self.add('ttvdb_alias', {'title': t, 'alias': a})

        except:
            self.functions.log([self.config.text('IO', 11, (self.filename, )), traceback.format_exc(), self.config.text('IO', 12)])
            self.filename = None
            self.config.opt_dict['disable_ttvdb'] = True

    def create_table(self, table):
        if table == 'programs':
            create_string = u"CREATE TABLE IF NOT EXISTS %s ('pid' TEXT PRIMARY KEY ON CONFLICT REPLACE, 'genre' TEXT DEFAULT 'overige'" % table

            for key in self.config.key_values['text']:
                if key in ( "prog_ID","detail_url"):
                    continue

                create_string = u"%s, '%s' TEXT DEFAULT NULL" % (create_string, key)
            #~ 'channelid', 'source', 'channel', 'unixtime', 'prefered description', 'merge-source', 'infourl',

            #~ for key in self.config.channelsource.keys():
                #~ create_string = u"%s, '%s' TEXT DEFAULT ''" % (create_string, self.config.channelsource[key].detail_id.lower())
                #~ create_string = u"%s, '%s' TEXT DEFAULT ''" % (create_string, self.config.channelsource[key].detail_url.lower())

            for key in self.config.key_values['datetime']:
                create_string = u"%s, '%s' datetime" % (create_string, key)

            for key in self.config.key_values['date']:
                create_string = u"%s, '%s' date DEFAULT NULL" % (create_string, key)

            for key in self.config.key_values['bool']:
                create_string = u"%s, '%s' boolean DEFAULT NULL" % (create_string, key)

            for key in self.config.key_values['int']:
                create_string = u"%s, '%s' INTEGER DEFAULT NULL" % (create_string, key)

            for key in self.config.key_values['video']:
                create_string = u"%s, '%s' boolean DEFAULT NULL" % (create_string, key)

            for key in self.config.key_values['list']:
                create_string = u"%s, '%s' rating DEFAULT NULL)" % (create_string, key)

        elif table == 'credits':
            create_string = u"CREATE TABLE IF NOT EXISTS %s " % table
            create_string += u"('pid' TEXT"
            create_string += u", 'title' TEXT"
            create_string += u", 'name' TEXT"
            create_string += u", 'role' TEXT DEFAULT NULL"
            create_string += u", PRIMARY KEY ('pid', 'title', 'name') ON CONFLICT REPLACE)"
            if (sqlite3.sqlite_version_info >= (3, 8, 2)):
                create_string += u" WITHOUT ROWID"


        elif table == 'ttvdb':
            create_string = u"CREATE TABLE IF NOT EXISTS %s "  % table
            create_string += u"('title' TEXT PRIMARY KEY ON CONFLICT REPLACE"
            create_string += u", 'tid' INTEGER"
            create_string += u", 'langs' listing"
            create_string += u", 'tdate' date)"
            if (sqlite3.sqlite_version_info >= (3, 8, 2)):
                create_string += u" WITHOUT ROWID"


        elif table == 'ttvdb_alias':
            create_string = u"CREATE TABLE IF NOT EXISTS %s "  % table
            create_string += u"('alias' TEXT PRIMARY KEY ON CONFLICT REPLACE"
            create_string += u", 'title' TEXT)"
            if (sqlite3.sqlite_version_info >= (3, 8, 2)):
                create_string += u" WITHOUT ROWID"

        elif table == 'episodes':
            create_string = u"CREATE TABLE IF NOT EXISTS %s "  % table
            create_string += u"('tid' INTEGER"
            create_string += u", 'sid' INTEGER"
            create_string += u", 'eid' INTEGER"
            create_string += u", 'lang' TEXT DEFAULT 'nl'"
            create_string += u", 'title' TEXT"
            create_string += u", 'description' TEXT"
            create_string += u", 'airdate' date"
            create_string += u", PRIMARY KEY ('tid', 'sid', 'eid', 'lang') ON CONFLICT REPLACE)"
            if (sqlite3.sqlite_version_info >= (3, 8, 2)):
                create_string += u" WITHOUT ROWID"


        elif table == 'channels':
            create_string = u"CREATE TABLE IF NOT EXISTS %s " % table
            create_string += u"('chanid' TEXT PRIMARY KEY ON CONFLICT REPLACE"
            create_string += u", 'cgroup' INTEGER DEFAULT 10"
            create_string += u", 'name' TEXT)"

        elif table == 'channelsource':
            create_string = u"CREATE TABLE IF NOT EXISTS %s " % table
            create_string += u"( 'chanid' TEXT"
            create_string += u", 'sourceid' INTEGER"
            create_string += u", 'scid' TEXT"
            create_string += u", 'name' TEXT"
            create_string += u", 'hd' boolean DEFAULT 'False'"
            create_string += u", 'emptycount' INTEGER DEFAULT 0"
            create_string += u", PRIMARY KEY ('chanid', 'sourceid') ON CONFLICT REPLACE)"
            if (sqlite3.sqlite_version_info >= (3, 8, 2)):
                create_string += u" WITHOUT ROWID"


        elif table == 'iconsource':
            create_string = u"CREATE TABLE IF NOT EXISTS %s " % table
            create_string += u"('chanid' TEXT"
            create_string += u", 'sourceid' INTEGER"
            create_string += u", 'icon' TEXT"
            create_string += u", PRIMARY KEY ('chanid', 'sourceid') ON CONFLICT REPLACE)"
            if (sqlite3.sqlite_version_info >= (3, 8, 2)):
                create_string += u" WITHOUT ROWID"

        else:
            return

        with self.pconn:
            try:
                self.pconn.execute(create_string)

            except:
                self.functions.log([self.config.text('IO', 13, (table, )), traceback.format_exc()])

    def check_collumns(self, table, clist):
        def add_collumn(table, collumn):
            try:
                with self.pconn:
                    self.pconn.execute(u"ALTER TABLE %s ADD %s" % (table, collumn))

            except:
                self.functions.log(self.config.text('IO', 14, (table, collumn)))

        def drop_table(table):
            with self.pconn:
                self.pconn.execute(u"DROP TABLE IF EXISTS %s" % (table,))

        if table == 'programs':
            if 'pid' not in clist.keys():
                drop_table(table)
                self.create_table(table)
                return

            if 'genre' not in clist.keys():
                add_collumn(table, u"'genre' TEXT DEFAULT 'overige'")

            for c in self.config.key_values['text']:
                if c in ( "prog_ID","detail_url"):
                    continue

                if c.lower() not in clist.keys():
                    add_collumn(table, u"'%s' TEXT DEFAULT NULL" % c)

            #~ for key in self.config.channelsource.keys():
                #~ if self.config.channelsource[key].detail_id.lower() not in clist.keys():
                    #~ add_collumn(table, u"'%s' TEXT DEFAULT ''" % self.config.channelsource[key].detail_id.lower())

                #~ if self.config.channelsource[key].detail_url.lower() not in clist.keys():
                    #~ add_collumn(table, u"'%s' TEXT DEFAULT ''" % self.config.channelsource[key].detail_url.lower())

            for c in self.config.key_values['datetime']:
                if c.lower() not in clist.keys():
                    add_collumn(table, u"'%s' datetime" % c)

            for c in self.config.key_values['date']:
                if c.lower() not in clist.keys():
                    add_collumn(table, u"'%s' date DEFAULT NULL" % c)

            for c in self.config.key_values['bool']:
                if c.lower() not in clist.keys():
                    add_collumn(table, u"'%s' boolean DEFAULT NULL" % c)

            for c in self.config.key_values['int']:
                if c.lower() not in clist.keys():
                    add_collumn(table, u"'%s' INTEGER DEFAULT NULL" % c)

            for c in self.config.key_values['video']:
                if c.lower() not in clist.keys():
                    add_collumn(table, u"'%s' boolean DEFAULT NULL" % c)

            for c in self.config.key_values['list']:
                if c.lower() not in clist.keys():
                    add_collumn(table, u"'%s' rating DEFAULT NULL" % c)

        elif table == 'credits':
            for c in ('pid', 'title', 'name', 'role'):
                if c.lower() not in clist.keys():
                    drop_table(table)
                    self.create_table(table)
                    return

        elif table == 'ttvdb':
            for c in ('title', ):
                if c.lower() not in clist.keys():
                    drop_table(table)
                    self.create_table(table)
                    drop_table('episodes')
                    self.create_table('episodes')
                    return

            if 'tid' not in clist.keys():
                add_collumn(table, u"'tid' INTEGER")

            if 'langs' not in clist.keys():
                add_collumn(table, u"'langs' listing")

            if 'tdate' not in clist.keys():
                add_collumn(table, u"'tdate' date")

        elif table == 'ttvdb_alias':
            for c in ('alias', ):
                if c.lower() not in clist.keys():
                    drop_table(table)
                    self.create_table(table)
                    return

            if 'title' not in clist.keys():
                add_collumn(table, u"'title' TEXT")

        elif table == 'episodes':
            for c in ('tid', 'sid', 'eid', 'lang'):
                if c.lower() not in clist.keys():
                    drop_table(table)
                    self.create_table(table)
                    return

            for c in ('title', 'description'):
                if c.lower() not in clist.keys():
                    add_collumn(table, u"'%s' TEXT" % c)

            if 'airdate' not in clist.keys():
                add_collumn(table, u"'airdate' date")

        elif table == 'channels':
            if 'chanid' not in clist.keys():
                drop_table(table)
                self.create_table(table)
                return

            if 'cgroup' not in clist.keys():
                add_collumn(table, u"'cgroup' INTEGER")

            if 'name' not in clist.keys():
                add_collumn(table, u"'name' TEXT")

        elif table == 'channelsource':
            for c in ('chanid', 'sourceid'):
                if c.lower() not in clist.keys():
                    drop_table(table)
                    self.create_table(table)
                    return

            for c in ('scid', 'name'):
                if c.lower() not in clist.keys():
                    add_collumn(table, u"'%s' TEXT" % c)

            if 'hd' not in clist.keys():
                add_collumn(table, u"'hd' boolean DEFAULT 'False'")

            if 'emptycount' not in clist.keys():
                add_collumn(table, u"'emptycount' INTEGER DEFAULT 0")

        elif table == 'iconsource':
            for c in ('chanid', 'sourceid'):
                if c.lower() not in clist.keys():
                    drop_table(table)
                    self.create_table(table)
                    return

            if 'icon' not in clist.keys():
                add_collumn(table, u"'icon' TEXT")

    def check_indexes(self, table):
        def add_index(table, i, clist):
            try:
                with self.pconn:
                    self.pconn.execute(u"CREATE INDEX IF NOT EXISTS '%s' ON %s %s" % (i, table, clist))

            except:
                self.functions.log(self.config.text('IO', 15, (table, i)))

        pcursor = self.pconn.cursor()
        # (id, Name, Type, Nullable = 0, Default, Pri_key index)
        pcursor.execute("PRAGMA main.index_list(%s)" % (table,))
        ilist = {}
        for r in pcursor.fetchall():
            ilist[r[1].lower()] = r

        if table == 'programs':
            if 'stoptime' not in ilist:
                add_index( table, 'stoptime', "('stop-time')")

        elif table == 'credits':
            if 'credtitle' not in ilist:
                add_index( table, 'credtitle', "('pid', 'title')")

        elif table == 'ttvdb':
            if 'ttvdbtid' not in ilist:
                add_index( table, 'ttvdbtid', "('tid')")

        elif table == 'episodes':
            if 'eptitle' not in ilist:
                add_index( table, 'eptitle', "('title')")

        elif table == 'channels':
            if 'cgroup' not in ilist:
                add_index( table, 'cgroup', "('cgroup')")

            if 'chan_name' not in ilist:
                add_index( table, 'chan_name', "('name')")

        elif table == 'channelsource':
            if 'scid' not in ilist:
                add_index( table, 'scid', "('scid')")

    def load_old(self):
        """
        Loads a pickled cache dict from file
        """
        try:
            pdict = pickle.load(open(self.filename,'r'))

        except:
            self.functions.log([self.config.text('IO', 16, (self.filename, )), traceback.format_exc()])
            return

        dnow = datetime.date.today()
        self.functions.log([self.config.text('IO', 17), self.config.text('IO', 18)])
        pcount = 0
        for p in pdict.values():
            if 'stop-time'  in p and 'name'  in p and \
                    p['stop-time'].date() >= dnow and \
                    type(p['name']) == unicode and \
                    p['name'].lower() != 'onbekend':

                self.add(p)
                pcount += 1

        self.functions.log(self.config.text('IO', 19, (pcount, )))

    def query(self, table='pid', item=None):
        """
        Updates/gets/whatever.
        """
        pcursor = self.pconn.cursor()
        if table == 'pid':
            pcursor.execute(u"SELECT * FROM programs WHERE pid = ?", (item,))
            r = pcursor.fetchone()
            if r == None:
                return

            program = self.config.fetch_func.checkout_program_dict()
            for item in r.keys():
                if item == 'pid':
                    continue

                elif item in self.config.fetch_func.video_values:
                    program['video'][item] = r[item]

                elif item in self.ID_list.keys():
                    program['prog_ID'][self.ID_list[item]] = r[item]

                elif item in self.url_list.keys():
                    program['detail_url'][self.url_list[item]] = r[item]

                else:
                    program[item] = r[item]

            pcursor.execute(u"SELECT * FROM credits WHERE pid = ?", (item,))
            for r in pcursor.fetchall():
                if not r[str('title')] in program['credits'].keys():
                    program['credits'][r[str('title')]] = []

                program['credits'][r[str('title')]].append(r[str('name')])

            program = self.config.fetch_func.checkout_program_dict(program)
            return program

        elif table == 'ttvdb':
            pcursor.execute(u"SELECT * FROM ttvdb WHERE tid = ?", (item,))
            r = pcursor.fetchone()
            if r == None:
                return

            serie = {}
            serie['tid'] = r[str('tid')]
            serie['title'] = r[str('title')]
            serie['tdate'] = r[str('tdate')]
            return serie

        elif table == 'ttvdb_aliasses':
            pcursor.execute(u"SELECT alias FROM ttvdb_alias WHERE lower(title) = ?", (item.lower(), ))
            r = pcursor.fetchall()
            aliasses = []
            if r != None:
                for a in r:
                    aliasses.append( a[0])

            return aliasses

        elif table == 'ttvdb_langs':
            pcursor.execute(u"SELECT langs FROM ttvdb WHERE tid = ?", (item['tid'],))
            r = pcursor.fetchone()
            aliasses = []
            if r == None:
                return r[0]

            else:
                return []

        elif table == 'ep_by_id':
            qstring = u"SELECT * FROM episodes WHERE tid = ?"
            qlist = [item['tid']]
            if item['sid'] > 0:
                qstring += u" and sid = ?"
                qlist.append(item['sid'])

            if item['eid'] > 0:
                qstring += u" and eid = ?"
                qlist.append(item['eid'])

            if 'lang' in item:
                qstring += u" and lang = ?"
                qlist.append(item['lang'])

            pcursor.execute(qstring, tuple(qlist))

            r = pcursor.fetchall()
            series = []
            for s in r:
                series.append({'tid': int(s[str('tid')]),
                                          'sid': int(s[str('sid')]),
                                          'eid': int(s[str('eid')]),
                                          'title': s[str('title')],
                                          'airdate': s[str('airdate')],
                                          'lang': s[str('lang')],
                                          'description': s[str('description')]})
            return series

        elif table == 'ep_by_title':
            pcursor.execute(u"SELECT * FROM episodes WHERE tid = ? and lower(title) = ?", (item['tid'], item['title'].lower(), ))
            r = pcursor.fetchone()
            if r == None:
                return

            serie = {}
            serie['tid'] = int(r[str('tid')])
            serie['sid'] = int(r[str('sid')])
            serie['eid'] = int(r[str('eid')])
            serie['title'] = r[str('title')]
            serie['airdate'] = r[str('airdate')]
            serie['lang'] = r[str('lang')]
            serie['description'] = r[str('description')]
            return serie
        elif table == 'icon':
            if item == None:
                pcursor.execute(u"SELECT chanid, sourceid, icon FROM iconsource")
                r = pcursor.fetchall()
                icons = {}
                if r != None:
                    for g in r:
                        if not g[0] in icons:
                            icons[g[0]] ={}

                        icons[g[0]][g[1]] = g[2]

                return icons

            else:
                pcursor.execute(u"SELECT icon FROM iconsource WHERE chanid = ? and sourceid = ?", (item['chanid'], item['sourceid']))
                r = pcursor.fetchone()
                if r == None:
                    return

                return {'sourceid':  item['sourceid'], 'icon': r[0]}

        elif table == 'chan_group':
            if item == None:
                pcursor.execute(u"SELECT chanid, cgroup, name FROM channels")
                r = pcursor.fetchall()
                changroups = {}
                if r != None:
                    for g in r:
                        changroups[g[0]] = {'name': g[2],'cgroup': int(g[1])}

                return changroups

            else:
                pcursor.execute(u"SELECT cgroup, name FROM channels WHERE chanid = ?", (item['chanid'],))
                r = pcursor.fetchone()
                if r == None:
                    return

                return {'cgroup':r[0], 'name': r[1]}

        elif table == 'chan_scid':
            if item == None:
                pcursor.execute(u"SELECT chanid, sourceid, scid, name, hd FROM channelsource")
                r = pcursor.fetchall()
                scids = {}
                if r != None:
                    for g in r:
                        if not g[0] in scids:
                            scids[g[0]] ={}

                        scids[g[0]][g[1]] = {'scid': g[2],'name': g[3], 'hd': g[4]}

                return scids

            elif 'chanid' in item and 'sourceid' in item:
                pcursor.execute(u"SELECT scid FROM channelsource WHERE chanid = ? and sourceid = ?", (item['chanid'], item['sourceid']))
                r = pcursor.fetchone()
                if r == None:
                    return

                return scid

            elif 'sourceid' in item:
                pcursor.execute(u"SELECT scid, chanid, name FROM channelsource WHERE sourceid = ?", (item['sourceid']))
                r = pcursor.fetchall()
                scids = {}
                if r != None:
                    for g in r:
                        if not g[0] in scids:
                            scids[g[0]] ={}

                        scids[g[0]] = {'chanid': g[1],'name': g[2]}

                return scids

    def query_id(self, table='program', item=None):
        """
        Check which ID is used
        """
        pcursor = self.pconn.cursor()
        if table == 'program':
            ID_list = [item['ID']]
            for key in self.config.channelsource.keys():
                if item['prog_ID'][key] != '' and item['prog_ID'][key] != None:
                    ID_list.append(item['prog_ID'][key])

            for id in ID_list:
                pcursor.execute(u"SELECT pid FROM programs WHERE pid = ?", (id,))
                if pcursor.fetchone() != None:
                    return id

            return None

        elif table == 'ttvdb':
            pcursor.execute(u"SELECT ttvdb.tid, tdate, ttvdb.title, ttvdb.langs FROM ttvdb JOIN ttvdb_alias " + \
                    "ON lower(ttvdb.title) = lower(ttvdb_alias.title) WHERE lower(alias) = ?", \
                    (item['title'].lower(), ))
            r = pcursor.fetchone()
            if r == None:
                pcursor.execute(u"SELECT tid, tdate, title, langs FROM ttvdb WHERE lower(title) = ?", (item['title'].lower(), ))
                r = pcursor.fetchone()
                if r == None:
                    return

            return {'tid': r[0], 'tdate': r[1], 'title': r[2], 'langs': r[3]}

        elif table == 'ttvdb_alias':
            pcursor.execute(u"SELECT title FROM ttvdb_alias WHERE lower(alias) = ?", (item['alias'].lower(), ))
            r = pcursor.fetchone()
            if r == None:
                if 'title' in item:
                    return False

                else:
                    return

            if 'title' in item:
                if item['title'].lower() == r[0].lower():
                    return True

                else:
                    return False

            else:
                return {'title': r[0]}

        elif table == 'tdate':
            pcursor.execute(u"SELECT tdate FROM ttvdb WHERE tid = ?", (item,))
            r = pcursor.fetchone()
            if r == None:
                return

            return r[0]

        elif table == 'chan_group':
            pcursor.execute(u"SELECT cgroup, name FROM channels WHERE chanid = ?", (item['chanid'],))
            r = pcursor.fetchone()
            if r == None:
                return

            return r[0]

    def add(self, table='program', item=None):
        """
        Adds a record
        """
        pcursor = self.pconn.cursor()
        rec = []
        rec_upd = []
        if table == 'program':
            cache_id = self.query_id('program', item)
            if cache_id != None:
                with self.pconn:
                    self.pconn.execute(u"DELETE FROM programs WHERE pid = ?", (cache_id,))
                    self.pconn.execute(u"DELETE FROM credits WHERE pid = ?", (cache_id,))

            if item['ID'] != '' and item['ID'] != None:
                id = item['ID']

            else:
                for key in self.config.channelsource.keys():
                    if item['prog_ID'][key] != '' and item['prog_ID'][key] != None:
                        id = item['prog_ID'][key]
                        break

                else:
                    self.functions.log(self.config.text('IO', 20, (item['name'], )))
                    return

            sql_flds = u"INSERT INTO programs ('pid'"
            sql_cnt = u"VALUES (?"
            sql_vals = [id]
            for f, v in item.items():
                if f in self.field_list:
                    sql_flds = u"%s, '%s'" % (sql_flds, f)
                    sql_cnt = u"%s, ?" % (sql_cnt)
                    sql_vals.append(v)

            for f, v in item['video'].items():
                sql_flds = u"%s, '%s'" % (sql_flds, f)
                sql_cnt = u"%s, ?" % (sql_cnt)
                sql_vals.append(v)

            for f, v in item['prog_ID'].items():
                sql_flds = u"%s, '%s'" % (sql_flds, self.config.channelsource[f].detail_id)
                sql_cnt = u"%s, ?" % (sql_cnt)
                sql_vals.append(v)

            for f, v in item['detail_url'].items():
                sql_flds = u"%s, '%s'" % (sql_flds, self.config.channelsource[f].detail_url)
                sql_cnt = u"%s, ?" % (sql_cnt)
                sql_vals.append(v)

            add_string = u"%s) %s)" % (sql_flds, sql_cnt)
            with self.pconn:
                self.pconn.execute(add_string, tuple(sql_vals))

            add_string = u"INSERT INTO credits (pid, title, name) VALUES (?, ?, ?)"
            for f, v in item['credits'].items():
                rec.append((id, f, v))

        elif table == 'channel':
            add_string = u"INSERT INTO channels ('chanid', 'cgroup', 'name') VALUES (?, ?, ?)"
            update_string = u"UPDATE channels SET `cgroup` = ?, `name` = ? WHERE chanid = ?"
            if isinstance(item, dict):
                item = [item]

            if isinstance(item, list):
                g = self.query('chan_group')

                for c in item:
                    if not c['chanid'] in g.keys():
                        rec.append((c['chanid'], c['cgroup'], c['name']))

                    elif g[c['chanid']]['name'].lower() != c['name'].lower() or g[c['chanid']]['cgroup'] != c['cgroup'] \
                      or (g[c['chanid']]['cgroup'] == 10 and c['cgroup'] not in (-1, 0, 10)):
                        rec_upd.append((c['cgroup'], c['name'] , c['chanid']))

        elif table == 'channelsource':
            add_string = u"INSERT INTO channelsource ('chanid', 'sourceid', 'scid', 'name', 'hd') VALUES (?, ?, ?, ?, ?)"
            update_string = u"UPDATE channelsource SET 'scid'= ?, 'name'= ?, 'hd'= ? WHERE chanid = ? and sourceid = ?"
            if isinstance(item, dict):
                item = [item]

            if isinstance(item, list):
                scids = self.query('chan_scid')
                for c in item:
                    if c['scid'] == '':
                        continue

                    if c['chanid'] in scids and c['sourceid'] in scids[c['chanid']]:
                        rec_upd.append((c['scid'], c['name'], c['hd'], c['chanid'], c['sourceid']))

                    else:
                        rec.append((c['chanid'], c['sourceid'], c['scid'], c['name'], c['hd']))

        elif table == 'icon':
            add_string = u"INSERT INTO iconsource ('chanid', 'sourceid', 'icon') VALUES (?, ?, ?)"
            update_string = u"UPDATE iconsource SET 'icon'= ? WHERE chanid = ? and sourceid = ?"
            if isinstance(item, dict):
                item = [item]

            if isinstance(item, list):
                icons = self.query('icon')
                for ic in item:
                    if ic['chanid'] in icons and ic['sourceid'] in icons[ic['chanid']] \
                      and icons[ic['chanid']][ic['sourceid']] != ic['icon']:
                        rec_upd.append((ic['icon'], ic['chanid'], ic['sourceid']))

                    else:
                        rec.append((ic['chanid'], ic['sourceid'], ic['icon']))

        elif table == 'ttvdb':
            add_string = u"INSERT INTO ttvdb ('tid', 'title', 'langs', 'tdate') VALUES (?, ?, ?, ?)"
            update_string = ''
            rec.append((int(item['tid']), item['title'], list(item['langs']), datetime.date.today()))

        elif table == 'ttvdb_lang':
            add_string = u"INSERT INTO ttvdb ('tid', 'title', 'tdate', 'langs') VALUES (?, ?, ?, ?)"
            update_string = u"UPDATE ttvdb SET langs = ?, tdate = ? WHERE tid = ?"
            g = self.query('ttvdb_langs', int(item['tid']))
            if len(g) == 0:
                rec.append((int(item['tid']), item['title'], datetime.date.today(), item['lang']))

            else:
                langs = g[0]
                if item['lang'] not in langs:
                    langs.append(item['lang'])
                    rec_upd.append((langs , datetime.date.today(), int(item['tid'])))

        elif table == 'ttvdb_alias':
            add_string = u"INSERT INTO ttvdb_alias ('title', 'alias') VALUES (?, ?)"
            aliasses = self.query('ttvdb_aliasses', item['title'])
            if isinstance(item['alias'], list) and len(item['alias']) > 0:
                for a in item['alias']:
                    if not a in aliasses:
                        rec.append((item['title'], a))

            else:
                if not item['alias'] in aliasses:
                    rec.append((item['title'], item['alias']))

        elif table == 'episode':
            add_string = u"INSERT INTO episodes ('tid', 'sid', 'eid', 'title', 'airdate', 'lang', 'description') " + \
                                  u"VALUES (?, ?, ?, ?, ?, ?, ?)"
            update_string = u"UPDATE episodes SET title = ?, airdate = ?, description = ? " + \
                                       u"WHERE tid = ? and sid = ? and eid = ? and lang = ?"
            if isinstance(item, dict):
                item = [item]

            if isinstance(item, list):
                rec = []
                rec_upd = []
                for e in item:
                    ep = self.query('ep_by_id', e)
                    if ep == None or len(ep) == 0:
                        rec.append((int(e['tid']), int(e['sid']), int(e['eid']), e['title'], e['airdate'], e['lang'], e['description']))

                    elif ep[0]['title'].lower() != e['title'].lower() or ep[0]['airdate'] != e['airdate']:
                        rec_upd.append((e['title'], e['airdate'], int(e['tid']), int(e['sid']), int(e['eid']), e['lang'], e['description']))

        if len(rec_upd) == 1:
            with self.pconn:
                self.pconn.execute(update_string, rec_upd[0])

        elif len(rec_upd) > 1:
            with self.pconn:
                self.pconn.executemany(update_string, rec_upd)

        if len(rec) == 1:
            with self.pconn:
                self.pconn.execute(add_string, rec[0])

        elif len(rec) > 1:
            with self.pconn:
                self.pconn.executemany(add_string, rec)

    def delete(self, table='ttvdb', item=None):
        if table == 'ttvdb':
            with self.pconn:
                self.pconn.execute(u"DELETE FROM ttvdb WHERE tid = ?",  (int(item['tid']), ))
                self.pconn.execute(u"DELETE FROM episodes WHERE tid = ?",  (int(item['tid']), ))

    def clear(self, table):
        """
        Clears the cache (i.e. empties it)
        """
        with self.pconn:
            self.pconn.execute(u"DROP TABLE IF EXISTS %s" % table)

        with self.pconn:
            self.pconn.execute(u"VACUUM")

        self.create_table(table)
        self.check_indexes(table)

    def clean(self):
        """
        Removes all cached programming before today.
        And ttvdb ids older then 30 days
        """
        dnow = int(time.mktime(datetime.date.today().timetuple())*1000)
        with self.pconn:
            self.pconn.execute(u"DELETE FROM programs WHERE 'stop-time' < ?", (dnow,))

        with self.pconn:
            self.pconn.execute(u"DELETE FROM credits WHERE NOT EXISTS (SELECT * FROM programs WHERE programs.pid = credits.pid)")

        dnow = datetime.date.today().toordinal()
        with self.pconn:
            self.pconn.execute(u"DELETE FROM ttvdb WHERE tdate < ?", (dnow - 30,))

        with self.pconn:
            self.pconn.execute(u"VACUUM")

# end ProgramCache

class InfoFiles():
    """used for gathering extra info to better the code"""
    def __init__(self, config, write_info_files = True):

        self.config = config
        self.functions = self.config.IO_func
        self.write_info_files = write_info_files
        self.info_lock = Lock()
        self.cache_return = Queue()
        self.detail_list = []
        self.raw_list = []
        self.raw_string = ''
        self.fetch_strings = {}
        self.lineup_changes = []
        self.url_failure = []
        if self.write_info_files:
            self.fetch_list = self.functions.open_file(self.config.opt_dict['xmltv_dir'] + '/fetched-programs3','w')
            self.raw_output =  self.functions.open_file(self.config.opt_dict['xmltv_dir']+'/raw_output3', 'w')

    def check_new_channels(self, source, source_channels):
        if not self.write_info_files:
            return

        if source.all_channels == {}:
            source.get_channels()

        for chan_scid, channel in source.all_channels.items():
            #~ if not (chan_scid in source_channels[source.proc_id].values() or chan_scid in empty_channels[source.proc_id]):
            if not (chan_scid in source_channels[source.proc_id].values() or chan_scid in source.empty_channels):
                self.lineup_changes.append( u'New channel on %s => %s (%s)\n' % (source.source, chan_scid, channel['name']))

        for chanid, chan_scid in source_channels[source.proc_id].items():
            #~ if not (chan_scid in source.all_channels.keys() or chan_scid in empty_channels[source.proc_id]):
            if not (chan_scid in source.all_channels.keys() or chan_scid in source.empty_channels):
                self.lineup_changes.append( u'Removed channel on %s => %s (%s)\n' % (source.source, chan_scid, chanid))

        #~ for chan_scid in empty_channels[source.proc_id]:
        for chan_scid in source.empty_channels:
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

    def write_fetch_list(self, programs, chanid = None, source = None, chan_name = '', sid = None, ismerge = False):
        def value(vname):
            if vname == 'ID':
                if sid == None:
                    return tdict['ID']

                elif 'prog_ID' in tdict:
                    return tdict['prog_ID']

                return '---'

            if not vname in tdict.keys():
                return '--- '

            if isinstance(tdict[vname], datetime.datetime):
                return self.config.in_output_tz(tdict[vname]).strftime('%d %b %H:%M')

            if isinstance(tdict[vname], bool):
                if tdict[vname]:
                    return 'True '

                return 'False '

            return tdict[vname]

        if (not self.write_info_files) or (self.fetch_list == None):
            return

        with self.info_lock:
            if isinstance(programs, tv_grab_channel.ChannelNode):

                if source in self.config.channelsource.keys():
                    fstr = u' (%3.0f) after merging from: %s\n' % (programs.program_count(), self.config.channelsource[source].source)

                else:
                    fstr = u' (%3.0f) after merging from: %s\n' % (programs.program_count(), source)

                pnode = programs.first_node
                while isinstance(pnode, tv_grab_channel.ProgramNode):
                    fstr += u'  %s-%s: [%s][%s] %s: %s\n' % (\
                                    pnode.get_value('start'), pnode.get_value('stop'), \
                                    pnode.get_value('ID').rjust(15), pnode.get_value('genre')[0:10].rjust(10), \
                                    pnode.get_value('name'), pnode.get_value('episode title'))

                    pnode = pnode.next

                fstr += u'#\n'

            else:
                plist = copy.deepcopy(programs)
                fstr = u' (%3.0f) from: %s\n' % (len(plist),  self.config.channelsource[source].source)

                plist.sort(key=lambda program: (program['start-time']))

                for tdict in plist:
                    extra = value('rerun') + value('teletext') + value('new') + value('last-chance') + value('premiere')
                    extra2 = value('HD') + value('widescreen') + value('blackwhite')

                    fstr += u'  %s-%s: [%s][%s] %s: %s [%s] [%s]\n' % (\
                                    value('start-time'), value('stop-time'), \
                                    value('ID').rjust(15), value('genre')[0:10].rjust(10), \
                                    value('name'), value('episode title'), \
                                    extra, extra2)

                    #~ fstr += u'  %s-%s: [%s][%s] %s: %s [%s/%s]\n' % (\
                                    #~ self.config.output_tz.normalize(tdict['start-time'].astimezone(self.config.output_tz)).strftime('%d %b %H:%M'), \
                                    #~ self.config.output_tz.normalize(tdict['stop-time'].astimezone(self.config.output_tz)).strftime('%d %b %H:%M'), \
                                    #~ psid.rjust(15), tdict['genre'][0:10].rjust(10), \
                                    #~ tdict['name'], tdict['episode title'], \
                                    #~ tdict['season'], tdict['episode'])

            if not chanid in  self.fetch_strings:
                 self.fetch_strings[chanid] = {}
                 self.fetch_strings[chanid]['name'] = u'Channel: %s\n' % chan_name

            if source in self.config.channelsource.keys():
                if not source in  self.fetch_strings[chanid]:
                    self.fetch_strings[chanid][source] = fstr

                else:
                    self.fetch_strings[chanid][source] += fstr

            elif not 'channels' in self.fetch_strings[chanid]:
                self.fetch_strings[chanid]['channels'] = fstr

            else:
                self.fetch_strings[chanid]['channels'] += fstr

    def write_xmloutput(self, xml):

        if self.write_info_files:
            xml_output =self.functions.open_file(self.config.opt_dict['xmltv_dir']+'/xml_output3', 'w')
            if xml_output == None:
                return

            xml_output.write(xml)
            xml_output.close()

    def close(self, channels, combined_channels, sources):
        if not self.write_info_files:
            return

        if self.config.opt_dict['mail_info_address'] == None:
            self.config.opt_dict['mail_info_address'] = self.config.opt_dict['mail_log_address']

        if self.config.opt_dict['mail_log'] and len(self.lineup_changes) > 0:
            self.config.logging.send_mail(self.lineup_changes, self.config.opt_dict['mail_info_address'], 'Tv_grab_nl_py lineup changes')

        if self.config.opt_dict['mail_log'] and len(self.url_failure) > 0:
            self.config.logging.send_mail(self.url_failure, self.config.opt_dict['mail_info_address'], 'Tv_grab_nl_py url failures')

        if self.fetch_list != None:
            chan_list = []
            combine_list = []
            for chanid in channels.keys():
                if (channels[chanid].active or channels[chanid].is_child) and chanid in self.fetch_strings:
                    if chanid in combined_channels.keys():
                        combine_list.append(chanid)

                    else:
                        chan_list.append(chanid)

            chan_list.extend(combine_list)
            for chanid in chan_list:
                self.fetch_list.write(self.fetch_strings[chanid]['name'])
                for s in channels[chanid].merge_order:
                    if s in self.fetch_strings[chanid].keys():
                        self.fetch_list.write(self.fetch_strings[chanid][s])

                if chanid in combined_channels.keys() and 'channels' in self.fetch_strings[chanid]:
                    self.fetch_list.write(self.fetch_strings[chanid]['channels'])

            self.fetch_list.close()

        if self.raw_output != None:
            self.raw_output.close()

        if len(self.detail_list) > 0:
            f = self.functions.open_file(self.config.opt_dict['xmltv_dir']+'/detail_output3')
            if (f != None):
                f.seek(0,0)
                for byteline in f.readlines():
                    line = self.functions.get_line(f, byteline, False)
                    if line:
                        self.detail_list.append(line)

                f.close()

            f = self.functions.open_file(self.config.opt_dict['xmltv_dir']+'/detail_output3', 'w')
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

