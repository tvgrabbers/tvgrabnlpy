#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Python 3 compatibility
from __future__ import unicode_literals
# from __future__ import print_function

import codecs, locale, re, os, sys, io, shutil, difflib
import traceback, smtplib, sqlite3
import datetime, time, pytz, copy
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
        if os.path.isfile(fle + '.' + save_ext):
            os.remove(fle + '.' + save_ext)

        if os.path.isfile(fle):
            os.rename(fle, fle + '.' + save_ext)

    # end save_oldfile()

    def restore_oldfile(self, fle, save_ext='old'):
        """ restore the old file from .old if it exists """
        if os.path.isfile(fle):
            os.remove(fle)

        if os.path.isfile(fle + '.' + save_ext):
            os.rename(fle + '.' + save_ext, fle)

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

class ChannelNode():
    def __init__(self, config, channel_config, programs = None, source = None):
        self.node_lock = RLock()
        with self.node_lock:
            self.prime_source = None
            self.config = config
            self.channel_config = channel_config
            self.chanid = channel_config.chanid
            self.name = channel_config.chan_name
            self.shortname = self.name[:15] if len(self.name) > 15 else self.name
            self.current_stats = {}
            self.adding_stats = {}
            self.merge_stats = {}
            if not self.chanid in self.config.channels.keys():
                return

            if not self.chanid in self.config.channelprogram_rename.keys():
                self.config.channelprogram_rename[self.chanid] = {}

            self.key_list = []
            for kl in self.config.key_values.values():
                self.key_list.extend(kl)

            self.key_list.append('genre')
            self.config.key_values['source'] = ['prog_ID', 'detail_url', 'detail_fetched']
            self.config.key_values['dict'] = ['credits', 'video']
            self.channel_nodes = []
            self.clear_all_programs()
            self.checkrange = [0]
            for i in range(1, 30):
                self.checkrange.extend([i, -i])

            self.child_times = []
            self.groupslot_names = self.config.groupslot_names[:]
            if self.chanid in self.config.combined_channels.keys():
                # This channel has children
                tz = self.config.fetch_timezone
                date_now = tz.normalize(datetime.datetime.now(pytz.utc).astimezone(tz)).toordinal()
                start_date = date_now + self.config.opt_dict['offset']
                start_time = self.config.fetch_func.merge_date_time(start_date, datetime.time(0, 0), self.config.combined_channels_tz)
                end_date = start_date + self.config.opt_dict['days']
                end_time = self.config.fetch_func.merge_date_time(end_date, datetime.time(0, 0), self.config.combined_channels_tz)
                clist = self.config.combined_channels[self.chanid]
                if 'start' in clist[0]:
                    # They have time restrictions
                    clist.sort(key=lambda ctime: (ctime['start']))
                    if clist[-1]['start'] > clist[-1]['end']:
                        cend = self.config.fetch_func.merge_date_time(start_date, clist[-1]['end'], self.config.combined_channels_tz)
                        last_date = {'start': start_time, 'stop': cend, 'chanid': clist[-1]['chanid'], 'slots': []}
                        if 'slots' in clist[-1]:
                            last_date['slots'] = clist[-1]['slots']

                    else:
                        last_date = {'start': start_time, 'stop': None, 'chanid': None, 'slots': []}

                    for offset in range(start_date, end_date):
                        for item in clist:
                            cstart = self.config.fetch_func.merge_date_time(offset, item['start'], self.config.combined_channels_tz)
                            if item['end'] > item['start']:
                                cend = self.config.fetch_func.merge_date_time(offset, item['end'], self.config.combined_channels_tz)
                            else:
                                cend = self.config.fetch_func.merge_date_time(offset + 1, item['end'], self.config.combined_channels_tz)

                            if last_date['stop'] == None or last_date['chanid'] == None or last_date['stop'] > cstart:
                                last_date['stop'] = cstart

                            self.child_times.append(last_date)
                            if last_date['stop'] < cstart:
                                self.child_times.append({'start': last_date['stop'], 'stop': cstart, 'chanid': None, 'slots': []})

                            last_date = {'start': cstart, 'stop': cend, 'chanid': item['chanid'], 'slots': []}
                            if 'slots' in item:
                                last_date['slots'] = item['slots']

                    if last_date['stop'] > end_time:
                        last_date['stop'] = end_time

                    self.child_times.append(last_date)

                for child in self.config.combined_channels[self.chanid]:
                    if 'slots' in child.keys():
                        if isinstance(child['slots'], (str, unicode)):
                            self.groupslot_names.append(re.sub('[-,. ]', '', self.config.fetch_func.remove_accents(child['slots']).lower().strip()))

                        elif isinstance(child['slots'], list):
                            for gs in child['slots']:
                                if isinstance(gs, (str, unicode)):
                                    self.groupslot_names.append(re.sub('[-,. ]', '', self.config.fetch_func.remove_accents(gs).lower().strip()))

            self.merge_type = None
            if source in self.config.channelsource.keys():
                self.prime_source = source
                self.merge_source(programs, source)

    def clear_all_programs(self):
        with self.node_lock:
            self.programs = []
            self.program_gaps = []
            self.group_slots = []
            self.programs_by_start = {}
            self.programs_by_stop = {}
            self.programs_by_name = {}
            self.programs_by_matchname = {}
            self.programs_with_no_genre = {}
            self.start = None
            self.stop = None
            self.first_node = None
            self.last_node = None

    def save_current_stats(self):
        with self.node_lock:
            self.current_stats['start'] = self.start
            self.current_stats['stop'] = self.stop
            self.current_stats['count'] = self.program_count()
            self.current_stats['groups'] = len(self.group_slots)
            self.current_stats['start-str'] = self.start.strftime('%d-%b %H:%M') if isinstance(self.start, datetime.datetime) else '            '
            self.current_stats['stop-str'] = self.stop.strftime('%d-%b %H:%M') if isinstance(self.stop, datetime.datetime) else '            '
            return self.current_stats

    def get_adding_stats(self, programs, group_slots = None):
        with self.node_lock:
            if isinstance(programs, ChannelNode):
                self.adding_stats = programs.save_current_stats()
                if self.adding_stats['count'] == 0:
                    return False

                else:
                    return True

            elif len(programs) == 0:
                return False

            else:
                self.adding_stats['count'] = len(programs)
                self.adding_stats['groups'] = 0
                try:
                    if isinstance(programs[0], ProgramNode):
                        programs.sort(key=lambda program: (program.start))
                        self.adding_stats['start'] = programs[0].start
                        self.adding_stats['stop'] = programs[-1].stop
                        if group_slots != None and len(group_slots) > 0:
                            self.adding_stats['groups'] = len(group_slots)
                            self.adding_stats['count'] += self.adding_stats['groups']
                            group_slots.sort(key=lambda program: (program.start))
                            if group_slots[0].start < self.adding_stats['start']:
                                self.adding_stats['start'] = group_slots[0].start

                            if group_slots[0].stop > self.adding_stats['stop']:
                                self.adding_stats['stop'] = group_slots[0].stop

                    else:
                        programs.sort(key=lambda program: (program['start-time']))
                        self.adding_stats['start'] = programs[0]['start-time']
                        self.adding_stats['stop'] = programs[-1]['stop-time']

                    stt = self.adding_stats['start']
                    self.adding_stats['start-str'] = stt.strftime('%d-%b %H:%M') if isinstance(stt, datetime.datetime) else '            '
                    stt = self.adding_stats['stop']
                    self.adding_stats['stop-str'] = stt.strftime('%d-%b %H:%M') if isinstance(stt, datetime.datetime) else '            '
                    return True

                except:
                    self.adding_stats['start'] = None
                    self.adding_stats['stop'] = None
                    self.adding_stats['count'] = 0
                    self.adding_stats['groups'] = 0
                    return False

    def init_merge_stats(self):
        with self.node_lock:
            self.merge_stats['new'] = 0
            self.merge_stats['matched'] = 0
            self.merge_stats['groupslot'] = 0
            self.merge_stats['unmatched'] = 0
            self.merge_stats['genre'] = 0

    def add_stat(self, type = 'matched', addcnt = 1):
        with self.node_lock:
            if not type in self.merge_stats:
                self.merge_stats[type] = 0

            self.merge_stats[type] += addcnt

            if self.merge_stats[type] < 0:
                self.merge_stats[type] = 0

    def log_merge_statistics(self, source):
        with self.node_lock:
            # merge_types
            # 0/1 adding/merging
            # 0/2/4 source/filtered channel/unfiltered channel
            self.merge_stats['new'] -= self.merge_stats['groupslot']
            if self.merge_type & 1:
                mtype = self.config.text('IO', 2, type = 'stats')

            else:
                mtype = self.config.text('IO', 1, type = 'stats')

            log_array = ['\n']
            if isinstance(source, ChannelNode):
                addingid = source.chanid
                addingname = source.shortname
                stype = self.config.text('IO', 6, type = 'stats')

            else:
                addingid = source
                addingname = self.config.channelsource[source].source
                stype = self.config.text('IO', 5, type = 'stats')

            log_array.append(self.config.text('IO', 9, \
                (mtype, self.name , self.channel_config.counter, self.config.chan_count, stype, addingname), 'stats'))
            log_array.append(self.config.text('IO', 10, \
                (self.current_stats['count'], self.shortname.ljust(15), self.current_stats['start-str'], \
                self.current_stats['stop-str'], self.current_stats['groups']), 'stats'))
            log_array.append(self.config.text('IO', 11, \
                (self.adding_stats['count'], addingname.ljust(15), self.adding_stats['start-str'], self.adding_stats['stop-str']), 'stats'))
            log_array.append('\n')
            log_array.append(self.config.text('IO', 14, (self.merge_stats['matched'], ), 'stats'))
            log_array.append(self.config.text('IO', 12, (self.merge_stats['new'], ), 'stats'))
            log_array.append(self.config.text('IO', 15, (self.merge_stats['groupslot'], ), 'stats'))
            log_array.append(self.config.text('IO', 13, (self.merge_stats['genre'], ), 'stats'))
            log_array.append(self.config.text('IO', 16, (self.merge_stats['unmatched'], addingname), 'stats'))
            log_array.append(self.config.text('IO', 17, (self.program_count(), len(self.group_slots)), 'stats'))
            log_array.append(self.config.text('IO', 18, (len(self.programs_with_no_genre), ), 'stats'))
            log_array.append('\n')
            self.config.log(log_array, 4, 3)
            self.merge_type = None

    def program_count(self):
        return len(self.programs)

    def merge_source(self, programs, source):
        def add_to_list(dlist, pp, is_groupslot = False):
            pn = ProgramNode(self, source, pp)
            if pn.is_valid:
                pn.is_groupslot = is_groupslot
                dlist.append(pn)

        def check_gaps(pp, is_groupslot = False):
            for gs in self.group_slots[:]:
                if gs.gs_start() <= pp['start-time'] <= gs.gs_stop() \
                  or gs.gs_start() <= pp['stop-time'] <= gs.gs_stop():
                    # if the groupslot is not detailed we only mark it matched
                    if not is_groupslot or len(gs.gs_detail) > 0:
                        add_to_list(gs.gs_detail, pp, is_groupslot)

                    break

            else:
                if pp['start-time'] < self.start:
                    add_to_list(add_to_start, pp, is_groupslot)
                    return

                if pp['stop-time'] > self.stop:
                    add_to_list(add_to_end, pp, is_groupslot)
                    return

                for pgap in self.program_gaps:
                    if pgap.start <= pp['start-time'] <= pgap.stop \
                      or pgap.start <= pp['stop-time'] <= pgap.stop:
                        # It falls into a gap
                        add_to_list(pgap.gap_detail, pp, is_groupslot)
                        break

                else:
                    # Unmatched
                    unmatched.append(pp)

        #Is it a valid source or does It look like a a channel merge
        if isinstance(programs, ChannelNode):
            self.merge_channel(programs)
            return

        # Is programs empty or is the source invalid?
        if not isinstance(programs, list) or len(programs) == 0 or not source in self.config.channelsource.keys():
            return

        with self.node_lock:
            self.save_current_stats()
            self.init_merge_stats()
            if not self.get_adding_stats(programs):
                return

            # Is this the first source?
            if self.program_count() == 0:
                self.config.log(['\n', self.config.text('IO', 7, (self.config.text('IO', 3, type='stats'), \
                    self.adding_stats['count'], self.config.channelsource[source].source, self.current_stats['count'], self.name), 'stats'), \
                    self.config.text('IO', 8, (self.channel_config.counter, self.config.chan_count), 'stats')], 2)

                self.merge_type = 0
                last_stop = self.start
                previous_node = None
                for index in range(len(programs)):
                    # Some sanity Check
                    if not 'stop-time' in programs[index] or not isinstance(programs[index]['stop-time'], datetime.datetime):
                        if index == len(programs) -1:
                            continue

                        programs[index]['stop-time'] = programs[index+1]['start-time']

                    if programs[index]['stop-time'] <= programs[index]['start-time']:
                        continue

                    if not 'length' in programs[index] or not isinstance(programs[index]['length'], datetime.timedelta):
                        programs[index]['length'] = programs[index]['stop-time'] - programs[index]['start-time']

                    if not 'name' in programs[index] or not isinstance(programs[index]['name'], unicode) or programs[index]['name'] == u'':
                        continue

                    # Check for renames
                    if programs[index]['name'].lower().strip() in self.config.channelprogram_rename[self.chanid].keys():
                        programs[index]['name'] = self.config.channelprogram_rename[self.chanid][programs[index]['name'].lower().strip()]

                    # Create the program node
                    pn = ProgramNode(self, source, programs[index])
                    if not pn.is_valid:
                        continue

                    if self.first_node == None:
                        self.first_node = pn
                        self.start = pn.start

                    # Link the nodes and check if there was a gap
                    gap =self.link_nodes(previous_node, pn)
                    if gap != None:
                        self.program_gaps.append(gap)

                    last_stop = pn.stop
                    previous_node = pn
                    self.add_new_program(pn)

                self.last_node = previous_node
                self.stop = last_stop
                self.adding_stats['groups'] = len(self.group_slots)

            else:
                self.config.log(['\n', self.config.text('IO', 7, (self.config.text('IO', 4, type='stats'), \
                    self.adding_stats['count'], self.config.channelsource[source].source, self.current_stats['count'], self.name), 'stats'), \
                    self.config.text('IO', 8, (self.channel_config.counter, self.config.chan_count), 'stats')], 2)

                self.merge_type = 1
                group_slots = []
                add_to_start = []
                add_to_end = []
                unmatched = []

                # first we do some general renaming and filter out the groupslots
                for p in programs[:]:
                    if p['name'].lower().strip() in self.config.channelprogram_rename[self.chanid].keys():
                        p['name'] = self.config.channelprogram_rename[self.chanid][p['name'].lower().strip()]

                    p['mname'] = re.sub('[-,. ]', '', self.config.fetch_func.remove_accents(p['name']).lower()).strip()
                    # It's a groupslot
                    if p['mname'] in self.groupslot_names:
                        group_slots.append(p)
                        programs.remove(p)
                        continue

                self.adding_stats['groups'] = len(group_slots)
                programs.sort(key=lambda program: (program['start-time']))
                # Try matching on time and name or check if it falls into a groupslot, a gap or outside the range
                for index in range(len(programs)):
                    for check in self.checkrange:
                        mstart = programs[index]['start-time'] + datetime.timedelta(0, 0, 0, 0, check)
                        if mstart in self.programs_by_start.keys() and self.programs_by_start[mstart].match_title(programs[index]['mname']):
                            # ### Check on split episodes
                            #~ l_diff = programs[index]['length'].total_seconds()/ self.programs_by_start[mstart].length.total_seconds()
                            #~ if l_diff >1.2 or l_diff < 1.2:
                                #~ pass

                            self.programs_by_start[mstart].add_source_data(programs[index], source)
                            self.add_stat('matched', 1)
                            break

                    else:
                        check_gaps(programs[index])

                # Check if any new groupslot falls in a detailed groupslot or outside current range or in gaps
                if len(group_slots) > 0:
                    self.program_gaps.sort(key=lambda program: (program.start))
                    group_slots.sort(key=lambda program: (program['start-time']))
                    for index in range(len(group_slots)):
                        check_gaps(group_slots[index], True)

                self.add_stat('unmatched', len(unmatched))
                # And add any program found new
                for gs in self.group_slots[:]:
                    self.fill_group(gs)

                self.fill_group(add_to_start)
                self.fill_group(add_to_end)
                for pgap in self.program_gaps[:]:
                    self.fill_group(pgap)

            # Finally we check if we can add any genres
            self.check_on_missing_genres()
            # Matching on genre
            self.log_merge_statistics(source)

    def merge_channel(self, channode):
        def add_to_list(dlist, pn):
            if pn.channode != self:
                pn = pn.copy(self)

            dlist.append(pn)

        def check_gaps(pn, add_always = True):
            for gs in self.group_slots[:]:
                if gs.gs_start() <= pn.start <= gs.gs_stop() \
                  or gs.gs_start() <= pn.stop <= gs.gs_stop():
                    # if the groupslot is not detailed we only mark it matched
                    if add_always or len(gs.gs_detail) > 0:
                        add_to_list(gs.gs_detail, pn)

                    break

            else:
                # Check if it falls outside current range
                if pn.start < self.start:
                    add_to_list(add_to_start, pn)
                    return

                if pn.stop > self.stop:
                    add_to_list(add_to_end, pn)
                    return

                for pgap in self.program_gaps:
                    if pgap.start <= pn.start <= pgap.stop \
                      or pgap.start <= pn.stop <= pgap.stop:
                        # It falls into a gap
                        add_to_list(pgap.gap_detail, pn)
                        break

                else:
                    # Unmatched
                    unmatched.append(pn)

        if not isinstance(channode, ChannelNode) or channode.program_count == 0:
            return

        with self.node_lock:
            self.save_current_stats()
            self.init_merge_stats()
            programs = []
            group_slots = []
            add_to_start = []
            add_to_end = []
            unmatched = []
            pnode = channode.first_node
            if len(self.child_times) > 0:
                # We filter the nodes
                self.merge_type = 2
                for pzone in self.child_times:
                    if pzone['chanid'] == channode.chanid:
                        while True:
                            if not isinstance(pnode, ProgramNode):
                                # We reached the last node
                                break

                            if pnode.stop <= pzone['start']:
                                # Before the zone
                                pnode = pnode.next
                                continue

                            if pnode.start >= pzone['stop']:
                                # We passed the zone, so go to the next
                                break

                            # Copy the node
                            cnode = pnode.copy(self)
                            if cnode.start < pzone['start']:
                                # Truncate the start
                                cnode.adjust_start(pzone['start'])

                            if cnode.stop >pzone['stop']:
                                # Truncate the end add the node and move to the next zone
                                cnode.adjust_stop(pzone['stop'])
                                if cnode.is_groupslot:
                                    group_slots.append(cnode)

                                else:
                                    programs.append(cnode)

                                break

                            # Add the node
                            if cnode.is_groupslot:
                                group_slots.append(cnode)

                            else:
                                programs.append(cnode)

                            # And go to the next node
                            pnode = pnode.next

                    if not isinstance(pnode, ProgramNode):
                        # We reached the last node
                        break
                self.get_adding_stats(programs, group_slots)

            else:
                self.merge_type = 4
                self.get_adding_stats(channode)
                while isinstance(pnode, ProgramNode):
                    if pnode.is_groupslot:
                        group_slots.append(pnode)

                    else:
                        programs.append(pnode)

                    pnode = pnode.next

            if self.program_count() == 0:
                # We add
                self.config.log(['\n', self.config.text('IO', 7, (self.config.text('IO', 3, type='stats'), \
                    self.adding_stats['count'], channode.name, self.current_stats['count'], self.name), 'stats'), \
                    self.config.text('IO', 8, (self.channel_config.counter, self.config.chan_count), 'stats')], 2)

                programs.extend(group_slots)
                programs.sort(key=lambda pnode: (pnode.start))
                self.first_node = programs[0]
                self.last_node = programs[-1]
                self.start = self.first_node.start
                self.stop = self.last_node.stop
                for index in range(len(programs) - 1):
                    gap = self.link_nodes(programs[index], programs[index + 1])
                    if gap != None:
                        self.program_gaps.append(gap)

                    self.add_new_program(programs[index])

                self.add_new_program(self.last_node)

            else:
                # Try matching on time and name or check if it falls into a groupslot, a gap or outside the range
                self.config.log(['\n', self.config.text('IO', 7, (self.config.text('IO', 4, type='stats'), \
                    self.adding_stats['count'], channode.name, self.current_stats['count'], self.name), 'stats'), \
                    self.config.text('IO', 8, (self.channel_config.counter, self.config.chan_count), 'stats')], 2)

                self.merge_type += 1
                programs.sort(key=lambda pnode: (pnode.start))
                for index in range(len(programs)):
                    for check in self.checkrange:
                        mstart = programs[index].start + datetime.timedelta(0, 0, 0, 0, check)
                        if mstart in self.programs_by_start.keys() and self.programs_by_start[mstart].match_title(programs[index].match_name):
                            # ### Check on split episodes
                            #~ l_diff = programs[index].length.total_seconds()/ self.programs_by_start[mstart].length.total_seconds()
                            #~ if l_diff >1.2 or l_diff < 1.2:
                                #~ pass

                            self.programs_by_start[mstart].add_node_data(programs[index])
                            self.add_stat('matched', 1)
                            break

                    else:
                        check_gaps(programs[index], True)

                # Check if any new groupslot falls in a detailed groupslot or outside current range or in gaps
                if len(group_slots) > 0:
                    self.program_gaps.sort(key=lambda program: (program.start))
                    group_slots.sort(key=lambda program: (program.start))
                    for index in range(len(group_slots)):
                        check_gaps(group_slots[index])

                self.add_stat('unmatched', len(unmatched))
                # And add any program found new
                for gs in self.group_slots[:]:
                    self.fill_group(gs)

                self.fill_group(add_to_start)
                self.fill_group(add_to_end)
                for pgap in self.program_gaps[:]:
                    self.fill_group(pgap)

            # Finally we check if we can add any genres
            self.check_on_missing_genres()
            self.log_merge_statistics(channode)

    def check_lineup(self, overlap_strategy = None):
        #~ self.channel_config.opt_dict['max_overlap']
        with self.node_lock:
            # We check overlap
            pass

    def link_nodes(self, node1, node2, adjust_overlap = None):
        with self.node_lock:
            if isinstance(node1, ProgramNode):
                node1.next = node2

            if isinstance(node2, ProgramNode):
                node2.previous = node1

            if not isinstance(node1, ProgramNode) or not isinstance(node2, ProgramNode):
                return None

            if abs(node2.start - node1.stop) > datetime.timedelta(minutes = self.channel_config.opt_dict['max_overlap']):
                gap = GapNode(self, node1, node2)
                return gap

            node1.next_gap = None
            node2.previous_gap = None
            if node1.stop > node2.start:
                if adjust_overlap == 'stop':
                    node1.adjust_stop(node2.start)

                elif adjust_overlap == 'start':
                    node2.adjust_start(node1.stop)

    def fill_group(self, pgrp):
        with self.node_lock:
            if isinstance(pgrp, ProgramNode):
                if len(pgrp.gs_detail) == 0:
                    return

                self.add_stat('groupslot', len(pgrp.gs_detail))
                gtype = 'gs'
                gdetail = pgrp.gs_detail
                gprevious = pgrp.previous
                gnext = pgrp.next

            elif isinstance(pgrp, GapNode):
                if len(pgrp.gap_detail) == 0:
                    return

                gtype = 'gap'
                gdetail = pgrp.gap_detail
                gprevious = pgrp.previous
                gnext = pgrp.next

            elif isinstance(pgrp, list):
                if len(pgrp) == 0:
                    return

                for pn in pgrp:
                    if not isinstance(pn, ProgramNode):
                        return

                gtype = 'list'
                gdetail = pgrp
                gprevious = None
                gnext = None

            else:
                return

            # We replace the group with the details
            gdetail.sort(key=lambda program: (program.start))
            if pgrp == self.first_node or gdetail[0].start < self.first_node.start:
                if gnext == None:
                    gnext = self.first_node

                self.first_node = gdetail[0]
                self.start = gdetail[0].start + datetime.timedelta(seconds = 5)

            if pgrp == self.last_node or gdetail[-1].stop > self.last_node.stop:
                if gprevious == None:
                    gprevious = self.last_node

                self.last_node = gdetail[-1]
                self.stop = gdetail[-1].stop - datetime.timedelta(seconds = 5)

            if gtype == 'gap':
                self.remove_gap(pgrp)

            elif gtype == 'gs':
                self.remove_gs(pgrp)
                self.remove_gap(pgrp.previous_gap)
                self.remove_gap(pgrp.next_gap)

            start_gap = self.link_nodes(gprevious, gdetail[0], 'start')
            stop_gap = self.link_nodes(gdetail[-1], gnext, 'stop')
            for index in range(1, len(gdetail)):
                gap = self.link_nodes(gdetail[index - 1], gdetail[index])
                if gap != None:
                    self.program_gaps.append(gap)

            for pn in gdetail:
                self.add_new_program(pn)

            if start_gap != None:
                self.program_gaps.append(start_gap)

            if stop_gap != None:
                self.program_gaps.append(stop_gap)

    def remove_gs(self, gs):
        if not isinstance(gs, ProgramNode):
            return

        with self.node_lock:
            if isinstance(gs.next, ProgramNode):
                gs.next.previous = None
                gs.next = None
                gs.next_gap = None

            if isinstance(gs.previous, ProgramNode):
                gs.previous.next = None
                gs.previous = None
                gs.previous_gap = None

            if gs in self.group_slots:
                self.group_slots.remove(gs)

            if gs in self.programs:
                self.programs.remove(gs)

    def remove_gap(self, pgap):
        if not isinstance(pgap, GapNode):
            return

        with self.node_lock:
            if isinstance(pgap.next, ProgramNode):
                pgap.next.previous_gap = None
                pgap.next = None

            if isinstance(pgap.previous, ProgramNode):
                pgap.previous.next_gap = None
                pgap.previous = None

            if pgap in self.program_gaps:
                self.program_gaps.remove(pgap)

    def add_new_program(self,pn):
        with self.node_lock:
            if not pn in self.programs:
                self.programs.append(pn)
                self.add_stat('new', 1)

            # Check if it has a groupslot name
            if pn.match_name in self.groupslot_names:
                self.group_slots.append(pn)
                pn.is_groupslot = True
                return

            self.programs_by_start[pn.start] = pn
            self.programs_by_stop[pn.stop] = pn
            if pn.name in self.programs_by_name.keys():
                self.programs_by_name[pn.name].append(pn)

            else:
                self.programs_by_name[pn.name] = [pn]

            if pn.match_name in self.programs_by_matchname.keys():
                self.programs_by_matchname[pn.match_name].append(pn)

            else:
                self.programs_by_matchname[pn.match_name] = [pn]

            if not pn.is_set('genre') or pn.get_value('genre').lower().strip() in ('', self.config.cattrans_unknown.lower().strip()):
                if pn.match_name in self.programs_with_no_genre.keys():
                    self.programs_with_no_genre[pn.match_name].append(pn)

                else:
                    self.programs_with_no_genre[pn.match_name] = [pn]

    def check_on_missing_genres(self):
        # Check if we can match any program without genre to one similar named with genre
        with self.node_lock:
            name_remove = []
            for k, pl in self.programs_with_no_genre.items():
                if len(pl) >= self.programs_by_matchname[k]:
                    continue

                for pn in self.programs_by_matchname[k]:
                    if pn.is_set('genre') and pn.get_value('genre').lower().strip() not in ('', self.config.cattrans_unknown.lower().strip()):
                        for pg in pl:
                            pg.set_value('genre', pn.get_value('genre'))
                            pg.set_value('subgenre', pn.get_value('subgenre'))
                            self.add_stat('genre', 1)

                        name_remove.append(k)
                        break

            for k in name_remove:
                if k in self.programs_with_no_genre.keys():
                    del self.programs_with_no_genre[k]

# end ChannelNode

class ProgramNode():
    def __init__(self, channode, source, data):
        if not isinstance(channode, ChannelNode):
            self.is_valid = False
            return

        self.node_lock = RLock()
        with self.node_lock:
            self.channode = channode
            self.config = channode.config
            self.channel_config = channode.channel_config
            self.start = None
            self.stop = None
            self.length = None
            self.name = None
            self.match_name = None
            self.previous = None
            self.next = None
            self.previous_gap = None
            self.next_gap = None
            self.is_groupslot = False
            self.gs_detail = []
            self.tdict = {}
            self.matchobject = difflib.SequenceMatcher(isjunk=lambda x: x in " '\",.-/", autojunk=False)
            self.first_source = True
            if isinstance(data, dict):
                self.is_valid =  self.add_source_data(data, source)

            else:
                self.is_valid = False

    def is_set(self, key):
        if key in self.tdict:
            return True

        if key == 'credits':
            for k in self.config.key_values['credits']:
                if k in self.tdict and len(self.tdict[k]['prime']) > 0:
                    return True

        elif key == 'video':
            for k in self.config.key_values['video']:
                if k in self.tdict:
                    return True

        return False

    def adjust_start(self, pstart):
        with self.node_lock:
            self.start = pstart.replace(second = 0, microsecond = 0)
            self.tdict['start-time']['prime'] = self.start
            self.length = self.stop - self.start
            self.tdict['length']['prime'] = self.length
            if isinstance(self.previous_gap, GapNode):
                self.previous_gap.adjust_stop(self.start)

    def adjust_stop(self, pstop):
        with self.node_lock:
            self.stop = pstop.replace(second = 0, microsecond = 0)
            self.tdict['stop-time']['prime'] = self.stop
            self.length = self.stop - self.start
            self.tdict['length']['prime'] = self.length
            if isinstance(self.next_gap, GapNode):
                self.next_gap.adjust_start(self.stop)

    def gs_start(self):
        if self.is_groupslot and isinstance(self.previous_gap, GapNode):
            return self.previous_gap.start

        else:
            return self.start

    def gs_stop(self):
        if self.is_groupslot and isinstance(self.next_gap, GapNode):
            return self.next_gap.stop

        else:
            return self.stop

    def print_start_name(self):
        pstart = self.config.output_tz.normalize(self.start.astimezone(self.config.output_tz)).strftime('%d %b %H:%M')
        return '%s: %s' % (pstart, self.name)

    def match_title(self, mname):
        if self.match_name == mname:
            return True

        if len(self.match_name) < len(mname) and self.match_name in mname:
            return True

        if len(mname) < len(self.match_name) and mname in self.match_name:
            return True

        self.matchobject.set_seqs(self.match_name, mname)
        if self.matchobject.ratio() > .8:
            return True

        return False
    def add_source_data(self, data, source):
        if not source in self.config.channelsource.keys() or not isinstance(data, dict):
            return

        with self.node_lock:
            if self.first_source:
                self.start = data['start-time'].replace(second = 0, microsecond = 0)
                if 'stop-time' in data and isinstance(data['stop-time'], datetime.datetime):
                    self.stop = data['stop-time'].replace(second = 0, microsecond = 0)
                    self.length = self.stop - self.start

                elif 'length'  in data and isinstance(data['length'], datetime.timedelta):
                    self.length = data['length']
                    self.stop = (self.start + self.length).replace(second = 0, microsecond = 0)

                else:
                    return False

                self.name = data['name']
                self.match_name = re.sub('[-,. ]', '', self.config.fetch_func.remove_accents(data['name']).lower()).strip()
            else:
                # Check if the new source is longer and if so extend over any gap
                start_diff = (self.start - data['start-time']).total_seconds() / 60
                if start_diff > 0:
                    if self.previous_gap != None:
                        if data['start-time'] <= self.previous_gap.start:
                            # We add the gap to the program
                            self.adjust_start(self.previous_gap.start.replace(second = 0, microsecond = 0))
                            self.channode.remove_gap(self.previous_gap)

                        else:
                            # We reduce the gap
                            self.adjust_start(data['start-time'].replace(second = 0, microsecond = 0))
                            self.previous_gap.adjust_stop(self.start)

                    elif self.previous == None and start_diff < self.channel_config.opt_dict['max_overlap']:
                        # It's the first program
                        self.adjust_start(data['start-time'].replace(second = 0, microsecond = 0))

                stop_diff = (data['stop-time'] - self.stop).total_seconds() / 60
                if stop_diff > 0:
                    if self.next_gap != None:
                        if data['stop-time'] >= self.next_gap.stop:
                            # We add the gap to the program
                            self.adjust_stop(self.next_gap.stop.replace(second = 0, microsecond = 0))
                            self.channode.remove_gap(self.next_gap)

                        else:
                            # We reduce the gap
                            self.adjust_stop(data['stop-time'].replace(second = 0, microsecond = 0))
                            self.next_gap.adjust_start(self.stop)

                    elif self.next == None and stop_diff < self.channel_config.opt_dict['max_overlap']:
                        # It's the last program
                        self.adjust_stop(data['stop-time'].replace(second = 0, microsecond = 0))

            self.length = self.stop - self.start
            # Check for allowed key values
            for k, v in data.items():
                if k in ('credits', 'video'):
                    for k2, v2 in v.items():
                        if k2 in self.config.key_values[k]:
                            self.set_value(k2, v2, source)

                elif k in self.channode.key_list:
                    self.set_value(k, v, source)

            self.first_source = False
            return True

    def add_node_data(self, pnode):
        if not isinstance(pnode, ProgramNode):
            return

        with self.node_lock:
            for k, v in pnode.tdict.items():
                pass

            self.first_source = False

    def set_value(self, key, value, source=None):
        def add_value(value):
            if not self.is_set(key):
                self.tdict[key] = {}
                self.tdict[key]['sources'] = {}
                self.tdict[key]['channels'] = {}
                self.tdict[key]['values'] = []
                self.tdict[key]['rank'] = []

            if source in self.config.channelsource.keys():
                self.tdict[key]['sources'][source] = value

            elif source in self.config.channels.keys():
                self.tdict[key]['channels'][source] = value

            if key in ( "ID", "prog_ID", "detail_url", "start-time", "stop-time", "length", "offset",
                            "name","episode title","originaltitle","genre", "subgenre"):
                if not 'prime' in self.tdict[key].keys():
                    self.tdict[key]['prime'] = value
                # These are further handled separately
                return

            elif key in ("actor", "guest"):
                if not 'prime names' in self.tdict[key].keys():
                    self.tdict[key]['prime names'] = []
                    self.tdict[key]['prime'] = []

                if isinstance(value, dict):
                    value = [value]

                if isinstance(value, list):
                    for v in value:
                        if v['name'].lower() in self.tdict[key]['prime names']:
                            if v['role'] == None:
                                continue

                            for index in range(len(self.tdict[key]['prime'])):
                                if self.tdict[key]['prime'][index]['name'].lower() == v['name'].lower():
                                    if self.tdict[key]['prime'][index]['role'] == None:
                                        self.tdict[key]['prime'][index]['role'] = v['role']

                        else:
                            self.tdict[key]['prime names'].append(v['name'].lower())
                            self.tdict[key]['prime'].append(v)

            elif key in self.config.key_values['credits']:
                if not 'prime' in self.tdict[key].keys():
                    self.tdict[key]['prime'] = []

                if isinstance(value, list):
                    for v in value:
                        if not v in self.tdict[key]['prime']:
                            self.tdict[key]['prime'].append(v)

                elif not value in self.tdict[key]['prime']:
                    self.tdict[key]['prime'].append(value)

            elif key in ("country", "rating"):
                if not 'prime' in self.tdict[key].keys():
                    self.tdict[key]['prime'] = []

                if isinstance(value, list):
                    for v in value:
                        if not v.lower() in self.tdict[key]['prime']:
                            self.tdict[key]['prime'].append(v.lower())

                elif not value.lower() in self.tdict[key]['prime']:
                    self.tdict[key]['prime'].append(value.lower())

            elif key == 'description':
                if not 'prime' in self.tdict[key].keys() or len(value) > len(self.tdict[key]['prime']):
                    self.tdict[key]['prime'] = value

                if source != None and self.channel_config.opt_dict['prefered_description'] == source:
                    self.tdict[key]['preferred'] = value

            elif key in self.config.key_values['bool'] or key in self.config.key_values['video']:
                if not 'prime' in self.tdict[key].keys():
                    self.tdict[key]['prime'] = value

                elif value:
                    self.tdict[key]['prime'] = True

            else:
                # Get the most common value
                if not 'prime' in self.tdict[key].keys():
                    self.tdict[key]['prime'] = value
                    self.tdict[key]['values'].append(value)
                    self.tdict[key]['rank'].append(1)

                else:
                    if not value in self.tdict[key]['values']:
                        self.tdict[key]['values'].append(value)
                        self.tdict[key]['rank'].append(1)

                    else:
                        for index in range(len(self.tdict[key]['values'])):
                            if value == self.tdict[key]['values'][index]:
                                self.tdict[key]['rank'][index] += 1
                                break

                    vcnt = 0
                    for index in range(len(self.tdict[key]['values'])):
                        if self.tdict[key]['rank'][index] > vcnt:
                            if key in ('season', 'episode') and self.tdict[key]['values'][index] == 0:
                                continue

                            vcnt= self.tdict[key]['rank'][index]
                            self.tdict[key]['prime'] = self.tdict[key]['values'][index]

        with self.node_lock:
            # validate the values
            if value in ('', None) or (key == 'genre' and value.lower().strip() == self.config.cattrans_unknown.lower().strip()):
                return

            if key in self.config.key_values['text']:
                if isinstance(value, list):
                    if len(value) == 0:
                        return

                    elif len(value) == 1:
                        value = value[0]

                    else:
                        for item in range(len(value)):
                            if isinstance(value[item], str):
                                value[item] = unicode(value[item])

                if isinstance(value, str):
                    value = unicode(value)

                if key == 'country':
                    rlist = []
                    if isinstance(value, unicode):
                        cd = re.split('[,()/]', re.sub('\.', '', value).upper())
                        for cstr in cd:
                            if cstr in self.config.coutrytrans.values():
                                rlist.append(cstr)

                            elif cstr in self.config.coutrytrans.keys():
                                rlist.append(self.config.coutrytrans[cstr])

                            elif self.config.write_info_files:
                                self.config.infofiles.addto_detail_list(u'new country => %s' % (cstr))

                    elif isinstance(value, (list,tuple)):
                        for item in value:
                            if not isinstance(item, unicode):
                                continue

                            cd = re.split('[,()/]', re.sub('\.', '', item).upper())
                            for cstr in cd:
                                if cstr == '':
                                    continue

                                if cstr in self.config.coutrytrans.values():
                                    rlist.append(cstr)

                                elif cstr in self.config.coutrytrans.keys():
                                    rlist.append(self.config.coutrytrans[cstr])

                                elif self.config.write_info_files:
                                    self.config.infofiles.addto_detail_list(u'new country => %s' % (cstr))

                    if len(rlist) > 0:
                        add_value(rlist)

                    return

                elif key == 'premiere year':
                    value = re.sub('[()]', '', value).strip()
                    if isinstance(value, unicode) and len(value) == 4:
                        try:
                            x = int(value)
                            add_value(value)

                        except:
                            return

                    return

                elif key == 'broadcaster':
                    add_value(re.sub('[()]', '', value).strip())
                    return

                elif key == 'description':
                    add_value(value)
                    return

            elif key in self.config.key_values['datetime']:
                if not isinstance(value, datetime.datetime):
                    return

            elif key in self.config.key_values['timedelta']:
                if not isinstance(value, datetime.timedelta):
                    return

            elif key in self.config.key_values['date']:
                if not isinstance(value, datetime.date):
                    return

            elif key in self.config.key_values['bool'] or key in self.config.key_values['video']:
                if not isinstance(value, bool):
                    return

            elif key in self.config.key_values['int']:
                if not isinstance(value, int):
                    return

            add_value(value)

    def get_value(self, key):
        if key in self.tdict:
            if key == 'description' and 'preferred' in self.tdict[key] and len(self.tdict[key]['preferred']) > 100:
                return self.tdict[key]['preferred']

            if key == 'country' and isinstance(self.tdict[key]['prime'], list):
                if len(self.tdict[key]['prime']) > 0:
                    return self.tdict[key]['prime'][0]

                else:
                    return ''

            return self.tdict[key]['prime']

        if key == 'genre':
            return self.config.cattrans_unknown.lower().strip()

        elif key in self.config.key_values['text']:
            return u''

        elif key in self.config.key_values['timedelta']:
            return datetime.timedelta(0)

        elif key in self.config.key_values['bool'] or key in self.config.key_values['video']:
            return False

        elif key in self.config.key_values['int']:
            return 0

        else:
            return u''

    def get_title(self):
        with self.node_lock:
            pass

    def get_genre(self):
        with self.node_lock:
            g = self.get_value('genre')
            sg = self.get_value('subgenre')
            if self.channel_config.opt_dict['cattrans']:
                cat0 = ('', '')
                cat1 = (g.lower(), '')
                cat2 = (g.lower(), sg.lower())
                if cat2 in self.config.cattrans.keys() and self.config.cattrans[cat2] != '':
                    cat = self.config.cattrans[cat2].capitalize()

                elif cat1 in self.config.cattrans.keys() and self.config.cattrans[cat1] != '':
                    cat = self.config.cattrans[cat1].capitalize()

                elif cat0 in self.config.cattrans.keys() and self.config.cattrans[cat0] != '':
                   cat = self.config.cattrans[cat0].capitalize()

                else:
                    cat = 'Unknown'

                return cat

            elif g == '':
                return self.config.cattrans_unknown.capitalize()

            else:
                return g.capitalize()

    def get_description(self):
        desc_line = u''
        with self.node_lock:
            if self.is_set('subgenre'):
                sg = self.get_value('subgenre')
                if sg != '':
                    desc_line = u'%s: ' % (sg)

            if self.is_set('broadcaster'):
                bc = self.get_value('broadcaster')
                if bc != '':
                    desc_line = u'%s(%s) ' % (desc_line, bc)

            if'description' in self.tdict:
                if 'preferred' in self.tdict['description'] and len(self.tdict['description']['preferred']) > 100:
                    description = self.tdict['description']['preferred']

                else:
                    description = self.tdict['description']['prime']

                desc_line = u'%s%s ' % (desc_line, description)

            # Limit the length of the description
            if desc_line != '':
                desc_line = re.sub('\n', ' ', desc_line)
                if len(desc_line) > self.channel_config.opt_dict['desc_length']:
                    spacepos = desc_line[0:self.channel_config.opt_dict['desc_length']-3].rfind(' ')
                    desc_line = desc_line[0:spacepos] + '...'

        return desc_line.strip()

    def copy(self, channode):
        if not isinstance(channode, ChannelNode):
            return

        with self.node_lock:
            new_pnode = ProgramNode(channode, None, None)
            new_pnode.tdict = copy.deepcopy(self.tdict)
            new_pnode.start = new_pnode.tdict['start-time']['prime']
            new_pnode.stop = new_pnode.tdict['stop-time']['prime']
            new_pnode.length = new_pnode.tdict['length']['prime']
            new_pnode.name = copy.copy(self.name)
            new_pnode.match_name = copy.copy(self.match_name)
            new_pnode.is_groupslot = self.is_groupslot
            new_pnode.gs_detail = copy.deepcopy(self.gs_detail)
            new_pnode.first_source = False
            new_pnode.is_valid = True
            return new_pnode

# end ProgramNode

class GapNode():
    def __init__(self, channode, previous_node, next_node):
        if not isinstance(channode, ChannelNode):
            self.is_valid = False
            return

        self.node_lock = RLock()
        with self.node_lock:
            self.channode = channode
            self.config = channode.config
            self.channel_config = channode.channel_config
            self.previous = previous_node
            self.next = next_node
            self.gap_detail = []
            self.is_overlap = False
            if isinstance(self.previous, ProgramNode):
                self.start = self.previous.stop
                self.previous.next_gap = self

            if isinstance(self.next, ProgramNode):
                self.adjust_stop(self.next.start)
                self.next.previous_gap = self

    def adjust_start(self, pstart):
        with self.node_lock:
            self.start = pstart.replace(second = 0, microsecond = 0)
            self.length = None
            self.length_in_min = None
            self.is_overlap = False
            if isinstance(self.start, datetime.datetime) and isinstance(self.stop, datetime.datetime):
                self.length = self.stop - self.start
                self.length_in_min = abs(self.length.total_seconds() / 60)
                if self.stop < self.start:
                    self.is_overlap = True

    def adjust_stop(self, pstop):
        with self.node_lock:
            self.stop = pstop.replace(second = 0, microsecond = 0)
            self.length = None
            self.length_in_min = None
            self.is_overlap = False
            if isinstance(self.start, datetime.datetime) and isinstance(self.stop, datetime.datetime):
                self.length = self.stop - self.start
                self.length_in_min = abs(self.length.total_seconds() / 60)
                if self.stop < self.start:
                    self.is_overlap = True

# end GapNode

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
            self.raw_output =  self.functions.open_file(self.config.opt_dict['xmltv_dir']+'/raw_output', 'w')

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

    def write_fetch_list(self, programs, chanid, source, chan_name = '', sid = None, ismerge = False):
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
                return self.config.output_tz.normalize(tdict[vname].astimezone(self.config.output_tz)).strftime('%d %b %H:%M')

            if isinstance(tdict[vname], bool):
                if tdict[vname]:
                    return 'True '

                return 'False '

            return tdict[vname]

        if (not self.write_info_files) or (self.fetch_list == None):
            return

        with self.info_lock:
            plist = copy.deepcopy(programs)
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
                extra = value('rerun') + value('teletext') + value('new') + value('last-chance') + value('premiere')
                extra2 = value('HD') + value('widescreen') + value('blackwhite')

                self.fetch_strings[chanid][source] += u'  %s-%s: [%s][%s] %s: %s [%s] [%s]\n' % (\
                                value('start-time'), value('stop-time'), \
                                value('ID').rjust(15), value('genre')[0:10].rjust(10), \
                                value('name'), value('episode title'), \
                                extra, extra2)

                #~ self.fetch_strings[chanid][source] += u'  %s-%s: [%s][%s] %s: %s [%s/%s]\n' % (\
                                #~ self.config.output_tz.normalize(tdict['start-time'].astimezone(self.config.output_tz)).strftime('%d %b %H:%M'), \
                                #~ self.config.output_tz.normalize(tdict['stop-time'].astimezone(self.config.output_tz)).strftime('%d %b %H:%M'), \
                                #~ psid.rjust(15), tdict['genre'][0:10].rjust(10), \
                                #~ tdict['name'], tdict['episode title'], \
                                #~ tdict['season'], tdict['episode'])

            if ismerge: self.fetch_strings[chanid][source] += u'#\n'

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
            f = self.functions.open_file(self.config.opt_dict['xmltv_dir']+'/detail_output')
            if (f != None):
                f.seek(0,0)
                for byteline in f.readlines():
                    line = self.functions.get_line(f, byteline, False)
                    if line:
                        self.detail_list.append(line)

                f.close()

            f = self.functions.open_file(self.config.opt_dict['xmltv_dir']+'/detail_output', 'w')
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

class XMLoutput():
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
        self.logo_provider = {}

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

    def format_timezone(self, td, only_date=False ):
        """
        Given a datetime object, returns a string in XMLTV format
        """
        if not self.config.opt_dict['use_utc']:
            td = self.config.output_tz.normalize(td.astimezone(self.config.output_tz))

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
        self.xml_channels[xmltvid].append(self.add_starttag('display-name', 4, 'lang="%s"' % (self.config.xml_language), \
            self.config.channels[chanid].chan_name, True))
        if (self.config.channels[chanid].opt_dict['logos']):
            if self.config.channels[chanid].icon_source in self.logo_provider.keys():
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
        #~ self.config.channels[chanid].all_programs.sort(key=lambda program: (program['start-time'],program['stop-time']))
        #~ for program in self.config.channels[chanid].all_programs[:]:
        channel_node = self.config.channels[chanid].channel_node
        if not isinstance(channel_node, ChannelNode):
            return
        program = channel_node.first_node
        while isinstance(program, ProgramNode):
            xml = []

            # Start/Stop
            attribs = 'start="%s" stop="%s" channel="%s%s"' % \
                (self.format_timezone(program.start), self.format_timezone(program.stop), \
                xmltvid, self.config.channels[chanid].opt_dict['compat'] and self.config.compat_text or '')

            #~ if 'clumpidx' in program and program['clumpidx'] != '':
                #~ attribs += 'clumpidx="%s"' % program['clumpidx']

            xml.append(self.add_starttag('programme', 2, attribs))

            # Title
            xml.append(self.add_starttag('title', 4, 'lang="%s"' % (self.config.xml_language), program.name, True))
            if program.is_set('originaltitle') and program.is_set('country') :
                xml.append(self.add_starttag('title', 4, 'lang="%s"' % (program.get_value('country').lower()), program.get_value('originaltitle'), True))

            # Subtitle
            if program.is_set('episode title') and program.get_value('episode title') != program.name:
                xml.append(self.add_starttag('sub-title', 4, 'lang="%s"' % (self.config.xml_language), program.get_value('episode title') ,True))

            # Description
            desc_line = program.get_description()
            if desc_line != '':
                xml.append(self.add_starttag('desc', 4, 'lang="%s"' % (self.config.xml_language), desc_line,True))

            # Process credits section if present.
            # This will generate director/actor/presenter info.
            if program.is_set('credits'):
                xml.append(self.add_starttag('credits', 4))
                for role in self.config.key_values['credits']:
                    if program.is_set(role):
                        rlist = program.get_value(role)
                        for name in rlist:
                            if isinstance(name, dict) and 'name'in name:
                                xml.append(self.add_starttag((role), 6, '', self.xmlescape(name['name']),True))

                            elif name != '':
                                xml.append(self.add_starttag((role), 6, '', self.xmlescape(name),True))

                xml.append(self.add_endtag('credits', 4))

            # Original Air-Date
            if program.is_set('airdate'):
                xml.append(self.add_starttag('date', 4, '',  \
                    self.format_timezone(program.get_value('airdate'),True), True))

            elif program.is_set('premiere year'):
                xml.append(self.add_starttag('date', 4, '', program.get_value('premiere year'),True))

            # Genre
            cat = program.get_genre()
            if self.config.channels[chanid].opt_dict['cattrans']:
                xml.append(self.add_starttag('category', 4 , '', cat, True))

            else:
                xml.append(self.add_starttag('category', 4, 'lang="%s"' % (self.config.xml_language), cat, True))

            # An available url
            if program.is_set('infourl'):
                xml.append(self.add_starttag('url', 4, '', program.get_value('infourl'),True))

            # A Country
            if program.is_set('country'):
                xml.append(self.add_starttag('country', 4, '', program.get_value('country').upper(),True))

            # Only add season/episode if relevant. i.e. Season can be 0 if it is a pilot season, but episode never.
            # Also exclude Sports for MythTV will make it into a Series
            if program.is_set('season') and program.is_set('episode') and cat.lower() not in self.config.episode_exclude_genres:
                se = program.get_value('season')
                ep = program.get_value('episode')
                if se != 0 and ep != 0:
                    if se == 0:
                        text = ' . %d . '  % (ep - 1)

                    else:
                        text = '%d . %d . '  % (se - 1, ep - 1)

                    xml.append(self.add_starttag('episode-num', 4, 'system="xmltv_ns"', text,True))

            # Process video/audio/teletext sections if present
            if program.get_value('widescreen') or program.get_value('blackwhite') \
              or (program.get_value('HD') and (self.config.channels[chanid].opt_dict['mark_hd'] or add_HD == True)):
                xml.append(self.add_starttag('video', 4))

                if program.get_value('widescreen'):
                    xml.append(self.add_starttag('aspect', 6, '', '16:9',True))

                if program.get_value('blackwhite'):
                    xml.append(self.add_starttag('colour', 6, '', 'no',True))

                if program.get_value('HD') and (self.config.channels[chanid].opt_dict['mark_hd'] or add_HD == True):
                    xml.append(self.add_starttag('quality', 6, '', 'HDTV',True))

                xml.append(self.add_endtag('video', 4))

            if program.is_set('audio'):
                xml.append(self.add_starttag('audio', 4))
                xml.append(self.add_starttag('stereo', 6, '',program.get_value('audio') ,True))
                xml.append(self.add_endtag('audio', 4))

            # It's been shown before
            if program.get_value('rerun'):
                xml.append(self.add_starttag('previously-shown', 4, '', '',True))

            # It's a first
            if program.get_value('premiere'):
                xml.append(self.add_starttag('premiere', 4, '', '',True))

            # It's the last showing
            if program.get_value('last-chance'):
                xml.append(self.add_starttag('last-chance', 4, '', '',True))

            # It's new
            if program.get_value('new'):
                xml.append(self.add_starttag('new', 4, '', '',True))

            # There are teletext subtitles
            if program.get_value('teletext'):
                xml.append(self.add_starttag('subtitles', 4, 'type="teletext"', '',True))

            # Add any rating items
            if program.is_set('rating') and self.config.opt_dict['kijkwijzerstijl'] in ('long', 'short', 'single'):
                pr = program.get_value('rating')
                kstring = ''
                # First only one age limit from high to low
                for k in self.config.rating['unique_codes'].keys():
                    if k in pr:
                        if self.config.opt_dict['kijkwijzerstijl'] == 'single':
                            kstring += (self.config.rating['unique_codes'][k]['code'] + ': ')

                        else:
                            xml.append(self.add_starttag('rating', 4, 'system="%s"' % (self.config.rating['name'])))
                            if self.config.opt_dict['kijkwijzerstijl'] == 'long':
                                xml.append(self.add_starttag('value', 6, '', self.config.rating['unique_codes'][k]['text'], True))

                            else:
                                xml.append(self.add_starttag('value', 6, '', self.config.rating['unique_codes'][k]['code'], True))

                            xml.append(self.add_starttag('icon', 6, 'src="%s"' % self.config.rating['unique_codes'][k]['icon'], '', True))
                            xml.append(self.add_endtag('rating', 4))
                        break

                # And only one of any of the others
                for k in self.config.rating['addon_codes'].keys():
                    if k in pr:
                        if self.config.opt_dict['kijkwijzerstijl'] == 'single':
                            kstring += k.upper()

                        else:
                            xml.append(self.add_starttag('rating', 4, 'system="%s"' % (self.config.rating['name'])))
                            if self.config.opt_dict['kijkwijzerstijl'] == 'long':
                                xml.append(self.add_starttag('value', 6, '', self.config.rating['addon_codes'][k]['text'], True))

                            else:
                                xml.append(self.add_starttag('value', 6, '', self.config.rating['addon_codes'][k]['code'], True))

                            xml.append(self.add_starttag('icon', 6, 'src="%s"' % self.config.rating['addon_codes'][k]['icon'], '', True))
                            xml.append(self.add_endtag('rating', 4))

                if self.config.opt_dict['kijkwijzerstijl'] == 'single' and kstring != '':
                    xml.append(self.add_starttag('rating', 4, 'system="%s"' % (self.config.rating['name'])))
                    xml.append(self.add_starttag('value', 6, '', kstring, True))
                    xml.append(self.add_endtag('rating', 4))

            # Set star-rating if applicable
            if program.is_set('star-rating'):
                xml.append(self.add_starttag('star-rating', 4))
                xml.append(self.add_starttag('value', 6, '',('%s/10' % (program.get_value('star-rating'))).strip(),True))
                xml.append(self.add_endtag('star-rating', 4))

            xml.append(self.add_endtag('programme', 2))
            self.xml_programs[xmltvid].append(xml)
            program = program.next

    def get_xmlstring(self):
        '''
        Compound the compleet XML output and return it
        '''
        if self.config.output == None:
            startstring =[u'<?xml version="1.0" encoding="%s"?>\n' % self.config.logging.local_encoding]

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
                sys.stdout.write(xml.encode(self.config.logging.local_encoding, 'replace'))

            else:
                self.config.output.write(xml)

            if self.config.write_info_files:
                self.config.infofiles.write_xmloutput(xml)

# end XMLoutput
