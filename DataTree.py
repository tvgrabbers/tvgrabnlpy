#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import re, sys, traceback
import time, datetime, pytz
from threading import RLock
try:
    from html.parser import HTMLParser, HTMLParseError
except ImportError:
    from HTMLParser import HTMLParser, HTMLParseError

try:
    from html.entities import name2codepoint
except ImportError:
    from htmlentitydefs import name2codepoint

dt_name = u'DataTree'
dt_major = 1
dt_minor = 0
dt_patch = 0
dt_patchdate = u'20160512'
dt_alfa = True
dt_beta = True

def version():
    return (dt_name, dt_major, dt_minor, dt_patch, dt_patchdate, dt_beta, dt_alfa)
# end version()

class NULLnode():
    value = None

# end NULLnode

class DATAnode():
    def __init__(self, dtree, parent = None):
        self.node_lock = RLock()
        with self.node_lock:
            self.children = []
            self.dtree = dtree
            self.parent = parent
            self.value = None
            self.child_index = 0
            self.level = 0
            self.link_value = {}

            self.is_root = bool(self.parent == None)
            n = self
            while not n.is_root:
                n = n.parent

            self.root = n
            if isinstance(parent, DATAnode):
                self.parent.append_child(self)
                self.level = parent.level + 1

    def append_child(self, node):
        with self.node_lock:
            node.child_index = len(self.children)
            self.children.append(node)

    def get_children(self, data_def = None, link_values=None):
        childs = []
        if not isinstance(link_values, dict):
            link_values = {}

        d_def = data_def if isinstance(data_def, list) else [data_def]
        if len(d_def) == 0 or d_def[0] == None:
            # It's not a child definition
            if self.dtree.show_result:
                self.dtree.print_text(u'    adding node %s\n'.encode('utf-8', 'replace') % (self.print_node()))
            return [self]

        nm = self.find_name(d_def[0])
        if self.match_node(node_def = d_def[0], link_values=link_values) == None:
            # It's not a child definition
            if len(d_def) == 1:
                if self.dtree.show_result:
                    self.dtree.print_text(u'    adding node %s; %s\n'.encode('utf-8', 'replace') % (self.print_node(), d_def[0]))

                if nm == None:
                    return [self]

                else:
                    return [{nm:self}]

            else:
                if len(self.link_value) > 0:
                    for k, v in self.link_value.items():
                        link_values[k] = v

                self.link_value = {}
                childs = self.get_children(data_def = d_def[1:], link_values=link_values)
                if nm == None:
                    return childs

                else:
                    return self.tag,{nm:childs}

        elif self.dtree.is_data_value('path', None, d_def[0]):
            sel_val = d_def[0]['path']
            if sel_val == 'parent' and not self.is_root:
                if self.dtree.show_result:
                    self.dtree.print_text(u'  found node %s; %s\n'.encode('utf-8', 'replace') % (self.parent.print_node(), d_def[0]))
                self.parent.match_node(node_def = d_def[0], link_values=link_values)
                if len(self.parent.link_value) > 0:
                    for k, v in self.parent.link_value.items():
                        link_values[k] = v

                self.parent.link_value = {}
                childs = self.parent.get_children(data_def = d_def[1:], link_values=link_values)
                if nm == None:
                    return childs

                else:
                    return {nm:childs}

            elif sel_val == 'root':
                if self.dtree.show_result:
                    self.dtree.print_text(u'  found node %s; %s\n'.encode('utf-8', 'replace') % (self.root.print_node(), d_def[0]))
                self.root.match_node(node_def = d_def[0], link_values=link_values)
                if len(self.root.link_value) > 0:
                    for k, v in self.root.link_value.items():
                        link_values[k] = v

                self.root.link_value = {}
                childs = self.root.get_children(data_def = d_def[1:], link_values=link_values)
                if nm == None:
                    return childs

                else:
                    return {nm:childs}

            elif sel_val == 'all':
                for item in self.children:
                    if self.dtree.show_result:
                        self.dtree.print_text(u'  found node %s; %s\n'.encode('utf-8', 'replace') % (item.print_node(), d_def[0]))
                    item.match_node(node_def = d_def[0], link_values=link_values)
                    if len(item.link_value) > 0:
                        for k, v in item.link_value.items():
                            link_values[k] = v

                    item.link_value = {}
                    jl = item.get_children(data_def = d_def[1:], link_values=link_values)
                    if isinstance(jl, list):
                        childs.extend(jl)

                    elif jl != None:
                        childs.append(jl)

                if nm == None:
                    return childs

                else:
                    return {nm:childs}

        else:
            for item in self.children:
                # We look for matching children
                if item.match_node(node_def = d_def[0], link_values=link_values):
                    # We found a matching child
                    if self.dtree.show_result:
                        self.dtree.print_text(u'  found node %s; %s\n'.encode('utf-8', 'replace') % (item.print_node(), d_def[0]))
                    if len(item.link_value) > 0:
                        for k, v in item.link_value.items():
                            link_values[k] = v

                    item.link_value = {}
                    jl = item.get_children(data_def = d_def[1:], link_values=link_values)
                    if isinstance(jl, list):
                        childs.extend(jl)

                    elif jl != None:
                        childs.append(jl)

            if nm == None:
                return childs

            else:
                return {nm:childs}

        #~ else:
            #~ if self.dtree.show_result:
                #~ self.dtree.print_text(u'    adding node %s; %s\n'.encode('utf-8', 'replace') % (self.print_node(), d_def[0]))
            #~ return [self]

        if nm == None:
            return childs

        else:
            return {nm:childs}

    def check_for_linkrequest(self, node_def):
        if self.dtree.is_data_value('link', int, node_def):
            self.link_value[node_def['link']] = self.find_value(node_def)
            if self.dtree.show_result:
                self.dtree.print_text(u'    saving link to node %s: %s\n'.encode('utf-8', 'replace') % (self.find_value(node_def), self.print_node()))

    def match_node(self, node_def = None, link_values = None):
        self.link_value = {}
        return False

    def find_name(self, node_def):
        return None

    def find_value(self, node_def = None):
        return self.dtree.calc_value(self.value, node_def)

    def print_node(self, print_all = False):
        return u'%s = %s' % (self.level, self.find_value())

    def print_tree(self):
        sstr =u'%s%s\n' % (self.dtree.get_leveltabs(self.level,4), self.print_node(True))
        self.dtree.print_text(sstr)
        for n in self.children:
            n.print_tree()

# end DATAnode

class HTMLnode(DATAnode):
    def __init__(self, dtree, data = None, parent = None):
        self.tag = u''
        self.text = u''
        self.tail = u''
        self.attributes = {}
        DATAnode.__init__(self, dtree, parent)
        if isinstance(data, (str, unicode)):
            self.tag = data.lower()

        elif isinstance(data, list):
            if len(data) > 0:
                self.tag = data[0].lower()

            if len(data) > 1 and isinstance(data[1], (list, tuple)):
                for a in data[1]:
                    self.attributes[a[0].lower()] = a[1]

    def get_attribute(self, name):
        if name.lower() in self.attributes.keys():
            return self.attributes[name.lower()]

        return None

    def is_attribute(self, name, value = None):
        if name.lower() in self.attributes.keys():
            if value == None or value.lower() == self.attributes[name.lower()].lower():
                return True

        return False

    def get_child(self, tag = None, attributes = None):
        childs = []
        if not isinstance(attributes,list):
            attributes = []

        for c in self.children:
            if c.match_node(tag, attributes):
                childs.append(c)

        return childs

    def match_node(self, tag = None, attributes = None, node_def = None, link_values=None):
        self.link_value = {}
        if not isinstance(attributes,list):
            attributes = []

        if not isinstance(link_values, dict):
            link_values ={}

        if node_def == None:
            if tag.lower() in (None, self.tag.lower()):
                if attributes == None:
                    return True

                if not isinstance(attributes, dict):
                    return False

                for a, v in attributes.items():
                    if not self.is_attribute(a, v):
                        return False

                return True

            else:
                return False

        elif self.dtree.is_data_value('tag', None, node_def):
            if node_def['tag'].lower() in (None, self.tag.lower()):
                # The tag matches
                if self.dtree.is_data_value(['index','link'], int, node_def):
                    # There is an index request to an earlier linked index
                    il = link_values[self.dtree.data_value(['index','link'], int, node_def)]
                    clist = self.dtree.data_value(['index','calc'], list, node_def)
                    if len(clist) == 2 and isinstance(clist[1], int):
                        if clist[0] == 'min':
                            il -= clist[1]

                        elif clist[0] == 'plus':
                            il += clist[1]

                    if self.child_index != il:
                        return False

                elif self.dtree.is_data_value(['index'], int, node_def):
                    # There is an index request to a set value
                    if self.child_index != self.dtree.data_value(['index'], int, node_def):
                        return False

            else:
                return False

        elif self.dtree.is_data_value(['index'], None, node_def):
            if self.dtree.is_data_value(['index','link'], int, node_def):
                # There is an index request to an earlier linked index
                il = link_values[self.dtree.data_value(['index','link'], int, node_def)]
                clist = self.dtree.data_value(['index','calc'], list, node_def)
                if len(clist) == 2 and isinstance(clist[1], int):
                    if clist[0] == 'min':
                        il -= clist[1]

                    elif clist[0] == 'plus':
                        il += clist[1]

                if self.child_index != il:
                    return False

            elif self.dtree.is_data_value(['index'], int, node_def):
                # There is an index request to a set value
                if self.child_index != self.dtree.data_value(['index'], int, node_def):
                    return False

            else:
                return False

        elif self.dtree.is_data_value('path', None, node_def):
            self.check_for_linkrequest(node_def)
            return False

        else:
            self.check_for_linkrequest(node_def)
            return None

        if self.dtree.is_data_value('text', str, node_def):
            if node_def['text'].lower() != self.text.lower():
                return False

        if self.dtree.is_data_value('tail', str, node_def):
            if node_def['tail'].lower() != self.tail.lower():
                return False

        if not self.dtree.is_data_value('attrs', dict, node_def):
            # And there are no attrib matches requested
            self.check_for_linkrequest(node_def)
            return True

        for a, v in node_def['attrs'].items():
            if self.dtree.is_data_value('not', list, v):
                # There is a negative attrib match requested
                for val in v['not']:
                    if self.is_attribute(a) and self.attributes[a] == val:
                        return False

            elif self.dtree.is_data_value('link', int, v):
                # The requested value is in link_values
                if not self.is_attribute(a, link_values[v["link"]]):
                    return False

            elif not self.is_attribute(a, v):
                return False

        self.check_for_linkrequest(node_def)
        return True

    def find_name(self, node_def):
        sv = None
        if self.dtree.is_data_value('name', dict, node_def):
            if self.dtree.is_data_value(['name','select'], str, node_def):
                if node_def[ 'name']['select'] == 'tag':
                    sv = self.tag

                elif node_def[ 'name']['select'] == 'text':
                    sv = self.text

                elif node_def[ 'name']['select'] == 'tail':
                    sv = self.tail

            elif self.dtree.is_data_value(['name','attr'], str, node_def):
                sv = self.get_attribute(node_def['name'][ 'attr'].lower())

        if sv != None:
            return self.dtree.calc_value(sv, node_def['name'])

    def find_value(self, node_def = None):
        if self.dtree.is_data_value('value', None, node_def):
            sv = node_def['value']

        elif self.dtree.is_data_value('attr', str, node_def):
            sv = self.get_attribute(node_def[ 'attr'].lower())

        elif self.dtree.is_data_value('select', str, node_def):
            if node_def[ 'select'] == 'index':
                sv = self.child_index

            elif node_def[ 'select'] == 'tag':
                sv = self.tag

            elif node_def[ 'select'] == 'text':
                sv = self.text

            elif node_def[ 'select'] == 'tail':
                sv = self.tail

            elif node_def[ 'select'] == 'presence':
                return True

            else:
                sv = self.text

        else:
            sv = self.text

        return self.dtree.calc_value(sv, node_def)

    def print_node(self, print_all = False):
        attributes = u''
        spc = self.dtree.get_leveltabs(self.level,4)
        if len(self.attributes) > 0:
            for a, v in self.attributes.items():
                vv = v
                if isinstance(v, (str,unicode)):
                    vv = re.sub('\r','', v)
                    vv = re.sub('\n', ' ', vv)
                attributes = u'%s%s = "%s",\n    %s' % (attributes, a, vv, spc)
            attributes = attributes[:-(len(spc)+6)]

        rstr = u'%s: %s(%s)' % (self.level, self.tag, attributes)
        if print_all:
            if self.text != '':
                rstr = u'%s\n    %stext: %s' % (rstr, spc, self.text)

            if self.tail != '':
                rstr = u'%s\n    %stail: %s' % (rstr, spc, self.tail)

        else:
            tx = self.find_value()
            if tx != "":
                rstr = u'%s\n    %s%s' % (rstr, spc, tx)

        return rstr
# end HTMLnode

class JSONnode(DATAnode):
    def __init__(self, dtree, data = None, parent = None, key = None):
        self.type = "value"
        self.key = key
        self.keys = []
        self.key_index = {}
        self.value = None
        DATAnode.__init__(self, dtree, parent)
        if isinstance(data, list):
            self.type = "list"
            for k in range(len(data)):
                JSONnode(self.dtree, data[k], self, k)

        elif isinstance(data, dict):
            self.type = "dict"
            for k, item in data.items():
                JSONnode(self.dtree, item, self, k)

        else:
            self.type = "value"
            self.value = data

    def append_child(self, node):
        with self.node_lock:
            node.child_index = len(self.children)
            self.key_index[node.key] = node.child_index
            self.children.append(node)
            self.keys.append(node.key)

    def get_child(self, key):
        if key in self.keys:
            return self.children[self.key_index[key]]

        return None

    def match_node(self, node_def = None, link_values = None):
        self.link_value = {}
        if not isinstance(link_values, dict):
            link_values ={}

        if self.dtree.is_data_value('key', None, node_def):
            if self.key == node_def["key"]:
                # The requested key matches
                self.check_for_linkrequest(node_def)
                return True

            return False

        elif self.dtree.is_data_value('keys', list, node_def):
            if self.key in node_def['keys']:
                # This key is in the list with requested keys
                self.check_for_linkrequest(node_def)
                return True

            return False

        elif self.dtree.is_data_value('keys', dict, node_def):
            # Does it contain the requested key/value pairs
            for item, v in node_def["keys"].items():
                if not item in self.keys:
                    return False

                val = v
                if self.dtree.is_data_value('link', int, v) and v["link"] in link_values.keys():
                    # The requested value is in link_values
                    val = link_values[v["link"]]

                if self.get_child(item).value != val:
                    return False

            self.check_for_linkrequest(node_def)
            return True

        elif self.dtree.is_data_value(['index','link'], int, node_def):
            # There is an index request to an earlier linked index
            il = link_values[self.dtree.data_value(['index','link'], int, node_def)]
            clist = self.dtree.data_value(['index','calc'], list, node_def)
            if len(clist) == 2 and isinstance(clist[1], int):
                if clist[0] == 'min':
                    il -= clist[1]

                elif clist[0] == 'plus':
                    il += clist[1]

            if self.child_index == il:
                return True

            else:
                return False

        elif self.dtree.is_data_value(['index'], int, node_def):
            # There is an index request to a set value
            if self.child_index == self.dtree.data_value(['index'], int, node_def):
                self.check_for_linkrequest(node_def)
                return True

            else:
                return False

        elif self.dtree.is_data_value('path', None, node_def):
            self.check_for_linkrequest(node_def)
            return False

        else:
            self.check_for_linkrequest(node_def)
            return None

    def find_name(self, node_def):
        sv = None
        if self.dtree.is_data_value('name', dict, node_def):
            if self.dtree.is_data_value(['name','select'], str, node_def):
                if node_def[ 'name']['select'] == 'key':
                    sv = self.key

                elif node_def[ 'name']['select'] == 'value':
                    sv = self.value

        if sv != None:
            return self.dtree.calc_value(sv, node_def[ 'name'])

    def find_value(self, node_def = None):
        if self.dtree.is_data_value('value', None, node_def):
            sv = node_def['value']

        elif self.dtree.is_data_value('select', None, node_def):
            if node_def[ 'select'] == 'index':
                sv = self.child_index

            elif node_def[ 'select'] == 'key':
                sv = self.key

            elif node_def[ 'select'] == 'value':
                sv = self.value

            elif node_def[ 'select'] == 'presence':
                return True

            else:
                sv = self.value

        else:
            sv = self.value

        return self.dtree.calc_value(sv, node_def)

    def print_node(self, print_all = False):
        value = self.find_value() if self.type == "value" else '"%s"' % self.type
        return u'%s = %s' % (self.key, value)

# end JSONnode

class DATAtree():
    def __init__(self, output = sys.stdout):
        self.tree_lock = RLock()
        with self.tree_lock:
            self.print_searchtree = False
            self.show_result = False
            self.fle = output
            self.extract_from_parent = False
            self.result = []
            self.month_names = []
            self.weekdays = []
            self.relative_weekdays = {}
            self.datetimestring = u"%Y-%m-%d %H:%M:%S"
            self.time_splitter = u':'
            self.date_sequence = ["y","m","d"]
            self.date_splitter = u'-'
            self.utc = pytz.utc
            self.timezone = pytz.utc
            self.value_filters = {}

    def find_start_node(self, data_def=None):
        with self.tree_lock:
            self.data_def = data_def if isinstance(datadef, dict) else {}
            if self.print_searchtree:
                self.print_text('The root Tree:\n')
                self.start_node.print_tree()

            init_path = self.data_value(['data',"init-path"],list)
            if self.show_result:
                self.print_text(self.root.print_node())

            sn = self.root.get_children(data_def = init_path)
            self.start_node = self.root if (sn == None or len(sn) == 0) else sn[0]

    def find_data_value(self, path_def, start_node = None, link_values = None):
        with self.tree_lock:
            if not isinstance(path_def, (list, tuple)) or len(path_def) == 0:
                return

            if start_node == None or not isinstance(start_node, DATAnode):
                start_node = self.start_node

            nlist = start_node.get_children(data_def = path_def, link_values = link_values)
            if self.data_value('select', str, path_def[-1]) == 'presence':
                # We return True if exactly one node is found, else False
                return bool(isinstance(nlist, DATAnode) or (isinstance(nlist, list) and len(nlist) == 1 and  isinstance(nlist[0], DATAnode)))

            # Nothing found, so give the default or None
            if nlist in ([], None):
                if self.data_value('type', None, path_def[-1]) == 'list':
                    return []

                else:
                    return self.data_value('default', None, path_def[-1])

            # We found multiple values
            elif len(nlist) > 1 or (isinstance(path_def, list) and len(path_def)>0 and self.data_value('type', None, path_def[-1]) == 'list'):
                vlist = []
                for node in nlist:
                    if isinstance(node, DATAnode):
                        vlist.append(node.find_value(path_def[-1]))

                    # There is a named subset of the found nodes
                    elif isinstance(node, dict):
                        for k, v in node.items():
                            slist = []
                            for item in v:
                                if isinstance(item, DATAnode):
                                    slist.append(item.find_value(path_def[-1]))

                            vlist.append({k: slist})

                return vlist

            # We found one value
            if not isinstance(nlist[0], DATAnode):
                if isinstance(path_def, list) and len(path_def)>0:
                    if self.data_value('type', None, path_def[-1]) == 'list':
                        return []

                    else:
                        return self.data_value('default', None, path_def[-1])

            else:
                return nlist[0].find_value(path_def[-1])

    def extract_datalist(self, data_def=None):
        with self.tree_lock:
            self.data_def = data_def if isinstance(datadef, dict) else {}
            if self.print_searchtree:
                self.print_text('The %s Tree:\n' % self.start_node.print_node())
                self.start_node.print_tree()
            self.result = []
            # Are there multiple data definitions
            if self.is_data_value(['data',"iter"],list):
                def_list = self.data_value(['data','iter'],list)

            # Or just one
            elif self.is_data_value('data',dict):
                def_list = [self.data_value('data',dict)]

            else:
                return

            for dset in def_list:
                # Get all the key nodes
                if self.is_data_value(["key-path"], list, dset):
                    kp = self.data_value(["key-path"], list, dset)
                    if len(kp) == 0:
                        continue

                    if self.show_result:
                        self.fle.write('parsing keypath %s\n'.encode('utf-8') % (kp[0]))

                    self.key_list = self.start_node.get_children(data_def = kp)
                    for k in self.key_list:
                        if not isinstance(k, DATAnode):
                            continue

                        # And if it's a valid node, find the belonging values (the last dict in a path list contains the value definition)
                        tlist = [k.find_value(kp[-1])]
                        link_values = {}
                        if self.is_data_value('link', int, kp[-1]):
                            link_values = {kp[-1]["link"]: k.find_value(kp[-1])}

                        for v in self.data_value(["values"], list, dset):
                            if not isinstance(v, list) or len(v) == 0:
                                tlist.append(None)
                                continue

                            if self.is_data_value('value',None, v[0]):
                                tlist.append(self.data_value('value',None, v[0]))
                                continue

                            if self.show_result:
                                self.fle.write('parsing key %s %s\n'.encode('utf-8') % ( [k.find_value(kp[-1])], v[-1]))

                            if self.extract_from_parent and isinstance(k.parent, DATAnode):
                                dv = self.find_data_value(v, k.parent, link_values)

                            else:
                                dv = self.find_data_value(v, k, link_values)

                            if isinstance(dv, NULLnode):
                                break

                            tlist.append(dv)

                        else:
                            self.result.append(tlist)

    def calc_value(self, value, node_def = None):
        if isinstance(value, (str, unicode)):
            # Is there something to strip of
            if self.is_data_value('lower', None, node_def):
                value = unicode(value).lower().strip()

            if self.is_data_value('upper', None, node_def):
                value = unicode(value).upper().strip()

            if self.is_data_value('capitalize', None, node_def):
                value = unicode(value).capitalize().strip()

            if self.is_data_value('ascii-replace', list, node_def) and len(node_def['ascii-replace']) > 0:
                arep = node_def['ascii-replace']
                value = value.lower()
                if len(arep) > 2:
                    value = re.sub(arep[2], arep[1], value)

                value = value.encode('ascii','replace')
                value = re.sub('\?', arep[0], value)

            if self.is_data_value('lstrip', str, node_def):
                if value.strip().lower()[:len(node_def['lstrip'])] == node_def['lstrip'].lower():
                    value = unicode(value[len(node_def['lstrip']):]).strip()

            if self.is_data_value('rstrip', str, node_def):
                if value.strip().lower()[-len(node_def['rstrip']):] == node_def['rstrip'].lower():
                    value = unicode(value[:-len(node_def['rstrip'])]).strip()

            # Is there something to substitute
            if self.is_data_value('sub', list, node_def) and len(node_def['sub']) > 1:
                for i in range(int(len(node_def['sub'])/2)):
                    value = re.sub(node_def['sub'][i*2], node_def['sub'][i*2+1], value).strip()

            #~ # Is there a search regex
            #~ if self.is_data_value('regex', str, node_def):
                #~ try:
                    #~ dd = re.search(node_def['regex'],  value, re.DOTALL)
                        #~ if dd.group(1) not in ('', None):
                            #~ value = dd.group(1)

            # Is there a split list
            if self.is_data_value('split', list, node_def) and len(node_def['split']) > 0:
                if not isinstance(node_def['split'][0],list):
                    slist = [node_def['split']]

                else:
                    slist = node_def['split']

                for sdef in slist:
                    if len(sdef) < 2 or not isinstance(sdef[0],(str,unicode)):
                        continue

                    try:
                        fill_char = sdef[0]
                        if fill_char in ('\\s', '\\t', '\\n', '\\r', '\\f', '\\v', ' '):
                            fill_char = ' '
                            value = value.strip()

                        dat = re.split(sdef[0],value)
                        if sdef[1] == 'list-all':
                            value = dat
                        elif isinstance(sdef[1], int):
                            value = dat[sdef[1]]
                            for i in range(2, len(sdef)):
                                if isinstance(sdef[i], int) and (( 0<= sdef[i] < len(dat)) or (-len(dat) <= sdef[i] < 0)):
                                    value = value + fill_char +  dat[sdef[i]]

                    except:
                        #~ traceback.print_exc()
                        pass

            if self.is_data_value('multiplier', int, node_def):
                try:
                    value = int(value) * node_def['multiplier']

                except:
                    #~ traceback.print_exc()
                    pass

            if self.is_data_value('devider', int, node_def):
                try:
                    value = int(value) / node_def['devider']

                except:
                    #~ traceback.print_exc()
                    pass

        # Is there a replace dict
        if self.is_data_value('replace', dict, node_def):
            if value == None:
                pass

            elif value.strip().lower() in node_def['replace'].keys():
                value = node_def['replace'][value.strip().lower()]

            else:
                value = None

        # is there a default
        if value == None and self.is_data_value('default', None, node_def):
            value = node_def['default']

        # Make sure a string is unicode and free of HTML entities
        if isinstance(value, (str, unicode)):
            value = re.sub('\n','', re.sub('\r','', self.un_escape(unicode(value)))).strip()

        # is there a type definition in node_def
        if self.is_data_value('type', unicode, node_def):
            try:
                if node_def['type'] == 'timestamp':
                    val = value
                    if self.is_data_value('multiplier', int, node_def):
                        val = value/node_def['multiplier']

                    value = datetime.datetime.fromtimestamp(float(val), self.utc)

                elif node_def['type'] == 'datetimestring':
                    dts = self.datetimestring
                    if self.is_data_value('datetimestring', str, node_def):
                        dts = self.data_value('datetimestring', str, node_def)

                    date = self.timezone.localize(datetime.datetime.strptime(value, dts))
                    value = self.utc.normalize(date.astimezone(self.utc))

                elif node_def['type'] == 'time':
                    try:
                        ts = self.time_splitter
                        if self.is_data_value('time-splitter', str, node_def):
                            ts = self.data_value('time-splitter', str, node_def)

                        t = re.split(ts, value)
                        if len(t) == 2:
                            value = datetime.time(int(t[0]), int(t[1]))

                        elif len(t) > 2:
                            value = datetime.time(int(t[0]), int(t[1]), int(t[2][:2]))

                    except:
                        #~ traceback.print_exc()
                        pass

                elif node_def['type'] == 'timedelta':
                    try:
                            value = datetime.timedelta(seconds = int(value))

                    except:
                        #~ traceback.print_exc()
                        pass

                elif node_def['type'] == 'date':
                    try:
                        current_date = self.timezone.normalize(datetime.datetime.now(pytz.utc).astimezone(self.timezone))
                        day = current_date.day
                        month = current_date.month
                        year = current_date.year
                        ds = self.date_splitter
                        if self.is_data_value('date-splitter', str, node_def):
                            ds = self.data_value('date-splitter', str, node_def)

                        dseq = self.date_sequence
                        if self.is_data_value('date-sequence', list, node_def):
                            dseq = self.data_value('date-sequence', list, node_def)

                        d = re.split(ds, value)
                        for index in range(len(d)):
                            if index > len(dseq)-1:
                                break

                            try:
                                d[index] = int(d[index])

                            except ValueError:
                                if d[index].lower() in self.month_names:
                                    d[index] = self.month_names.index(d[index].lower())

                                else:
                                    continue

                            if dseq[index].lower() == 'd':
                                day = d[index]

                            if dseq[index].lower() == 'm':
                                month = d[index]

                            if dseq[index].lower() == 'y':
                                year = d[index]

                        value = datetime.date(year, month, day)

                    except:
                        #~ traceback.print_exc()
                        pass


                elif node_def['type'] == 'datestamp':
                    val = value
                    if self.is_data_value('multiplier', int, node_def):
                        val = value/node_def['multiplier']

                    value = datetime.date.fromtimestamp(float(val))

                elif node_def['type'] == 'relative-weekday':
                    if value.strip().lower() in self.relative_weekdays.keys():
                        value = self.relative_weekdays[value.strip().lower()]

                elif node_def['type'] == 'string':
                    value = unicode(value)

                elif node_def['type'] == 'int':
                    try:
                        value = int(value)

                    except:
                        value = 0

                elif node_def['type'] == 'float':
                    try:
                        value = float(value)

                    except:
                        value = 0

                elif node_def['type'] == 'boolean':
                    if not isinstance(value, bool):
                        if isinstance(value, int):
                            value = bool(value>0)

                        elif isinstance(value, (str, unicode)):
                            value = bool(len(value) > 0 and value != '0')

                        else:
                            value = False

                elif node_def['type'] == 'lower-ascii' and isinstance(value, (str, unicode)):
                    value = value.lower()
                    value =re.sub('[ /]', '_', value)
                    value =re.sub('[!(),]', '', value)
                    value = re.sub('á','a', value)
                    value = re.sub('à','a', value)
                    value = re.sub('ä','a', value)
                    value = re.sub('â','a', value)
                    value = re.sub('ã','a', value)
                    value = re.sub('@','a', value)
                    value = re.sub('é','e', value)
                    value = re.sub('è','e', value)
                    value = re.sub('ë','e', value)
                    value = re.sub('ê','e', value)
                    value = re.sub('í','i', value)
                    value = re.sub('ì','i', value)
                    value = re.sub('ï','i', value)
                    value = re.sub('î','i', value)
                    value = re.sub('ó','o', value)
                    value = re.sub('ò','o', value)
                    value = re.sub('ö','o', value)
                    value = re.sub('ô','o', value)
                    value = re.sub('õ','o', value)
                    value = re.sub('ú','u', value)
                    value = re.sub('ù','u', value)
                    value = re.sub('ü','u', value)
                    value = re.sub('û','u', value)
                    value = re.sub('ý','y', value)
                    value = re.sub('ÿ','y', value)
                    value = value.encode('ascii','replace')

                elif node_def['type'] == '':
                    pass

            except:
                #~ traceback.print_exc()
                pass

        if self.is_data_value('member-off', unicode, node_def) and self.data_value('member-off', unicode, node_def) in self.value_filters.keys():
            vf = self.value_filters[self.data_value('member-off', unicode, node_def)]
            if not value in vf:
                value = NULLnode()

        return value

    def un_escape(self, text):
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

        return unicode(re.sub("&#?\w+;", fixup, text))
    def print_text(self, text):
        self.fle.write(text.encode('utf-8', 'replace'))

    def get_leveltabs(self, level, spaces=3):
        stab = u''
        for i in range(spaces):
            stab += u' '

        sstr = u''
        for i in range(level):
            sstr += stab

        return sstr

    def is_data_value(self, dpath, dtype = None, subpath = None):
        if isinstance(dpath, (str, unicode)):
            dpath = [dpath]

        if not isinstance(dpath, (list, tuple)):
            return False

        if subpath == None:
            subpath = self.data_def

        for d in dpath:
            if not isinstance(subpath, dict):
                return False

            if not d in subpath.keys():
                return False

            subpath = subpath[d]

        #~ if subpath in (None, "", {}, []):
            #~ return False

        if dtype == None:
            return True

        if dtype == float:
            return bool(isinstance(subpath, (float, int)))

        if dtype in (str, unicode, 'string'):
            return bool(isinstance(subpath, (str, unicode)))

        if dtype in (list, tuple, 'list'):
            return bool(isinstance(subpath, (list, tuple)))

        return bool(isinstance(subpath, dtype))

    def data_value(self, dpath, dtype = None, subpath = None, default = None):
        if self.is_data_value(dpath, dtype, subpath):
            if isinstance(dpath, (str, unicode)):
                dpath = [dpath]

            if subpath == None:
                subpath = self.data_def

            for d in dpath:
                subpath = subpath[d]

        else:
            subpath = None

        if subpath == None:
            if default != None:
                return default

            elif dtype in (str, unicode, 'string'):
                return ""

            elif dtype == dict:
                return {}

            elif dtype in (list, tuple, 'list'):
                return []

        return subpath

# end DATAtree

class HTMLtree(HTMLParser, DATAtree):
    def __init__(self, data='', autoclose_tags=[], print_tags = False, output = sys.stdout):
        HTMLParser.__init__(self)
        DATAtree.__init__(self, output)
        with self.tree_lock:
            self.print_tags = print_tags
            self.autoclose_tags = autoclose_tags
            self.is_tail = False
            self.root = HTMLnode(self, 'root')
            self.current_node = self.root
            self.last_node = None
            self.text = u''
            self.open_tags = {}
            self.count_tags(data)
            # read the html page into the tree
            self.feed(data)
            self.reset()
            # And find the dataset into self.result
            self.start_node = self.root

    def count_tags(self, data):
        tag_list = re.compile("\<(.*?)\>", re.DOTALL)
        self.tag_count = {}
        for t in tag_list.findall(data):
            if t[0] == '\\':
                t = t[1:]

            if t[0] == '/':
                sub = 'close'
                tag = t.split (' ')[0][1:].lower()

            elif t[:3] == '!--':
                continue
                sub = 'comment'
                tag = t[3:].lower()

            elif t[0] == '?':
                continue
                sub = 'pi'
                tag = t[1:].lower()

            elif t[0] == '!':
                continue
                sub = 'html'
                tag = t[1:].lower()

            elif t[-1] == '/':
                sub = 'auto'
                tag = t.split(' ')[0].lower()

            else:
                sub = 'start'
                tag = t.split (' ')[0].lower()

            if not tag in self.tag_count.keys():
                self.tag_count[tag] ={}
                self.tag_count[tag]['close'] = 0
                self.tag_count[tag]['comment'] = 0
                self.tag_count[tag]['pi'] = 0
                self.tag_count[tag]['html'] = 0
                self.tag_count[tag]['auto'] = 0
                self.tag_count[tag]['start'] = 0

            self.tag_count[tag][sub] += 1

        for t, c in self.tag_count.items():
            if c['close'] == 0 and (c['start'] >0 or c['auto'] > 0):
                self.autoclose_tags.append(t)

            if self.print_tags:
                self.print_text(u'%5.0f %5.0f %5.0f %s\n' % (c['start'], c['close'], c['auto'], t))

    def handle_starttag(self, tag, attrs):
        if not tag in self.open_tags.keys():
            self.open_tags[tag] = 0

        self.open_tags[tag] += 1
        if self.print_tags:
            if len(attrs) > 0:
                self.print_text(u'%sstarting %s %s %s\n' % (self.get_leveltabs(self.current_node.level,2), self.current_node.level+1, tag, attrs[0]))
                for a in range(1, len(attrs)):
                    self.print_text(u'%s        %s\n' % (self.get_leveltabs(self.current_node.level,2), attrs[a]))

            else:
                self.print_text(u'%sstarting %s %s\n' % (self.get_leveltabs(self.current_node.level,2), self.current_node.level,tag))

        node = HTMLnode(self, [tag.lower(), attrs], self.current_node)
        self.add_text()
        self.current_node = node
        self.is_tail = False
        if tag.lower() in self.autoclose_tags:
            self.handle_endtag(tag)
            return False

        return True

    def handle_endtag(self, tag):
        if not tag in self.open_tags.keys() or self.open_tags[tag] == 0:
            return

        self.open_tags[tag] -= 1
        if self.current_node.tag != tag.lower():
            # To catch missing close tags
            #~ self.remove_text()
            self.handle_endtag(self.current_node.tag)

        self.add_text()
        if self.print_tags:
            if self.current_node.text.strip() != '':
                self.print_text(u'%s        %s\n' % (self.get_leveltabs(self.current_node.level-1,2), self.current_node.text.strip()))
            self.print_text(u'%sclosing %s %s %s\n' % (self.get_leveltabs(self.current_node.level-1,2), self.current_node.level,tag, self.current_node.tag))

        self.last_node = self.current_node
        self.is_tail = True
        self.current_node = self.current_node.parent
        if self.current_node.is_root:
            self.reset()

    def handle_startendtag(self, tag, attrs):
        if self.handle_starttag(tag, attrs):
            self.handle_endtag(tag)

    def handle_data(self, data):
        self.text += data

    def handle_entityref(self, name):
        try:
            c = unichr(name2codepoint[name])
            self.text += c

        except:
            pass

    def handle_charref(self, name):
        if name.startswith('x'):
            c = unichr(int(name[1:], 16))

        else:
            c = unichr(int(name))

        self.text += c

    def handle_comment(self, data):
        # <!--comment-->
        pass

    def handle_decl(self, decl):
        # <!DOCTYPE html>
        pass

    def handle_pi(self, data):
        # <?proc color='red'>
        pass

    def add_text(self):
        if self.is_tail:
            self.last_node.tail += unicode(re.sub('\n','', re.sub('\r','', self.text)).strip())

        else:
            self.current_node.text += unicode(re.sub('\n','', re.sub('\r','', self.text)).strip())

        self.text = u''

    def remove_text(self):
        if self.is_tail:
            self.text += self.current_node.tail
            self.current_node.tail = u''

        else:
            self.text += self.current_node.text
            self.current_node.text = u''

# end HTMLtree

class JSONtree(DATAtree):
    def __init__(self, data, output = sys.stdout):
        DATAtree.__init__(self, output)
        with self.tree_lock:
            self.extract_from_parent = True
            self.data = data
            # Read the json data into the tree
            self.root = JSONnode(self, data, key = 'ROOT')
            self.start_node = self.root

# end JSONtree

