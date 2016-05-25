#!/usr/bin/env python2

'''
This Package contains an API for tv_grabbers Alle data is defined in JSON files
Including where and how to get the tv data. Multiple source can be defined
and the resulting data is integrated in to one XMLTV output file.
The detailed behaviour is highly configurable. See our site for more details.'''

from distutils.core import setup
from os import environ, name
from tvgrabpyAPI import version, __version__

if 'HOME' in environ:
    home_dir = environ['HOME']
elif 'HOMEPATH' in environ:
    home_dir = environ['HOMEPATH']

if name == 'nt' and 'USERPROFILE' in environ:
    home_dir = environ['USERPROFILE']
    source_dir = u'%s/.xmltv/sources' % home_dir
else:
    source_dir = u'/var/lib/tvgrabpyAPI'

setup(
    name = version()[0],
    version =  __version__,
    description = 'xlmtv API based on JSON datafiles',
    packages = ['tvgrabpyAPI'],
    package_data={'tvgrabpyAPI': ['texts/tv_grab_text.*']},
    scripts=['tv_grab_nl3.py'],
    data_files=[(source_dir, ['sources/tv_grab_API.json',
                            'sources/tv_grab_nl.json',
                            'sources/source-virtual.nl.json',
                            'sources/source-horizon.tv.json',
                            'sources/source-humo.be.json',
                            'sources/source-nieuwsblad.be.json',
                            'sources/source-npo.nl.json',
                            'sources/source-oorboekje.nl.json',
                            'sources/source-primo.eu.json',
                            'sources/source-rtl.nl.json',
                            'sources/source-tvgids.nl.json',
                            'sources/source-tvgids.tv.json',
                            'sources/source-vpro.nl.json',
                            'sources/source-vrt.be.json'])],
    requires = ['pytz', 'requests', 'DataTreeGrab'],
    provides = ['%s (%s.%s)' % (version()[0], version()[1], version()[2])],
    long_description = __doc__,
    maintainer = 'Hika van den Hoven',
    maintainer_email = 'hikavdh at gmail dot com',
    license='GPL',
    url='https://github.com/tvgrabbers/tvgrabnlpy',
)
