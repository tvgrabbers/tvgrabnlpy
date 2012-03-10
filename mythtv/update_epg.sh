#!/bin/sh

basename=`date '+%A%Hh'`
mkdir -p "/var/tmp/tvgrabnl"
mkdir -p "/var/log/tvgrabnl"
xmlfile="/var/tmp/tvgrabnl/$basename.xml"
logfile="/var/log/tvgrabnl/$basename.log"
conffile="/home/user/.xmltv/tv_grab_py.conf"

/usr/local/bin/tv_grab_nl.py --config-file $conffile --days 7 --slowdays 3 --output $xmlfile --cache tvgrab.cache 2> $logfile

# Import into mythTV
export QTDIR=/usr/lib/qt3
/usr/local/bin/mythfilldatabase --update --file 1 /var/tmp/tvgrabnlpy/tvguide.xml

# Run this script with a cron job, preferably after 04:00 at night. E.g.
# 24 05 * * * /home/user/bin/update_epg.sh
