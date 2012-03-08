#!/bin/sh
cd /Users/freek/Repository/tvgrabnlpy
basename=`date '+%A%Hh'`
mkdir -p "data"
xmlfile="data/$basename.xml"
logfile="data/$basename.log"

# Fetch TV guide from www.ziggo.nl/#tvgids
./tv_grab_nl_py --config-file tvgrab.conf --days 6 --slowdays 2 --output $xmlfile --cache tvgrab.cache 2> $logfile

# open EyeTV with file
open -a EyeTV $xmlfile
