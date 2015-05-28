# tvgrabnlpy
**NOTE: We have moved from [Google Code](https://code.google.com/p/tvgrabnlpy/) to here**. The old [wiki](https://code.google.com/p/tvgrabnlpy/w/list) is still there and can be watched [there](https://code.google.com/p/tvgrabnlpy/w/list)

[ACTUELE INFO](https://github.com/tvgrabbers/tvgrabnlpy/wiki/actueel)

[Ga naar de Wiki](https://github.com/tvgrabbers/tvgrabnlpy/wiki)  
[Go to the English Wiki](https://github.com/tvgrabbers/tvgrabnlpy/wiki/English)  
[Ga naar de downloads](https://github.com/tvgrabbers/tvgrabnlpy/releases)  
[Download laatste stabiele versie](https://github.com/tvgrabbers/tvgrabnlpy/releases/latest)  
[Ga naar de discussie groep](https://groups.google.com/forum/#!forum/tvgrabnlpy)  

###Samenvatting

tv_grab_nl_py is een [XMLTV](http://xmltv.org)-compatibele grabber voor Nederlandse en Vlaamse televisie die [TVGids.nl](http://www.tvgids.nl), [TVGids.tv](http://www.tvgids.tv), [TeVeBlad.be](http://www.teveblad.be), [RTL.nl](http://www.rtl.nl) en [NPO.nl](http://www.npo.nl) als bron gebruikt.

Prettige eigenschappen zijn:
  * detailinformatie wordt gecached
  * categorie-informatie wordt geconverteerd zodat de kleurcodering werkt in de EPG van MythTV
  * instelbaar aantal dagen dat met detailinformatie wordt opgehaald
  * links naar zenderlogo's worden automatisch toegevoegd

Sinds versie 2:
  * meerdere bronnen
  * meer kanalen
  * meer configuratie mogelijkheden
  * tot 14 dagen programma informatie!
  * deels seizoen/episode informatie

###English Summary

tv_grab_nl_py is an [XMLTV](http://xmltv.org)-compatible grabber for Dutch and Flemish television that uses [TVGids.nl](http://www.tvgids.nl), [TVGids.tv](http://www.tvgids.tv), [TeVeBlad.be](http://www.teveblad.be), [RTL.nl](http://www.rtl.nl) and [NPO.nl](http://www.npo.nl) as a source.

###Release Notes Versie 2
**p20150528**   version 2.1.7 beta  
Optimized the new source, adding a new option `use_npo`, which is on by default and can be set both global and per channel. Real endtimes are used, separately programming the info/commercial breaks.

**p20150527**   version 2.1.7 beta  
Added a new source npo.nl, which only provides 'superior' timings for three days for the dutch public NPO and regional channels. It's use for now is implied.

**p20150525**   version 2.1.7 beta  
Added Kijkwijzer info from rtl.nl
Added new, last-chance and premiere from teveblad.be

**p20150524**   version 2.1.7 beta  
Added Kijkwijzer info from tvgids.nl  
Added imdb start-rating from tvgids.tv  
Added country, original title from teveblad.be

**p20150523**   version 2.1.7 beta  
Switched teveblad to the old solopages, with the grouppages as backup.

**p20150514**   version 2.1.7 beta  
            Some fine-tuning  
            Some further changes to conform with the xmltv standard  
            Added new channeloption add_hd_id to create two listings for a channel, one with HD tagging and one without  

**p20150512**   version 2.1.6 stable  
            New base fetch for teveblad.be with the old one left for redundancy  
            New detail fetch for the renewed tvgids.nl pages  
            Added a json detail fetch for tvgids.nl for redundancy  
            Added beter conformity with the xmltv standard  

**p20150501**   version 2.1.5 stable  

**p20150415**   version 2.1.5-beta  
            Fixed a bug, that could cause the script to hang on an error in a detailpage, resuling in excessive processor use.
            
**p20150406**   version 2.1.5-beta  
            Added 'prefered_description' channel option.  
            This option sets the preferred source for the descriptions for that channel, falling back to defaults(the longest one found) if no description present.  
            Also removed the adding of 'slowdays = None' to a new config. It means use default, so not needed.

**p20150406**   version 2.1.4  
            Bugfix on rtl.nl offset  
            Bugfix on detailfetch storing

**p20150403**   version 2.1.3  
            Bugfix on new cache creation

**p20150329**   version 2.1.2 Declared stable  
            Minor updates  
            With updated MythTV script

**p20150307**   version 2.1.2 beta  
            Preparations for future external graphical frontend  
            Added some option value checks  
            Made it possible (as suggested) to run without cache. But who wants that?  
            Fixed illogical detail fetch counter since 2.1.0  

**p20150303**   version 2.1.1  
            Added a warning on configure to enable the desired channels

**p20150302**   version 2.1.0 Declared stable  
            Bugfix for tvgids.tv changed current date format

**p20150223**   Some bugfixes  
            Added a retry loop around the prime page fetches.

**p20150219**   Some bugfixes

**p20150218**   Added the channel switch 'append_tvgidstv' (intended to, but forgot)  
            Fixed rtl.nl fetching a day to long (days versus days ahead)  
            Enhanced some output  

**p20150217**   Initial 2.1.0-beta release  
            Remodeled  configuration file giving the possibility to configure on channel level.  
            This will get automatically upgraded on the first run.  
            Added the possibility to grab channels without counterpart on tvgids.nl.  
            There are now a possible 178 channels. Fetching all for one day takes about 2 houres 20!  
            You can set which source will be leading in the timmings.  
            Added further multy threading, giving every channel and every source it's own thread, thus optimizing the fetch  
            Added use of very nice teveblad icons  
            Made the code more universal to easy add extra sources.

**p20150302**   version 2.0.4  
            Bugfix for tvgids.tv changed current date format  
            With updated MythTV script

**p20150210**   version 2.0.3  
            Small change in --description argument for compatibility with tvheadend.

**p20150210**   version 2.0.2  
            fixed small bug in configuring new channel list.

**p20150210**   version 2.0.1  
            added further channel ID linking between tvgids.nl and tvgids.tv

**p20150209**   Declared Stable

**p20150201**   Fixed stupid error in groupnameremove table processing  
            Updated default cattrans table

**p20150129**   Fixed some more failures  
            Added On/Off as boolean values in the configuration file  
            Adjusted Statistics Log output  
            Updated default cattrans table  
            Made default groupnameremove table only be used on first creation.

**p20150127**   Fixed some failures on detail fetching due to invalid HTML/XMLtags in the website  
            Adjusted some helptext

**p20150126**   Initial release Version 2.0.0-beta
