# tvgrabnlpy
**NOTE: We are moving from Google Code to here**. Expect all code to be here in a couple of days.

[Ga naar de Wiki](https://github.com/tvgrabbers/tvgrabnlpy/wiki)  
[Ga naar de downloads](https://github.com/tvgrabbers/tvgrabnlpy/releases)

###Samenvatting

tv_grab_nl_py is een [XMLTV](http://xmltv.org)-compatibele grabber voor Nederlandse en Vlaamse televisie die [TVGids.nl](http://www.tvgids.nl), [TVGids.tv](http://www.tvgids.tv), [TeVeBlad.be](http://www.teveblad.be) en [RTL.nl](http://www.rtl.nl) als bron gebruikt.

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

tv_grab_nl_py is an [XMLTV](http://xmltv.org)-compatible grabber for Dutch and Flemish television that uses [TVGids.nl](http://www.tvgids.nl), [TVGids.tv](http://www.tvgids.tv), [TeVeBlad.be](http://www.teveblad.be) and [RTL.nl](http://www.rtl.nl) as a source.

###Release Notes Versie 2

**p20150307**   version 2.1.2 beta Preparations for future external graphical frontend  
            Added some option value checks  
            Made it possible (as suggested) to run without cache. But who wants that?  
            Fixed illogical detail fetch counter since 2.1.0  

**p20150303**   version 2.1.1 Added a warning on configure to enable the desired channels

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
            Added further multy threading, giving every channel and every source it's own thread,  
            thus optimizing the fetch  
            Added use of very nice teveblad icons  
            Made the code more universal to easy add extra sources.

**p20150302**   version 2.0.4 Bugfix for tvgids.tv changed current date format

**p20150210**   version 2.0.3 Small change in --description argument for compatibility with tvheadend.

**p20150210**   version 2.0.2 fixed small bug in configuring new channel list.

**p20150210**   version 2.0.1 added further channel ID linking between tvgids.nl and tvgids.tv

**p20150209**   Declared Stable

**p20150201**   Fixed stupid error in groupnameremove table processing  
            Updated default cattrans table

**p20150129**   Fixed some more failures  
            Added On/Off as boolean values in the configuration file  
            Adjusted Statistics Log output  
            Updated default cattrans table  
            Made default groupnameremove table only be used on first creation.

**p20150127**   Fixed some failures on detail fetching due to invalid HTML/XMLtags  
              in the website  
            Adjusted some helptext

**p20150126**   Initial release Version 2.0.0-beta
