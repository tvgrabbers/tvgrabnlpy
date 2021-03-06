# tvgrabnlpy
**NOTE: We have moved from [Google Code](https://code.google.com/p/tvgrabnlpy/) to here**.

The new version 3 included in [tv_grab_py_API v1](https://github.com/tvgrabbers/tvgrabpyAPI) has moved to its own repository at: https://github.com/tvgrabbers/tvgrabpyAPI

[ACTUELE INFO](https://github.com/tvgrabbers/tvgrabnlpy/wiki/actueel)

[Ga naar de Wiki](https://github.com/tvgrabbers/tvgrabnlpy/wiki)  
[Go to the English Wiki](https://github.com/tvgrabbers/tvgrabnlpy/wiki/English)  
[Ga naar de downloads](https://github.com/tvgrabbers/tvgrabnlpy/releases)  
[Download laatste stabiele versie](https://github.com/tvgrabbers/tvgrabnlpy/releases/latest)  
[Ga naar de discussie groep](https://groups.google.com/forum/#!forum/tvgrabnlpy)  

###Samenvatting

tv_grab_nl_py is een [XMLTV](http://xmltv.org)-compatibele grabber voor Nederlandse en Vlaamse televisie die [TVGids.nl](http://www.tvgids.nl), [TVGids.tv](http://www.tvgids.tv), [RTL.nl](http://www.rtl.nl), [NPO.nl](http://www.npo.nl), [Horizon.tv](http://www.horizon.tv), [Humo.be](http://www.humo.be), [VPRO.nl](http://www.vpro.nl), [NieuwsBlad.be](http://www.nieuwsblad.be) en [Primo.eu](http://www.primo.eu) als bron gebruikt.

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

Sinds versie 2.2:
  * sqlite als cache format
  * nog meer bronnen
  * bijna complete seizoen/episode informatie mede dankzij [thetvdb.com](http://www.thetvdb.com) lookup.
  * nog betere configurabiliteit

###English Summary

tv_grab_nl_py is an [XMLTV](http://xmltv.org)-compatible grabber for Dutch and Flemish television that uses [TVGids.nl](http://www.tvgids.nl), [TVGids.tv](http://www.tvgids.tv), [RTL.nl](http://www.rtl.nl), [NPO.nl](http://www.npo.nl), [horizon.tv](http://www.horizon.tv), [Humo.be](http://www.humo.be), [VPRO.nl](http://www.vpro.nl), [Nieuwsblad.be](http://www.nieuwsblad.be) and [Primo.eu](http://www.primo.eu) as a source.

###Release Notes Versie 2
**p20151217**   version 2.2.7  
* With a New Belgium Source Primo.eu. Supplying some extra Flemish channels and description etc. for channels, like the Flemish Regionals on nieuwsblad.be.
* Some updates on handeling sourcematching.json
* Some small updates.
* Added log mailing option:
set `mail_log = True` in your config and give a valid mailserver (default = localhost), mailport (default = 25) and mail_log_address (default = postmaster)
You should test in a console as the log is already closed when sending it!
* `sourcematching.json` moved to a separate repository https://github.com/tvgrabbers/sourcematching

**p20151130**   version 2.2.6  
* Updates on source linking and channelgrouping. Split between Dutch/Flemish listings and between French/German listings.  
* New option `--group_active_channels` to add to `--configure` to group all active channels together on the top.
* Added channel merging for combined channels like bbc3/cbbc and bbc4/cbeebies, Veronica/DisneyXD and Ketnet/Canvas+/Eén+. This can mean you have to change the XMLTVid.
* The not merged XMLTVids are:
  *  BBC 3:    300
  *  BBC 4:    301
  *  Cbeebies:    cbeebies
  *  Ketnet:    8-ketnet
  *  Eén+:    8-eenplus
  *  Veronica:    34
  *  Disney XD:    311  
* The mergedd XMLTVids are:
  *  BBC Three / CBBC:    5-24443943013
  *  BBC Four / Cbeebies:    5-24443943080
  *  Ketnet / Canvas+ / Eén+:    ketnet-canvas-2
  *  Veronica / Disney XD:    veronica
* All the data-tables needed by configure are moved to a separate file `sourcematching.json`. This file will be downloaded every run. This means also that the minimal required Python version has gone up to 2.7.9.
Whenever this file is updated you get a message in your log to run `--configure`. Also when a new stable release is available you will get a message in your log!
* Added config option `always_use_json`. This is by default on, but can be turned off if you have custom channel-names, groups or prime_sources. They then will be maintained, but also not all updates through `sourcematching.json` might come through. If a prime_source setting from `sourcematching.json` is ignored, it will be mentioned in your log.
* Bugfix on vpro.nl source crashing on the end of the month.
**p20151116**   version 2.2.5  
* Especially for our Flemish users, nieuwsblad.be as a new source.  
This adds some of the on humo.be removed channels AND Flemish Regional channels.  
For now it has only time and title information (unless one of the other sources brings in more) and for 6 or 7 days ahead. Possibly at a later stage I can add the descriptions from their detail pages. But maybe someone knows a better source? Maybe also for extra (Flemish) radio channels?  
* Further added new season/episode info from the tvgids.nl detail pages.  
* And some fixes

**p20151110**   version 2.2.4 
* Updated humo.be source. See issue #46. xmltvid's originating from this source (starting with '6-') are no longer valid. They either changed or the channels where removed on the source!  
* New vpro.nl source with especially more radio channels.  
* Fixed the `--configure` option to properly update the configuration also removing outdated (invalid) sourceid's especially from humo.be  
* Some missing icons added.  
* Some fresh User agents  
* Some small bugs fixed.  

**p20151030**   version 2.2.3 
* Small fix for extra space in important tag on tvgids.nl preventing the page being read.

**p20151022**   version 2.2.2 
* Pure for Windows users optional CP1252 output coding with `--output-windows-codeset`option  
* Small bugfix (see output for NPO 3 sa 24-10 23:09)

**p20150927**   version 2.2.1
Bug fix for channels hanging when neither requesting details nor ttvdb lookups.
(This already would fix itself after 30 minutes of inactivity)

**p20150917**   version 2.2.0
* Renewed logging module.
* Renewed structure to easier add new sources. You now also can simply disable a source.
* Renewed, more flexible configuration format. It will automatically be converted the first time. If you come from earlier alfa or beta releases, you might have to throw away your old config.
* The cache is moved to an sqlite database and now also contains buffered channel and theTVDB.com data. It will automatically be converted the first time. This can, depending on the size of your old cache, take some time.
* Since the first beta some tuning has been done on the sqlite engine and it is much faster. The tuning means that in case of a computercrash the db possibly can get corrupted. Therefore a backup is made at the start of every run
* New sources: Horizon.tv and Humo.be.
* Updated NPO.nl source. It now also contains the public radio stations
* theTVDB.com lookup for incomplete episode information. Now unlike in the alfa release, fully integrated. So no longer dependent on MythTV functionality
* There has been limited testing under Windows7 and some things have been adapted.
* and lots more. See the Configuratie WIKI page.

**p20150907**   version 2.1.13  
Fix on previous  

**p20150901**   version 2.1.12  
Disabled no longer existing teveblad.be source 

**p20150820**   version 2.1.11  
Fix for the tvgids.nl cookie block  

**p20150817**   version 2.1.10  
A lot of improvements to prevent hanging. Especially better inter-thread communication and locking.  
Wrapper script for tvheadend.  
Fix on a following episode disappearing due to equal ID.  
Removed line breaks from the description.  
Added log flushing.  

**p20150628**   version 2.1.9  
Fix for changed tvgids.tv detailpage and new npo.nl page.  
Invalidated Python 2.6.  
Better cache crash recovery.  
Fixed a hang on certain crashes.  
Fetch for today on teveblad.be without date.  
Use fresh title in stead of from the cache.  
Some small fixes and code sanitation.  

**p20150616**   version 2.1.8  
Intermediate version to fix disabling npo.

**p20150531**   version 2.1.7  
Also stripped Episode numbering from Sports events. MythTV else will change category to Series.  
Optimized the new source, adding a new option `use_npo`, which is on by default and can be set both global and per channel. Real endtimes are used, separately programming the info/commercial breaks.  
Added a new source npo.nl, which only provides 'superior' timings for three days for the dutch public NPO and regional channels. It's use for now is implied.  
Added Kijkwijzer info from rtl.nl
Added new, last-chance and premiere from teveblad.be  
Added Kijkwijzer info from tvgids.nl  
Added imdb start-rating from tvgids.tv  
Added country, original title from teveblad.be  
Switched teveblad to the old solopages, with the grouppages as backup.  
Some fine-tuning  
Some further changes to conform with the xmltv standard  
Added new channeloption add_hd_id to create two listings for a channel, one with HD tagging and one without  

**p20150512**   version 2.1.6  
New base fetch for teveblad.be with the old one left for redundancy  
New detail fetch for the renewed tvgids.nl pages  
Added a json detail fetch for tvgids.nl for redundancy  
Added beter conformity with the xmltv standard  

**p20150501**   version 2.1.5  
Fixed a bug, that could cause the script to hang on an error in a detailpage, resuling in excessive processor use.  
Added 'prefered_description' channel option.  
This option sets the preferred source for the descriptions for that channel, falling back to defaults(the longest one found) if no description present.  
Also removed the adding of 'slowdays = None' to a new config. It means use default, so not needed.

**p20150406**   version 2.1.4  
Bugfix on rtl.nl offset  
Bugfix on detailfetch storing

**p20150403**   version 2.1.3  
Bugfix on new cache creation

**p20150329**   version 2.1.2  
Minor updates  
With updated MythTV script
Preparations for future external graphical frontend  
Added some option value checks  
Made it possible (as suggested) to run without cache. But who wants that?  
Fixed illogical detail fetch counter since 2.1.0  

**p20150303**   version 2.1.1  
Added a warning on configure to enable the desired channels

**p20150302**   version 2.1.0  
Bugfix for tvgids.tv changed current date format  
Added a retry loop around the prime page fetches.  
Added the channel switch 'append_tvgidstv'  
Fixed rtl.nl fetching a day to long (days versus days ahead)  
Enhanced some output  
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

**p20150209**   version 2.0.0  
Fixed stupid error in groupnameremove table processing  
Updated default cattrans table  
Fixed some more failures  
Added On/Off as boolean values in the configuration file  
Adjusted Statistics Log output  
Updated default cattrans table  
Made default groupnameremove table only be used on first creation.  
Fixed some failures on detail fetching due to invalid HTML/XMLtags in the website  
Adjusted some helptext  

**p20150126**   Initial release Version 2.0.0-beta
