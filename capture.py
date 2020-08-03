###################################################################
# CAPTURE: CAsa Pipeline-cum-Toolkit for Upgraded GMRT data REduction
###################################################################
# Pipeline for analysing data from the GMRT and the uGMRT.
# Combination of pipelines done by Ruta Kale based on pipelines developed independently by Ruta Kale 
# and Ishwar Chandra.
# Date: 8th Aug 2019
# README : Please read the following instructions to run this pipeline on your data
# Files and paths required
# 0. This script should be placed and executed in the directory where your data files are located.
# 1. If starting from lta file, please provide the paths to the listscan and gvfits binaries in "gvbinpath" as shown.
# 2. Keep the vla-cals.list file in the same area.
# 3. The settings True/False at the beginning are relevant for processing the file from a given stage.
# 4. Below these there are inputs to be set for your data.
# 5. You will not need to change anything below the Inputs block under normal circumstances.
# 6. You will have self-calibrated image of your target source at the end of the pipeline.
# Please email ruta@ncra.tifr.res.in if you run into any issue and cannot solve.
# 



import logging
import os
from datetime import datetime
logfile_name = datetime.now().strftime('capture_%H_%M_%d_%m_%Y.log')
logging.basicConfig(filename=logfile_name,level=logging.DEBUG)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

print("LOGFILE =", logfile_name)
logging.info("#######################################################################################")
logging.info("You are using CAPTURE: CAsa Pipeline-cum-Toolkit for Upgraded GMRT data REduction.")
logging.info("This has been developed at NCRA by Ruta Kale and Ishwara Chandra.")
logging.info("#######################################################################################")


import ConfigParser
config = ConfigParser.ConfigParser()
config.read('ugpipe_config.ini')


fromlta = config.getboolean('basic', 'fromlta')
fromfits = config.getboolean('basic', 'fromfits')
frommultisrcms = config.getboolean('basic','frommultisrcms')
findbadants = config.getboolean('basic','findbadants')                          
flagbadants= config.getboolean('basic','flagbadants')                      
findbadchans = config.getboolean('basic','findbadchans')                         
flagbadfreq= config.getboolean('basic','flagbadfreq')                           
flaginit = config.getboolean('basic','flaginit')                             
doinitcal = config.getboolean('basic','doinitcal')                              
mydoflag = config.getboolean('basic','mydoflag')                              
redocal = config.getboolean('basic','redocal')                              
dosplit = config.getboolean('basic','dosplit')                               
flagsplitfile = config.getboolean('basic','flagsplitfile')                            
dosplitavg = config.getboolean('basic','dosplitavg')                             
doflagavg = config.getboolean('basic','doflagavg')                             
makedirty = config.getboolean('basic','makedirty')                            
doselfcal = config.getboolean('basic','doselfcal')                              
usetclean = config.getboolean('basic','usetclean')                        
ltafile =config.get('basic','ltafile')
gvbinpath = config.get('basic', 'gvbinpath').split(',')
fitsfile = config.get('basic','fitsfile')
msfilename =config.get('basic','msfilename')
splitfilename =config.get('basic','splitfilename')
splitavgfilename = config.get('basic','splitavgfilename')
setquackinterval = config.getfloat('basic','setquackinterval')
ref_ant = config.get('basic','ref_ant')
uvracal =config.get('basic','uvracal')
clipfluxcal = [float(config.get('basic','clipfluxcal').split(',')[0]),float(config.get('basic','clipfluxcal').split(',')[1])]
clipphasecal =[float(config.get('basic','clipphasecal').split(',')[0]),float(config.get('basic','clipphasecal').split(',')[1])]
cliptarget =[float(config.get('basic','cliptarget').split(',')[0]),float(config.get('basic','cliptarget').split(',')[1])]   
clipresid=[float(config.get('basic','clipresid').split(',')[0]),float(config.get('basic','clipresid').split(',')[1])]
chanavg = config.getint('basic','chanavg')
imcellsize = [config.get('basic','imcellsize')]
imsize_pix = int(config.get('basic','imsize_pix'))
scaloops = config.getint('basic','scaloops')
mJythreshold = float(config.get('basic','mJythreshold'))
mypcaloops = config.getint('basic','mypcaloops')
scalsolints = config.get('basic','scalsolints').split(',')
niter_start = int(config.get('basic','niter_start'))
use_nterms = config.getint('basic','use_nterms')
nwprojpl = config.getint('basic','nwprojpl')
uvrascal=config.get('basic','uvrascal')
target = config.getboolean('basic','target')

execfile('ugpipe.py')


if fromlta == True:
	logging.info("You have chosen to convert lta to FITS.")
	testltafile = os.path.isfile(ltafile)
	if testltafile == True:
		logging.info("The lta %s file exists.", ltafile)
		testlistscan = os.path.isfile(gvbinpath[0])
		testgvfits = os.path.isfile(gvbinpath[1])
		if testlistscan and testgvfits == True:
			os.system(gvbinpath[0]+' '+ltafile)
                        if fitsfile != 'TEST.FITS':
                            os.system("sed -i 's/TEST.FITS/'"+fitsfile+"/ "+ltafile.split('.')[0]+'.log')
			os.system(gvbinpath[1]+' '+ltafile.split('.')[0]+'.log')
    		else:	
			logging.info("Error: Check if listscan and gvfits are present and executable.")
	else:
		logging.info("The given lta file does not exist. Exiting the code.")
		logging.info("If you are not starting from lta file please set fromlta to False and rerun.")
		sys.exit()


testfitsfile = False 

if fromfits == True:
	testfitsfile = os.path.isfile(fitsfile)
	if testfitsfile == False:
		logging.info("The FITS file does not exist. Exiting the code...")
		sys.exit()
		

if testfitsfile == True:
	#myfitsfile = fitsfile
	#myoutvis = msfilename
	default(importgmrt)
	importgmrt(fitsfile=fitsfile, vis = msfilename)
	if os.path.isfile(msfilename+'.list') == True:
		os.system('rm '+msfilename+'.list')
	vislistobs(msfilename)
	print("You have the following fields in your file:",getfields(msfilename))
	logging.info("You have the following fields in your file",getfields(msfilename))
	



testms = False

if frommultisrcms == True:
	testms = os.path.isdir(msfilename)
	if testms == False:
		logging.info("The MS file does not exist. Exiting the code...")
#                logging.info("If you are starting from a split or splitavg file, please set frommultisrcms to False and reriun.")
                sys.exit()


if testms == True:
        gmrt235 = False
        gmrt610 = False
        gmrtfreq = 0.0
# check if single pol data
        mypol = getpols(msfilename)
        print("Printing which polarization=",mypol)
	logging.info('Your file contains %s polarization products.', mypol)
#        print("Printing polarization types=",mypols(msfilename,mypol))
        if mypol == 1:
                print("This dataset contains only single polarization data.")
                logging.info('This dataset contains only single polarization data.')
                mychnu = freq_info(msfilename)
                print mychnu[0]
                if 200E6< mychnu[0]<300E6:
                        poldata = 'LL'
                        gmrt235 = True
                        gmrt610 = False
			mynchan = getnchan(msfilename)
			if mynchan !=256:
	                        print("You have data in the 235 MHz band of dual frequency mode of the GMRT. Currently files only with 256 channels are supported in this pipeline.")
	                        logging.info('You have data in the 235 MHz band of dual frequency mode of the GMRT. Currently files only with 256 channels are supported in this pipeline.')
				sys.exit()
                elif 590E6<mychnu[0]<700E6:
                        poldata = 'RR'
                        gmrt235 = False
                        gmrt610 = True
			mynchan = getnchan(msfilename)
			if mynchan != 256:
                        	print("You have data in the 610 MHz band of the dual frequency mode of the legacy GMRT. Currently files only with 256 channels are supported in this pipeline.")
                        	logging.info('You have data in the 610 MHz band of the dual frequency mode of the legacy GMRT. Currently files only with 256 channels are supported in this pipeline.')
				sys.exit()
                else:
                        gmrtfreq = mychnu[0]
                        print("You have data in a single polarization - most likely GMRT hardware correlator. This pipeline currently does not support reduction of single pol HW correlator data.")
                        logging.info('You have data in a single polarization - most likely GMRT hardware correlator. This pipeline currently does not support reduction of single pol HW correlator data.')
                        print("The number of channels in this file are", mychnu[0])
                        logging.info('The number of channels in this file are %d', mychnu[0])
			sys.exit()
##################
	mynchan = getnchan(msfilename)
	logging.info('The number of channels in your file %d' % mynchan)
	if mynchan == 1024:
		mygoodchans = '0:250~300'   # used for visstat
		flagspw = '0:51~950'
		gainspw = '0:101~900'
		gainspw2 = ''   # central good channels after split file for self-cal	
	elif mynchan == 2048:
		mygoodchans = '0:500~600'   # used for visstat
		flagspw = '0:101~1900'
		gainspw = '0:201~1800'
		gainspw2 = ''   # central good channels after split file for self-cal
	elif mynchan == 4096:
		mygoodchans = '0:1000~1200'
		flagspw = '0:41~4050'
		gainspw = '0:201~3600'
		gainspw2 = ''   # central good channels after split file for self-cal
	elif mynchan == 8192:
		mygoodchans = '0:2000~3000'
		flagspw = '0:500~7800'
		gainspw = '0:1000~7000'
		gainspw2 = ''   # central good channels after split file for self-cal
	elif mynchan == 16384:
		mygoodchans = '0:4000~6000'
		flagspw = '0:1000~14500'
		gainspw = '0:2000~13500'
		gainspw2 = ''   # central good channels after split file for self-cal
	elif mynchan == 128:
		mygoodchans = '0:50~70'
		flagspw = '0:5~115'
		gainspw = '0:11~115'
		gainspw2 = ''   # central good channels after split file for self-cal
	elif mynchan == 256:
#               if poldata == 'LL':
                if gmrt235 == True:
                        mygoodchans = '0:150~160'
                        flagspw = '0:70~220'
                        gainspw = '0:91~190'
                        gainspw2 = ''   # central good channels after split file for self-cal
                elif gmrt610 == True:
                        mygoodchans = '0:100~120'
                        flagspw = '0:11~240'
                        gainspw = '0:21~230'
                        gainspw2 = ''   # central good channels after split file for self-cal   
                else:
                        mygoodchans = '0:150~160'
                        flagspw = '0:11~240'
                        gainspw = '0:21~230'
                        gainspw2 = ''   # central good channels after split file for self-cal
	elif mynchan == 512:
		mygoodchans = '0:200~240'
		flagspw = '0:21~500'
		gainspw = '0:41~490'
		gainspw2 = ''   # central good channels after split file for self-cal	

# fix targets
	myfields = getfields(msfilename)
	stdcals = ['3C48','3C147','3C286','0542+498','1331+305','0137+331']
	vlacals = np.loadtxt('./vla-cals.list',dtype='string')
	myampcals =[]
	mypcals=[]
	mytargets=[]
	for i in range(0,len(myfields)):
		if myfields[i] in stdcals:
			myampcals.append(myfields[i])
		elif myfields[i] in vlacals:
			mypcals.append(myfields[i])
		else:
			mytargets.append(myfields[i])
	mybpcals = myampcals
##################################
#	if mypcals==[]:
#		mypcals = myampcals
##################################
#	print("Amplitude caibrators are", myampcals)
	logging.info('Amplitude caibrators are %s', str(myampcals))
#	print("Phase calibrators are", mypcals)
	logging.info('Phase calibrators are %s', str(mypcals))
#	print("Target sources are", mytargets)
	logging.info('Target sources are %s', str(mytargets))
# need a condition to see if the pcal is same as 
#if frommultisrcms==True:
	ampcalscans =[]
#	ampcalscans=0
	for i in range(0,len(myampcals)):
		ampcalscans.extend(getscans(msfilename, myampcals[i]))
#		ampcalscans=ampcalscans+ getscans(msfilename, myampcals[i])
	pcalscans=[]
#	pcalscans=0
	for i in range(0,len(mypcals)):
		pcalscans.extend(getscans(msfilename, mypcals[i]))
#		pcalscans=pcalscans+getscans(msfilename, mypcals[i])
	tgtscans=[]
#	tgtscans=0
	for i in range(0,len(mytargets)):
		tgtscans.extend(getscans(msfilename,mytargets[i]))
#		tgtscans=tgtscans+(getscans(msfilename,mytargets[i]))
	print ampcalscans
	print pcalscans	
	print tgtscans
	allscanlist= ampcalscans+pcalscans+tgtscans
###################################
# get a list of antennas
	antsused = getantlist(msfilename,int(allscanlist[0]))
	print antsused
###################################
# find band ants
	if flagbadants==True:
		findbadants = True
	if findbadants == True:
		myantlist = antsused
		mycmds = []
		meancutoff = 0.2    # uncalibrated mean cutoff
		mycorr1='rr'
		mycorr2='ll'
		mygoodchans1=mygoodchans
		mycalscans = ampcalscans+pcalscans
		print(mycalscans)
		logging.info(mycalscans)
#		myscan1 = pcalscans
		allbadants=[]
		for j in range(0,len(mycalscans)):
			myantmeans = []
			badantlist = []
			for i in range(0,len(myantlist)):
                                if mypol == 1:
                                        if poldata == 'RR':
                                                oneantmean1 = myvisstatampraw(msfilename,mygoodchans,myantlist[i],mycorr1,str(mycalscans[j]))
                                                oneantmean2 =oneantmean1*100.
                                        elif poldata == 'LL':
                                                oneantmean2 = myvisstatampraw(msfilename,mygoodchans,myantlist[i],mycorr2,str(mycalscans[j]))
                                                oneantmean1=oneantmean2*100.
                                else:
                                        oneantmean1 = myvisstatampraw(msfilename,mygoodchans,myantlist[i],mycorr1,str(mycalscans[j]))
                                        oneantmean2 = myvisstatampraw(msfilename,mygoodchans,myantlist[i],mycorr2,str(mycalscans[j]))
#				oneantmean1 = myvisstatampraw(msfilename,mygoodchans,myantlist[i],mycorr1,str(mycalscans[j]))
#				oneantmean2 = myvisstatampraw(msfilename,mygoodchans,myantlist[i],mycorr2,str(mycalscans[j]))
				oneantmean = min(oneantmean1,oneantmean2)
				myantmeans.append(oneantmean)
#				print myantlist[i], oneantmean1, oneantmean2
				if oneantmean < meancutoff:
					badantlist.append(myantlist[i])
					allbadants.append(myantlist[i])
#			print("The following antennas are bad for the given scan numbers.")
			logging.info("The following antennas are bad for the given scan numbers.")
#			print(badantlist, str(mycalscans[j]))
			logging.info('%s, %s',str(badantlist), str(mycalscans[j]))
			if badantlist!=[]:
				myflgcmd = "mode='manual' antenna='%s' scan='%s'" % (str('; '.join(badantlist)), str(mycalscans[j]))
				mycmds.append(myflgcmd)
				print(myflgcmd)
				logging.info(myflgcmd)
				onelessscan = mycalscans[j] - 1
				onemorescan = mycalscans[j] + 1
				if onelessscan in tgtscans:
					myflgcmd = "mode='manual' antenna='%s' scan='%s'" % (str('; '.join(badantlist)), str(mycalscans[j]-1))
					mycmds.append(myflgcmd)
					print(myflgcmd)
					logging.info(myflgcmd)
				if onemorescan in tgtscans:
					myflgcmd = "mode='manual' antenna='%s' scan='%s'" % (str('; '.join(badantlist)), str(mycalscans[j]+1))
					mycmds.append(myflgcmd)
					print(myflgcmd)
					logging.info(myflgcmd)
# execute the flagging commands accumulated in cmds
		print mycmds
		if flagbadants==True:
			print("Now flagging the bad antennas.")
			logging.info("Now flagging the bad antennas.")
			default(flagdata)
			flagdata(vis=msfilename,mode='list', inpfile=mycmds)	
######### Bad channel flagging for known persistent RFI.
	if flagbadfreq==True:
		findbadchans = True
	if findbadchans ==True:
		rfifreqall =[0.36E09,0.3796E09,0.486E09,0.49355E09,0.8808E09,0.885596E09,0.7646E09,0.769092E09] # always bad
		myfreqs =  freq_info(msfilename)
		mybadchans=[]
		for j in range(0,len(rfifreqall)-1,2):
#			print rfifreqall[j]
			for i in range(0,len(myfreqs)):
				if (myfreqs[i] > rfifreqall[j] and myfreqs[i] < rfifreqall[j+1]): #(myfreqs[i] > 0.486E09 and myfreqs[i] < 0.49355E09):
					mybadchans.append('0:'+str(i))
		mychanflag = str(', '.join(mybadchans))
#		print mychanflag
		if mybadchans!=[]:
#			print mychanflag
			myflgcmd = ["mode='manual' spw='%s'" % (mychanflag)]
			if flagbadfreq==True:
				default(flagdata)
				flagdata(vis=msfilename,mode='list', inpfile=myflgcmd)
		else:
#			print("No bad frequencies found in the range.")
			logging.info("No bad frequencies found in the range.")

############ Initial flagging ################

if flaginit == True:
	assert os.path.isdir(msfilename)
	casalog.filter('INFO')
#Step 1 : Flag the first channel.
	default(flagdata)
	flagdata(vis=msfilename, mode='manual', field='', spw='0:0', antenna='', correlation='', action='apply', savepars=True,
		cmdreason='badchan', outfile='flg1.dat')
#Step 3: Do a quack step 
	default(flagdata)
	flagdata(vis=msfilename, mode='quack', field='', spw='0', antenna='', correlation='', timerange='',
		quackinterval=setquackinterval, quackmode='beg', action='apply', savepars=True, cmdreason='quackbeg',
	        outfile='flg3.dat')
	default(flagdata)
	flagdata(vis=msfilename, mode='quack', field='', spw='0', antenna='', correlation='', timerange='', quackinterval=setquackinterval,
		quackmode='endb', action='apply', savepars=True, cmdreason='quackendb', outfile='flg3.dat')
# Clip at high amp levels
	if myampcals !=[]:
		default(flagdata)
		flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(','.join(myampcals)), clipminmax=clipfluxcal, datacolumn="DATA",clipoutside=True, clipzeros=True, extendpols=False, 
        		action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
	if mypcals !=[]:
		default(flagdata)
		flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(','.join(mypcals)), clipminmax=clipphasecal, datacolumn="DATA",clipoutside=True, clipzeros=True, extendpols=False, 
        		action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
# After clip, now flag using 'tfcrop' option for flux and phase cal tight flagging
		flagdata(vis=msfilename,mode="tfcrop", datacolumn="DATA", field=str(','.join(mypcals)), ntime="scan",
		        timecutoff=5.0, freqcutoff=5.0, timefit="line",freqfit="line",flagdimension="freqtime", 
		        extendflags=False, timedevscale=5.0,freqdevscale=5.0, extendpols=False,growaround=False,
		        action="apply", flagbackup=True,overwrite=True, writeflags=True)
# Now extend the flags (80% more means full flag, change if required)
		flagdata(vis=msfilename,mode="extend",spw=flagspw,field=str(','.join(mypcals)),datacolumn="DATA",clipzeros=True,
		         ntime="scan", extendflags=False, extendpols=True,growtime=80.0, growfreq=80.0,growaround=False,
		         flagneartime=False, flagnearfreq=False, action="apply", flagbackup=True,overwrite=True, writeflags=True)
######### target flagging ### clip first
	if target == True:
		if mytargets !=[]:
			flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(','.join(mytargets)), clipminmax=cliptarget, datacolumn="DATA",clipoutside=True, clipzeros=True, extendpols=False, 
		        	action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
# flagging with tfcrop before calibration
			default(flagdata)
			flagdata(vis=msfilename,mode="tfcrop", datacolumn="DATA", field=str(','.join(mytargets)), ntime="scan",
		        	timecutoff=6.0, freqcutoff=6.0, timefit="poly",freqfit="poly",flagdimension="freqtime", 
		        	extendflags=False, timedevscale=5.0,freqdevscale=5.0, extendpols=False,growaround=False,
		        	action="apply", flagbackup=True,overwrite=True, writeflags=True)
# Now extend the flags (80% more means full flag, change if required)
			flagdata(vis=msfilename,mode="extend",spw=flagspw,field=str(','.join(mytargets)),datacolumn="DATA",clipzeros=True,
		        	 ntime="scan", extendflags=False, extendpols=True,growtime=80.0, growfreq=80.0,growaround=False,
		        	 flagneartime=False, flagnearfreq=False, action="apply", flagbackup=True,overwrite=True, writeflags=True)
# Now summary
		flagdata(vis=msfilename,mode="summary",datacolumn="DATA", extendflags=True, 
	        	 name=vis+'summary.split', action="apply", flagbackup=True,overwrite=True, writeflags=True)	
#####################################################################
#if doinitcal==True:
#	print "After initial flagging:"
#	myflagtabs = flagmanager(vis = msfilename, mode ='list')
#	print 'myflagtabs=',myflagtabs

# Calibration begins.
if doinitcal == True:
	assert os.path.isdir(msfilename)
	mycalsuffix = ''
	casalog.filter('INFO')
#	print "Summary of flagtables before initial calibration:"
#	myflagtabs = flagmanager(vis = msfilename, mode ='list')
#	print 'myflagtabs=',myflagtabs
	clearcal(vis=msfilename)
#delmod step to keep model column free of spurious values
	for i in range(0,len(myampcals)):
#		delmod(vis=msfilename)	
		default(setjy)
		setjy(vis=msfilename, spw=flagspw, field=myampcals[i])
		print "Done setjy on %s"%(myampcals[i])
# Delay calibration  using the first flux calibrator in the list - should depend on which is less flagged
	if os.path.isdir(str(msfilename)+'.K1'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.K1'+mycalsuffix)
	gaincal(vis=msfilename, caltable=str(msfilename)+'.K1'+mycalsuffix, spw =flagspw, field=myampcals[0], 
		solint='60s', refant=ref_ant,	solnorm= True, gaintype='K', gaintable=[], parang=True)
	kcorrfield =myampcals[0]
#	print 'wrote table',str(msfilename)+'.K1'
# an initial bandpass
	if os.path.isdir(str(msfilename)+'.AP.G0'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.AP.G0'+mycalsuffix)
	default(gaincal)
#	gaincal(vis=msfilename, caltable=str(msfilename)+'.AP.G0', append=True, field=str(','.join(mybpcals)), 
#		spw =flagspw, solint = 'int', refant = ref_ant, minsnr = 2.0, solmode ='L1R', gaintype = 'G', calmode = 'ap', gaintable = [str(msfilename)+'.K1'],
#		interp = ['nearest,nearestflag', 'nearest,nearestflag' ], parang = True)
	gaincal(vis=msfilename, caltable=str(msfilename)+'.AP.G0'+mycalsuffix, append=True, field=str(','.join(mybpcals)), 
		spw =flagspw, solint = 'int', refant = ref_ant, minsnr = 2.0, gaintype = 'G', calmode = 'ap', gaintable = [str(msfilename)+'.K1'+mycalsuffix],
		interp = ['nearest,nearestflag', 'nearest,nearestflag' ], parang = True)
	if os.path.isdir(str(msfilename)+'.B1'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.B1'+mycalsuffix)
	default(bandpass)
	bandpass(vis=msfilename, caltable=str(msfilename)+'.B1'+mycalsuffix, spw =flagspw, field=str(','.join(mybpcals)), solint='inf', refant=ref_ant, solnorm = True,
		minsnr=2.0, fillgaps=8, parang = True, gaintable=[str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.AP.G0'+mycalsuffix], interp=['nearest,nearestflag','nearest,nearestflag'])
# do a gaincal on all calibrators
	mycals=myampcals+mypcals
	i=0
	if os.path.isdir(str(msfilename)+'.AP.G'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.AP.G'+mycalsuffix)
	for i in range(0,len(mycals)):
		mygaincal_ap2(msfilename,mycals[i],ref_ant,gainspw,uvracal,mycalsuffix)
# Get flux scale
	if os.path.isdir(str(msfilename)+'.fluxscale'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.fluxscale'+mycalsuffix)
######################################
	if mypcals !=[]:
		if '3C286' in myampcals:
			myfluxscale= getfluxcal2(msfilename,'3C286',str(', '.join(mypcals)),mycalsuffix)
			myfluxscaleref = '3C286'
		elif '3C147' in myampcals:
			myfluxscale= getfluxcal2(msfilename,'3C147',str(', '.join(mypcals)),mycalsuffix)
			myfluxscaleref = '3C147'
		else:
			myfluxscale= getfluxcal2(msfilename,myampcals[0],str(', '.join(mypcals)),mycalsuffix)
			myfluxscaleref = myampcals[0]
#		print(myfluxscale)
		logging.info(myfluxscale)
		mygaintables =[str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
	else:
		mygaintables =[str(msfilename)+'.AP.G'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
##############################
	for i in range(0,len(myampcals)):
		default(applycal)
		applycal(vis=msfilename, field=myampcals[i], spw = flagspw, gaintable=mygaintables, gainfield=[myampcals[i],'',''], 
        		 interp=['nearest','',''], calwt=[False], parang=False)
#For phase calibrator:
	if mypcals !=[]:
		default(applycal)
		applycal(vis=msfilename, field=str(', '.join(mypcals)), spw = flagspw, gaintable=mygaintables, gainfield=str(', '.join(mypcals)), 
		         interp=['nearest','','nearest'], calwt=[False], parang=False)
#For the target:
	if target ==True:
		if mypcals !=[]:
			default(applycal)
			applycal(vis=msfilename, field=str(', '.join(mytargets)), spw = flagspw, gaintable=mygaintables,
		        	 gainfield=[str(', '.join(mypcals)),'',''],interp=['linear','','nearest'], calwt=[False], parang=False)
		else:
			default(applycal)
			applycal(vis=msfilename, field=str(', '.join(mytargets)), spw = flagspw, gaintable=mygaintables,
		        	 gainfield=[str(', '.join(myampcals)),'',''],interp=['linear','','nearest'], calwt=[False], parang=False)	
#	print("Finished initial calibration.")
	logging.info("Finished initial calibration.")
#	print "Summary of flagtables after initial calibration:"
#	myflagtabs = flagmanager(vis = msfilename, mode ='list')
#	print 'myflagtabs=',myflagtabs

#############################################################################3

# Do tfcrop on the file first - only for the target
# No need for antenna selection




#######Ishwar post calibration flagging
if mydoflag == True:
	assert os.path.isdir(msfilename)
#	print("You have chosen to flag after the initial calibration.")
	logging.info("You have chosen to flag after the initial calibration.")
	default(flagdata)
	if myampcals !=[]:
		flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(', '.join(myampcals)), clipminmax=clipfluxcal,
        		datacolumn="corrected",clipoutside=True, clipzeros=True, extendpols=False, 
        		action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
	if mypcals !=[]:
		flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(', '.join(mypcals)), clipminmax=clipphasecal,
        		datacolumn="corrected",clipoutside=True, clipzeros=True, extendpols=False, 
        		action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
# After clip, now flag using 'tfcrop' option for flux and phase cal tight flagging
		flagdata(vis=msfilename,mode="tfcrop", datacolumn="corrected", field=str(', '.join(mypcals)), ntime="scan",
        		timecutoff=6.0, freqcutoff=5.0, timefit="line",freqfit="line",flagdimension="freqtime", 
        		extendflags=False, timedevscale=5.0,freqdevscale=5.0, extendpols=False,growaround=False,
        		action="apply", flagbackup=True,overwrite=True, writeflags=True)
# now flag using 'rflag' option  for flux and phase cal tight flagging
		flagdata(vis=msfilename,mode="rflag",datacolumn="corrected",field=str(', '.join(mypcals)), timecutoff=5.0, 
		        freqcutoff=5.0,timefit="poly",freqfit="line",flagdimension="freqtime", extendflags=False,
		        timedevscale=4.0,freqdevscale=4.0,spectralmax=500.0,extendpols=False, growaround=False,
		        flagneartime=False,flagnearfreq=False,action="apply",flagbackup=True,overwrite=True, writeflags=True)
# Now extend the flags (70% more means full flag, change if required)
		flagdata(vis=msfilename,mode="extend",spw=flagspw,field=str(', '.join(mypcals)),datacolumn="corrected",clipzeros=True,
		         ntime="scan", extendflags=False, extendpols=False,growtime=90.0, growfreq=90.0,growaround=False,
		         flagneartime=False, flagnearfreq=False, action="apply", flagbackup=True,overwrite=True, writeflags=True)
# Now flag for target - moderate flagging, more flagging in self-cal cycles
	if mytargets !=[]:
		flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(', '.join(mytargets)), clipminmax=cliptarget,
		        datacolumn="corrected",clipoutside=True, clipzeros=True, extendpols=False, 
		        action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
# C-C baselines are selected
		a, b = getbllists(msfilename)
		flagdata(vis=msfilename,mode="tfcrop", datacolumn="corrected", field=str(', '.join(mytargets)), antenna=a[0],
			ntime="scan", timecutoff=8.0, freqcutoff=8.0, timefit="poly",freqfit="line",flagdimension="freqtime", 
		        extendflags=False, timedevscale=5.0,freqdevscale=5.0, extendpols=False,growaround=False,
		        action="apply", flagbackup=True,overwrite=True, writeflags=True)
# C- arm antennas and arm-arm baselines are selected.
		flagdata(vis=msfilename,mode="tfcrop", datacolumn="corrected", field=str(', '.join(mytargets)), antenna=b[0],
			ntime="scan", timecutoff=6.0, freqcutoff=5.0, timefit="poly",freqfit="line",flagdimension="freqtime", 
        		extendflags=False, timedevscale=5.0,freqdevscale=5.0, extendpols=False,growaround=False,
        		action="apply", flagbackup=True,overwrite=True, writeflags=True)
# now flag using 'rflag' option
# C-C baselines are selected
		flagdata(vis=msfilename,mode="rflag",datacolumn="corrected",field=str(', '.join(mytargets)), timecutoff=5.0, antenna=a[0],
        		freqcutoff=8.0,timefit="poly",freqfit="poly",flagdimension="freqtime", extendflags=False,
        		timedevscale=8.0,freqdevscale=5.0,spectralmax=500.0,extendpols=False, growaround=False,
        		flagneartime=False,flagnearfreq=False,action="apply",flagbackup=True,overwrite=True, writeflags=True)
# C- arm antennas and arm-arm baselines are selected.
		flagdata(vis=msfilename,mode="rflag",datacolumn="corrected",field=str(', '.join(mytargets)), timecutoff=5.0, antenna=b[0],
        		freqcutoff=5.0,timefit="poly",freqfit="poly",flagdimension="freqtime", extendflags=False,
        		timedevscale=5.0,freqdevscale=5.0,spectralmax=500.0,extendpols=False, growaround=False,
        		flagneartime=False,flagnearfreq=False,action="apply",flagbackup=True,overwrite=True, writeflags=True)
# Now summary
	flagdata(vis=msfilename,mode="summary",datacolumn="corrected", extendflags=True, 
         name=vis+'summary.split', action="apply", flagbackup=True,overwrite=True, writeflags=True)


#################### new redocal #########################3
# Calibration begins.
if redocal == True:
	assert os.path.isdir(msfilename)
#	print("You have chosen to redo the calibration on your data.")
	logging.info("You have chosen to redo the calibration on your data.")
	mycalsuffix = 'recal'
	casalog.filter('INFO')
#	print "Summary of flagtables before calibration:"
#	myflagtabs = flagmanager(vis = msfilename, mode ='list')
#	print 'myflagtabs=',myflagtabs
	clearcal(vis=msfilename)
#delmod step to keep model column free of spurious values
	for i in range(0,len(myampcals)):
#		delmod(vis=msfilename)	
		default(setjy)
		setjy(vis=msfilename, spw=flagspw, field=myampcals[i])
#		print("Done setjy on %s"%(myampcals[i]))
		logging.info("Done setjy on %s"%(myampcals[i]))
# Delay calibration  using the first flux calibrator in the list - should depend on which is less flagged
	if os.path.isdir(str(msfilename)+'.K1'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.K1'+mycalsuffix)
	gaincal(vis=msfilename, caltable=str(msfilename)+'.K1'+mycalsuffix, spw =flagspw, field=myampcals[0], 
		solint='60s', refant=ref_ant,	solnorm= True, gaintype='K', gaintable=[], parang=True)
	kcorrfield =myampcals[0]
#	print 'wrote table',str(msfilename)+'.K1'+mycalsuffix
# an initial bandpass
	if os.path.isdir(str(msfilename)+'.AP.G0'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.AP.G0'+mycalsuffix)
	default(gaincal)
#	gaincal(vis=msfilename, caltable=str(msfilename)+'.AP.G0', append=True, field=str(','.join(mybpcals)), 
#		spw =flagspw, solint = 'int', refant = ref_ant, minsnr = 2.0, solmode ='L1R', gaintype = 'G', calmode = 'ap', gaintable = [str(msfilename)+'.K1'],
#		interp = ['nearest,nearestflag', 'nearest,nearestflag' ], parang = True)
	gaincal(vis=msfilename, caltable=str(msfilename)+'.AP.G0'+mycalsuffix, append=True, field=str(','.join(mybpcals)), 
		spw =flagspw, solint = 'int', refant = ref_ant, minsnr = 2.0, gaintype = 'G', calmode = 'ap', gaintable = [str(msfilename)+'.K1'],
		interp = ['nearest,nearestflag', 'nearest,nearestflag' ], parang = True)
	if os.path.isdir(str(msfilename)+'.B1'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.B1'+mycalsuffix)
	default(bandpass)
	bandpass(vis=msfilename, caltable=str(msfilename)+'.B1'+mycalsuffix, spw =flagspw, field=str(','.join(mybpcals)), solint='inf', refant=ref_ant, solnorm = True,
		minsnr=2.0, fillgaps=8, parang = True, gaintable=[str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.AP.G0'+mycalsuffix], interp=['nearest,nearestflag','nearest,nearestflag'])
# do a gaingal on alll calibrators
	mycals=myampcals+mypcals
	i=0
	if os.path.isdir(str(msfilename)+'.AP.G'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.AP.G'+mycalsuffix)
	for i in range(0,len(mycals)):
		mygaincal_ap2(msfilename,mycals[i],ref_ant,gainspw,uvracal,mycalsuffix)
# Get flux scale
#if doinitcal == True:	
	if os.path.isdir(str(msfilename)+'.fluxscale'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.fluxscale'+mycalsuffix)
########################################
	if mypcals !=[]:
		if '3C286' in myampcals:
#			myfluxscale= getfluxcal(msfilename,'3C286',str(', '.join(mypcals)))
			myfluxscale= getfluxcal2(msfilename,'3C286',str(', '.join(mypcals)),mycalsuffix)
			myfluxscaleref = '3C286'
		elif '3C147' in myampcals:
#			myfluxscale= getfluxcal(msfilename,'3C147',str(', '.join(mypcals)))
			myfluxscale= getfluxcal2(msfilename,'3C147',str(', '.join(mypcals)),mycalsuffix)
			myfluxscaleref = '3C147'
		else:
#			myfluxscale= getfluxcal(msfilename,myampcals[0],str(', '.join(mypcals)))
			myfluxscale= getfluxcal2(msfilename,myampcals[0],str(', '.join(mypcals)),mycalsuffix)
			myfluxscaleref = myampcals[0]
#		print(myfluxscale)
		logging.info(myfluxscale)
		mygaintables =[str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
	else:
		mygaintables =[str(msfilename)+'.AP.G'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
###############################################################
	for i in range(0,len(myampcals)):
		default(applycal)
		applycal(vis=msfilename, field=myampcals[i], spw = flagspw, gaintable=mygaintables, gainfield=[myampcals[i],'',''], 
        		 interp=['nearest','',''], calwt=[False], parang=False)
#For phase calibrator:
	if mypcals !=[]:
		default(applycal)
		applycal(vis=msfilename, field=str(', '.join(mypcals)), spw = flagspw, gaintable=mygaintables, gainfield=str(', '.join(mypcals)), 
		         interp=['nearest','','nearest'], calwt=[False], parang=False)
#For the target:
	if target ==True:
		if mypcals !=[]:
			default(applycal)
			applycal(vis=msfilename, field=str(', '.join(mytargets)), spw = flagspw, gaintable=mygaintables,
		        	 gainfield=[str(', '.join(mypcals)),'',''],interp=['linear','','nearest'], calwt=[False], parang=False)
		else:
			default(applycal)
			applycal(vis=msfilename, field=str(', '.join(mytargets)), spw = flagspw, gaintable=mygaintables,
		        	 gainfield=[str(', '.join(myampcals)),'',''],interp=['linear','','nearest'], calwt=[False], parang=False)			
#	print("Finished re-calibration.")
	logging.info("Finished re-calibration.")
#	print "Summary of flagtables after initial calibration:"
#	myflagtabs = flagmanager(vis = msfilename, mode ='list')
#	print 'myflagtabs=',myflagtabs

###############################################################
#############################################################
# SPLIT step
#############################################################
if dosplit == True:
	assert os.path.isdir(msfilename)
#	print("The data on targets will be split into separate files.")
	logging.info("The data on targets will be split into separate files.")
	casalog.filter('INFO')
# fix targets
	myfields = getfields(msfilename)
	stdcals = ['3C48','3C147','3C286','0542+498','1331+305','0137+331']
	vlacals = np.loadtxt('./vla-cals.list',dtype='string')
	myampcals =[]
	mypcals=[]
	mytargets=[]
	for i in range(0,len(myfields)):
		if myfields[i] in stdcals:
			myampcals.append(myfields[i])
		elif myfields[i] in vlacals:
			mypcals.append(myfields[i])
		else:
			mytargets.append(myfields[i])
	for i in range(0,len(mytargets)):
		if os.path.isdir(mytargets[i]+'split.ms') == True:
			os.system('rm -rf '+mytargets[i]+'split.ms')
		print("The following spw is split for the target source:", gainspw)
		splitfilename = mysplitinit(msfilename,mytargets[i],gainspw,1)

#############################################################
# Flagging on split file
#############################################################

if flagsplitfile == True:
	assert os.path.isdir(splitfilename)
	print("You have chosen to flag on the split file.")
	myantselect =''
	mytfcrop(splitfilename,'',myantselect,8.0,8.0,'DATA','')
	a, b = getbllists(splitfilename)
	tdev = 6.0
	fdev = 6.0
	myrflag(splitfilename,'',a[0],tdev,fdev,'DATA','')
	tdev = 5.0
	fdev = 5.0
	myrflag(splitfilename,'',b[0],tdev,fdev,'DATA','')
	

#############################################################
# SPLIT AVERAGE
#############################################################
if dosplitavg == True:
	assert os.path.isdir(splitfilename)
#	print("Your data will be averaged in frequency.")
	logging.info("Your data will be averaged in frequency.")
	if os.path.isdir(mytargets[i]+'avg-split.ms') == True:
		os.system('rm -rf '+mytargets[i]+'avg-split.ms')
	splitavgfilename = mysplitavg(splitfilename,'','',chanavg)


if doflagavg == True:
	assert os.path.isdir(splitavgfilename)
#	print("Flagging on frequency averaged data.")
	logging.info("Flagging on freqeuncy averaged data.")
	a, b = getbllists(splitavgfilename)
	myrflagavg(splitavgfilename,'',b[0],6.0,6.0,'DATA','')
	myrflagavg(splitavgfilename,'',a[0],6.0,6.0,'DATA','')


############################################################

if makedirty == True:
	assert os.path.isdir(splitavgfilename)
	myfile2 = [splitavgfilename]
	usetclean = True
	if usetclean == True:
		myselfcal(myfile2,ref_ant,scaloops,mypcaloops,mJythreshold,imcellsize,imsize_pix,use_nterms,nwprojpl,scalsolints,clipresid,'','',makedirty,niter_start)

if doselfcal == True:
 	assert os.path.isdir(splitavgfilename)
	casalog.filter('INFO')
	clearcal(vis = splitavgfilename)
	myfile2 = [splitavgfilename]
	if usetclean == True:
		myselfcal(myfile2,ref_ant,scaloops,mypcaloops,mJythreshold,imcellsize,imsize_pix,use_nterms,nwprojpl,scalsolints,clipresid,'','',makedirty,niter_start)


