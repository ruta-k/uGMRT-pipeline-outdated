###################################################################
# CAPTURE: CAsa Pipeline-cum-Toolkit for Upgraded GMRT data REduction
###################################################################
# Pipeline for analysing data from the GMRT and the uGMRT.
# Combination of pipelines done by Ruta Kale based on pipelines developed independently by Ruta Kale 
# and Ishwar Chandra.
# Date: 8th Aug 2019
# README : Please read the following instructions to run this pipeline on your data
# Files and paths required
# 0. This files from git should be placed and executed in the directory where your data files are located.
# 1. If starting from lta file, please provide the paths to the listscan and gvfits executable binaries in "gvbinpath" as shown.
# 2. Keep the vla-cals.list file in the same area.
# Please email ruta@ncra.tifr.res.in if you run into any issue and cannot solve.
# 



import logging
import os
from datetime import datetime
logfile_name = datetime.now().strftime('capture_%H_%M_%S_%d_%m_%Y.log')
logging.basicConfig(filename=logfile_name,level=logging.DEBUG)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

logging.info("#######################################################################################")
logging.info("You are using CAPTURE: CAsa Pipeline-cum-Toolkit for Upgraded GMRT data REduction.")
logging.info("This has been developed at NCRA by Ruta Kale and Ishwara Chandra.")
logging.info("#######################################################################################")
logging.info("LOGFILE = %s", logfile_name)
logging.info("CASA_LOGFILE = %s", 'casa-'+logfile_name)
logging.info("#######################################################################################")

CASA_logfile = 'casa-'+logfile_name
casalog.setlogfile(CASA_logfile)

import ConfigParser
config = ConfigParser.ConfigParser()
config.read('config_capture.ini')


fromlta = config.getboolean('basic', 'fromlta')
fromfits = config.getboolean('basic', 'fromfits')
frommultisrcms = config.getboolean('basic','frommultisrcms')
findbadants = config.getboolean('basic','findbadants')                          
flagbadants= config.getboolean('basic','flagbadants')                      
findbadchans = config.getboolean('basic','findbadchans')                         
flagbadfreq= config.getboolean('basic','flagbadfreq')                           
flaginit = config.getboolean('basic','flaginit')                             
doinitcal = config.getboolean('basic','doinitcal')                              
doflag = config.getboolean('basic','doflag')                              
redocal = config.getboolean('basic','redocal')                              
dosplit = config.getboolean('basic','dosplit')                               
flagsplitfile = config.getboolean('basic','flagsplitfile')                            
dosplitavg = config.getboolean('basic','dosplitavg')                             
doflagavg = config.getboolean('basic','doflagavg')                             
makedirty = config.getboolean('basic','makedirty')                            
doselfcal = config.getboolean('basic','doselfcal') 
dosubbandselfcal = config.getboolean('basic','dosubbandselfcal')
usetclean = config.getboolean('default','usetclean')                        
ltafile =config.get('basic','ltafile')
gvbinpath = config.get('basic', 'gvbinpath').split(',')
fits_file = config.get('basic','fits_file')
msfilename =config.get('basic','msfilename')
splitfilename =config.get('basic','splitfilename')
splitavgfilename = config.get('basic','splitavgfilename')
setquackinterval = config.getfloat('basic','setquackinterval')
ref_ant = config.get('basic','ref_ant')
clipfluxcal = [float(config.get('basic','clipfluxcal').split(',')[0]),float(config.get('basic','clipfluxcal').split(',')[1])]
clipphasecal =[float(config.get('basic','clipphasecal').split(',')[0]),float(config.get('basic','clipphasecal').split(',')[1])]
cliptarget =[float(config.get('basic','cliptarget').split(',')[0]),float(config.get('basic','cliptarget').split(',')[1])]   
clipresid=[float(config.get('basic','clipresid').split(',')[0]),float(config.get('basic','clipresid').split(',')[1])]
chanavg = config.getint('basic','chanavg')
subbandchan = config.getint('basic','subbandchan')
imcellsize = [config.get('basic','imcellsize')]
imsize_pix = int(config.get('basic','imsize_pix'))
scaloops = config.getint('basic','scaloops')
mJythreshold = float(config.get('basic','mJythreshold'))
pcaloops = config.getint('basic','pcaloops')
scalsolints = config.get('basic','scalsolints').split(',')
niter_start = int(config.get('basic','niter_start'))
use_nterms = config.getint('basic','use_nterms')
nwprojpl = config.getint('basic','nwprojpl')
uvracal=config.get('default','uvracal')
uvrascal=config.get('default','uvrascal')
target = config.getboolean('default','target')


execfile('ugfunctions.py')

testfitsfile = False

if fromlta == True:
	logging.info("You have chosen to convert lta to FITS.")
	testltafile = os.path.isfile(ltafile)
	if testltafile == True:
		logging.info("The lta %s file exists.", ltafile)
		testlistscan = os.path.isfile(gvbinpath[0])
		testgvfits = os.path.isfile(gvbinpath[1])
		if testlistscan and testgvfits == True:
			os.system(gvbinpath[0]+' '+ltafile)
                        if fits_file!= '' and fits_file != 'TEST.FITS':
                                os.system("sed -i 's/TEST.FITS/'"+fits_file+"/ "+ltafile.split('.')[0]+'.log')
                        try:
                                assert os.path.isfile(fits_file)
                                testfitsfile = True
                        except AssertionError:
                                if os.path.isfile('TEST.FITS') == True: 
#                                assert os.path.isfile('TEST.FITS'), 
                                        logging.info("The file TEST.FITS file already exists. New will not be created.")
                                        testfitsfile = True
                                        fits_file = 'TEST.FITS'
                                else:
		                        os.system(gvbinpath[1]+' '+ltafile.split('.')[0]+'.log')
                                        testfitsfile = True
    		else:	
			logging.info("Error: Check if listscan and gvfits are present and executable.")
	else:
		logging.info("The given lta file does not exist. Exiting the code.")
		logging.info("If you are not starting from lta file please set fromlta to False and rerun.")
		sys.exit()


#testfitsfile = False 

if fromfits == True:
        if fits_file != '':
                try:
                        assert os.path.isfile(fits_file)
                        testfitsfile = True
                except AssertionError:
                        try:
                                assert os.path.isfile('TEST.FITS')
                                testfitsfile = True
                        except AssertionError:
                                logging.info("Please provide the name of the FITS file.")
                                sys.exit()

		

if testfitsfile == True:
        if msfilename != '':
                try:
                        assert os.path.isdir(msfilename), "The given msfile already exists, will not create new."
                except AssertionError:
                        logging.info("The given msfile does not exist, will create new.")
        else:
                try:
                        assert os.path.isdir(fits_file+'.MS')
                except AssertionError:
                        msfilename = fits_file+'.MS'           
	default(importgmrt)
	importgmrt(fitsfile=fits_file, vis = msfilename)
	if os.path.isfile(msfilename+'.list') == True:
		os.system('rm '+msfilename+'.list')
	vislistobs(msfilename)
        logging.info("Please see the text file with the extension .list to find out more about your data.")
	

testms = False

if frommultisrcms == True:
        if msfilename != '':
	        testms = os.path.isdir(msfilename)
        else:
                try:
                        assert os.path.isdir('TEST.FITS.MS')
                        testms = True
                        msfilename = 'TEST.FITS.MS'
                except AssertionError:
                        logging.info("Tried to find the MS file with default name. File not found. Please provide the name of the msfile or create the MS by setting fromfits = True.")
                        sys.exit()
	if testms == False:
		logging.info("The MS file does not exist. Please provide msfilename. Exiting the code...")
                sys.exit()


if testms == True:
        gainspw, mygoodchans, flagspw, mypol = getgainspw(msfilename)
	logging.info("Channel range for calibration:")
        logging.info(gainspw)
	logging.info("Assumed clean channel range:")
        logging.info(mygoodchans)
	logging.info("Channel range for flagging:")
        logging.info(flagspw)
	logging.info("Polarizations in the file:")
        logging.info(mypol)
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
	logging.info('Amplitude caibrators are %s', str(myampcals))
	logging.info('Phase calibrators are %s', str(mypcals))
	logging.info('Target sources are %s', str(mytargets))
# need a condition to see if the pcal is same as 
	ampcalscans =[]
	for i in range(0,len(myampcals)):
		ampcalscans.extend(getscans(msfilename, myampcals[i]))
	pcalscans=[]
	for i in range(0,len(mypcals)):
		pcalscans.extend(getscans(msfilename, mypcals[i]))
	tgtscans=[]
	for i in range(0,len(mytargets)):
		tgtscans.extend(getscans(msfilename,mytargets[i]))
#	print(ampcalscans)
	logging.info("Amplitude calibrator scans are:")
        logging.info(ampcalscans)
#	print(pcalscans)
	logging.info("Phase calibrator scans are:")
        logging.info(pcalscans)
#	print(tgtscans)
	logging.info("Target source scans are:")
        logging.info(tgtscans)
	allscanlist= ampcalscans+pcalscans+tgtscans
###################################
# get a list of antennas
	antsused = getantlist(msfilename,int(allscanlist[0]))
	logging.info("Antennas in the file:")
        logging.info(antsused)
###################################
# find band ants
	if flagbadants==True:
		findbadants = True
	if findbadants == True:
		myantlist = antsused
		mycmds = []
#############
		meancutoff = getbandcut(msfilename)
#############
		mycorr1='rr'
		mycorr2='ll'
		mygoodchans1=mygoodchans
		mycalscans = ampcalscans+pcalscans
#		print(mycalscans)
		logging.info("Calibrator scan numbers:")
		logging.info(mycalscans)
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
				oneantmean = min(oneantmean1,oneantmean2)
				myantmeans.append(oneantmean)
				if oneantmean < meancutoff:
					badantlist.append(myantlist[i])
					allbadants.append(myantlist[i])
			logging.info("The following antennas are bad for the given scan numbers.")
			logging.info('%s, %s',str(badantlist), str(mycalscans[j]))
			if badantlist!=[]:
				myflgcmd = "mode='manual' antenna='%s' scan='%s'" % (str('; '.join(badantlist)), str(mycalscans[j]))
				mycmds.append(myflgcmd)
				logging.info(myflgcmd)
				onelessscan = mycalscans[j] - 1
				onemorescan = mycalscans[j] + 1
				if onelessscan in tgtscans:
					myflgcmd = "mode='manual' antenna='%s' scan='%s'" % (str('; '.join(badantlist)), str(mycalscans[j]-1))
					mycmds.append(myflgcmd)
					logging.info(myflgcmd)
				if onemorescan in tgtscans:
					myflgcmd = "mode='manual' antenna='%s' scan='%s'" % (str('; '.join(badantlist)), str(mycalscans[j]+1))
					mycmds.append(myflgcmd)
					logging.info(myflgcmd)
# execute the flagging commands accumulated in cmds
		if flagbadants==True:
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
			for i in range(0,len(myfreqs)):
				if (myfreqs[i] > rfifreqall[j] and myfreqs[i] < rfifreqall[j+1]): #(myfreqs[i] > 0.486E09 and myfreqs[i] < 0.49355E09):
					mybadchans.append('0:'+str(i))
		mychanflag = str(', '.join(mybadchans))
		if mybadchans!=[]:
			myflgcmd = ["mode='manual' spw='%s'" % (mychanflag)]
			if flagbadfreq==True:
				default(flagdata)
				flagdata(vis=msfilename,mode='list', inpfile=myflgcmd)
		else:
			logging.info("None of the well-known RFI-prone frequencies were found in the data.")
############ Initial flagging ################
if flaginit == True:
        try:
                assert os.path.isdir(msfilename), "flaginit = True but ms file not found."
        except AssertionError:
                logging.info("flaginit = True but ms file not found.")
                sys.exit()
	casalog.filter('INFO')
#Step 1 : Flag the first channel.
	default(flagdata)
	flagdata(vis=msfilename, mode='manual', field='', spw='0:0', antenna='', correlation='', action='apply', savepars=True,
		cmdreason='badchan', outfile='')
#Step 3: Do a quack step 
	default(flagdata)
	flagdata(vis=msfilename, mode='quack', field='', spw='0', antenna='', correlation='', timerange='',
		quackinterval=setquackinterval, quackmode='beg', action='apply', savepars=True, cmdreason='quackbeg',
	        outfile='')
	default(flagdata)
	flagdata(vis=msfilename, mode='quack', field='', spw='0', antenna='', correlation='', timerange='', quackinterval=setquackinterval,
		quackmode='endb', action='apply', savepars=True, cmdreason='quackendb', outfile='')
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
        logging.info("A flagging summary is provided for the MS file.")
        flagsummary(msfilename)
#####################################################################
# Calibration begins.
if doinitcal == True:
	assert os.path.isdir(msfilename)
        try:
                assert os.path.isdir(msfilename), "doinitcal = True but ms file not found."
        except AssertionError:
                logging.info("doinitcal = True but ms file not found.")
                sys.exit()
	mycalsuffix = ''
	casalog.filter('INFO')
	clearcal(vis=msfilename)
	for i in range(0,len(myampcals)):
		default(setjy)
		setjy(vis=msfilename, spw=flagspw, field=myampcals[i])
# Delay calibration  using the first flux calibrator in the list - should depend on which is less flagged
	if os.path.isdir(str(msfilename)+'.K1'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.K1'+mycalsuffix)
	gaincal(vis=msfilename, caltable=str(msfilename)+'.K1'+mycalsuffix, spw =flagspw, field=myampcals[0], 
		solint='60s', refant=ref_ant,	solnorm= True, gaintype='K', gaintable=[], parang=True)
	kcorrfield =myampcals[0]
# an initial bandpass
	if os.path.isdir(str(msfilename)+'.AP.G0'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.AP.G0'+mycalsuffix)
	default(gaincal)
	gaincal(vis=msfilename, caltable=str(msfilename)+'.AP.G0'+mycalsuffix, append=True, field=str(','.join(mybpcals)), 
		spw =flagspw, solint = 'int', refant = ref_ant, minsnr = 2.0, solmode = 'L1R', gaintype = 'G', calmode = 'ap', gaintable = [str(msfilename)+'.K1'+mycalsuffix],
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
	logging.info("Finished initial calibration.")
        logging.info("A flagging summary is provided for the MS file.")
        flagsummary(msfilename)
#############################################################################3
#######Ishwar post calibration flagging
if doflag == True:
        try:
                assert os.path.isdir(msfilename), "doflag = True but ms file not found."
        except AssertionError:
                logging.info("doflag = True but ms file not found.")
                sys.exit()
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
        logging.info("A flagging summary is provided for the MS file.")
        flagsummary(msfilename)

#################### new redocal #########################3
# Calibration begins.
if redocal == True:
        try:
                assert os.path.isdir(msfilename), "redocal = True but ms file not found."
        except AssertionError:
                logging.info("redocal = True but ms file not found.")
                sys.exit()
	logging.info("You have chosen to redo the calibration on your data.")
	mycalsuffix = 'recal'
	casalog.filter('INFO')
	clearcal(vis=msfilename)
	for i in range(0,len(myampcals)):
		default(setjy)
		setjy(vis=msfilename, spw=flagspw, field=myampcals[i])
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
	gaincal(vis=msfilename, caltable=str(msfilename)+'.AP.G0'+mycalsuffix, append=True, field=str(','.join(mybpcals)), 
		spw =flagspw, solint = 'int', refant = ref_ant, minsnr = 2.0, solmode ='L1R', gaintype = 'G', calmode = 'ap', gaintable = [str(msfilename)+'.K1'],
		interp = ['nearest,nearestflag', 'nearest,nearestflag' ], parang = True)
	if os.path.isdir(str(msfilename)+'.B1'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.B1'+mycalsuffix)
	default(bandpass)
	bandpass(vis=msfilename, caltable=str(msfilename)+'.B1'+mycalsuffix, spw =flagspw, field=str(','.join(mybpcals)), solint='inf', refant=ref_ant, solnorm = True,
		minsnr=2.0, fillgaps=8, parang = True, gaintable=[str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.AP.G0'+mycalsuffix], interp=['nearest,nearestflag','nearest,nearestflag'])
# do a gaingal on all calibrators
	mycals=myampcals+mypcals
	i=0
	if os.path.isdir(str(msfilename)+'.AP.G'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.AP.G'+mycalsuffix)
	for i in range(0,len(mycals)):
		mygaincal_ap2(msfilename,mycals[i],ref_ant,gainspw,uvracal,mycalsuffix)
# Get flux scale
	if os.path.isdir(str(msfilename)+'.fluxscale'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.fluxscale'+mycalsuffix)
########################################
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
	logging.info("Finished re-calibration.")
        logging.info("A flagging summary is provided for the MS file.")
        flagsummary(msfilename)
#############################################################
# SPLIT step
#############################################################
if dosplit == True:
#	assert os.path.isdir(msfilename), "dosplit = True but ms file not found."
        try:
                assert os.path.isdir(msfilename), "dosplit = True but ms file not found."
        except AssertionError:
                logging.info("dosplit = True but ms file not found.")
                sys.exit()
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
        gainspw1,goodchans,flg_chans,pols = getgainspw(msfilename)
	for i in range(0,len(mytargets)):
		if os.path.isdir(mytargets[i]+'split.ms') == True:
                        logging.info("The existing split file will be deleted.")
			os.system('rm -rf '+mytargets[i]+'split.ms')
                logging.info("Splitting target source data.")
                logging.info(gainspw1)
		splitfilename = mysplitinit(msfilename,mytargets[i],gainspw1,1)
#############################################################
# Flagging on split file
#############################################################

if flagsplitfile == True:
        try:
                assert os.path.isdir(splitfilename), "flagsplitfile = True but the split file not found."
        except AssertionError:
                logging.info("flagsplitfile = True but the split file not found.")
                sys.exit()
        logging.info("Now proceeding to flag on the split file.")
	myantselect =''
	mytfcrop(splitfilename,'',myantselect,8.0,8.0,'DATA','')
	a, b = getbllists(splitfilename)
	tdev = 6.0
	fdev = 6.0
	myrflag(splitfilename,'',a[0],tdev,fdev,'DATA','')
	tdev = 5.0
	fdev = 5.0
	myrflag(splitfilename,'',b[0],tdev,fdev,'DATA','')
	logging.info("A flagging summary is provided for the MS file.")
        flagsummary(splitfilename)
#############################################################
# SPLIT AVERAGE
#############################################################
if dosplitavg == True:
        try:
                assert os.path.isdir(splitfilename), "dosplitavg = True but the split file not found."
        except AssertionError:
                logging.info("dosplitavg = True but the split file not found.")
                sys.exit()
	logging.info("Your data will be averaged in frequency.")
        if os.path.isdir('avg-'+splitfilename) == True:
                os.system('rm -rf avg-'+splitfilename)
                if os.path.isdir('avg-'+splitfilename+'.flagversions') == True:
                       os.system('rm -rf avg-'+splitfilename+'.flagversions')
	splitavgfilename = mysplitavg(splitfilename,'','',chanavg)


if doflagavg == True:
        try:
                assert os.path.isdir(splitavgfilename), "doflagavg = True but the splitavg file not found."
        except AssertionError:
                logging.info("doflagavg = True but the splitavg file not found.")
                sys.exit()
	logging.info("Flagging on freqeuncy averaged data.")
	a, b = getbllists(splitavgfilename)
	myrflagavg(splitavgfilename,'',b[0],6.0,6.0,'DATA','')
	myrflagavg(splitavgfilename,'',a[0],6.0,6.0,'DATA','')
        logging.info("A flagging summary is provided for the MS file.")
        flagsummary(splitavgfilename)

############################################################

if makedirty == True:
        try:
                assert os.path.isdir(splitavgfilename), "makedirty = True but the splitavg file not found."
        except AssertionError:
                logging.info("makedirty = True but the splitavg file not found.")
                sys.exit()
	myfile2 = splitavgfilename
	logging.info("A flagging summary is provided for the MS file.")
	flagsummary(splitavgfilename)
        mytclean(myfile2,0,mJythreshold,0,imcellsize,imsize_pix,use_nterms,nwprojpl)

if doselfcal == True:
	if dosubbandselfcal == True:
		try:
		        assert os.path.isdir(splitavgfilename), "dosubbandselfcal = True but the splitavg file not found."
		except AssertionError:
		        logging.info("dosubbandselfcal = True but the splitavg file not found.")
		        sys.exit()
		bw=getbw(splitavgfilename)
		if bw<=32E06:
			raise Exception("GSB files cannot be subbanded. Make dosubbandselfcal False")
		casalog.filter('INFO')
		logging.info("A flagging summary is provided for the MS file.")
		flagsummary(splitavgfilename)
		clearcal(vis = splitavgfilename)
		myfile2 = [splitavgfilename]
		if usetclean == True:
			mysubbandselfcal(myfile2,subbandchan,ref_ant,scaloops,pcaloops,mJythreshold,imcellsize,imsize_pix,use_nterms,nwprojpl,scalsolints,clipresid,'','',False,niter_start)
	else:
		try:
		        assert os.path.isdir(splitavgfilename), "doselfcal = True but the splitavg file not found."
		except AssertionError:
		        logging.info("doselfcal = True but the splitavg file not found.")
		        sys.exit()
		casalog.filter('INFO')
		logging.info("A flagging summary is provided for the MS file.")
		flagsummary(splitavgfilename)
		clearcal(vis = splitavgfilename)
		myfile2 = [splitavgfilename]
		if usetclean == True:
			myselfcal(myfile2,ref_ant,scaloops,pcaloops,mJythreshold,imcellsize,imsize_pix,use_nterms,nwprojpl,scalsolints,clipresid,'','',False,niter_start)
			




