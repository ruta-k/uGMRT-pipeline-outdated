



# FUNCTIONS
###############################################################
# A library of function that are used in the pipeline

def vislistobs(msfile):
	'''Writes the verbose output of the task listobs.'''
	ms.open(msfile)  
	outr=ms.summary(verbose=True,listfile=msfile+'.list')
	print("A file containing listobs output is saved.")
	return outr

def getpols(msfile):
        '''Get the number of polarizations in the file'''
        msmd.open(msfile)
        polid = msmd.ncorrforpol(0)
        msmd.done()
        return polid

def mypols(inpvis,mypolid):
    msmd.open(inpvis)
    # get correlation types for polarization ID 3
    corrtypes = msmd.corrprodsforpol(0)
    msmd.done()
    return corrtypes

def getfields(msfile):
	'''get list of field names in the ms'''
	msmd.open(msfile)  
	fieldnames = msmd.fieldnames()
	msmd.done()
	return fieldnames

def getscans(msfile, mysrc):
	'''get a list of scan numbers for the specified source'''
	msmd.open(msfile)
	myscan_numbers = msmd.scansforfield(mysrc)
	myscanlist = myscan_numbers.tolist()
	msmd.done()
	return myscanlist

def getantlist(myvis,scanno):
	msmd.open(myvis)
	antenna_name = msmd.antennasforscan(scanno)
	antlist=[]
	for i in range(0,len(antenna_name)):
		antlist.append(msmd.antennanames(antenna_name[i])[0])
	return antlist


def getnchan(msfile):
	msmd.open(msfile)
	nchan = msmd.nchan(0)
	msmd.done()
	return nchan

def read_caltable(CALTABLE):
        CAL=tb.open(CALTABLE,nomodify=True)
        gains=tb.getcol('CPARAM') # Extract the CPARAM=GAIN column from the bandpass table
	ants=tb.getcol('ANTENNA1')
        tb.close()
        return gains, ants


def getchanrange1(bptable,gainlim):
	bpexists = os.path.isdir(bptable)
	if bpexists == True:
        	c1, ant = read_caltable(bptable)
        	c1_amp = np.absolute(c1)
		xx = np.mean(c1_amp, axis = 0)
		yy = np.mean(xx, axis = 1)
		sel_chan_range = []
		for j in range(0,len(yy)):
			if yy[j] > gainlim:
				sel_chan_range.append(j)	
		chan_min = min(sel_chan_range)
		if chan_min == 0:
			chan_min = min(sel_chan_range[1:])
		chan_max = max(sel_chan_range)
	else:
		chan_min = 0
		chan_max = 0	
	return chan_min, chan_max

def getchanrange(bptable,gainlim):
	c1, ant = read_caltable(bptable)
        c1_amp = np.absolute(c1)
        xx = np.mean(c1_amp, axis = 0)
        yy = np.mean(xx, axis = 1)
        sel_chan_range = []
        for j in range(0,len(yy)):
        	if yy[j] > gainlim:
                	sel_chan_range.append(j)
                chan_min = min(sel_chan_range)
        if chan_min == 0:
                chan_min = min(sel_chan_range[1:])
        chan_max = max(sel_chan_range)
        return chan_min, chan_max




def forcechanrange(msfile):
        mynchan = getnchan(msfile)
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
#                if gmrt235 == True:
                mygoodchans = '0:150~160'
                flagspw = '0:70~220'
                gainspw = '0:91~190'
                gainspw2 = ''   # central good channels after split file for self-cal
#               elif gmrt610 == True:
#                        mygoodchans = '0:100~120'
#                        flagspw = '0:11~240'
        splitspw = gainspw
	print("Split spw=%s", str(splitspw))
        return splitspw




def freq_info(ms_file):									
	sw = 0
	msmd.open(ms_file)
	freq=msmd.chanfreqs(sw)								
	msmd.done()
	return freq									

def makebl(ant1,ant2):
	mybl = ant1+'&'+ant2
	return mybl


def getbllists(myfile):
	myfields = getfields(myfile)
	myallscans =[]
	for i in range(0,len(myfields)):
		myallscans.extend(getscans(myfile, myfields[i]))
	myantlist = getantlist(myfile1,int(myallscans[0]))
	allbl=[]
	for i in range(0,len(myantlist)):
		for j in range(0,len(myantlist)):
			if j>i:
				allbl.append(makebl(myantlist[i],myantlist[j]))
	mycc=[]
	mycaa=[]
	for i in range(0,len(allbl)):
		if allbl[i].count('C')==2:
			mycc.append(allbl[i])
		else:
			mycaa.append(allbl[i])
	myshortbl =[]
	myshortbl.append(str('; '.join(mycc)))
	mylongbl =[]
	mylongbl.append(str('; '.join(mycaa)))
	return myshortbl, mylongbl

def myvisstatampraw1(myfile,myfield,myspw,myant,mycorr,myscan):
	default(visstat)
	mystat = visstat(vis=myfile,axis="amp",datacolumn="data",useflags=False,spw=myspw,
		field=myfield,selectdata=True,antenna=myant,uvrange="",timerange="",
		correlation=mycorr,scan=myscan,array="",observation="",timeaverage=False,
		timebin="0s",timespan="",maxuvwdistance=0.0,disableparallel=None,ddistart=None,
		taql=None,monolithic_processing=None,intent="",reportingaxes="ddid")
	mymean1 = mystat['DATA_DESC_ID=0']['mean']
	return mymean1

def myvisstatampraw(myfile,myspw,myant,mycorr,myscan):
	default(visstat)
	mystat = visstat(vis=myfile,axis="amp",datacolumn="data",useflags=False,spw=myspw,
		selectdata=True,antenna=myant,uvrange="",timerange="",
		correlation=mycorr,scan=myscan,array="",observation="",timeaverage=False,
		timebin="0s",timespan="",maxuvwdistance=0.0,disableparallel=None,ddistart=None,
		taql=None,monolithic_processing=None,intent="",reportingaxes="ddid")
	mymean1 = mystat['DATA_DESC_ID=0']['mean']
	return mymean1


def mygaincal_ap1(myfile,mycal,myref,myflagspw,myuvracal,calsuffix):
	default(gaincal)
#	gaincal(vis=myfile, caltable=str(myfile)+'.AP.G.', spw =myflagspw,uvrange=myuvracal,append=True,
#		field=mycal,solint = 'int',refant = myref, minsnr = 2.0, solmode ='L1R', gaintype = 'G', calmode = 'ap',
#		gaintable = [str(myfile)+'.K1', str(myfile)+'.B1' ], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
#		parang = True )
	gaincal(vis=myfile, caltable=str(myfile)+'.AP.G.', spw =myflagspw,uvrange=myuvracal,append=True,
		field=mycal,solint = '120s',refant = myref, minsnr = 2.0, gaintype = 'G', calmode = 'ap',
		gaintable = [str(myfile)+'.K1', str(myfile)+'.B1' ], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
		parang = True )
	return gaintable


def mygaincal_ap2(myfile,mycal,myref,myflagspw,myuvracal,calsuffix):
	default(gaincal)
#	gaincal(vis=myfile, caltable=str(myfile)+'.AP.G.'+'recal', append=True, field=mycal, spw =myflagspw,
#		uvrange=myuvracal, solint = 'int', refant = myref, solmode ='L1R', minsnr = 2.0, gaintype = 'G', calmode = 'ap',
#		gaintable = [str(myfile)+'.K1'+'recal', str(myfile)+'.B1'+'recal' ], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
#		parang = True )
	gaincal(vis=myfile, caltable=str(myfile)+'.AP.G.'+calsuffix, spw =myflagspw,uvrange=myuvracal,append=True,
		field=mycal,solint = '120s',refant = myref, minsnr = 2.0, gaintype = 'G', calmode = 'ap',
		gaintable = [str(myfile)+'.K1'+calsuffix, str(myfile)+'.B1'+calsuffix ], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
		parang = True )
	return gaintable

def getfluxcal(myfile,mycalref,myscal):
	myscale = fluxscale(vis=myfile, caltable=str(myfile)+'.AP.G.', fluxtable=str(myfile)+'.fluxscale', reference=mycalref, transfer=myscal,
                    incremental=False)
#	myscale = fluxscale(vis=myfile, caltable=str(myfile)+'.AP.G.'+'recal', fluxtable=str(myfile)+'.fluxscale'+'recal', reference=mycalref,
#                    transfer=myscal, incremental=False)
	return myscale


def getfluxcal2(myfile,mycalref,myscal,calsuffix):
#	if calsuffix == '':
#	myscale = fluxscale(vis=myfile, caltable=str(myfile)+'.AP.G.', fluxtable=str(myfile)+'.fluxscale', reference=mycalref, transfer=myscal,
#                    incremental=False)
#	if calsuffix == 'recal':
	myscale = fluxscale(vis=myfile, caltable=str(myfile)+'.AP.G.'+calsuffix, fluxtable=str(myfile)+'.fluxscale'+calsuffix, reference=mycalref,
       	            transfer=myscal, incremental=False)
	return myscale



def mygaincal_ap_redo(myfile,mycal,myref,myflagspw,myuvracal):
	default(gaincal)
#	gaincal(vis=myfile, caltable=str(myfile)+'.AP.G.'+'recal', append=True, field=mycal, spw =myflagspw,
#		uvrange=myuvracal, solint = 'int', refant = myref, solmode ='L1R', minsnr = 2.0, gaintype = 'G', calmode = 'ap',
#		gaintable = [str(myfile)+'.K1'+'recal', str(myfile)+'.B1'+'recal' ], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
#		parang = True )
	gaincal(vis=myfile, caltable=str(myfile)+'.AP.G.'+'recal', append=True, spw =myflagspw, uvrange=myuvracal,
		field=mycal,solint = '120s',refant = myref, minsnr = 2.0, gaintype = 'G', calmode = 'ap',
		gaintable = [str(myfile)+'.K1'+'recal', str(myfile)+'.B1'+'recal' ], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
		parang = True )
	return gaintable

def getfluxcal_redo(myfile,mycalref,myscal):
	myscale = fluxscale(vis=myfile, caltable=str(myfile)+'.AP.G.'+'recal', fluxtable=str(myfile)+'.fluxscale'+'recal', reference=mycalref,
                    transfer=myscal, incremental=False)
	return myscale

def mytfcrop(myfile,myfield,myants,tcut,fcut,mydatcol,myflagspw):
	default(flagdata)
	flagdata(vis=myfile, antenna = myants, field = myfield,	spw = myflagspw, mode='tfcrop', ntime='300s', combinescans=False,
		datacolumn=mydatcol, timecutoff=tcut, freqcutoff=fcut, timefit='line', freqfit='line', flagdimension='freqtime',
		usewindowstats='sum', extendflags = False, action='apply', display='none')
	return


def myrflag(myfile,myfield, myants, mytimdev, myfdev,mydatcol,myflagspw):
	default(flagdata)
	flagdata(vis=myfile, field = myfield, spw = myflagspw, antenna = myants, mode='rflag', ntime='scan', combinescans=False,
		datacolumn=mydatcol, winsize=3, timedevscale=mytimdev, freqdevscale=myfdev, spectralmax=1000000.0, spectralmin=0.0,
		extendflags=False, channelavg=False, timeavg=False, action='apply', display='none')
	return


def myrflagavg(myfile,myfield, myants, mytimdev, myfdev,mydatcol,myflagspw):
	default(flagdata)
	flagdata(vis=myfile, field = myfield, spw = myflagspw, antenna = myants, mode='rflag', ntime='300s', combinescans=True,
		datacolumn=mydatcol, winsize=3,	minchanfrac= 0.8, flagneartime = True, basecnt = True, fieldcnt = True,
		timedevscale=mytimdev, freqdevscale=myfdev, spectralmax=1000000.0, spectralmin=0.0, extendflags=False,
		channelavg=False, timeavg=False, action='apply', display='none')
	return




def mysplitinit(myfile,myfield,myspw,mywidth):
	'''function to split corrected data for any field'''
	default(mstransform)
	mstransform(vis=myfile, field=myfield, spw=myspw, chanaverage=False, chanbin=mywidth, datacolumn='corrected', outputvis=str(myfield)+'split.ms')
	myoutvis=str(myfield)+'split.ms'
	return myoutvis


def mysplitavg(myfile,myfield,myspw,mywidth):
	'''function to split corrected data for any field'''
	myoutname=myfile.split('s')[0]+'avg-split.ms'
	default(mstransform)
	mstransform(vis=myfile, field=myfield, spw=myspw, chanaverage=True, chanbin=mywidth, datacolumn='data', outputvis=myoutname)
	return myoutname


def mytclean(myfile,myniter,mythresh,srno,cell,imsize, mynterms1,mywproj):    # you may change the multi-scale inputs as per your field
	nameprefix = getfields(myfile)[0]#myfile.split('.')[0]
	print("The image files have the following prefix =",nameprefix)
	if myniter==0:
		myoutimg = nameprefix+'-dirty-img'
	else:
		myoutimg = nameprefix+'-selfcal'+'img'+str(srno)
	default(tclean)
	if mynterms1 > 1:
		tclean(vis=myfile,
       			imagename=myoutimg, selectdata= True, field='0', spw='0', imsize=imsize, cell=cell, robust=0, weighting='briggs', 
       			specmode='mfs',	nterms=mynterms1, niter=myniter, usemask='auto-multithresh',minbeamfrac=0.1, sidelobethreshold = 1.5,
#			minpsffraction=0.05,
#			maxpsffraction=0.8,
			smallscalebias=0.6, threshold= mythresh, aterm =True, pblimit=-1,
	        	deconvolver='mtmfs', gridder='wproject', wprojplanes=mywproj, scales=[0,5,15],wbawp=False,
			restoration = True, savemodel='modelcolumn', cyclefactor = 0.5, parallel=False,
       			interactive=False)
	else:
		tclean(vis=myfile,
       			imagename=myoutimg, selectdata= True, field='0', spw='0', imsize=imsize, cell=cell, robust=0, weighting='briggs', 
       			specmode='mfs',	nterms=mynterms1, niter=myniter, usemask='auto-multithresh',minbeamfrac=0.1,sidelobethreshold = 1.5,
#			minpsffraction=0.05,
#			maxpsffraction=0.8,
			smallscalebias=0.6, threshold= mythresh, aterm =True, pblimit=-1,
	        	deconvolver='multiscale', gridder='wproject', wprojplanes=mywproj, scales=[0,5,15],wbawp=False,
			restoration = True, savemodel='modelcolumn', cyclefactor = 0.5, parallel=False,
       			interactive=False)
	return myoutimg

def myonlyclean(myfile,myniter,mythresh,srno,cell,imsize,mynterms1,mywproj):
	default(clean)
	clean(vis=myfile,
	selectdata=True,
	spw='0',
	imagename='selfcal'+'img'+str(srno),
	imsize=imsize,
	cell=cell,
	mode='mfs',
	reffreq='',
	weighting='briggs',
	niter=myniter,
	threshold=mythresh,
	nterms=mynterms1,
	gridmode='widefield',
	wprojplanes=mywproj,
	interactive=False,
	usescratch=True)
	myname = 'selfcal'+'img'+str(srno)
	return myname


def mysplit(myfile,srno):
#	filname_pre = myfile.split('.')[0]
	filname_pre = getfields(myfile)[0]
	default(mstransform)
#	mstransform(vis=myfile, field='0', spw='0', datacolumn='corrected', outputvis='vis-selfcal'+str(srno)+'.ms')
	mstransform(vis=myfile, field='0', spw='0', datacolumn='corrected', outputvis=filname_pre+'-selfcal'+str(srno)+'.ms')
	myoutvis=filname_pre+'-selfcal'+str(srno)+'.ms'
#	myoutvis='vis-selfcal'+str(srno)+'.ms'
	return myoutvis


def mygaincal_ap(myfile,myref,mygtable,srno,pap,mysolint,myuvrascal,mygainspw):
#	fprefix = myfile.split('.')[0]
	fprefix = getfields(myfile)[0]
	if pap=='ap':
		mycalmode='ap'
		mysol= mysolint[srno] 
		mysolnorm = True
	else:
		mycalmode='p'
		mysol= mysolint[srno] 
		mysolnorm = False
	if os.path.isdir(fprefix+str(pap)+str(srno)+'.GT'):
		os.system('rm -rf '+fprefix+str(pap)+str(srno)+'.GT')
	default(gaincal)
#	gaincal(vis=myfile, caltable=str(pap)+str(srno)+'.GT', append=False, field='0', spw=mygainspw,
#		uvrange=myuvrascal, solint = mysol, refant = myref, minsnr = 2.0, gaintype = 'G',
# for casa 5.5 release
#		solmode='L1R', solnorm= mysolnorm, 
# new options in gaincal
#		calmode = mycalmode, gaintable = [], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
#		parang = True )
	gaincal(vis=myfile, caltable=fprefix+str(pap)+str(srno)+'.GT', append=False, field='0', spw=mygainspw,
		uvrange=myuvrascal, solint = mysol, refant = myref, minsnr = 2.0, gaintype = 'G',
		solnorm= mysolnorm, calmode = mycalmode, gaintable = [], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
		parang = True )
	mycal = fprefix+str(pap)+str(srno)+'.GT'
	return mycal


def myapplycal(myfile,mygaintables):
	# applycal
	default(applycal)
	applycal(vis=myfile, field='0', gaintable=mygaintables, gainfield=['0'], applymode='calflag', 
	         interp=['linear'], calwt=False, parang=False)
	print 'Did applycal.'




def flagresidual(myfile,myclipresid,myflagspw):
	default(flagdata)
	flagdata(vis=myfile, mode ='rflag', datacolumn="RESIDUAL_DATA", field='', timecutoff=6.0,  freqcutoff=6.0,
		timefit="line", freqfit="line",	flagdimension="freqtime", extendflags=False, timedevscale=6.0,
		freqdevscale=6.0, spectralmax=500.0, extendpols=False, growaround=False, flagneartime=False,
		flagnearfreq=False, action="apply", flagbackup=True, overwrite=True, writeflags=True)
	default(flagdata)
	flagdata(vis=myfile, mode ='clip', datacolumn="RESIDUAL_DATA", clipminmax=myclipresid,
		clipoutside=True, clipzeros=True, field='', spw=myflagspw, extendflags=False,
		extendpols=False, growaround=False, flagneartime=False,	flagnearfreq=False,
		action="apply",	flagbackup=True, overwrite=True, writeflags=True)
	flagdata(vis=myfile,mode="summary",datacolumn="RESIDUAL_DATA", extendflags=False, 
		name=myfile+'temp.summary', action="apply", flagbackup=True,overwrite=True, writeflags=True)
#


	 

def myselfcal(myfile,myref,nloops,nploops,myvalinit,mycellsize,myimagesize,mynterms2,mywproj1,mysolint1,myclipresid,myflagspw,mygainspw2,mymakedirty,niterstart):
	myref = myref
	nscal = nloops # number of selfcal loops
	npal = nploops # number of phasecal loops
	# selfcal loop
	myimages=[]
	mygt=[]
	myniterstart = niterstart
	myniterend = 200000	
#	myval= myvalinit # mJy
	if nscal == 0:
		i = nscal
		myniter = 0 # this is to make a dirty image
#		mythresh = myval[i]   #str(startthreshold/(count+1))+'mJy'
		mythresh = str(myvalinit/(i+1))+'mJy'
		print "Using "+ myfile[i]+" for making only an image."
		if usetclean == False:
			myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # clean
		else:
			myimg = mytclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # tclean
#		exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
                if mynterms2 > 1:
                        exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
                else:
                        exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')

	else:
		for i in range(0,nscal+1): # plan 4 P and 4AP iterations
			if mymakedirty == True:
				if i == 0:
					myniter = 0 # this is to make a dirty image
#					mythresh = myval[i]
					mythresh = str(myvalinit/(i+1))+'mJy'
					print "Using "+ myfile[i]+" for making only a dirty image."
					if usetclean == False:
						myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # clean
					else:
						myimg = mytclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # tclean
					if mynterms2 > 1:
						exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
					else:
						exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')

			else:
				myniter=int(myniterstart*2**i) #myniterstart*(2**i)  # niter is doubled with every iteration int(startniter*2**count)
				if myniter > myniterend:
					myniter = myniterend
#				mythresh = myval[i]
				mythresh = str(myvalinit/(i+1))+'mJy'
#				print i, 'mythreshold=',mythresh
				if i < npal:
					mypap = 'p'
					print "Using "+ myfile[i]+" for imaging."
					if usetclean == False:
						myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # clean
					else:
						myimg = mytclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # tclean
					if mynterms2 > 1:
						exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
					else:
						exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')
					myimages.append(myimg)	# list of all the images created so far
					flagresidual(myfile[i],clipresid,'')
					if i>0:
						myctables = mygaincal_ap(myfile[i],myref,mygt[i-1],i,mypap,mysolint1,uvrascal,mygainspw2)
					else:					
						myctables = mygaincal_ap(myfile[i],myref,mygt,i,mypap,mysolint1,uvrascal,mygainspw2)						
					mygt.append(myctables) # full list of gaintables
					if i < nscal+1:
						myapplycal(myfile[i],mygt[i])
						myoutfile= mysplit(myfile[i],i)
						myfile.append(myoutfile)
#						print i, myfile1
				else:
					mypap = 'ap'
					print "Using "+ myfile[i]+" for imaging."
					if usetclean == False:
						myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # clean
					else:
						myimg = mytclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # tclean
					if mynterms2 > 1:
						exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
					else:
						exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')
					myimages.append(myimg)	# list of all the images created so far
					flagresidual(myfile[i],clipresid,'')
					if i!= nscal:
						myctables = mygaincal_ap(myfile[i],myref,mygt[i-1],i,mypap,mysolint1,'',mygainspw2)
						mygt.append(myctables) # full list of gaintables
						if i < nscal+1:
							myapplycal(myfile[i],mygt[i])
							myoutfile= mysplit(myfile[i],i)
							myfile.append(myoutfile)
							print i, myfile1
				print "Visibilities from the previous selfcal will be deleted."
				if i < nscal:
					fprefix = getfields(myfile[i])[0]
#					myoldvis = myfile[i].split('.')[0]+'-selfcal'+str(i-1)+'.ms'
					myoldvis = fprefix+'-selfcal'+str(i-1)+'.ms'
					print "Deleting "+str(myoldvis)
					os.system('rm -rf '+str(myoldvis))
			print 'Ran the selfcal loop'
	return myfile, mygt, myimages

def myflagsum(myfile,myfields):
	flagsum = flagdata(vis=myfile, field =myfields[0], mode='summary')
	for i in range(0,len(myfields)):
		src = myfields[i]
		flagpercentage = 100.0 * flagsum['field'][src]['flagged'] / flagsum['field'][src]['total']
#		print("\n %2.1f%% of the source are flagged.\n" % (100.0 * flagsum['field'][src]['flagged'] / flagsum['field'][src]['total']))
	return flagpercentage

#############End of functions##############################################################################
print "#######################################################################################"
