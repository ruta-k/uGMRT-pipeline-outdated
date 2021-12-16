# uGMRT-pipeline (This is outdated and will not be maintained further.)

# The repository CAPTURE-CASA6 is the new avatar of uGMRT-pipeline that will be maintained further: https://github.com/ruta-k/CAPTURE-CASA6

CAPTURE: A CAsa Pipeline-cum-Toolkit for Upgraded Giant Metrewave Radio Telescope data REduction
uGMRT-pipeline

The pipeline is published in the paper https://ui.adsabs.harvard.edu/abs/2020ExA...tmp...46K/abstract. Kindly cite this paper if you use the pipeline or its parts.

This is a continuum data reduction pipeline for the Upgraded GMRT developed by Ruta Kale and Ishwara Chandra. It has been tested on bands 3, 4 and 5 of the uGMRT and 325, 610 and 1400 MHz bands of the legacy GMRT. uGMRT Band 2 data can also be reduced with minor modification to the pipeline - interested users can get in touch with me for that.

To use CAPTURE:

Open config_capture.ini in a text editor. Change and save the settings as per your requirements.

Run the pipeline using:

casa -c capture.py OR

execfile("capture.py")

############################################################################################ 
CAVEATS for CAPTURE v1.0.0:
1. Use CASA versions 5.5 or later.

2. LTA to FITS conversion: If you are starting from a "lta" file - you need to make sure that the listscan and gvfits are executable before starting to run the pipeline. You can convert these to executable files using the commands e.g.: $chmod +x listscan $chmod +x gvfits

3. For the FITS file provide the name in capital letters such as, MYSOURCE_20JULY2019.FITS or TEST.FITS etc. 

4. In case of legacy GMRT dual frequency data please convert the lta to FITS outside the pipeline by choosing one polarization at a time in the .log file. The pipeline will only work for the FITS file directly provided. 

5. Primary beam correction: The images produced by the pipeline are not corrected for the effect of the primary beam. You need to run the primary beam correction separately.

