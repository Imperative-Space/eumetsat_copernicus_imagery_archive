#! /usr/bin/env python

import os
import sys
import h5py
import datetime
import numpy as np
import gdal, osr, ogr
from ftplib import FTP
from progress.bar import Bar
import subprocess
from os.path import expanduser

class metopDownloader:
    def __init__(self):

        self.debug = False
        for arg in sys.argv:
            if arg == '-v':
                self.debug = True
        self.home = expanduser("~")
        if sys.platform == "linux2":
            self.FTP_DIR = "/home/ubuntu/mnt_s3_bucket/"
            self.OUTPUT_DIR = "/home/ubuntu/eumetsat_archive/tempmetop/"
            self.COLOUR_FOLDER = "/home/ubuntu/eumetsat_archive/colour_files/"
        else:
            self.OUTPUT_DIR = self.home + "/repos/eumetsat_archive/tempmetop/"
            self.FTP_DIR = self.home + "/repos/eumetsat_archive/ftpdir/"
            self.COLOUR_FOLDER = self.home + "/repos/eumetsat_archive/colour_files/"

        if not os.path.exists(self.OUTPUT_DIR):
            os.makedirs(self.OUTPUT_DIR)

    def run(self,num):


        numSubtractionDays = datetime.timedelta(days=num)
        self.fromdate = (datetime.datetime.today() - numSubtractionDays).strftime('%Y/%m/%d')
        self.fromdate_output = (datetime.datetime.today() - numSubtractionDays).strftime('%Y-%m-%d')

        self.outSHPfn = self.OUTPUT_DIR + '_metop.shp'
        shpDriver = ogr.GetDriverByName("ESRI Shapefile")
        if os.path.exists(self.outSHPfn):
            shpDriver.DeleteDataSource(self.outSHPfn)
        outDataSource = shpDriver.CreateDataSource(self.outSHPfn)
        self.outLayer = outDataSource.CreateLayer(self.outSHPfn, geom_type=ogr.wkbPolygon)

        o3Field = ogr.FieldDefn('o3', ogr.OFTReal)
        o3Field.SetWidth(32)
        self.outLayer.CreateField(o3Field)
        no2Field = ogr.FieldDefn('no2', ogr.OFTReal)
        no2Field.SetWidth(32)
        self.outLayer.CreateField(no2Field)
        so2Field = ogr.FieldDefn('so2', ogr.OFTReal)
        so2Field.SetWidth(32)
        self.outLayer.CreateField(so2Field)
        hchoField = ogr.FieldDefn('hcho', ogr.OFTReal)
        hchoField.SetWidth(32)
        self.outLayer.CreateField(hchoField)
        no2tropo = ogr.FieldDefn('no2tropo', ogr.OFTReal)
        no2tropo.SetWidth(32)
        self.outLayer.CreateField(no2tropo)

        featureDefn = self.outLayer.GetLayerDefn()
        self.outFeature = ogr.Feature(featureDefn)

        directory = ["gome2b", "gome2a"]
        for dir in directory:
            metopDownloader.downloadData(self,dir)

        outDataSource.Destroy()


    def downloadData(self, dir):
        ftp = FTP('atmos.caf.dlr.de')
        ftp.login("{{USERNAME}}", "{{PASSWORD}}")
        ftp.cwd(dir + '/offline/' + self.fromdate + '/')
        listing = []
        ftp.retrlines('LIST', listing.append)

        for list in listing:

            words = list.split(None, 8)
            filename = words[-1].lstrip()
            if "GOME_O3-NO2-NO2Tropo-BrO-SO2-H2O-HCHO_L2_" in filename:
                print self.OUTPUT_DIR + "/" + filename
                if not os.path.exists(self.OUTPUT_DIR + filename):
                    print 'Downloading file ' + filename
                    file = open(self.OUTPUT_DIR + filename, 'wb')
                    ftp.retrbinary('RETR %s' % filename, file.write)
                    file.close()

                #i.e. f = h5py.File("GOME_O3-NO2-NO2Tropo-BrO-SO2-H2O-HCHO_L2_20161120000813_051_METOPB_21656_DLR_04.HDF5", "r")
                f = h5py.File(self.OUTPUT_DIR + filename, "r")

                # print f.keys()
                detailedResults = f['DETAILED_RESULTS']
                # print detailedResults.keys()
                # print f['TOTAL_COLUMNS'].keys()
                totalO3columns = f['TOTAL_COLUMNS']['O3']
                totalNO2columns = f['TOTAL_COLUMNS']['NO2']
                totalSO2columns = f['TOTAL_COLUMNS']['SO2']
                totalHCHOcolumns = f['TOTAL_COLUMNS']['HCHO']
                totalNO2Tropocolumns = f['TOTAL_COLUMNS']['NO2Tropo']

                counter = 0
                geolocation = f['GEOLOCATION']
                geolocationLatCount = np.array(geolocation['LatitudeA'])
                indexInScan = geolocation['IndexInScan']
                orbitalMode = geolocation['OrbitalMode']
                bar = Bar('Processing', max=len(geolocationLatCount))
                for count in geolocationLatCount:
                    if indexInScan[counter] != 3: #and orbitalMode != 1:
                        ring = ogr.Geometry(ogr.wkbLinearRing)
                        ring.AddPoint(float(geolocation['LongitudeA'][counter]), float(geolocation['LatitudeA'][counter]))
                        ring.AddPoint(float(geolocation['LongitudeB'][counter]), float(geolocation['LatitudeB'][counter]))
                        ring.AddPoint(float(geolocation['LongitudeD'][counter]), float(geolocation['LatitudeD'][counter]))
                        ring.AddPoint(float(geolocation['LongitudeC'][counter]), float(geolocation['LatitudeC'][counter]))
                        ring.AddPoint(float(geolocation['LongitudeA'][counter]), float(geolocation['LatitudeA'][counter]))
                        poly = ogr.Geometry(ogr.wkbPolygon)
                        poly.AddGeometry(ring)
                        self.outFeature.SetGeometry(poly)
                        self.outFeature.SetField('o3', float(totalO3columns[counter]))
                        self.outFeature.SetField('no2', float(totalNO2columns[counter]))
                        self.outFeature.SetField('so2', float(totalSO2columns[counter]))
                        self.outFeature.SetField('hcho', float(totalHCHOcolumns[counter]))
                        self.outFeature.SetField('no2tropo', float(totalNO2Tropocolumns[counter]))
                        geom = self.outFeature.GetGeometryRef()
                        area = geom.GetArea()
                        bar.next()

                        # outLayer.CreateFeature(outFeature)
                        if area < 10:
                            # print area
                            self.outLayer.CreateFeature(self.outFeature)
                            # Test break below.
                            #if counter > 10:
                            #    break

                    counter = counter + 1
                bar.finish()

    def createoutput(self):
        products = ["o3", "no2", "so2", "hcho", "no2tropo"]

        for product in products:
            if not os.path.exists(self.FTP_DIR + "METOP-" + product.upper() + "/"):
                os.makedirs(self.FTP_DIR + "METOP-" + product.upper() + "/")

            gdalrasterizecommand = "gdal_rasterize -a " + product + " -ts 8192 4096 " + self.outSHPfn + " " + self.OUTPUT_DIR + product + ".tif"
            print gdalrasterizecommand
            subprocess.call(gdalrasterizecommand, shell=True)
            outputFileName = self.FTP_DIR + "METOP-" + product.upper() + "/" + self.fromdate_output + ".png"

            gdaldemcommand = "gdaldem color-relief -of PNG -alpha " + \
                             self.OUTPUT_DIR + product + ".tif " + \
                             self.COLOUR_FOLDER + "METOP-" + product.upper() + ".txt  " + \
                             outputFileName
            print gdaldemcommand
            subprocess.call(gdaldemcommand, shell=True)
            subprocess.call("convert " + outputFileName + " -gravity Center -crop 8150x4096+0+0 +repage " + outputFileName, shell=True)
            subprocess.call("rm " + outputFileName + ".*", shell=True)


    def cleanup(self):
        if not self.debug:
            subprocess.call("rm " + self.OUTPUT_DIR + "*.*", shell=True)


def main():

    """ Main loop to begin the processing.

    """
    BACKFILL_DAYS = 4
    downloader = metopDownloader()
    downloader.cleanup()
    for num in range(3, BACKFILL_DAYS):
        downloader.run(num)
        downloader.createoutput()
    downloader.cleanup()


if __name__ == '__main__':
    main()
