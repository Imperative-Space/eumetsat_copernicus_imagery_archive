#! /usr/bin/env python
# Downloading specific datasets for the
# EUMETSAT Copernicus Imagery Archive Project 2016
# Author: Tobias Reinicke
# On behalf of: Imperative Space


import simplejson
import os
import datetime
import subprocess
import sys
import logging
from ecmwfapi import ECMWFDataServer
from os.path import expanduser


class Downloader:
    def __init__(self):

        """
        Initialises all variables needed. Sets the environment for the current
         platform. Handy to have when testing on local platform.
        """

        # This is one of the more important variables. This setting defines how far back into history (in days) we go.
        # I.e. to do a whole year we would set this to 365 etc.
        self.BACKFILL_DAYS = 10

        self.home = expanduser("~")
        self.server = ECMWFDataServer()

        self.ERROR_FILES = []
        self.DATE = datetime.datetime.today().strftime('%Y-%m-%d')
        if sys.platform == "linux2":
            self.MOTU_CLIENT_DIR = "/home/ubuntu/eumetsat_archive/motu-client-python"
            self.FTP_DIR = "/home/ubuntu/mnt_s3_bucket"
            self.DATA_FILE = "/home/ubuntu/eumetsat_archive/data.json"
            self.OUTPUT_DIR = "/home/ubuntu/eumetsat_archive/temp"
            self.COLOUR_FOLDER = "/home/ubuntu/eumetsat_archive/colour_files/"
        else:
            self.OUTPUT_DIR = self.home + "/repos/eumetsat_archive/temp"
            self.DATA_FILE = "data.json"
            self.FTP_DIR = self.home + "/repos/eumetsat_archive/ftpdir"
            self.MOTU_CLIENT_DIR = self.home + "/repos/eumetsat_archive/motu-client-python"
            self.COLOUR_FOLDER = self.home + "/repos/eumetsat_archive/colour_files/"


        if not os.path.exists(self.OUTPUT_DIR):
            os.makedirs(self.OUTPUT_DIR)
        # Set up logging.
        self.logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
        self.logger = logging.getLogger()
        self.handler = logging.StreamHandler()
        self.formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.INFO)
        self.logPath = self.FTP_DIR
        self.fileName = datetime.datetime.today().strftime('%Y-%m-%d')
        if not os.path.exists(self.logPath + "/logs"):
            os.makedirs(self.logPath + "/logs")
        self.fileHandler = logging.FileHandler("{0}/{1}.txt".format(self.logPath + "/logs", self.fileName))
        self.fileHandler.setFormatter(self.logFormatter)
        self.logger.addHandler(self.fileHandler)


        self.fromdate = ""
        self.todate = ""
        self.datasetname = ""
        argscounter = 0
        self.counter = 0
        self.dataCount = 0

        # Get arguments. This enabled the scripts to be used via the following command;
        # python batchDownload.py -fromdate 20161010 -todate 20161010 -datasetname CERSAT-GLO-BLENDED_WIND_L4-V3-OBS_FULL_TIME_SERIE
        # Using -v will enable verbose logging, as well as maintaining the downloaded temp files.

        for arg in sys.argv:
            if arg == '-v':
                self.debug = True
                self.logger.setLevel(logging.DEBUG)
            elif arg == '-fromdate':
                self.fromdate = sys.argv[argscounter + 1]
            elif arg == '-todate':
                self.todate = sys.argv[argscounter + 1]
            elif arg == '-datasetname':
                self.datasetname = sys.argv[argscounter + 1]
            elif arg == '-backfilldays':
                self.BACKFILL_DAYS = int(sys.argv[argscounter + 1])
            else:
                self.debug = False
                self.logger.setLevel(logging.INFO)
            argscounter = argscounter + 1
        pass

    def load_file(self):

        """
        Loads the json file into memory.
        """

        with open(self.DATA_FILE) as data_file:
            self.jsondata = simplejson.load(data_file)
        for item in self.jsondata:
            if item["process"] == True:
                self.dataCount = self.dataCount + 1

    def run(self):

        """
        Run function. This will decide if we have passed arguments or are running the process
        completely.
        """

        self.load_file()

        if self.fromdate != "" and self.todate != "" and self.datasetname != "":
            fromdate = datetime.datetime.strptime(self.fromdate, "%Y%m%d").date()
            toDate = datetime.datetime.strptime(self.todate, "%Y%m%d").date()
            self.DATE = fromdate.strftime('%Y-%m-%d')
            item = self.findItem(self.datasetname)
            self.logger.info("==== Processing " + self.datasetname.upper() + " =====")
            ##TODO: In here at some point let's get the proper dates to enable using ranges.
            ##i.e. transofrm fromdate / todate to actual dates maybe?
            self.processItem(item, fromdate, toDate)

        else:
            self.processJsonData()

    def findItem(self, datasetname):

        """
        For the selected dataset via the command line, get the object from the JSON file
        """

        for item in self.jsondata:
            if item["dataset"] == datasetname:
                ##TODO: Enable the use of variables.
                return item

    def processItem(self, item, fromdate, toDate):

        """ This is the main part of the program. Here we retrieve the parameters from the
        object that we get from the JSON file.

        :param item: object from the JSON file.
        :param fromdate: from which date to start processing
        :param toDate: what the end date for processing should be
        """

        dataset = item["dataset"]
        try:
            variable = item["variable"]
        except:
            pass
        try:
            te = item["te"]
        except:
            te = " -180 -90 180 90"

        variableName = ""
        try:
            variableName = "_" + item["variableName"]
        except:
            variableName = ""

        try:
            z = item["depth"]
        except:
            z = -99

        try:
            ts = item["ts"]
        except:
            ts = "8192 0"

        dateformat = item["dateformat"]
        ftpdatafolder = self.FTP_DIR + "/" + dataset.upper() + variableName.upper()
        file = ftpdatafolder + "/" + fromdate.strftime('%Y-%m-%d') + ".png"

        # If the file exists - don't do anything.
        if not os.path.exists(file):
            self.logger.info(fromdate.strftime('%Y-%m-%d') + ".png doesn't exist. Backfilling...")
            fromdate = fromdate.strftime(dateformat)
            toDate = toDate.strftime(dateformat)
            if item["type"] == 'useMotu':

                # If this is a dataset coming from the MOTU download service, handle it accordingly.
                username = item["username"]
                password = item["password"]
                m = item["m"]
                s = item["s"]

                self.getMotuDataset(username, password, m, s, dataset, fromdate, toDate, variable, te, z, variableName,
                                    ts)

            elif item["type"] == 'ecmwf':

                # If this uses the ECMWF python download service, handle it accordingly.
                self.logger.info("Processing ECMWF data")
                ecmfwdataset = item["ecmwf_dataset"]
                try:
                    time =  item["time"]
                except:
                    time = "12:00:00"
                self.getECMWFDataset(dataset, ecmfwdataset, fromdate, item["param"], "", ts, item["expver"],time)

            elif item["type"] == 'ftp':

                # Otherwise it's an FTP download.
                url = item["url"]
                self.getFTPDataset(url, dataset, variable, fromdate, te, ts)
        else:
            self.logger.info("Already got " + fromdate.strftime('%Y-%m-%d') + ".png Moving on.")

    def cropData(self, dataset):

        """
        For the MACC datasets, they all need to be cropped to a certain size, this is because there is a column
        of data on the far right which contains header information, and renders as null data.
        """

        self.logger.info("Cropping")
        ftpdatafolder = dataset.upper()
        filename = str(self.FTP_DIR) + '/' + str(ftpdatafolder) + '/' + str(self.DATE) + '.png'
        cropCommand = "convert " + filename + " -crop 8183x4096+0+0 +repage + " + filename
        subprocess.call(cropCommand, shell=True)

    def processJsonData(self):

        """
        If we are processing the entire data.json file then this is where we do it.
        """
        self.logger.info(" Found " + str(self.dataCount) + " datasets to process. Starting process.")
        for item in self.jsondata:
            # If we want to process the data according to the 'process' flag on the object.
            if item["process"] == True:
                self.counter = self.counter + 1
                dataset = item["dataset"]
                self.logger.info("=========== " + str(self.counter) + " / " + str(
                    self.dataCount) + " : " + dataset + " =============")
                try:
                    gap = item["gap"]
                except:
                    gap = 0
                variableName = ""
                try:
                    variableName = "_" + item["variableName"]
                except:
                    variableName = ""
                daysago = item["daysAgo"]

                self.logger.info("==== Processing " + dataset.upper() + variableName + " =====")
                for num in range(daysago, self.BACKFILL_DAYS + 1):
                    now = datetime.datetime.today()
                    numSubtractionDays = datetime.timedelta(days=num)
                    fromdate = now - numSubtractionDays
                    toDate = fromdate + datetime.timedelta(days=gap)
                    self.DATE = fromdate.strftime('%Y-%m-%d')
                    self.processItem(item, fromdate, toDate)

        if len(self.ERROR_FILES) > 0:
            self.logger.error(str(len(self.ERROR_FILES)) + " file(s) failed.:")
            for file in self.ERROR_FILES:
                self.logger.info(file)
        else:
            self.logger.info("All successful.")
            self.cleanup()

        self.logger.info("=====================================")
        self.logger.info("=====================================")

    def getMotuDataset(self, username, password, m, s, dataset, fromdate, todate, variable, te, depth, variableName,
                       ts):
        """
        Function to retrieve datasets that are supported by the MOTU python client.
        Parameters are fairly self explanatory.
        """

        filename = dataset + variableName + "_" + self.DATE
        tempfilename = self.OUTPUT_DIR + "/" + filename + ".nc"
        if os.path.exists(tempfilename):
            self.logger.info(tempfilename + " already exists so just reprocessing it.")
        else:
            self.logger.info("Downloading " + filename)
            if self.debug:
                motucommand = "python " + self.MOTU_CLIENT_DIR + "/motu-client.py "
            else:
                motucommand = "python " + self.MOTU_CLIENT_DIR + "/motu-client.py -q "

            motucommand = (motucommand +
                           " -u " + username +
                           " -p " + password +
                           " -m " + m +
                           " -s " + s +
                           " -d " + dataset +
                           " -x -180 -X 180 -y -90 -Y 90 " +
                           " -t " + fromdate +
                           " -T " + todate +
                           " -v " + variable +
                           " -o " + self.OUTPUT_DIR +
                           " -f " + filename + ".nc")
            # Occasionally datasets are requested at specific depths. If this is the case
            # we create a range for the depth variable.
            if depth != -99:
                motucommand = (motucommand +
                               " -z " + str(depth) +
                               " -Z " + str(depth + 0.0002))

            self.logger.debug(motucommand)
            dataset = dataset + variableName
            subprocess.check_output(motucommand, shell=True)

        try:
            self.processImagery(dataset, filename, variable, te, self.DATE, ts)
            self.moveToFtpFolder(dataset, filename + ".png", fromdate)
            self.cleanup()
        except:
            self.logger.error("Error processing " + filename)
            self.ERROR_FILES.append(filename)
            pass

    def getECMWFDataset(self, dataset, ecmfwdataset, fromdate, param, te, ts, expver, time):

        """
        When downloading ECMWF data we use the ECMWF python api. This allows us to specify
        various parameters, the most important one the "param" one - which indicates the
        dataset that we're after. the "grid" specifies the resolution (in pixels per km2).
        """

        self.logger.info("Processing " + dataset)
        filename = dataset + "_" + self.DATE
        tempfilename = self.OUTPUT_DIR + "/" + filename + ".nc"
        if os.path.exists(tempfilename):
            self.logger.info(tempfilename + " already exists so just reprocessing it.")
        else:
            ecmwfCommand = 'server.retrieve({' \
                           '"class": "mc", ' \
                           '"dataset": "' + ecmfwdataset + '",' \
                            '"date": "' + fromdate + '",' \
                            '"expver": "' + expver + '",' \
                            '"grid": "0.1/0.1",' \
                            '"levtype": "sfc",' \
                            '"param": "' + param + '",' \
                            '"step": "3",' \
                            '"stream": "oper",' \
                            '"time": "' + time + '",' \
                            '"type": "fc",' \
                            '"format": "netcdf",' \
                            '"target": "' + self.OUTPUT_DIR + "/" + filename + ".nc" + '"})'
            self.logger.debug(ecmwfCommand)

            self.server.retrieve({
                "class": "mc",
                "dataset": ecmfwdataset,
                "date": fromdate,
                "expver": expver,
                "grid": "0.1/0.1",
                "levtype": "sfc",
                "param": param,
                "step": "3",
                "stream": "oper",
                "time": time,
                "type": "fc",
                "format": "netcdf",
                "target": self.OUTPUT_DIR + "/" + filename + ".nc"
            })

        self.processImagery(dataset, filename, "", " 0 -90 360 90", self.DATE, ts)
        self.moveToFtpFolder(dataset, filename + ".png", fromdate)
        self.cropData(dataset)
        self.cleanup()

    def getFTPDataset(self, url, dataset, variable, fromdate, te, ts):

        """
        This will download data from most FTP sites. The file construction just needs to be created properly.
        """

        filename = dataset + "_" + fromdate
        formattedFileName = dataset + "_" + self.DATE
        tempfilename = self.OUTPUT_DIR + "/" + formattedFileName + ".nc"
        if os.path.exists(tempfilename):
            self.logger.info(tempfilename + " already exists so just reprocessing it.")
        else:
            self.logger.debug("Downloading " + filename)

            if self.debug:
                wgetCommand = ("wget " + url + filename + ".nc -O " + self.OUTPUT_DIR + "/" + formattedFileName + ".nc")
                self.logger.info(wgetCommand)
            else:
                wgetCommand = (
                "wget -q " + url + filename + ".nc -O " + self.OUTPUT_DIR + "/" + formattedFileName + ".nc")

            subprocess.call(wgetCommand, shell=True)
        self.processImagery(dataset, formattedFileName, variable, te, fromdate, ts)
        self.moveToFtpFolder(dataset, formattedFileName + ".png", fromdate)
        self.cleanup()

    def processImagery(self, dataset, filename, myvariable, te, fromdate, ts):

        """
        Once the netcdf data is downloaded, this function will convert the imagery to a tiff file
        and then convert that tiff file to a png with the correct parameters.
        """

        if os.path.exists(self.OUTPUT_DIR + "/" + filename + ".nc"):
            if myvariable:
                myvariable = ":" + myvariable
            else:
                myvariable = ""
            self.logger.info("Processing " + filename)
            if self.debug:
                gdalwarpcommand = "gdalwarp "
            else:
                gdalwarpcommand = "gdalwarp -q "

            gdalwarpcommand = gdalwarpcommand + " \
                            -of GTiff \
                            -t_srs epsg:4326 \
                            -te" + te + " \
                            -ts " + ts + " \
                            NETCDF:" + self.OUTPUT_DIR + "/" + filename + ".nc" + myvariable + " " + \
                              self.OUTPUT_DIR + "/" + filename + ".tif"

            if self.debug:
                gdaldemcommand = "gdaldem color-relief "
            else:
                gdaldemcommand = "gdaldem color-relief -q "
            gdaldemcommand = gdaldemcommand + " \
                            -of PNG -alpha " + \
                             self.OUTPUT_DIR + "/" + filename + ".tif " + \
                             self.COLOUR_FOLDER + dataset + ".txt  " + \
                             self.OUTPUT_DIR + "/" + filename + ".png"
            self.logger.debug(gdalwarpcommand)
            self.logger.debug(gdaldemcommand)
            subprocess.call(gdalwarpcommand, shell=True)
            subprocess.call(gdaldemcommand, shell=True)

        else:
            self.logger.info(filename + " doesn't exist. Moving on.")

    def moveToFtpFolder(self, target_foldername, filename, fromdate):

        """
        After processing, the png file is currently in a temporary folder. This function
         moves the file to the correct folder according to the global variable FTP_DIR
        """

        self.logger.info("Moving")
        ftpdatafolder = target_foldername.upper()
        if not os.path.exists(self.FTP_DIR + '/' + ftpdatafolder):
            os.makedirs(self.FTP_DIR + '/' + ftpdatafolder)
        origin = str(self.OUTPUT_DIR) + '/' + str(filename)
        destination = str(self.FTP_DIR) + '/' + str(ftpdatafolder) + '/' + str(self.DATE) + '.png'
        self.logger.debug('from: ' + origin)
        self.logger.debug('to: ' + destination)
        subprocess.call("mv " + origin + " " + destination, shell=True)
        subprocess.call("chmod 644 " + destination, shell=True)
        self.logger.info("Processing finished")
        try:
            self.logger.debug("Removing: " + self.OUTPUT_DIR + "/" + target_foldername + "_" + fromdate + ".png")
            os.remove(self.OUTPUT_DIR + "/" + target_foldername + "_" + fromdate + ".png")
        except:
            pass

    def cleanup(self):

        """
        This function removes all temporary files, unless we're in debug mode.
        """

        if self.debug:
            pass
        else:
            try:
                filelist = os.listdir(self.OUTPUT_DIR)
                for f in filelist:
                    os.remove(self.OUTPUT_DIR + "/" + f)
                self.logger.info("Temp files deleted")
            except:
                self.logger.info("Temp files not deleted")
                pass


def main():

    """ Main loop to begin the processing.
    Copying api keyfile to $HOME directory to enable ecmwf api
    """

    subprocess.call("cp .ecmwfapirc $HOME/", shell=True)
    download = Downloader()
    download.run()


if __name__ == '__main__':
    main()
