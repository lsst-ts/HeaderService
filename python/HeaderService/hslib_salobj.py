# This file is part of HeaderService
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
import sys
import socket
import asyncio
import time
import types
import subprocess
from . import hutils
from . import hscalc
from lsst.ts import salobj
import HeaderService
import importlib
import json
import copy
import logging

try:
    HEADERSERVICE_DIR = os.environ['HEADERSERVICE_DIR']
except KeyError:
    HEADERSERVICE_DIR = __file__.split('python')[0]

# The allowed values for fixed collection events
FIXED_COLLECTION_EVENTS = frozenset(["start_collection_event", "end_collection_event"])


class HSWorker(salobj.BaseCsc):

    """ A Class to run and manage the Header Service"""

    def __init__(self, **keys):

        # initialize current state to None
        self.current_state = None

        # Load the configurarion
        self.config = types.SimpleNamespace(**keys)

        # Create a salobj.BaseCsc and get logger
        self.create_BaseCsc()

        # Get ready non-SAL related tasks
        self.prepare()

        # Extract the unique channel by topic/device
        self.get_channels()

        # Make the connections using saloj.Remote
        self.create_Remotes()

        # Define the callbacks for start/end
        self.define_evt_callbacks()

        # Load enum xml libraries
        self.load_enums_xml()

        # Define global lock to update dictionaries
        self.dlock = asyncio.Lock()

    async def close_tasks(self):
        """Close tasks on super and evt timeout"""
        await super().close_tasks()
        self.cancel_timeout_tasks()

    async def end_evt_timeout(self, imageName, timeout):
        """Timeout timer for end event telemetry callback"""
        await asyncio.sleep(timeout)
        self.log.info(f"{self.name_end} for {imageName} not seen in {timeout} [s]; giving up")
        # Send the timeout warning using the salobj log
        self.log.warning(f"Timeout while waiting for {self.name_end} Event from {imageName}")
        async with self.dlock:
            self.clean(imageName)

    def cancel_timeout_tasks(self):
        """Cancel the per-image timeout tasks"""
        # Get the imageName to cancel
        list_to_cancel = copy.deepcopy(list(self.end_evt_timeout_task.keys()))
        if list_to_cancel:
            self.log.info(f"Will cancel tasks: {list_to_cancel}")
            for imageName in list_to_cancel:
                self.end_evt_timeout_task[imageName].cancel()
                self.clean(imageName)

    async def handle_summary_state(self):

        # Check that we have the right looger once the HS is running:
        if self.log.name != self.salinfo.name:
            self.log.info(f"Redirecting logger from {self.log} to {self.salinfo.log}")
            self.setup_logging()

        # if current_state hasn't been set, and the summary_state is STANDBY,
        # we're just starting up, so don't do anything but set the current
        # state to STANBY
        if (self.current_state is None) and (self.summary_state == salobj.State.STANDBY):
            self.current_state = self.summary_state

        self.log.info(f"Current state is: {self.current_state.name}; transition to {self.summary_state.name}")

        if self.summary_state != salobj.State.ENABLED:
            self.cancel_timeout_tasks()

        # Check that services are running -- if not will go into FAULT
        if (self.current_state == salobj.State.DISABLED) and (self.summary_state == salobj.State.ENABLED):
            await self.check_services()

        self.log.info(f"Current state is: {self.summary_state.name}")
        # Save the current_state for next
        self.current_state = self.summary_state

    def define_evt_callbacks(self):
        """Set the callback functions based on configuration"""

        # Select the start_collection callback
        devname = get_channel_devname(self.config.start_collection_event)
        topic = self.config.start_collection_event['topic']
        getattr(self.Remote[devname], f"evt_{topic}").callback = self.start_collection_event_callback
        self.log.info(f"Defining START callback for {devname} {topic}")

        # Select the end_collection callback
        devname = get_channel_devname(self.config.end_collection_event)
        topic = self.config.end_collection_event['topic']
        getattr(self.Remote[devname], f"evt_{topic}").callback = self.end_collection_event_callback
        self.log.info(f"Defining END callback for {devname} {topic}")

        # Store the callback for the generic collection
        if len(self.collection_events_names) > 0:
            self.collect_callback = {}
            for name, c in self.collection_events.items():
                if name in [self.name_start, self.name_end]:
                    self.log.debug(f"Ignoring callback creation for {name}")
                    continue
                devname = get_channel_devname(c)
                topic = c['topic']
                self.collect_callback[name] = self.callback_collection_builder(name)
                self.log.info(f"Created COLLECT callback function for: {name}")
                getattr(self.Remote[devname], f"evt_{topic}").callback = self.collect_callback[name]
                self.log.info(f"Defining COLLECT callback for: {devname} {topic}")

        # Store the callback for the events we want to monitor
        if len(self.monitor_event_channels) > 0:
            self.monitor_callback = {}
            for name, c in self.monitor_event_channels.items():
                devname = get_channel_devname(c)
                topic = c['topic']
                # Get the name of the callback function
                self.monitor_callback[name] = self.callback_monitor_builder(name)
                self.log.info(f"Created callback function for: {name}")
                getattr(self.Remote[devname], f"evt_{topic}").callback = self.monitor_callback[name]
                self.log.info(f"Defining callback for {devname} {topic}")

    def callback_monitor_builder(self, monitor_event_name):
        """Function builder for monitor's callbacks"""
        def generic_monitor_callback(myData):
            """
            The generic callback function for the monitor collection event
            """
            setattr(generic_monitor_callback, '__monitor_event_name__', monitor_event_name)
            setattr(generic_monitor_callback, '__name__', monitor_event_name)

            # If not in ENABLED mode we do nothing
            if self.summary_state != salobj.State.ENABLED:
                self.log.info(f"Received: {monitor_event_name}")
                self.log.info(f"Ignoring as current state is {self.summary_state.name}")
                return

            # Extract the list of imageNames to update metadata
            imageName_list = self.metadata.keys()
            # If list is empty we do nothing
            if len(imageName_list) == 0:
                self.log.info(f"Ignoring event: {monitor_event_name} -- no imageName present")
                return

            # The keywords we want to update
            keywords = self.monitor_event_channels_keys[monitor_event_name]
            self.log.info(f"Collecting metadata for: {monitor_event_name} and keys: {keywords}")
            # Update the imageName metadata with new dict
            for imageName in self.metadata.keys():
                self.log.info(f"Updating monitored metadata for: {imageName}")
                self.update_monitor_metadata(imageName, keywords)

        return generic_monitor_callback

    def callback_collection_builder(self, event_name):
        """Function builder for collect event callbacks"""
        def generic_collection_callback(myData):
            """
            Generic collection callback function. The event needs to be from
            Camera as we will extract imageName from the event payload
            """
            setattr(generic_collection_callback, '__collect_event_name__', event_name)
            setattr(generic_collection_callback, '__name__', event_name)
            # Extract the key to match start/end events
            imageName = self.get_imageName(myData)
            # If not in ENABLED mode we do nothing
            if self.summary_state != salobj.State.ENABLED:
                self.log.info(f"Received: {event_name} Event for {imageName}")
                self.log.info(f"Ignoring as current state is {self.summary_state.name}")
                return

            self.log.info(f"----- Received: {event_name} Event for {imageName} -----")
            self.log.info(f"Starting callback {event_name} for imageName: {imageName}")
            # The keywords we want to update
            keywords = self.collection_events_keys[event_name]
            self.log.info(f"Collecting Metadata GENERIC: {event_name} Event and keys: {keywords}")
            self.log.info(f"Updating metadata for: {imageName}")
            self.metadata[imageName].update(self.collect(keywords))

        return generic_collection_callback

    def start_collection_event_callback(self, myData):
        """ The callback function for the START collection event"""

        # Extract the key to match start/end events
        imageName = self.get_imageName(myData)

        # If not in ENABLED mode we do nothing
        if self.summary_state != salobj.State.ENABLED:
            self.log.info(f"Received: {self.name_start} Event for {imageName}")
            self.log.info(f"Ignoring as current state is {self.summary_state.name}")
            return

        self.log.info(f"----- Received: {self.name_start} Event for {imageName} -----")
        self.log.info(f"Starting callback START for imageName: {imageName}")

        # Update header object and metadata dictionaries with lock and wait
        asyncio.ensure_future(self.complete_tasks_START(imageName))

    def end_collection_event_callback(self, myData):
        """ The callback function for the END collection event"""

        # Extract the key to match start/end events
        imageName = self.get_imageName(myData)

        # If not in ENABLED mode we do nothing
        if self.summary_state != salobj.State.ENABLED:
            self.log.info(f"Received: {self.name_end} Event for {imageName}")
            self.log.info(f"Ignoring as current state is {self.summary_state.name}")
            return

        if imageName not in self.end_evt_timeout_task:
            self.log.warning(f"Received orphan {self.name_end} Event without a timeout task")
            self.log.warning(f"{self.name_end} will be ignored for: {imageName}")
            self.log.info(f"Current State is {self.summary_state.name}")
            return

        # Check for rogue end collection events
        self.log.info(f"----- Received: {self.name_end} Event for {imageName} -----")
        self.log.info(f"Starting callback END for imageName: {imageName}")
        # Cancel/stop the timeout task because we got the END callback
        self.log.info(f"Calling cancel() timeout_task for: {imageName}")
        self.end_evt_timeout_task[imageName].cancel()

        # Final collection using asyncio lock, will call functions that
        # take care of updating data structures. We also write the header file.
        asyncio.ensure_future(self.complete_tasks_END(imageName))

    def get_playlist_dir(self):
        """Figure the location for the playlist folder"""
        if 'HEADERSERVICE_PLAYLIST_DIR' in os.environ:
            self.playlist_dir = os.path.join(os.environ['HEADERSERVICE_PLAYLIST_DIR'],
                                             self.config.instrument)
        else:
            self.playlist_dir = os.path.join(HEADERSERVICE_DIR,
                                             "etc/playback/lib", self.config.instrument)
        # Make sure that the directory exists
        if not os.path.exists(self.playlist_dir):
            msg = f"Directory: {self.playlist_dir} not found"
            self.log.error(msg)
            raise FileNotFoundError(msg)
        else:
            self.log.info(f"Will use: {self.playlist_dir} for playback folder")

    def get_tstand(self):
        """Figure the Test Stand in use"""
        # Check if definced in self.config or the environment
        if self.config.tstand:
            self.tstand = self.config.tstand
            self.log.info(f"Will use TSTAND: {self.tstand} in configurarion")
        elif 'TSTAND_HEADERSERVICE' in os.environ:
            self.tstand = os.environ['TSTAND_HEADERSERVICE']
            self.log.info(f"Will use TSTAND: {self.tstand} in from environment")
        else:
            # Try to auto-figure out from location
            self.log.warning("The TSTAND was not defined in config/environment")
            address = socket.getfqdn()
            if address.find('.ncsa.') >= 0:
                self.tstand = "NCSA"
                self.log.info(f"Will use auto-config tstand: {self.tstand}")
            else:
                self.tstand = None
                self.log.info("Unable to auto-config tstand -- will be left undefined")

    def get_ip(self):
        """Figure out the IP we will be using to broadcast"""

        # Check if definced in self.config or the environment
        if self.config.ip_address:
            self.ip_address = self.config.ip_address
            self.log.info(f"Will use IP: {self.ip_address} in config for web service")
        elif 'IP_HEADERSERVICE' in os.environ:
            self.ip_address = os.environ['IP_HEADERSERVICE']
            self.log.info(f"Will use IP: {self.ip_address} fron environment for web service")
        else:
            self.ip_address = socket.gethostbyname(socket.gethostname())
            self.log.info(f"Will use IP: {self.ip_address} auto-config for web service")

    def get_s3instance(self):
        """
        Figure the location where are running to define the s3instance
        in case it wasn't defined.
        """
        # Check if defined in self.config or the environment
        if self.config.s3instance:
            s3instance = self.config.s3instance
            self.log.info(f"Will use s3instance in config: {s3instance}")
            return s3instance
        elif 'S3INSTANCE' in os.environ:
            s3instance = os.environ['S3INSTANCE']
            self.log.info(f"Will use s3instance from environment: {s3instance}")
            return s3instance

        # Try to auto-figure out from location
        self.log.warning("The s3instance was not defined in config")
        address = socket.getfqdn()
        if address.find('.ncsa.') >= 0:
            s3instance = 'nts'
        elif address.find('tuc') >= 0:
            s3instance = 'tuc'
        elif address.find('.cp.') >= 0:
            s3instance = 'cp'
        else:
            s3instance = 'dummy'
        self.log.info(f"Will use auto-config s3instance: {s3instance}")
        return s3instance

    def define_s3bucket(self):
        """
        Get the s3instance and define the name for the
        s3 bucket using salobj

        To access the S3 server, the environment variables are set via:

        export S3_ENDPOINT_URL=http://lsst-nfs.ncsa.illinois.edu:9000
        export AWS_ACCESS_KEY_ID={access_key}
        export AWS_SECRET_ACCESS_KEY={secret_key}
        """

        s3instance = self.get_s3instance()
        self.s3bucket_name = salobj.AsyncS3Bucket.make_bucket_name(s3instance=s3instance)
        self.log.info(f"Will use Bucket name: {self.s3bucket_name}")

        # 2. Use AsyncS3Bucket to make bucket + S3 connection
        self.s3bucket = salobj.AsyncS3Bucket(name=self.s3bucket_name, domock=False)
        self.log.info(f"Defined AsyncS3Bucket: {self.s3bucket_name}")

        # We will re-use the connection made by salobj
        self.s3conn = self.s3bucket.service_resource
        self.log.info(f"Will use s3 endpoint_url: {self.s3conn.meta.client.meta.endpoint_url}")

        # 3. Make sure the bucket exists in the list of bucket names:
        try:
            bucket_names = [b.name for b in self.s3conn.buckets.all()]
            self.log.info(f"Found s3 buckets: {bucket_names}")
        except Exception as e:
            self.log.error(f"Cannot connect to bucket: {self.s3bucket_name}")
            self.log.exception(str(e))
            s3bucket_OK = False
            return s3bucket_OK
        if self.s3bucket_name not in bucket_names:
            self.s3conn.create_bucket(Bucket=self.s3bucket_name)
            self.log.info(f"Created Bucket: {self.s3bucket_name}")
        else:
            self.log.info(f"Bucket Name: {self.s3bucket_name} already exists")
        s3bucket_OK = True
        return s3bucket_OK

    def start_web_server(self, logfile, httpserver="http.server"):

        """
        Start a light web service to serve the header files to the EFD
        """

        # Get the hostname and IP address
        self.get_ip()

        # Change PYTHONUNBUFFERED to 1 to allow continuous writing.
        os.environ['PYTHONUNBUFFERED'] = '1'

        # Open the file handle
        weblog = open(logfile, 'a')
        weblog.flush()

        self.log.info(f"Will send web server logs to: {logfile}")
        # Get the system's python
        python_exe = sys.executable
        # Make sure there isn't another process running
        cmd = f"ps -ax | grep \"{httpserver} {self.config.port_number}\" | grep -v grep | awk '{{print $1}}'"
        self.log.info(f"Checking for webserver running: {cmd}")
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        pid = p.stdout.read().decode()
        self.log.info(f"Writing web log to: {logfile}")

        if pid == '':
            # Store the current location so we can go back here
            cur_dirname = os.getcwd()
            os.chdir(self.config.filepath)
            # The subprocess call
            self.log.info(f"Will start web server on dir: {self.config.filepath}")
            self.log.info(f"Serving at port: {self.config.port_number}")
            p = subprocess.Popen([python_exe, '-m', httpserver, str(self.config.port_number)],
                                 stdout=weblog, stderr=weblog)
            time.sleep(1)
            self.log.info("Done Starting web server")
            # Get back to where we were
            os.chdir(cur_dirname)
        elif int(pid) > 0:
            self.log.info(f"{httpserver} already running with pid:{int(pid)}  ... Bye")
        else:
            self.log.info("Warning: Wrong process id - will not start www service")

    def setup_logging(self):
        """
        Simple Logger definitions across this module, we call the generic
        function defined in hutils.py
        """
        # Make sure the directory exists
        dirname = os.path.dirname(self.config.logfile)
        if dirname != '':
            self.check_outdir(dirname)

        # We cannot use the CSC logger before it starts
        if not self.salinfo.running:
            self.log = logging.getLogger(__name__)
            print(f"{self.salinfo} is not running -- will use own logger for now")
        else:
            self.log = self.salinfo.log

        hutils.configure_logger(self.log, logfile=self.config.logfile,
                                level=self.config.loglevel,
                                log_format=self.config.log_format,
                                log_format_date=self.config.log_format_date)
        self.log.info(f"Logging Started at level:{self.config.loglevel}")
        self.log.info(f"Will send logging to: {self.config.logfile}")
        self.log.info(f"Running HeaderService version: {HeaderService.__version__}")

    def create_BaseCsc(self):
        """
        Create a BaseCsc for the the HeaderService. We initialize the
        State object that keeps track of the HS current state.  We use
        a start_state to set the initial state
        """

        self.version = HeaderService.__version__
        self.valid_simulation_modes = (0, 1)

        super().__init__(name=self.config.hs_name, index=self.config.hs_index,
                         initial_state=getattr(salobj.State, self.config.hs_initial_state),
                         simulation_mode=self.config.hs_simulation_mode)
        # Logging
        self.setup_logging()
        # Version information
        self.log.info(f"Starting the CSC with {self.config.hs_name}")
        self.log.info(f"Creating worker for: {self.config.hs_name}")
        self.log.info(f"Running salobj version: {salobj.__version__}")
        self.log.info(f"Starting in State:{self.summary_state.name}")
        self.log.info(f"Starting in simulation_mode: {self.config.hs_simulation_mode}")

        # Set the CSC version using softwareVersions
        self.log.info(f"Setting softwareVersions Event with version: {HeaderService.__version__}")
        self.evt_softwareVersions.set(cscVersion=HeaderService.__version__)

        # Set the simulation Mode using the event simulationMode
        self.log.info(f"Setting simulationMode Event with mode: {self.config.hs_simulation_mode}")
        self.evt_simulationMode.set(mode=self.config.hs_simulation_mode)

        # Only for playback mode we remove all but one entry from the
        # self.config.telemetry to block the HS from listeing to other CSCs
        if self.config.playback:
            # keywords_keep = ['EMUIMAGE', 'DATE-OBS', 'DATE-BEG', 'DATE-END']
            self.log.info("Playback mode, unsubscribing from telemetry")
            telemetry_copy = self.config.telemetry.copy()
            for keyword in telemetry_copy:
                if keyword not in self.config.playback_keywords_keep:
                    del self.config.telemetry[keyword]
                    self.log.info(f"Removing keyword: {keyword} from subscribed telemetry")

    def create_Remotes(self):
        """
        Create the Remotes to collect telemetry/Events for channels as
        defined by the meta-data
        """
        self.log.info("*** Starting Connections for Meta-data ***")
        # The list containing the unique devices (CSCs) to make connection
        self.devices = []
        # The dict containing all of the threads and connections
        self.Remote = {}
        self.Remote_get = {}
        for channel_name, c in self.channels.items():
            devname = get_channel_devname(c)
            # Make sure we only create these once
            if devname not in self.devices:
                self.devices.append(devname)
                self.Remote[devname] = salobj.Remote(domain=self.domain,
                                                     name=c['device'],
                                                     index=c['device_index'],
                                                     include=self.device_topics[devname],
                                                     )
                self.log.info(f"Created Remote for {devname}")
                self.log.info(f"with include topics: {self.device_topics[devname]}")

            # capture the evt.get() function for the channel
            if c['Stype'] == 'Event':
                self.Remote_get[channel_name] = getattr(self.Remote[devname], f"evt_{c['topic']}").get
                self.log.info(f"Storing Remote.evt_{c['topic']}.get() for {channel_name}")
            if c['Stype'] == 'Telemetry':
                self.Remote_get[channel_name] = getattr(self.Remote[devname], f"tel_{c['topic']}").get
                self.log.info(f"Storing Remote.tel_{c['topic']}.get() for {channel_name}")

    def load_enums_xml(self):
        """
        Load using importlib the xml libraries for the enumerated CSCs
        """
        # Get the list of enum enum_csc
        self.log.info("Extracting enum CSC's from telemetry dictionary")
        self.enum_csc = get_enum_cscs(self.config.telemetry)
        self.xml_lib = {}
        for csc in self.enum_csc:
            self.log.info(f"importing lsst.ts.xml.enums.{csc}")
            self.xml_lib[csc] = importlib.import_module("lsst.ts.xml.enums.{}".format(csc))
        self.log.info("enums imported")

    def get_channels(self):
        """Extract the unique channels by topic/device"""
        self.log.info("Extracting Telemetry channels and topics from telemetry dictionary")
        self.channels, self.device_topics = extract_telemetry_channels(
            self.config.telemetry,
            start_collection_event=self.config.start_collection_event,
            end_collection_event=self.config.end_collection_event,
            imageParam_event=self.config.imageParam_event,
            cameraConf_event=self.config.cameraConf_event)

        # Get the events where we want to collect telemetry
        self.log.info("Extracting collection events from telemetry dictionary")
        (self.collection_events,
         self.collection_events_names,
         self.collection_events_keys) = get_collection_events(self.config)
        self.log.info(f"Extracted events to collect: {self.collection_events_names}")

        # Select the start_collection channel
        self.name_start = get_channel_name(self.config.start_collection_event)
        # Select the end_collection channel
        self.name_end = get_channel_name(self.config.end_collection_event)

        # Separate the keys to collect at the 'end' from the ones at 'start'
        self.keywords_start = self.collection_events_keys[self.name_start]
        self.keywords_end = self.collection_events_keys[self.name_end]

        # Get the events we want to monitor
        self.log.info("Extracting Telemetry channels to monitor from telemetry dictionary")
        (self.monitor_event_channels,
         self.monitor_event_channels_names,
         self.monitor_event_channels_keys) = get_monitor_channels(self.config.telemetry)
        if len(self.monitor_event_channels) > 0:
            self.log.info(f"Extracted channels to monitor: {self.monitor_event_channels_names}")

    def check_outdir(self, filepath):
        """ Make sure that we have a place to put the files"""
        if not os.path.exists(filepath):
            os.makedirs(filepath)
            self.log.info(f"Created dirname:{filepath}")

    async def check_services(self):
        """ Check that services needed s3/web are working"""

        self.log.info("Checking for services")
        s3bucket_OK = True
        webserver_OK = True
        # Define/check s3 buckets
        if self.config.lfa_mode == 's3':
            s3bucket_OK = self.define_s3bucket()
        # Start the web server
        elif self.config.lfa_mode == 'http':
            try:
                self.start_web_server(self.config.weblogfile)
                webserver_OK = True
            except Exception as e:
                self.log.error("Cannot start webserver")
                self.log.exception(str(e))
                webserver_OK = False

        # if start is not okay, we go to fault
        if s3bucket_OK is False or webserver_OK is False:
            self.log.error("Checking for services -- failed")
            await self.fault(code=9, report="Checking for services -- failed")
        else:
            self.log.info("Checking for services -- completed")
            self.check_services_OK = True

    def prepare(self):
        """
        Non-SAL/salobj related task that need to be prepared
        """

        # The user running the process
        self.USER = os.environ['USER']

        # Make sure that we have a place to put the files
        self.check_outdir(self.config.filepath)

        # Get the TSTAND
        self.get_tstand()

        # Get the playlist directory
        if self.config.playback:
            self.get_playlist_dir()

        # Create dictionaries keyed to imageName
        self.create_dicts()

        # Define if we need sensor infornation in templates
        # For LSSTCam, MTCamera is charge the Camera metadata
        # if self.config.instrument == 'LSSTCam':
        #    self.nosensors = False
        # else:
        # Will try sensors for LSSTCam
        self.nosensors = False

        # Check for segment name in configuration
        if not hasattr(self.config, 'segname'):
            self.log.info("Setting segname to None")
            self.config.segname = None

        # Check for imageParam_event
        if not hasattr(self.config, 'imageParam_event'):
            self.log.info("Setting imageParam_event to None")
            self.config.imageParam_event = None

        self.log.info(f"Setting nosensors to: {self.nosensors} for {self.config.instrument}")

    async def complete_tasks_START(self, imageName):
        """
        Update data objects at START with asyncio lock
        and complete all tasks started with START event.
        """
        async with self.dlock:

            # Collect metadata at start of integration and
            # load it on the self.metadata dictionary
            self.log.info(f"Collecting Metadata START : {self.name_start} Event")
            self.log.info(f"Creating metadata for: {imageName}")
            self.metadata[imageName] = self.collect(self.keywords_start)

            # Create the HDR object to be populated with the collected metadata
            # when loading the templates we get a HDR.header object
            self.log.info(f"Creating header object for : {imageName}")

            # Get the self.vendors and self.sensors
            self.get_vendors_and_sensors()
            self.log.info(f"Will use vendors: {self.vendor_names}")
            self.log.info(f"Will use sensors: {self.sensors}")
            self.HDR[imageName] = hutils.HDRTEMPL(logger=self.log,
                                                  section=self.config.section,
                                                  instrument=self.config.instrument,
                                                  nosensors=self.nosensors,
                                                  segname=self.config.segname,
                                                  vendor_names=self.vendor_names,
                                                  sensor_names=self.sensors,
                                                  write_mode=self.config.write_mode)
            self.HDR[imageName].load_templates()
            # Get the filenames and imageName from the start event payload.
            self.log.info(f"Defining filenames for: {imageName}")
            self.get_filenames(imageName)

            # Get the requested exposure time to estimate the total timeout
            try:
                timeout_camera = self.read_timeout_from_camera()
                self.log.info(f"Extracted timeout: {timeout_camera}[s] from Camera")
            except AttributeError:
                self.log.warning("Cannot extract timout from camera, will use EXPTIME instead")
                exptime_key = self.config.timeout_keyword
                self.log.info("Collecting key to set timeout as key %s + %s" %
                              (exptime_key, self.config.timeout_exptime))
                metadata_tmp = self.collect([exptime_key])
                timeout_camera = metadata_tmp[exptime_key]
                self.log.info(f"Extracted timeout: {timeout_camera}[s] from {exptime_key}")
            timeout = timeout_camera + self.config.timeout_exptime
            self.log.info(f"Setting timeout: {timeout_camera} + {self.config.timeout_exptime} [s]")
            self.log.info(f"Using timeout: {timeout} [s]")

            # Store completed_OK
            self.completed_OK[imageName] = False
            # Create timeout_task per imageName
            self.end_evt_timeout_task[imageName] = asyncio.ensure_future(self.end_evt_timeout(imageName,
                                                                                              timeout))
            self.log.info(f"Waiting {timeout} [s] for {self.name_end} Event for: {imageName}")

    async def complete_tasks_END(self, imageName):
        """
        Update data objects at END with asyncio lock
        and complete all tasks started with END event
        """
        async with self.dlock:

            # Collect metadata at end of integration
            self.log.info(f"Collecting Metadata END: {self.name_end} Event")
            self.log.info(f"Updating metadata for: {imageName}")
            self.metadata[imageName].update(self.collect(self.keywords_end))
            # Collect metadata created by the HeaderService
            self.log.info("Collecting Metadata from HeaderService")
            # Update header with information from HS
            self.collect_from_HeaderService(imageName)
            # We set completed_OK to True, and only change to False later in
            # case we get an exception
            self.completed_OK[imageName] = True

            # Update header using the information from the camera geometry
            self.log.info("Updating Header with Camera information")
            try:
                self.update_header_geometry(imageName)
            except Exception as e:
                # We still want to write a header if we fail to update geometry
                self.log.warning("Failed call to update_header_geometry")
                self.log.warning(e)

            # In case of playback mode, we want to override any metadata
            # collected in self.metadata[imageName] dictionary. We want to do
            # this after all collection is done
            if self.config.playback:
                try:
                    # Update the metadata dictionary with json values
                    self.update_header_emuimage(imageName)
                except Exception as e:
                    self.completed_OK[imageName] = False
                    self.log.error(e)
                    self.log.error("Failed call to update_header_emuimage")

            # Update header object with metadata dictionary
            if self.completed_OK[imageName]:
                self.update_header(imageName)

            # Write the header only if so far if completed_OK is True, if write
            # fails it will set self.completed_OK to False
            if self.completed_OK[imageName]:
                self.write(imageName)

            # if completed_OK is False we go to FAULT
            if self.completed_OK[imageName] is False:
                self.log.warning("Sending the system to FAULT state")
                await self.fault(code=9, report=f"Cannot write header for: {imageName}")
                self.log.error(f"----- Failed: {imageName} -----")
            else:
                # Announce/upload LFO if write_OK is True
                self.completed_OK[imageName] = True
                await self.announce(imageName)
                self.log.info(f"----- Done: {imageName} -----")

                # Clean up
                self.clean(imageName)

        if self.summary_state == salobj.State.ENABLED:
            self.log.info("----- Ready for next image -----")

        self.log.info(f"Current state is: {self.summary_state.name}")

    def get_vendors_and_sensors(self):

        if self.nosensors:
            self.log.info(f"Will not get vendors/ccdnames for {self.config.instrument}")
            self.vendor_names = []
            self.sensors = []
            return

        # Try to get the list of sensor from the Camera Configuration event
        try:
            self.vendor_names, self.sensors = self.read_camera_vendors()
            self.log.info("Extracted vendors/ccdnames from Camera Configuration")
        except Exception:
            # In the absense of a message from camera to provide the list
            # of sensors and vendors, we build the list using a function
            # in hutils
            self.log.warning("Cannot read camera vendor list from event")
            self.log.warning("Will use defaults from config file instead")
            self.sensors = hutils.build_sensor_list(self.config.instrument)
            self.vendor_names = self.config.vendor_names

        return

    def update_header_emuimage(self, imageName):
        """
        Read in the json file for emulatedImage and update the metadata
        dictionary for selected keywords
        """
        emuimage = self.metadata[imageName]['EMUIMAGE']
        self.log.info(f"Playback mode emulatedImage: {emuimage}")
        emuimage_file = os.path.join(self.playlist_dir, emuimage+".json")
        self.log.info(f"Reading json file: {emuimage_file}")
        with open(emuimage_file) as f:
            emuimage_dict = json.load(f)

        # In case we have a __COMMON__ section in the dictionary
        if '__COMMON__' in emuimage_dict.keys():
            emuimage_values = emuimage_dict['__COMMON__']
        else:
            emuimage_values = emuimage_dict

        # Now we update the metadata in the json emulated file
        for keyword in emuimage_values:
            if keyword in self.config.playback_keywords:
                self.log.info(f"EMUIMAGE -- updating {keyword:8s} = {emuimage_values[keyword]}")
                self.metadata[imageName][keyword] = emuimage_values[keyword]
            else:
                self.log.debug(f"EMUIMAGE -- ignoring {keyword} from emulatedImage")

    def read_camera_vendors(self, sep=":"):
        """ Read the vendor/ccdLocation from camera event """
        name = get_channel_name(self.config.cameraConf_event)
        array_keys = self.config.cameraConf_event['array_keys']
        param = self.config.cameraConf_event['value']
        myData = self.Remote_get[name]()
        # exit in case we cannot get data from SAL
        if myData is None:
            self.log.warning("Cannot get myData from {}".format(name))
            return

        # 1 We get the keywords List from myData
        payload = getattr(myData, array_keys)
        ccdnames = hutils.split_esc(payload, sep)
        if len(ccdnames) <= 1:
            self.log.warning(f"List keys for {name} is <= 1")
            self.log.info(f"For {name}, extracted '{array_keys}': {ccdnames}")
        # 2 Get the actual list of vendor names
        payload = getattr(myData, param)
        vendor_names = hutils.split_esc(payload, sep)
        self.log.info("Successfully read vendors/ccdnames from Camera config event")
        return vendor_names, ccdnames

    def update_header_geometry(self, imageName):
        """ Update the image geometry Camera Event """

        # if config.imageParam_event is False, we skip
        if not self.config.imageParam_event:
            self.log.info("No imageParam_event, will not update_header_geometry")
            return

        # Image paramters
        self.log.info("Extracting CCD/Sensor Image Parameters")
        # Extract from telemetry and identify the channel
        name = get_channel_name(self.config.imageParam_event)
        array_keys = self.config.imageParam_event['array_keys']
        myData = self.Remote_get[name]()
        # exit in case we cannot get data from SAL
        if myData is None:
            self.log.warning("Cannot get geometry myData from {}".format(name))
            return

        # Obtain the geometry that we'll use for each segment.
        geom = hutils.get_image_size_from_imageReadoutParameters(myData, array_keys)

        # Update the geometry for the HDR object
        self.log.info(f"Updating header CCD geom for {imageName}")
        self.HDR[imageName].load_geometry(geom)
        self.log.info("Templates Updated")

    def read_timeout_from_camera(self):
        """Extract the timeout from Camera Event"""
        # Extract from telemetry and identify the channel
        name = get_channel_name(self.config.timeout_event)
        myData = self.Remote_get[name]()
        param = self.config.timeout_event['value']
        device = self.config.timeout_event['device']
        if myData is None:
            self.log.warning("Cannot get timeout myData from {}".format(name))
            return
        timeout_camera = getattr(myData, param)
        self.log.info(f"Extracted timeout from {device}: {timeout_camera}")
        return timeout_camera

    def update_header(self, imageName):

        """Update FITSIO header object using the captured metadata"""
        for keyword, value in self.metadata[imageName].items():
            # Check if dictionary with per-sensor values
            if isinstance(value, dict):
                for sensor in value.keys():
                    extname = self.HDR[imageName].get_primary_extname(sensor)
                    self.HDR[imageName].update_record(keyword, value[sensor], extname)
                    self.log.debug(f"Updating header[{extname}] with {keyword:8s} = {value[sensor]}")
            # Otherwise we put it into the PRIMARY
            else:
                extname = 'PRIMARY'
                self.log.info(f"Updating header[{extname}] with {keyword:8s} = {value}")
                self.HDR[imageName].update_record(keyword, value, extname)

    def get_imageName(self, myData):
        """
        Method to extract the key to match start/end events
        (i.e: imageName) uniformly across the class
        """
        imageName = getattr(myData, self.config.imageName_event['value'])
        return imageName

    def get_filenames(self, imageName):
        """
        Figure out the section of the telemetry from which we will extract
        'imageName' and define the output names based on that ID
        """
        # Construct the hdr and fits filename
        self.filename_FITS[imageName] = self.config.format_FITS.format(imageName)
        self.filename_HDR[imageName] = os.path.join(self.config.filepath,
                                                    self.config.format_HDR.format(imageName))

    async def announce(self, imageName):
        """
        Upload and broadcast the LFO Event for the HeaderService
        """

        # if S3 bucket, upload before announcing to get
        # the url that will be broadcast.
        # Upload header file and get key/url
        # key should be like:
        # CCHeaderService/header/2020/05/21/CCHeaderService_header_CC_O_20200521_000008.yaml
        # For date we use 'DATE' in the metadata dictionary as this should
        # always be available beacuse is created by the HeaderService
        if self.config.lfa_mode == 's3':
            key = self.s3bucket.make_key(
                salname=self.config.hs_name,
                salindexname=self.config.hs_index,
                other=imageName,
                generator='header',
                date=self.metadata[imageName]['DATE'],
                suffix=".yaml"
            )
            # In case we want to go back to an s3 url
            # url = f"s3://{self.s3bucket.name}/{key}"

            # The url for simple a wget/curl fetch
            # i.e. http://S3_ENDPOINT_URL/s3buket_name/key
            url = f"{self.s3conn.meta.client.meta.endpoint_url}/{self.s3bucket.name}/{key}"
            t0 = time.time()
            with open(self.filename_HDR[imageName], "rb") as f:
                s3upload = await self.upload_to_s3(f, key, imageName, nt=2)

            if s3upload is False:
                await self.fault(code=9, report=f"Failed s3 bucket upload for: {imageName}")
                self.completed_OK[imageName] = False
                return

            self.log.info(f"Header s3 upload time: {hutils.elapsed_time(t0)}")
            self.log.info(f"Will use s3 key: {key}")
            self.log.info(f"Will use s3 url: {url}")
        elif self.config.lfa_mode == 'http':
            url = self.config.url_format.format(
                ip_address=self.ip_address,
                port_number=self.config.port_number,
                filename_HDR=os.path.basename(self.filename_HDR[imageName]))
            self.log.info(f"Will use http url: {url}")
        else:
            self.log.error(f"lfa_mode: {self.config.lfa_mode} not supported")

        # Get the md5 for the header file
        md5value = hutils.md5Checksum(self.filename_HDR[imageName])
        bytesize = os.path.getsize(self.filename_HDR[imageName])
        self.log.info("Got MD5SUM: {}".format(md5value))

        # Now we publish filename and MD5
        # Build the kwargs
        kw = {'byteSize': bytesize,
              'checkSum': md5value,
              'generator': self.config.hs_name,
              'mimeType': self.config.write_mode.upper(),
              'url': url,
              'id': imageName,
              'version': 1,
              }

        await self.evt_largeFileObjectAvailable.set_write(**kw)
        self.log.info(f"Sent {self.config.hs_name} largeFileObjectAvailable: {kw}")
        self.completed_OK[imageName] = True

    async def upload_to_s3(self, fileobj, key, imageName, nt=2):
        """Loop call to salobj.s3bucket.upload with a number of tries."""
        s3upload = False
        k = 1
        while k <= nt and s3upload is False:
            try:
                await self.s3bucket.upload(fileobj=fileobj, key=key)
                s3upload = True
            except Exception as e:
                self.log.error(f"Failed s3bucket.upload attempt # {k}/{nt} for: {imageName}")
                self.log.exception(str(e))
            k += 1
        return s3upload

    def write(self, imageName):
        """ Function to call to write the header"""

        try:
            self.HDR[imageName].write_header(self.filename_HDR[imageName])
            self.log.info(f"Wrote header to filesystem: {self.filename_HDR[imageName]}")
        except Exception as e:
            self.log.error(f"Cannot write header to filesystem {self.filename_HDR[imageName]}")
            self.log.error(f"{e.__class__.__name__}: {e}")
            self.log.exception(str(e))
            self.completed_OK[imageName] = False

    def clean(self, imageName):
        """ Clean up imageName data structures"""
        self.log.info(f"Cleaning data for: {imageName}")
        del self.end_evt_timeout_task[imageName]
        del self.metadata[imageName]
        del self.HDR[imageName]
        del self.filename_HDR[imageName]
        del self.filename_FITS[imageName]
        del self.completed_OK[imageName]

    def create_dicts(self):
        """
        Create the dictionaries holding per image information, such as:
        timeout tasks, metadata and headers
        """
        self.log.info("Creating per imageName dictionaries")
        self.end_evt_timeout_task = {}
        self.metadata = {}
        self.HDR = {}
        self.filename_FITS = {}
        self.filename_HDR = {}
        self.completed_OK = {}

    def collect(self, keys):
        """ Collect meta-data from the telemetry-connected channels
        and store it in the 'metadata' dictionary"""

        # Define myData and metadata dictionaries
        # myData: holds the payload from Telem/Events
        # metadata: holds the metadata to be inserted into the Header object
        myData = {}
        metadata = {}
        for keyword in keys:
            name = get_channel_name(self.config.telemetry[keyword])
            # Access data payload only once
            if name not in myData:
                myData[name] = self.Remote_get[name]()
                self.log.info(f"Checking expiration for {name}")
                if self.check_telemetry_expired(myData[name]):
                    self.log.warning(f"Expired telemetry for {name} -- will ignore")
                    myData[name] = None
            # Only update metadata if myData is defined (not None)
            if myData[name] is None:
                self.log.warning(f"Cannot get keyword: {keyword} from topic: {name}")
            else:
                try:
                    metadata[keyword] = self.extract_from_myData(keyword, myData[name])
                    self.log.debug(f"Extracted {keyword}: {metadata[keyword]}")
                    # Scale by `scale` if it was defined
                    if 'scale' in self.config.telemetry[keyword]:
                        metadata[keyword] = metadata[keyword]*self.config.telemetry[keyword]['scale']
                        self.log.info(f"Scaled key: {keyword} by: {self.config.telemetry[keyword]['scale']}")
                except Exception as err:
                    self.log.error(f"Error while extracting keyword: {keyword} from topic: {name}")
                    self.log.error(f"{err.__class__.__name__}: {err}")

        return metadata

    def check_telemetry_expired(self, myData):
        """ Check is telemetry has expired using expiresAt parameter"""
        has_expired = False
        # Check if it has the expiresAt attribute
        if hasattr(myData, 'expiresAt'):
            expiresAt = getattr(myData, 'expiresAt')
            self.log.info(f"Found expiresAt: {expiresAt} in payload")
            timestamp_now = time.time()  # unix UTC time
            if timestamp_now > expiresAt:
                has_expired = True
        return has_expired

    def extract_from_myData(self, keyword, myData, sep=":"):

        param = self.config.telemetry[keyword]['value']
        payload = getattr(myData, param)

        # Case 1 -- we want just one value per key (scalar)
        if 'array' not in self.config.telemetry[keyword]:
            self.log.debug(f"{keyword} is a scalar")
            extracted_payload = payload
        # Case 2 -- array of values per sensor
        elif self.config.telemetry[keyword]['array'] == 'CCD_array':
            self.log.debug(f"{keyword} is an array: CCD_array")
            ccdnames = self.get_array_keys(keyword, myData, sep)
            # When ATCamera sends (via SAL/DDS) and array with just one element
            # this is actually not send as a list/array, but as scalar instead.
            # Therefore, if expecting and list/array and length is (1), then we
            # need to recast SAL payload as a list.
            if len(ccdnames) == 1 and not isinstance(payload, list):
                payload = [payload]
                self.log.warning(f"Recasting payload to a list for {keyword}:{payload}")
            extracted_payload = dict(zip(ccdnames, payload))
        elif self.config.telemetry[keyword]['array'] == 'CCD_array_str':
            self.log.debug(f"{keyword} is string array: CCD_array_str")
            ccdnames = self.get_array_keys(keyword, myData, sep)
            # Split the payload into an array of strings
            extracted_payload = dict(zip(ccdnames, hutils.split_esc(payload, sep)))
        elif self.config.telemetry[keyword]['array'] == 'indexed_array':
            self.log.debug(f"{keyword} is an array: indexed_array")
            index = self.config.telemetry[keyword]['array_index']
            # Extract the requested index
            extracted_payload = payload[index]
        elif self.config.telemetry[keyword]['array'] == 'keyed_array':
            self.log.debug(f"{keyword} is an array: keyed_array")
            keywords = self.get_array_keys(keyword, myData, sep)
            key = self.config.telemetry[keyword]['array_keyname']
            # Extract only the requested key from the dictionary
            extracted_payload = dict(zip(keywords, hutils.split_esc(payload, sep)))[key]
        # Case 3 -- enumeration using xml libraries
        elif self.config.telemetry[keyword]['array'] == 'enum':
            device = self.config.telemetry[keyword]['device']
            array_name = self.config.telemetry[keyword]['array_name']
            extracted_payload = getattr(self.xml_lib[device], array_name)(payload).name
        # If some kind of array take first element
        elif hasattr(payload, "__len__") and not isinstance(payload, str):
            self.log.debug(f"{keyword} is just an array")
            extracted_payload = payload[0]
        else:
            self.log.debug(f"Undefined type for {keyword}")
            extracted_payload = None
        return extracted_payload

    def get_array_keys(self, keyword, myData, sep=":"):
        """
        Function to extract a list of keywords for the ':'-separated string
        published by Camera
        """
        array_keys = self.config.telemetry[keyword]['array_keys']
        payload = getattr(myData, array_keys)
        # Make sure we get back something
        if payload is None:
            self.log.warning(f"Cannot get list of keys for: {keyword}")
            keywords_list = None
        else:
            # we extract them using the separator (i.e. ':')
            keywords_list = hutils.split_esc(payload, sep)
            if len(keywords_list) <= 1:
                self.log.warning(f"List keys for {keyword} is <= 1")
            self.log.info(f"For {keyword}, extracted '{array_keys}': {keywords_list}")
        return keywords_list

    def collect_from_HeaderService(self, imageName):

        """
        Collect and update custom meta-data generated or transformed by
        the HeaderService
        """
        # Simplify code with shortcuts for imageName
        metadata = self.metadata[imageName]

        # Reformat and calculate dates based on different timeStamps
        # NOTE: For now the timestamp are coming in UTC from Camera and are
        # transformed to TAI by the function hscalc.get_date()
        # Store the creation date of the header file -- i.e. now!!
        DATE = hscalc.get_date(time.time())
        metadata['DATE'] = DATE.isot

        # Need to force MJD dates to floats for yaml header
        if 'DATE-OBS' in metadata:
            DATE_OBS = hscalc.get_date(metadata['DATE-OBS'])
            metadata['DATE-OBS'] = DATE_OBS.isot
            metadata['MJD-OBS'] = float(DATE_OBS.mjd)

        if 'DATE-BEG' in metadata:
            DATE_BEG = hscalc.get_date(metadata['DATE-BEG'])
            metadata['DATE-BEG'] = DATE_BEG.isot
            metadata['MJD-BEG'] = float(DATE_BEG.mjd)

        if 'DATE-END' in metadata:
            DATE_END = hscalc.get_date(metadata['DATE-END'])
            metadata['DATE-END'] = DATE_END.isot
            metadata['MJD-END'] = float(DATE_END.mjd)

        metadata['FILENAME'] = self.filename_FITS[imageName]
        if self.tstand:
            metadata['TSTAND'] = self.tstand

        # Update the imageName metadata with new dict
        self.metadata[imageName].update(metadata)

    def update_monitor_metadata(self, imageName, keywords):

        # Collect new metadata for keywords
        metadata = self.collect(keywords)
        # Apply rule for each keyword
        for keyword in keywords:

            latest_value = metadata[keyword]
            current_value = self.metadata[imageName][keyword]

            if 'array' not in self.config.telemetry[keyword]:
                self.log.info(f"keyword:{keyword} not an array")
                isenum = False
                pass
            # Check if keyword is enum type and transform back to numerator
            elif self.config.telemetry[keyword]['array'] == 'enum':
                isenum = True
                latest_value = metadata[keyword]
                current_value = self.metadata[imageName][keyword]
                device = self.config.telemetry[keyword]['device']
                array_name = self.config.telemetry[keyword]['array_name']
                device_lib = getattr(self.xml_lib[device], array_name)
                latest_value = getattr(device_lib, latest_value).numerator
                current_value = getattr(device_lib, current_value).numerator
            else:
                msg = f"array: {self.config.telemetry[keyword]['array']} not supported for monitor"
                self.log.error(msg)
                raise ValueError(msg)

            # Make sure that we have a rule defined to extract for the keyword
            if 'rule' not in self.config.telemetry[keyword]:
                self.log.warning(f"rule not defined for {keyword} -- will use current value")
                updated_value = latest_value
            elif self.config.telemetry[keyword]['rule'] == 'latest':
                updated_value = latest_value
            elif self.config.telemetry[keyword]['rule'] == 'max':
                updated_value = max(current_value, latest_value)
            elif self.config.telemetry[keyword]['rule'] == 'min':
                updated_value = min(current_value, latest_value)
            else:
                msg = f"rule: {self.config.telemetry[keyword]['rule']} not supported"
                self.log.error(msg)
                raise ValueError(msg)

            # Translate to enun name string
            if isenum:
                updated_value = device_lib(updated_value).name
                current_value = device_lib(current_value).name

            self.metadata[imageName][keyword] = updated_value
            self.log.info(f"Monitor updated {keyword} value from {current_value} --> {updated_value}")

        return

# --- end of class ----


def get_channel_name(c):
    """ Standard formatting for the name of a channel across modules"""
    # Assume index=0 if not defined
    if 'device_index' not in c:
        c['device_index'] = 0
    return '{}_{}_{}'.format(c['device'], c['device_index'], c['topic'])


def get_channel_device(c):
    """ Standard formatting for the device name of a channel across modules"""
    if 'device_index' not in c:
        c['device_index'] = 0
    return c['device']


def get_channel_devname(c):
    """ Standard formatting for the 'devname' of a channel across modules"""
    if 'device_index' not in c:
        c['device_index'] = 0
    return "{}_{}".format(c['device'], c['device_index'])


def get_channel_topic(c):
    """ Standard formatting for the topic of a channel across modules"""
    return c['topic']


def collect_device_topics(c, device_topics):
    """
    Collect and update/augment topics for each device in the input
    device_topics dictionary
    """
    devname = get_channel_devname(c)
    topic = get_channel_topic(c)
    device_topics.setdefault(devname, [])
    if topic not in device_topics[devname]:
        device_topics[devname].append(topic)
    return device_topics


def get_enum_cscs(telem):
    """
    Get only the enumerated devices described
    in the telemetry section of the config
    """
    enum_cscs = []
    for key in telem:
        if 'array' in telem[key] and telem[key]['array'] == 'enum':
            if telem[key]['device'] not in enum_cscs:
                enum_cscs.append(telem[key]['device'])
    return enum_cscs


def get_collection_events(config):
    """
    Get the names of the collection event, where we want to collect telemetry
    """

    telem = config.telemetry
    collection_events = {}
    collection_events_names = []
    collection_events_keys = {}
    for key in telem:
        # Extract the string or dictionary that for 'collect_after_event'
        collect_after_event = telem[key]['collect_after_event']
        # Case 1: it is one of the fixed collection events, so we get the info
        # from the config section
        if isinstance(collect_after_event, str) and collect_after_event in FIXED_COLLECTION_EVENTS:
            collect_dict = getattr(config, collect_after_event)
        # Case 2: it is a custom collection event, defined as a dictionary in
        # the telemetry section of the config.
        elif isinstance(collect_after_event, dict):
            collect_dict = telem[key]['collect_after_event']
        else:
            msg = f"Wrong definition 'collect_after_event' for keyword:{key}"
            raise ValueError(msg)

        # Get the device and topic for collection event
        device = collect_dict['device']
        topic = collect_dict['topic']
        name = get_channel_name(collect_dict)
        # Append channel (i.e.: event name) if not in the list already
        if name not in collection_events_names:
            collection_events_names.append(name)
            collection_events[name] = {'device': device, 'topic': topic}
            collection_events_keys[name] = [key]
        else:
            collection_events_keys[name].append(key)

    return collection_events, collection_events_names, collection_events_keys


def get_monitor_channels(telem):
    """
    Get only events that we need to monitor
    in the telemetry section of the config
    """

    # The allowed rules for monitored telemetry
    valid_rules = frozenset(["min", "max", "latest"])

    monitor_event_channels_names = []
    monitor_event_channels_keys = {}
    monitor_event_channels = {}
    for key in telem:
        if 'monitor' in telem[key] and telem[key]['monitor'] is True:
            # Extract rule and set to default value if not defined
            if 'rule' not in telem[key]:
                rule = 'latest'
            else:
                rule = telem[key]['rule']

            if rule not in valid_rules:
                msg = f"Wrong rule definition:{rule} for keyword:{key}"
                raise ValueError(msg)

            channel_name = get_channel_name(telem[key])
            if channel_name not in monitor_event_channels_names:
                monitor_event_channels_names.append(channel_name)
                monitor_event_channels[channel_name] = telem[key]
                monitor_event_channels_keys[channel_name] = [key]
            else:
                monitor_event_channels_keys[channel_name].append(key)
    return monitor_event_channels, monitor_event_channels_names, monitor_event_channels_keys


def extract_telemetry_channels(telem, start_collection_event=None,
                               end_collection_event=None,
                               imageParam_event=None,
                               cameraConf_event=None):
    """
    Get the unique telemetry channels from telemetry dictionary to
    define the topics that we need to subscribe to
    """
    channels = {}
    device_topics = {}
    for key in telem:
        # Extract the string or dictionary that for 'collect_after_event'
        collect_after_event = telem[key]['collect_after_event']
        # Case 1: it is one of the fixed collection events
        print(f"{key} -- collect_after_event: {collect_after_event}")
        if isinstance(collect_after_event, str) and collect_after_event in FIXED_COLLECTION_EVENTS:
            collect_dict = None

        # Case 2: it is a custom collection event, defined as a dictionary in
        # the telemetry section of the config.
        elif isinstance(collect_after_event, dict):
            collect_dict = telem[key]['collect_after_event']
        else:
            msg = f"Wrong definition 'collect_after_event' for keyword:{key}"
            raise ValueError(msg)

        # Add array qualifier -- REVISE or replace by array
        if 'type' not in telem[key]:
            telem[key]['type'] = 'scalar'
        # Add default index=0 if undefined
        if 'device_index' not in telem[key]:
            telem[key]['device_index'] = 0
        name = get_channel_name(telem[key])
        # Make sure we don't create extra channels
        if name not in channels.keys():
            channels[name] = telem[key]
        # Store the topics for the device
        if collect_dict is not None:
            device_topics = collect_device_topics(collect_dict, device_topics)
        device_topics = collect_device_topics(telem[key], device_topics)

    # We also need to make sure that we subscribe to the start/end
    # collection Events in case these were not contained by the
    if start_collection_event:
        # Shortcut to variable c, note that when we update c
        # we also update the end_collection_event dictionary
        c = start_collection_event
        # Assume index=0 if not defined
        if 'device_index' not in c:
            c['device_index'] = 0
        name = get_channel_name(c)
        if name not in channels.keys():
            c['Stype'] = 'Event'
            channels[name] = c
        # Store the topics for the device
        device_topics = collect_device_topics(c, device_topics)

    if end_collection_event:
        # Shortcut to variable c, note that when we update c
        # we also update the end_collection_event dictionary
        c = end_collection_event
        # Assume index=0 if not defined
        if 'device_index' not in c:
            c['device_index'] = 0
        name = get_channel_name(c)
        if name not in list(channels.keys()):
            c['Stype'] = 'Event'
            channels[name] = c
        # Store the topics for the device
        device_topics = collect_device_topics(c, device_topics)

    # The imageParam event
    if imageParam_event:
        c = imageParam_event
        # Assume index=0 if not defined
        if 'device_index' not in c:
            c['device_index'] = 0
        name = get_channel_name(c)
        if name not in channels.keys():
            c['Stype'] = 'Event'
            channels[name] = c
        # Store the topics for the device
        device_topics = collect_device_topics(c, device_topics)

    # The cameraConf_event event
    if cameraConf_event:
        c = cameraConf_event
        # Assume index=0 if not defined
        if 'device_index' not in c:
            c['device_index'] = 0
        name = get_channel_name(c)
        if name not in channels.keys():
            c['Stype'] = 'Event'
            channels[name] = c
        # Store the topics for the device
        device_topics = collect_device_topics(c, device_topics)

    return channels, device_topics
