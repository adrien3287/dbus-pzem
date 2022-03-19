#!/usr/bin/python -u

import sys, os
import json
import logging
from itertools import groupby, count, izip_longest, izip
from argparse import ArgumentParser
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'ext', 'velib_python'))

from dbus.mainloop.glib import DBusGMainLoop
import dbus
import gobject
from vedbus import VeDbusService
from settingsdevice import SettingsDevice

from bridge import MqttGObjectBridge

VERSION = '0.1'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# We define these classes to avoid connection sharing to dbus. This is to allow
# more than one service to be held by a single python process.
class SystemBus(dbus.bus.BusConnection):
	def __new__(cls):
		return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SYSTEM)

class SessionBus(dbus.bus.BusConnection):
	def __new__(cls):
		return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SESSION)

def dbusconnection():
    return SessionBus() if 'DBUS_SESSION_BUS_ADDRESS' in os.environ else SystemBus()

class Meter(object):
    """ Represent a meter object on dbus. """

    def __init__(self, name, host, base, instance, cts):
        self.instance = instance
        self.cts = cts
        self.service = service = VeDbusService(
            "{}.smappee_{:02d}".format(base, instance), bus=dbusconnection())

        # Add objects required by ve-api
        service.add_path('/Management/ProcessName', __file__)
        service.add_path('/Management/ProcessVersion', VERSION)
        service.add_path('/Management/Connection', host)
        service.add_path('/DeviceInstance', instance)
        service.add_path('/ProductId', 0xFFFF) # 0xB012 ?
        service.add_path('/ProductName', "PZEM"
        service.add_path('/FirmwareVersion', None)
        service.add_path('/Serial', None)
        service.add_path('/Connected', 1)

        _kwh = lambda p, v: (str(v) + 'kWh')
        _a = lambda p, v: (str(v) + 'A')
        _w = lambda p, v: (str(v) + 'W')
        _v = lambda p, v: (str(v) + 'V')
	_hz = lambda p, v: (str(v) + 'Hz')

        service.add_path('/Ac/Energy/Forward', None, gettextcallback=_kwh)
        service.add_path('/Ac/Current', None, gettextcallback=_a)
        service.add_path('/Ac/Voltage', None, gettextcallback=_v)
        service.add_path('/Ac/Power', None, gettextcallback=_w)
        service.add_path('/Ac/Frequency', None, gettextcallback=_hz)

        # Provide debug info about what cts make up what meter
        service.add_path('/Debug/Cts', ','.join(str(c) for c in cts))

    def set_path(self, path, value):
        if self.service[path] != value:
            self.service[path] = value

    def update(self, voltages, powers):
        self.set_path('/Ac/Power', d['power']/10)
        self.set_path('/Ac/Energy/Forward', d['energy']/1000)
        self.set_path('/Ac/Current', d['current']/10)
        self.set_path('/Ac/Voltage', d['voltage']/10)
	self.set_path('/Ac/frequency', d['frequency']/10)
	
	
    def __repr__(self):
        return self.__class__.__name__ + "(" + str(self.cts) + ")"

    def __del__(self):
        self.service.__del__()

class Bridge(MqttGObjectBridge):
    def __init__(self, base, host, *args, **kwargs):
        super(Bridge, self).__init__(host, *args, **kwargs)
        self.base = base
        self.host = host
        self.meters = []

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except ValueError:
            logger.warning('Malformed payload received')
            return

    def _on_connect(self, client, userdata, di, rc):
        self._client.subscribe('pzem', 0)

def main():
    parser = ArgumentParser(description=sys.argv[0])
    parser.add_argument('--servicebase',
        help='Base service name on dbus, default is com.victronenergy',
        default='com.victronenergy.grid')
    parser.add_argument('host', help='MQTT Host')
    args = parser.parse_args()

    DBusGMainLoop(set_as_default=True)

    # MQTT connection
    bridge = Bridge(args.servicebase, args.host)

    mainloop = gobject.MainLoop()
    mainloop.run()

if __name__ == "__main__":
    main()
