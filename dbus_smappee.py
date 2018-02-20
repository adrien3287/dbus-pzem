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
        service.add_path('/ProductName', "Smappee - {}".format(name))
        service.add_path('/FirmwareVersion', None)
        service.add_path('/Serial', None)
        service.add_path('/Connected', 1)

        _kwh = lambda p, v: (str(v) + 'KWh')
        _a = lambda p, v: (str(v) + 'A')
        _w = lambda p, v: (str(v) + 'W')
        _v = lambda p, v: (str(v) + 'V')

        service.add_path('/Ac/Energy/Forward', None, gettextcallback=_kwh)
        service.add_path('/Ac/Energy/Reverse', None, gettextcallback=_kwh)
        service.add_path('/Ac/L1/Current', None, gettextcallback=_a)
        service.add_path('/Ac/L1/Energy/Forward', None, gettextcallback=_kwh)
        service.add_path('/Ac/L1/Energy/Reverse', None, gettextcallback=_kwh)
        service.add_path('/Ac/L1/Power', None, gettextcallback=_w)
        service.add_path('/Ac/L1/Voltage', None, gettextcallback=_v)
        service.add_path('/Ac/L2/Current', None, gettextcallback=_a)
        service.add_path('/Ac/L2/Energy/Forward', None, gettextcallback=_kwh)
        service.add_path('/Ac/L2/Energy/Reverse', None, gettextcallback=_kwh)
        service.add_path('/Ac/L2/Power', None, gettextcallback=_w)
        service.add_path('/Ac/L2/Voltage', None, gettextcallback=_v)
        service.add_path('/Ac/L3/Current', None, gettextcallback=_a)
        service.add_path('/Ac/L3/Energy/Forward', None, gettextcallback=_kwh)
        service.add_path('/Ac/L3/Energy/Reverse', None, gettextcallback=_kwh)
        service.add_path('/Ac/L3/Power', None, gettextcallback=_w)
        service.add_path('/Ac/L3/Voltage', None, gettextcallback=_v)
        service.add_path('/Ac/Power', None, gettextcallback=_w)

        # Provide debug info about what cts make up what meter
        service.add_path('/Debug/Cts', ','.join(str(c) for c in cts))

    def set_path(self, path, value):
        if self.service[path] != value:
            self.service[path] = value

    def update(self, voltages, powers):
        totalpower = totalforward = totalreverse = 0
        for phase, ct in izip(count(), self.cts):
            # Fill in the values
            d = powers[ct]
            line = '/Ac/L{}'.format(phase+1)
            self.set_path('{}/Current'.format(line), d['current'])
            self.set_path('{}/Energy/Forward'.format(line), round(d['importEnergy']/3600000, 1))
            self.set_path('{}/Energy/Reverse'.format(line), round(d['exportEnergy']/3600000, 1))
            self.set_path('{}/Power'.format(line), d['power'])
            self.set_path('{}/Voltage'.format(line), voltages.get(phase, None))

            totalpower += d['power']
            totalforward += d['importEnergy']
            totalreverse += d['exportEnergy']

        # Update the totals
        self.set_path('/Ac/Power', totalpower)
        self.set_path('/Ac/Energy/Forward', round(totalforward/3600000, 1))
        self.set_path('/Ac/Energy/Reverse', round(totalreverse/3600000, 1))

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

    def _allocate_meters(self, name, channels, n, base2, instance_offset):
        """ Allocates up to n meters from channels, attempting
            to make three-phase meters. """
        meters = []
        phases = {i: [] for i in range(3)}
        for phase, _channels in groupby(channels, lambda x: x['phase']):
            phases[phase].extend(_channels)

        spread = izip_longest(phases[0], phases[1], phases[2])
        for c, phasedata in izip(count(), spread):
            # stop when we have enough
            if n is not None and c >= n: break

            # Current sensors that makes up this meter
            ids = [x['ctInput'] for x in phasedata]
            meters.append(Meter(name, self.host,
                '{}.{}'.format(self.base, base2), c+instance_offset, ids))

        return meters

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except ValueError:
            logger.warning('Malformed payload received')
            return

        # Channel config.
        if msg.topic.endswith('/channelConfig'):
            # Construct meter objects
            channels = data['inputChannels']
            consumption_channels = [c for c in channels \
                if c['inputChannelType'] == 'CONSUMPTION']
            production_channels = [c for c in channels \
                if c['inputChannelType'] == 'PRODUCTION']

            # Use DeviceInstance values from 50 up.
            self.meters = self._allocate_meters('Consumption',
                consumption_channels, 1, "grid", 50)
            self.meters.extend(
                self._allocate_meters('Production',
                    production_channels, None, "pvinverter", 51))

            return

        if msg.topic.endswith('/realtime'):
            # Index the voltage by phase, and the powers by CT. Pass it
            # to each meter so the meter can pick its values from the
            # dictionary.
            voltages = dict((d['phaseId'], d['voltage']) for d in data['voltages'])
            powers = dict((d['ctInput'], d) for d in data['channelPowers'])
            for meter in self.meters:
                meter.update(voltages, powers)

            # Update the firmware and serial on each meter
            for meter in self.meters:
                meter.set_path('/FirmwareVersion', data.get('firmwareVersion'))
                meter.set_path('/Serial', data.get('serialNr'))

    def _on_connect(self, client, userdata, di, rc):
        self._client.subscribe('servicelocation/+/realtime', 0)
        self._client.subscribe('servicelocation/+/channelConfig', 0)

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
