#!./venv/bin/python3

# Copyright (C) 2020 OpenMotics BV
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either versio 3 of the
# License, or (at your option) any later versio.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import time
import logging
import json
import requests
from collections import deque


class SIDPersister(object):

    def __init__(self, path, logger):
        self.path = path
        self.logger = logger

    def get(self):
        try:
            with open(self.path, 'r') as f:
                sid = json.load(f)['sid']
                self.logger('Retrieving previously stored sessionid')
                return sid
        except Exception as ex:
            logging.exception(ex)
            return None

    def save(self, sid):
        try:
            with open(self.path, 'w') as f:
                json.dump({'sid':sid}, f)
            self.logger('Persisted new session id')
        except Exception as ex:
            logging.exception(ex)


class SMAWebConnect(object):
    """
    Reads out an SMA inverter using WebConnect
    """

    name = 'SMAWebConnect'
    version = '0.0.26'
    interfaces = [('config', '1.0'), ('metrics', '1.0')]

    config_description = [{'name': 'sample_rate',
                           'type': 'int',
                           'description': 'How frequent (every x seconds) to fetch the sensor data, Default: 30'},
                          {'name': 'devices',
                           'type': 'section',
                           'description': 'List of all SMA devices.',
                           'repeat': True,
                           'min': 1,
                           'content': [{'name': 'sma_inverter_ip',
                                        'type': 'str',
                                        'description': 'IP or hostname of the SMA inverter including the scheme (e.g. http:// or https://).'},
                                       {'name': 'password',
                                        'type': 'str',
                                        'description': 'The password of the `User` account'}]}]

    default_config = {}

    FIELD_MAPPING = {'6100_40263F00': {'name': 'grid_power',
                                       'description': 'Grid power',
                                       'unit': 'W', 'type': 'gauge',
                                       'factor': 1.0},
                     '6100_00465700': {'name': 'frequency',
                                       'description': 'Frequency',
                                       'unit': 'Hz', 'type': 'gauge',
                                       'factor': 100.0},
                     '6100_00464800': {'name': 'voltage_l1',
                                       'description': 'Voltage L1',
                                       'unit': 'V', 'type': 'gauge',
                                       'factor': 100.0},
                     '6100_00464900': {'name': 'voltage_l2',
                                       'description': 'Voltage L2',
                                       'unit': 'V', 'type': 'gauge',
                                       'factor': 100.0},
                     '6100_00464A00': {'name': 'voltage_l3',
                                       'description': 'Voltage L3',
                                       'unit': 'V', 'type': 'gauge',
                                       'factor': 100.0},
                     '6100_40465300': {'name': 'current_l1',
                                       'description': 'Current L1',
                                       'unit': 'A', 'type': 'gauge',
                                       'factor': 1000.0},
                     '6100_40465400': {'name': 'current_l2',
                                       'description': 'Current L2',
                                       'unit': 'A', 'type': 'gauge',
                                       'factor': 1000.0},
                     '6100_40465500': {'name': 'current_l3',
                                       'description': 'Current L3',
                                       'unit': 'A', 'type': 'gauge',
                                       'factor': 1000.0},
                     '6100_0046C200': {'name': 'pv_power',
                                       'description': 'PV power',
                                       'unit': 'W', 'type': 'gauge',
                                       'factor': 1.0},
                     '6380_40451F00': {'name': 'pv_voltage',
                                       'description': 'PV voltage (average of all PV channels)',
                                       'unit': 'V', 'type': 'gauge',
                                       'factor': 100.0},
                     '6380_40452100': {'name': 'pv_current',
                                       'description': 'PV current (average of all PV channels)',
                                       'unit': 'A', 'type': 'gauge',
                                       'factor': 1000.0},
                     '6400_0046C300': {'name': 'pv_gen_meter',
                                       'description': 'PV generation meter',
                                       'unit': 'Wh', 'type': 'counter',
                                       'factor': 1.0},
                     '6400_00260100': {'name': 'total_yield',
                                       'description': 'Total yield',
                                       'unit': 'Wh', 'type': 'counter',
                                       'factor': 1.0},
                     '6400_00262200': {'name': 'daily_yield',
                                       'description': 'Daily yield',
                                       'unit': 'Wh', 'type': 'counter',
                                       'factor': 1.0},
                     '6100_40463600': {'name': 'grid_power_supplied',
                                       'description': 'Grid power supplied',
                                       'unit': 'W', 'type': 'gauge',
                                       'factor': 1.0},
                     '6100_40463700': {'name': 'grid_power_absorbed',
                                       'description': 'Grid power absorbed',
                                       'unit': 'W', 'type': 'gauge',
                                       'factor': 1.0},
                     '6400_00462400': {'name': 'grid_total_yield',
                                       'description': 'Grid total yield',
                                       'unit': 'Wh', 'type': 'counter',
                                       'factor': 1.0},
                     '6400_00462500': {'name': 'grid_total_absorbed',
                                       'description': 'Grid total absorbed',
                                       'unit': 'Wh', 'type': 'counter',
                                       'factor': 1.0},
                     '6100_00543100': {'name': 'current_consumption',
                                       'description': 'Current consumption',
                                       'unit': 'W', 'type': 'gauge',
                                       'factor': 1.0},
                     '6400_00543A00': {'name': 'total_consumption',
                                       'description': 'Total consumption',
                                       'unit': 'Wh', 'type': 'counter',
                                       'factor': 1.0}}

    metric_definitions = [{'type': 'sma',
                           'tags': ['device'],
                           'metrics': [{'name': 'online',
                                        'description': 'Indicates if the SMA device is operating',
                                        'type': 'gauge', 'unit': 'Boolean'}] +
                                      [{'name': entry['name'], 'description': entry['description'],
                                        'unit': entry['unit'], 'type': entry['type']}
                                       for entry in FIELD_MAPPING.values()]}]

    def __init__(self, device_ip, device_password, logger, sid_persister):
        self.logger = logger
        self._metrics_queue = deque()
        self._sample_rate = 30
        web_address = f'https://{device_ip}'
        self._sma_devices = [ {'sma_inverter_ip':web_address, 'password':device_password} ]
        self._sid_persister = sid_persister
        session_id = self._sid_persister.get()
        self._sma_sid = { }
        if session_id:
            self._sma_sid = { web_address:session_id }

        # Disable HTTPS warnings becasue of self-signed HTTPS certificate on the SMA inverter
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    def _log_debug(self, message):
        self.logger(message)

    def _log_separator(self):
        self.logger('-'*40)

    def run(self):
        while True:
            for sma_device in self._sma_devices:
                try:
                    self._read_data(sma_device)
                except Exception as ex:
                    logging.exception(ex)
                    self.logger('Could not read SMA device values: {0}'.format(ex))
            self._log_separator()
            self.logger(f'{time.asctime()} - Sleeping for {self._sample_rate} seconds')
            time.sleep(self._sample_rate)

    def _read_data(self, sma_device):
        metrics_values = {}
        ip = sma_device['sma_inverter_ip']
        while True:
            sid = self._sma_sid.get(ip, '')
            endpoint = f'{ip}/dyn/getValues.json?sid={sid}'
            response = requests.post(endpoint,
                                     json={'destDev': [], 'keys': list(SMAWebConnect.FIELD_MAPPING.keys())},
                                     verify=False).json()
            if response.get('err') == 401:
                self._login(sma_device)
                continue
            break
        if 'result' not in response or len(response['result']) != 1:
            raise RuntimeError('Unexpected response: {0}'.format(response))
        serial = list(response['result'].keys())[0]
        data = response['result'][serial]
        if data is None:
            raise RuntimeError('Unexpected response: {0}'.format(response))
        self._log_separator()
        self._log_debug(f'{time.asctime()}')
        for key, info in SMAWebConnect.FIELD_MAPPING.items():
            name = info['name']
            description = info['description']
            unit = info['unit']
            if key in data:
                values = self._extract_values(key, data[key], info['factor'])
                if len(values) == 0:
                    self._log_debug('* {0}: No values'.format(description))
                elif len(values) == 1:
                    value = values[0]
                    self._log_debug('* {0}: {1}{2}'.format(description, value, unit if value is not None else ''))
                    if value is not None:
                        metrics_values[name] = value
                else:
                    self._log_debug('* {0}:'.format(name))
                    for value in values:
                        self._log_debug('    {0}{1}'.format(description, unit if value is not None else ''))
                    values = [value for value in values
                              if value is not None]
                    if len(values) == 1:
                        metrics_values[name] = values[0]
                    elif len(values) > 1:
                        metrics_values[name] = sum(values) / len(values)
            else:
                self._log_debug(f'* Missing key: {key} - {description}')
        for key in data:
            if key not in SMAWebConnect.FIELD_MAPPING.keys():
                self._log_debug('* Unknown key {0}: {1}'.format(key, data[key]))
        offline = 'frequency' not in metrics_values or metrics_values['frequency'] is None
        metrics_values['online'] = not offline
        self._enqueue_metrics(serial, metrics_values)

    def _extract_values(self, key, values, factor):
        if len(values) != 1 or '1' not in values:
            self.logger('* Unexpected structure for {0}: {1}'.format(key, values))
            return []
        values = values['1']
        if len(values) == 0:
            return []
        if len(values) == 1:
            return [self._clean_value(key, values[0], factor)]
        return_data = []
        for raw_value in values:
            value = self._clean_value(key, raw_value, factor)
            if value is not None:
                return_data.append(value)
        return return_data

    def _clean_value(self, key, value_container, factor):
        if 'val' not in value_container:
            self.logger('* Unexpected structure for {0}: {1}'.format(key, value_container))
            return None
        value = value_container['val']
        if value is None:
            return None
        return float(value) / factor

    def _login(self, sma_device):
        self.logger('Doing new login')
        ip = sma_device['sma_inverter_ip']
        endpoint = '{0}/dyn/login.json'.format(ip)
        response = requests.post(endpoint,
                                 json={'right': 'usr',
                                       'pass': sma_device['password']},
                                 verify=False).json()
        if 'result' in response and 'sid' in response['result']:
            sid = response['result']['sid']
            self._sma_sid[ip] = sid
            self._sid_persister.save(sid)
        else:
            error_code = response.get('err', 'unknown')
            if error_code == 503:
                raise RuntimeError('Maximum amount of sessions')
            raise RuntimeError('Could not login: {0}'.format(error_code))

    def _enqueue_metrics(self, device_id, values):
        try:
            now = time.time()
            self._metrics_queue.appendleft({'type': 'sma',
                                            'timestamp': now,
                                            'tags': {'device': device_id},
                                            'values': values})
        except Exception as ex:
            self.logger('Got unexpected error while enqueueing metrics: {0}'.format(ex))

    def collect_metrics(self):
        try:
            while True:
                yield self._metrics_queue.pop()
        except IndexError:
            pass


def create_logger():
    formatter = logging.Formatter('> %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logging.root.addHandler(ch)
    logging.root.setLevel(logging.INFO)
    return lambda msg: logging.info(msg)


if __name__ == '__main__':
    ip = '192.168.0.230'
    password = 'SMA123smak!!'
    logger = create_logger()
    sid_persister = SIDPersister('/tmp/sids.json', logger)
    plugin = SMAWebConnect(ip, password, logger, sid_persister)
    plugin.run()
