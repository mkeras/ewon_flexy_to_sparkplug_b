import paho.mqtt.client as mqtt
from pyapp.protobuf import sparkplug_b_pb2
from pyapp import config
from google.protobuf.json_format import ParseDict, MessageToDict
from enum import Enum
import json
import time

import logging
import datetime as dt

from secrets import token_urlsafe

td = dt.datetime.today

user = 'ignition_maker'
password = 'LP3yWvuHx5mwN6a'


def millis() -> int:
    return int(time.time() * 1000)


def str_to_int(string) -> int:
    return int(float(string))


def str_to_bool(string) -> bool:
    return bool(float(string))


class Sequencer:
    def __init__(self, minimum: int = 0, maximum: int = 255, increment: int = 1, start: int = None):
        self.__min = minimum
        self.__max = maximum
        self.__current = minimum if start is None else start
        self.__increment = increment

    def increment(self):
        new_val = self.__current + self.__increment
        if new_val > self.__max:
            new_val = self.__min
        self.__current = new_val

    def reset(self):
        self.__current = self.__min

    @property
    def current_value(self) -> int:
        return self.__current


bd_seq_start = None  # Todo make this a durable value that persists across script restarts
last_bdseq = None
bd_seq = Sequencer(start=bd_seq_start)
sequence = Sequencer()


class SparkplugNode:
    NAMESPACE = 'spBv1.0'

    def __init__(self, group_id: str, node_id: str):
        self.group_id = group_id
        self.node_id = node_id
        self.__node_topic = f'{self.NAMESPACE}/{group_id}/%%%/{node_id}'
        self.__birth_published = False

    @property
    def birth_published(self) -> bool:
        return self.__birth_published

    @property
    def current_timestamp(self):
        return millis()

    @property
    def nbirth_topic(self):
        return self.__node_topic.replace('/%%%/', '/NBIRTH/')

    @property
    def ndeath_topic(self):
        return self.__node_topic.replace('/%%%/', '/NDEATH/')

    @property
    def ndata_topic(self):
        return self.__node_topic.replace('/%%%/', '/NDATA/')

    @property
    def ncmd_topic(self):
        return self.__node_topic.replace('/%%%/', '/NCMD/')

    def dbirth_topic(self, device_id: str):
        return self.__node_topic.replace('/%%%/', '/DBIRTH/') + f'/{device_id}'

    def ddeath_topic(self, device_id: str):
        return self.__node_topic.replace('/%%%/', '/DDEATH/') + f'/{device_id}'

    def ddata_topic(self, device_id: str):
        return self.__node_topic.replace('/%%%/', '/DDATA/') + f'/{device_id}'

    def dcmd_topic(self, device_id: str):
        return self.__node_topic.replace('/%%%/', '/DCMD/') + f'/{device_id}'

    def on_disconnect(self):
        self.__birth_published = False

    def on_publish_birth(self):
        self.__birth_published = True

    @staticmethod
    def payload_dict_to_bytes(payload_data: dict) -> bytes:
        sp_payload = ParseDict(payload_data, sparkplug_b_pb2.Payload())
        return sp_payload.SerializeToString()

    def nbirth_payload(self, bdseq: int):
        payload_data = {
            'seq': 0,
            'timestamp': self.current_timestamp,
            'metrics': [
                {
                    'name': 'Node Control/Rebirth',
                    'timestamp': self.current_timestamp,
                    'datatype': 11,
                    'boolean_value': False
                },
                {
                    'name': 'bdSeq',
                    'timestamp': self.current_timestamp,
                    'datatype': 4,
                    'long_value': bdseq
                },
                {
                    'name': 'sample metric 1',
                    'timestamp': self.current_timestamp,
                    'datatype': 11,
                    'boolean_value': True
                }
            ]
        }
        return self.payload_dict_to_bytes(payload_data)

    def ndeath_payload(self, bdseq: int):
        payload_data = {
            'timestamp': self.current_timestamp,
            'metrics': [
                {
                    'name': 'bdSeq',
                    'timestamp': self.current_timestamp,
                    'datatype': 4,
                    'long_value': bdseq
                }
            ]
        }
        return self.payload_dict_to_bytes(payload_data)


class FlexyDataTypes(Enum):
    float = {'sparkplug_code': 10, 'cast_fn': float, 'key': 'double_value'}             # Sparkplug double type code
    integer = {'sparkplug_code': 4, 'cast_fn': str_to_int, 'key': 'long_value'}        # int64 sparkplug type code
    boolean = {'sparkplug_code': 11, 'cast_fn': str_to_bool, 'key': 'boolean_value'}   # boolean sparkplug type code
    string = {'sparkplug_code': 12, 'cast_fn': str, 'key': 'string_value'}             # string sparkplug type code

    @property
    def sparkplug_value_key(self) -> str:
        return self.value['key']

    @property
    def sparkplug_code(self) -> int:
        return self.value['sparkplug_code']

    def cast_value(self, value):
        return self.value['cast_fn'](value)


class FlexyTranslatorNode:
    def __init__(self, sparkplug_node: SparkplugNode):
        self.__flexy_devices = dict()
        self.__flexy_data_types = {e.name: e for e in FlexyDataTypes}
        self.__sparkplug_node = sparkplug_node
        self.__payload_queue = []

    @staticmethod
    def decode_flexy_payload(payload: bytes or str) -> dict or None:
        try:
            decoded = json.loads(payload)
            return decoded
        except json.JSONDecodeError as err:
            print(err)
        return None

    @staticmethod
    def flexy_metric_to_sparkplug_metric(metric: dict, birth: bool = False):
        pass

    @staticmethod
    def _trim_topic(topic: str):
        topic_split = topic.split('/')
        if not topic_split or len(topic_split) < 4:
            return None
        return '/'.join(topic_split[:4])

    def get_data_type(self, data_type: str) -> FlexyDataTypes:
        if data_type in self.__flexy_data_types:
            return self.__flexy_data_types[data_type]
        return FlexyDataTypes.string

    def get_flexy_device(self, topic: str):
        return self.__flexy_devices.get(self._trim_topic(topic))

    def get_flexy_device_by_id(self, device_id: str):
        for data in self.__flexy_devices.values():
            if data['device_id'] == device_id:
                return data

    def _make_sparkplug_payload(self, topic: str, seq: int, is_birth: bool = False) -> bytes or None:
        if not topic:
            return
        device_data = self.get_flexy_device(topic)
        if not device_data:
            return

        metrics = []
        if is_birth:
            metrics.append({
                'timestamp': self.__sparkplug_node.current_timestamp,
                'name': 'Device Control/Rebirth',
                'datatype': 11,
                'boolean_value': False
            })
        for metric_data in device_data['metrics'].values():
            metric_datatype: FlexyDataTypes = metric_data['datatype']
            if is_birth:
                metrics.append({
                    'timestamp': metric_data['timestamp'],
                    'name': metric_data['name'],
                    'alias': metric_data['alias'],
                    'datatype': metric_datatype.sparkplug_code,
                    metric_datatype.sparkplug_value_key: metric_datatype.cast_value(metric_data['value']),
                    'properties': {
                        'keys': [
                            'Quality',
                            'readOnly'
                        ],
                        'values': [
                            {
                                'type': 3,
                                'intValue': 192
                            },
                            {
                                'type': 11,
                                'booleanValue': True
                            }
                        ]
                    }
                })
                metric_data['value_changed'] = False
                continue
            if not metric_data['value_changed']:
                continue
            metrics.append({
                'timestamp': metric_data['timestamp'],
                'alias': metric_data['alias'],
                'datatype': metric_datatype.sparkplug_code,
                metric_datatype.sparkplug_value_key: metric_datatype.cast_value(metric_data['value'])
            })
            metric_data['value_changed'] = False

        payload = dict(
            metrics=metrics,
            timestamp=millis(),
            seq=seq
        )
        return self.__sparkplug_node.payload_dict_to_bytes(payload)

    def process_flexy_birth_message(self, client, userdata, message):
        payload_data = self.decode_flexy_payload(message.payload)
        if not payload_data:
            return

        flexy_topic = self._trim_topic(message.topic)
        _, client_id, iono2x_serial, flexy_serial = flexy_topic.split('/')

        if flexy_topic not in self.__flexy_devices.keys():
            dcmd_topic = self.__sparkplug_node.dcmd_topic(flexy_serial)
            print(f'Subscribe to topic: "{dcmd_topic}"')
            client.subscribe(dcmd_topic)

        device_dict = {}

        timestamp = payload_data['t'] * 1000
        for metric_dict in payload_data['m']:
            alias = metric_dict['a']
            device_dict[alias] = dict(
                datatype=self.get_data_type(metric_dict['t']),
                name=f'{client_id}/{iono2x_serial}/' + metric_dict['n'].replace('.', '/'),
                value_previous=None,
                value_changed=False,
                value=metric_dict['v'],
                timestamp=timestamp,
                alias=alias
            )

        self.__flexy_devices[flexy_topic] = dict(metrics=device_dict,
                                                 timestamp=timestamp,
                                                 device_id=flexy_serial,
                                                 flexy_topic=flexy_topic)

        self.publish_birth(client, bd_seq.current_value)

    def process_flexy_data_message(self, client, userdata, message):
        topic = self._trim_topic(message.topic)
        _, client_id, iono2x_serial, flexy_serial = topic.split('/')
        device_data = self.get_flexy_device(topic)
        if not device_data:
            print(f'DATA FROM UNCACHED DEVICE RECEIVED')
            client.publish(f'{topic}/CMD', b'REBIRTH')
            return

        payload_data = self.decode_flexy_payload(message.payload)
        if not payload_data:
            return

        timestamp = payload_data['t'] * 1000
        for data in payload_data['m']:
            if data['a'] not in device_data['metrics']:
                print(f'Unknown alias received, request rebirth')
                client.publish(f'{topic}/CMD', b'REBIRTH')
                return
            device_data["metrics"][data["a"]]['value_previous'] = device_data["metrics"][data["a"]]['value']
            device_data["metrics"][data["a"]]['value'] = data['v']
            device_data["metrics"][data["a"]]['value_changed'] = True
            device_data["metrics"][data["a"]]['timestamp'] = timestamp

        sequence.increment()
        payload = self._make_sparkplug_payload(topic=topic, is_birth=False, seq=sequence.current_value)

        client.publish(self.__sparkplug_node.ddata_topic(flexy_serial), payload)

        print(dt.datetime.fromtimestamp(timestamp / 1000), 'DDATA PUBLISHED, source: process_flexy_data_message()')

    def process_flexy_state_message(self, client, userdata, message):
        topic = self._trim_topic(message.topic)
        if message.payload == b'ONLINE':
            client.publish(f'{topic}/CMD', b'REBIRTH')
        elif message.payload == b'OFFLINE':
            topic = self._trim_topic(message.topic)
            _, client_id, iono2x_serial, flexy_serial = topic.split('/')
            device_data = self.get_flexy_device(topic)
            if not device_data:
                print(f'OFFLINE FROM UNCACHED DEVICE, IGNORE')
                return
            sequence.increment()
            ddeath_payload = {
                'timestamp': self.__sparkplug_node.current_timestamp,
                'seq': sequence.current_value
            }
            payload_bytes = self.__sparkplug_node.payload_dict_to_bytes(ddeath_payload)
            topic = self.__sparkplug_node.ddeath_topic(flexy_serial)
            client.publish(topic, payload_bytes)

    def process_dcmd_message(self, client, userdata, message):
        spb_payload = sparkplug_b_pb2.Payload()
        spb_payload.ParseFromString(message.payload)
        payload_data = MessageToDict(spb_payload)
        if 'metrics' not in payload_data.keys():
            return
        for metric_data in payload_data['metrics']:
            if metric_data['name'] == 'Device Control/Rebirth':
                break
        else:
            print('No rebirth command, skipping')
            return

        _, group_id, _, node_id, device_id = message.topic.split('/')
        device_data = self.get_flexy_device_by_id(device_id)
        if not device_data:
            sequence.increment()
            ddeath_payload = {
                'timestamp': self.__sparkplug_node.current_timestamp,
                'seq': sequence.current_value
            }
            payload_bytes = self.__sparkplug_node.payload_dict_to_bytes(ddeath_payload)
            client.publish(message.topic.replace('/DCMD/', '/DDEATH/'), payload_bytes)
            print(f'DEVICE WITH device_id "{device_id}" not found!')
            return
        client.publish(device_data['flexy_topic'] + '/CMD', b'REBIRTH')
        print('FLEXY REBIRTH REQUESTED', device_data['flexy_topic'])

    def process_ncmd_message(self, client, userdata, message):
        spb_payload = sparkplug_b_pb2.Payload()
        spb_payload.ParseFromString(message.payload)
        payload_data = MessageToDict(spb_payload)
        if 'metrics' not in payload_data.keys():
            return
        for metric_data in payload_data['metrics']:
            if metric_data['name'] == 'Node Control/Rebirth':
                break
        else:
            print('No rebirth command, skipping')
            return

        _, group_id, _, node_id = message.topic.split('/')
        self.publish_birth(client, bd_seq.current_value)

    @property
    def flexy_devices_count(self) -> int:
        return len(self.__flexy_devices)

    @property
    def flexy_device_ids(self):
        return [v['device_id'] for v in self.__flexy_devices.values()]

    def publish_birth(self, client, bdseq: int):
        client.publish(self.__sparkplug_node.nbirth_topic, self.__sparkplug_node.nbirth_payload(bdseq))
        sequence.reset()
        for i, (flexy_topic, data) in enumerate(self.__flexy_devices.items()):
            sequence.increment()
            seq = sequence.current_value
            dbirth_topic = self.__sparkplug_node.dbirth_topic(data['device_id'])
            dbirth_payload = self._make_sparkplug_payload(topic=flexy_topic, is_birth=True, seq=seq)
            client.publish(dbirth_topic, dbirth_payload)

        print(dt.datetime.fromtimestamp(self.__sparkplug_node.current_timestamp / 1000),
              'NBIRTH + DBIRTH(s) PUBLISHED, source: publish_birth()')



sparkplug_node = SparkplugNode(group_id=config.SPARKPLUG_GROUP_ID, node_id=config.SPARKPLUG_EDGE_NODE_ID)
flexy_node = FlexyTranslatorNode(sparkplug_node=sparkplug_node)


def publish_birth(client):
    # flexy_node.fill_birth_payload_queue()
    # flexy_node.publish_payload_queue(client)
    flexy_node.publish_birth(client, sparkplug_node.last_bdseq)
    sparkplug_node.on_publish_birth()


def on_connect(client, userdata, flags, rc):
    print('CLIENT CONNECTED, SUBSCRIBING TO TOPICS\n\n')
    client.subscribe('flexy_v1.0/#')
    client.subscribe(sparkplug_node.ncmd_topic)
    flexy_node.publish_birth(client, bd_seq.current_value)


def on_disconnect(client, userdata, flags, rc):
    print('ON DISCONNECT')
    bd_seq.increment()
    client.will_set(topic=sparkplug_node.ndeath_topic,
                    payload=sparkplug_node.ndeath_payload(bd_seq.current_value))
    sparkplug_node.on_disconnect()


def on_message(client, userdata, message):
    print('\n', dt.datetime.today(), '--->', message.topic)


mqtt_client = mqtt.Client(client_id=config.MQTT_CLIENT_ID, protocol=mqtt.MQTTv311)

mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect

mqtt_client.message_callback_add('flexy_v1.0/+/+/+/STATE', flexy_node.process_flexy_state_message)
mqtt_client.message_callback_add('flexy_v1.0/+/+/+/DATA', flexy_node.process_flexy_data_message)
mqtt_client.message_callback_add('flexy_v1.0/+/+/+/BIRTH', flexy_node.process_flexy_birth_message)
# mqtt_client.message_callback_add('flexy_v1.0/+/+/+/CMD', on_message)
mqtt_client.message_callback_add('spBv1.0/+/NCMD/+', flexy_node.process_ncmd_message)
mqtt_client.message_callback_add('spBv1.0/+/DCMD/+/+', flexy_node.process_dcmd_message)

# mqtt_client.on_message = on_message


def start():
    mqtt_client.username_pw_set(username=config.MQTT_USERNAME, password=config.MQTT_PASSWORD)

    if config.MQTT_USE_TLS:
        mqtt_client.tls_set(cert_reqs=mqtt.ssl.CERT_REQUIRED)


    mqtt_client.will_set(topic=sparkplug_node.ndeath_topic,
                        payload=sparkplug_node.ndeath_payload(bd_seq.current_value))


    mqtt_client.connect(host=config.MQTT_HOST, port=config.MQTT_PORT)
    mqtt_client.loop_forever()



