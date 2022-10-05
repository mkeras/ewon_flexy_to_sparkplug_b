from dataclasses import dataclass, field
from enum import Enum
from pyapp.errors import *
from pyapp import config as cfg
from sparkplug_b import spb_dataclasses as spb
from sparkplug_b import enums as spb_enums
import json
from logging import warning

class FlexyMessageTypes(Enum):
    CMD = 'CMD'
    STATE = 'STATE'
    BIRTH = 'BIRTH'
    DATA = 'DATA'


def str_to_int(floatstr: str):
    return int(floatstr.split('.')[0])


@dataclass(frozen=True)
class FlexyTopic:
    namespace: str = field(default='flexy_v1.0', init=False)

    __data: dict = field(default_factory=dict, init=False)

    group_id: str
    node_id: str
    device_id: str
    message_type: FlexyMessageTypes

    def __post_init__(self, *args, **kwargs):
        self.__validate()
        self.__data['topic_string'] = '/'.join([self.namespace,
                                                self.group_id,
                                                self.node_id,
                                                self.device_id,
                                                self.message_type.value])
        self.__data['base_topic'] = '/'.join([self.namespace,
                                              self.group_id,
                                              self.node_id,
                                              self.device_id]) + '/'

    def __validate(self):
        if self.message_type not in list(FlexyMessageTypes):
            raise IllegalTopicError(f'Message type is invalid')
        for name in ['group_id', 'node_id', 'device_id']:
            component = getattr(self, name)
            if not component:
                raise IllegalTopicError(f'"{name}" may not be null')
            elif not self.validate_topic_str(component):
                raise IllegalTopicError(f'{name} contains illegal character(s)')

    @staticmethod
    def validate_topic_str(string: str) -> bool:
        invalid_chars = '/+#.'
        for char in invalid_chars:
            if char in string:
                return False
        return True

    @classmethod
    def from_topic(cls, topic: str):
        topic_split = topic.split('/')
        if topic_split[0] != cls.namespace:
            raise TopicDecodeError(f'Topic has incorrect namespace of "{topic_split[0]}"')
        if len(topic_split) != 5:
            raise TopicDecodeError(f'Topic has invalid length of {len(topic_split)}')

        if topic_split[4] in [e.name for e in FlexyMessageTypes]:
            topic_split[4] = FlexyMessageTypes[topic_split[4]]
        elif topic_split[4] in [e.value for e in FlexyMessageTypes]:
            topic_split[4] = FlexyMessageTypes(topic_split[4])
        try:
            topic_obj = cls(*topic_split[1:])
            return topic_obj
        except IllegalTopicError as err:
            raise TopicDecodeError(err)

    @property
    def topic_string(self):
        return self.__data['topic_string']

    @property
    def base_topic(self):
        return self.__data['base_topic']

    def __repr__(self):
        return self.topic_string


class FlexyDevice:
    saved_devices = {}

    def __init__(self, topic: FlexyTopic):
        if topic.topic_string in self.saved_devices.keys():
            raise DeviceAlreadySavedError(f'Device with topic "{topic.topic_string}" already saved')
        self.base_topic = topic.base_topic
        self.saved_devices[self.base_topic] = self
        self.__metrics_meta = {}
        self.__datatypes_map = dict(
            string=(str, spb_enums.DataTypes.String),
            float=(float, spb_enums.DataTypes.Float),
            integer=(str_to_int, spb_enums.DataTypes.Int32))

    @classmethod
    def get_device(cls, topic: FlexyTopic):
        if topic.base_topic in cls.saved_devices.keys():
            return cls.saved_devices[topic.base_topic]
        return cls(topic)

    @staticmethod
    def read_payload(payload: bytes):
        payload = payload.decode()
        try:
            payload = json.loads(payload)
        except json.decoder.JSONDecodeError as err:
            pass
        return payload

    @staticmethod
    def get_alias(alias: str):
        return int(alias)

    def set_metrics_meta(self, birth_payload: dict):
        self.__metrics_meta = {}
        for metric_data in birth_payload['m']:
            datatype, spb_type = self.__datatypes_map.get(metric_data['t'], (str, spb_enums.DataTypes.String))
            self.__metrics_meta[metric_data['a']] = dict(
                alias=self.get_alias(metric_data['a']),
                name=metric_data['n'],
                type=datatype,
                spb_type=spb_type
            )

    def translate_metrics(self, data_payload: dict, birth: bool = False) -> list[dict]:
        metrics_data = []
        timestamp = int(data_payload['t']*1000)
        for metric_data in data_payload['m']:
            metric_meta = self.__metrics_meta.get(metric_data['a'], None)
            if metric_meta is None:
                warning('BUG: metric_meta NOT FOUND')
                continue

            metrics_data.append(dict(
                timestamp=timestamp,
                alias=metric_meta['alias'],
                value=metric_meta['type'](metric_data['v']),
                datatype=metric_meta['spb_type']
            ))
            if birth:
                metrics_data[-1]['name'] = metric_meta['name']

        return metrics_data


def flexy_to_sparkplug(topic: FlexyTopic, payload: bytes):

    device = FlexyDevice.get_device(topic)
    payload = device.read_payload(payload)

    device_id = f'{topic.group_id}.{topic.node_id}.{topic.device_id}'

    if topic.message_type == FlexyMessageTypes.STATE:
        if payload == 'OFFLINE':
            spb_topic = f'spBv1.0/{cfg.SPARKPLUG_GROUP_ID}/DDEATH/{cfg.SPARKPLUG_EDGE_NODE_ID}/{device_id}'
            return spb_topic, None
        return None, None

    spb_message_type = spb_enums.MessageTypes.DBIRTH if topic.message_type == FlexyMessageTypes.BIRTH else spb_enums.MessageTypes.DDATA
    birth = spb_message_type == spb_enums.MessageTypes.DBIRTH

    if birth:
        device.set_metrics_meta(payload)

    spb_topic = f'spBv1.0/{cfg.SPARKPLUG_GROUP_ID}/{spb_message_type.value}/{cfg.SPARKPLUG_EDGE_NODE_ID}/{device_id}'
    metrics = device.translate_metrics(payload, birth=birth)

    return spb_topic, metrics
