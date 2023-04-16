import paho.mqtt.client as mqtt
import pyapp.config as cfg
from sparkplug_b.enums import MessageTypes, DataTypes
from sparkplug_b import functions as spb
from sparkplug_b import spb_dataclasses
import pyapp.errors as errors
from pyapp import flexy_v1_0

import time

import datetime as dt

from logging import warning

td = dt.datetime.today

PAYLOAD_SEQUENCER = spb.Sequencer()
BD_SEQUENCER = spb.Sequencer(first=0, last=2**32)


def node_death_payload(bd_seq: int) -> bytes:
    # warning('bdSeq DEATH', bd_seq)
    payload = spb_dataclasses.Payload(
        timestamp=spb.millis(),
        seq=0,
        metrics=[
            spb_dataclasses.Metric(
                timestamp=spb.millis(),
                name='bdSeq',
                value=bd_seq,
                datatype=DataTypes.Int64
            )
        ]
    )
    return payload.serialize()


def node_birth_payload(bd_seq: int) -> bytes:
    # warning('bdSeq BIRTH', bd_seq)
    payload = spb_dataclasses.Payload(
        timestamp=spb.millis(),
        seq=PAYLOAD_SEQUENCER.reset()(),
        metrics=[
            spb_dataclasses.Metric(
                timestamp=spb.millis(),
                name='bdSeq',
                value=bd_seq,
                datatype=DataTypes.Int64
            ),
            spb_dataclasses.Metric(
                timestamp=spb.millis(),
                name='Node Control/Rebirth',
                value=False,
                datatype=DataTypes.Boolean
            )
        ]
    )
    return payload.serialize()


def on_spb_message(client, userdata, message):
    try:
        payload = spb_dataclasses.Payload.from_mqtt_payload(message.payload)
        if message.topic == cfg.SPARKPLUG_REBIRTH_TOPIC:
            if not payload.metrics:
                return
            for metric in payload.metrics:
                if metric.name == 'Node Control/Rebirth' and metric.boolean_value:
                    client.publish(cfg.SPARKPLUG_BIRTH_TOPIC, node_birth_payload(BD_SEQUENCER.current_value))
        elif message.topic.startswith(cfg.SPARKPLUG_DCMD_PREFIX):
            if not payload.metrics:
                return
            for metric in payload.metrics:
                if metric.name == 'Device Control/Rebirth' and metric.boolean_value:
                    device_id = message.topic.replace(cfg.SPARKPLUG_DCMD_PREFIX, '')
                    flexy_topic = f'flexy_v1.0/{device_id.replace(".", "/")}/CMD'
                    client.publish(flexy_topic, 'REBIRTH')
    except TypeError as err:
        warning(err)
    except Exception as err:
        warning(err)

def device_birth_metrics() -> list[spb_dataclasses.Metric]:
    return [
        spb_dataclasses.Metric(
            name='Device Control/Rebirth',
            timestamp=spb.millis(),
            value=False,
            datatype=DataTypes.Boolean
        )
    ]


def spb_dcmd(client, userdata, message):
    payload = spb_dataclasses.Payload.from_mqtt_payload(message.payload)
    raise NotImplementedError


def spb_ncmd(client, userdata, message):
    payload = spb_dataclasses.Payload.from_mqtt_payload(message.payload)
    raise NotImplementedError


def on_connect(client, userdata, flags, rc):
    warning('MQTT Connected')
    client.publish(cfg.SPARKPLUG_BIRTH_TOPIC, node_birth_payload(BD_SEQUENCER.current_value))

    client.will_set(topic=cfg.SPARKPLUG_DEATH_TOPIC,
                    payload=node_death_payload(BD_SEQUENCER()))
    warning(f'Published message: {cfg.SPARKPLUG_BIRTH_TOPIC}')

    client.subscribe(f'spBv1.0/{cfg.SPARKPLUG_GROUP_ID}/NCMD/{cfg.SPARKPLUG_EDGE_NODE_ID}')
    client.subscribe(f'spBv1.0/{cfg.SPARKPLUG_GROUP_ID}/DCMD/{cfg.SPARKPLUG_EDGE_NODE_ID}/+')

    client.subscribe('flexy_v1.0/+/+/+/BIRTH')
    client.subscribe('flexy_v1.0/+/+/+/DATA')
    client.subscribe('flexy_v1.0/+/+/+/STATE')


def on_message(client, userdata, message):
    if message.topic.startswith('spBv1.0/'):
        on_spb_message(client, userdata, message)
        return

    try:
        topic = flexy_v1_0.FlexyTopic.from_topic(message.topic)
    except errors.TopicDecodeError as err:
        warning(err)
        return

    if topic.message_type == flexy_v1_0.FlexyMessageTypes.STATE:
        if message.payload == b'ONLINE':
            client.publish(topic.base_topic + 'CMD', 'REBIRTH')
            return

    elif topic.message_type == flexy_v1_0.FlexyMessageTypes.CMD:
        # Ignore flexy commands
        return

    spb_topic, spb_metrics = flexy_v1_0.flexy_to_sparkplug(topic, message.payload)
    if spb_topic is None:
        return

    payload = dict(
        timestamp=int(time.time() * 1000),
        seq=PAYLOAD_SEQUENCER()
    )

    metrics = []

    if topic.message_type == flexy_v1_0.FlexyMessageTypes.BIRTH:
        metrics.extend(device_birth_metrics())

    if spb_metrics:
        metrics.extend([spb_dataclasses.Metric(**m) for m in spb_metrics])

    if metrics:
        payload['metrics'] = metrics

    payload = spb_dataclasses.Payload(**payload)
    client.publish(spb_topic, payload.serialize())
    warning(f'Published message: {spb_topic}')


# Container for each



def start_node():
    warning('NODE STARTING')
    client = mqtt.Client(client_id=cfg.MQTT_CLIENT_ID, protocol=mqtt.MQTTv311)

    client.on_connect = on_connect
    client.on_message = on_message

    client.username_pw_set(username=cfg.MQTT_USERNAME, password=cfg.MQTT_PASSWORD)

    if cfg.MQTT_USE_TLS:
        client.tls_set(cert_reqs=mqtt.ssl.CERT_REQUIRED)

    client.will_set(topic=cfg.SPARKPLUG_DEATH_TOPIC,
                    payload=node_death_payload(BD_SEQUENCER()))

    client.connect(host=cfg.MQTT_HOST, port=cfg.MQTT_PORT, keepalive=cfg.MQTT_KEEPALIVE)

    client.loop_forever()
