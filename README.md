# ewon_flexy_to_sparkplug_b
Converts MQTT Messages from ewon flexy to sparkplug b and republishes them into broker.


the program.bas is the program used by the ewon flexy to publish ewon tags to an MQTT Broker.


The Docker image connects to the MQTT Broker, subscribes to the ewon flexy messages, converts them to sparkplug b topic and payload format, and republishes them. The Docker image is a sparkplug 'edge node', and the ewon flexys are sparkplug 'devices'.

This project is pre-alpha.
