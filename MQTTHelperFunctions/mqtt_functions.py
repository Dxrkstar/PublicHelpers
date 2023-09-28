import paho.mqtt.client as mqtt
import logging


def initialize_mqtt_client(client_id, broker_address, port):
    """
    Initialize an MQTT client instance.

    Args:
        client_id (str): The client's unique identifier.
        broker_address (str): The MQTT broker's address.
        port (int): The MQTT broker's port.

    Returns:
        mqtt.Client: The initialized MQTT client instance.
    """
    client = mqtt.Client(client_id)
    client.on_connect = on_connect
    client.on_message = on_message
    client.logger = logging.getLogger("MQTTClient")
    client.connected = False
    client.broker_address = broker_address
    client.port = port
    
    return client


def connect_mqtt_client(client):
    """
    Connect the MQTT client to the broker.

    Args:
        client (mqtt.Client): The MQTT client instance.
    """
    try:
        client.connect(client.broker_address, client.port)
        client.loop_start()
        client.logger.info("Connected to the MQTT Broker at %s:%d", client.broker_address, client.port)
        client.connected = True
    except Exception as e:
        client.logger.error("Failed to connect to the MQTT Broker: %s", str(e))


def disconnect_mqtt_client(client):
    """
    Disconnect the MQTT client from the broker.

    Args:
        client (mqtt.Client): The MQTT client instance.
    """
    if client.connected:
        client.disconnect()
        client.logger.info("Disconnected from the MQTT Broker")
        client.connected = False


def subscribe(client, topic, qos=0):
    """
    Subscribe to an MQTT topic.

    Args:
        client (mqtt.Client): The MQTT client instance.
        topic (str): The topic to subscribe to.
        qos (int): The quality of service level (0, 1, or 2).
    """
    if client.connected:
        client.subscribe(topic, qos)
        client.logger.info("Subscribed to topic: %s", topic)
    else:
        client.logger.error("Client is not connected. Cannot subscribe to topic: %s", topic)


def publish(client, topic, message, qos=0, retain=False):
    """
    Publish a message to an MQTT topic.

    Args:
        client (mqtt.Client): The MQTT client instance.
        topic (str): The topic to publish to.
        message (str): The message to publish.
        qos (int): The quality of service level (0, 1, or 2).
        retain (bool): Whether to retain the message on the broker.
    """
    if client.connected:
        client.publish(topic, message, qos=qos, retain=retain)
        client.logger.info("Published message to topic '%s': %s", topic, message)
    else:
        client.logger.error("Client is not connected. Cannot publish message to topic: %s", topic)


def on_connect(client, userdata, flags, rc):
    """
    Callback when the MQTT client successfully connects to the broker.

    Args:
        client (mqtt.Client): The MQTT client instance.
        userdata: The user data associated with the client (unused).
        flags: The connection flags (unused).
        rc (int): The result code indicating the success of the connection.
    """
    if rc == 0:
        client.logger.info("Connected to MQTT Broker with result code %d", rc)
    else:
        client.logger.error("Connection to MQTT Broker failed with result code %d", rc)


def on_message(client, userdata, message):
    """
    Callback for handling incoming MQTT messages.

    Args:
        client (mqtt.Client): The MQTT client instance.
        userdata: The user data associated with the client (unused).
        message (mqtt.MQTTMessage): The received MQTT message.
    """
    client.logger.info("Received message on topic '%s': %s", message.topic, message.payload.decode())
