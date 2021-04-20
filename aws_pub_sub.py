
'''
python3 pubsub.py --endpoint <endpoint> --root-ca <file> --cert <file> --key <file>
'''
from app import app
import argparse
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import sys
import threading
import time
from uuid import uuid4

app.config['use_websocket'] = False
app.config['proxy_host'] = None
app.config['proxy_port'] = 8080
app.config['count'] = 10
app.config['signing_region'] = 'us-east-1'
app.config['endpoint'] = 'endpoint'
app.config['signing_region']= 'signing_region'
app.config['root_ca'] = '/Users/aishwaryaraghavan/Desktop/DATA_GIT_VENV_ALL/MST SID/AWS_Int/app/aws/AmazonRootCA1.pem' 
app.config['client_id'] = 'client_id' 
app.config['cert'] = 'cert'
app.config['pri_key_filepath'] = '/Users/aishwaryaraghavan/Desktop/DATA_GIT_VENV_ALL/MST SID/AWS_Int/app/aws/fd5096f9bf-private.pem.key'
app.config['topic'] = 'topic' 
app.config['verbosity'] = io.LogLevel.NoLogs.name


def parse_args():
    parser = argparse.ArgumentParser(description="Send and receive messages through and MQTT connection.")
    parser.add_argument('--endpoint', required=True, help="Your AWS IoT custom endpoint, not including a port. " +
                                                        "Ex: \"abcd123456wxyz-ats.iot.us-east-1.amazonaws.com\"")
    parser.add_argument('--cert', help="File path to your client certificate, in PEM format.")
    parser.add_argument('--key', help="File path to your private key, in PEM format.")
    parser.add_argument('--root-ca', help="File path to root certificate authority, in PEM format. " +
                                        "Necessary if MQTT server uses a certificate that's not already in " +
                                        "your trust store.")
    parser.add_argument('--client-id', default="test-" + str(uuid4()), help="Client ID for MQTT connection.")
    parser.add_argument('--topic', default="test/topic", help="Topic to subscribe to, and publish messages to.")
    parser.add_argument('--message', default="Hello World!", help="Message to publish. " +
                                                                "Specify empty string to publish nothing.")
    parser.add_argument('--count', default=10, type=int, help="Number of messages to publish/receive before exiting. " +
                                                            "Specify 0 to run forever.")
    parser.add_argument('--use-websocket', default=False, action='store_true',
        help="To use a websocket instead of raw mqtt. If you " +
        "specify this option you must specify a region for signing, you can also enable proxy mode.")
    parser.add_argument('--signing-region', default='us-east-1', help="If you specify --use-web-socket, this " +
        "is the region that will be used for computing the Sigv4 signature")
    parser.add_argument('--proxy-host', help="Hostname for proxy to connect to. Note: if you use this feature, " +
        "you will likely need to set --root-ca to the ca for your proxy.")
    parser.add_argument('--proxy-port', type=int, default=8080, help="Port for proxy to connect to.")
    parser.add_argument('--verbosity', choices=[x.name for x in io.LogLevel], default=io.LogLevel.NoLogs.name,
        help='Logging level')

    # Using globals to simplify sample code
    args = parser.parse_args()
    return args

def create_mqtt_connection():
    # Spin up resources
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
    
    if app.config['use_websocket'] == True:
        proxy_options = None
        if (app.config['proxy_host']):
            proxy_options = http.HttpProxyOptions(host_name=app.config['proxy_host'], port=app.config['proxy_port'])

        credentials_provider = auth.AwsCredentialsProvider.new_default_chain(client_bootstrap)
        mqtt_connection = mqtt_connection_builder.websockets_with_default_aws_signing(
            endpoint=app.config['endpoint'],
            client_bootstrap=client_bootstrap,
            region=app.config['signing_region'],
            credentials_provider=credentials_provider,
            websocket_proxy_options=proxy_options,
            ca_filepath=app.config['root_ca'],
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=app.config['client_id'],
            clean_session=False,
            keep_alive_secs=6)

    else:
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=app.config['endpoint'],
            cert_filepath=app.config['cert'],
            pri_key_filepath=app.config['pri_key_filepath'],
            client_bootstrap=client_bootstrap,
            ca_filepath=app.config['root_ca'],
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=app.config['client_id'],
            clean_session=False,
            keep_alive_secs=6)
    return mqtt_connection

io.init_logging(getattr(io.LogLevel, app.config['verbosity']), 'stderr')

received_count = 0
received_all_event = threading.Event()

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {}".format(resubscribe_results))

        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))


# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))
    global received_count
    received_count += 1
    if received_count == app.config['count']:
        received_all_event.set()

def publish_event(mqtt_connection, message):
    mqtt_connection.publish(
            topic= app.config['topic'],
            payload=message,
            qos=mqtt.QoS.AT_LEAST_ONCE)
    pass

def subscribe_event():
    # Subscribe
    print("Subscribing to topic '{}'...".format(app.config['topic']))
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=app.config['topic'],
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received)

    subscribe_result = subscribe_future.result()
    print("Subscribed with {}".format(str(subscribe_result['qos'])))

    if app.config['count'] != 0 and not received_all_event.is_set():
        print("Waiting for all messages to be received...")

    received_all_event.wait()
    print("{} message(s) received.".format(received_count))

def connect(mqtt_connection):
    print("Connecting to {} with client ID '{}'...".format(
        app.config['endpoint'], app.config['client_id']))

    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

def disconnect(mqtt_connection):
    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")

if __name__ == '__main__':
    # args = parse_args()
    mqtt_connection = create_mqtt_connection()
    connect(mqtt_connection)
    messaage = 'test'
    publish_event(mqtt_connection, message)
    disconnect(mqtt_connection)






    # # Publish message to server desired number of times.
    # # This step is skipped if message is blank.
    # # This step loops forever if count was set to 0.
    # if args.message:
    #     if args.count == 0:
    #         print ("Sending messages until program killed")
    #     else:
    #         print ("Sending {} message(s)".format(args.count))

    #     publish_count = 1
    #     while (publish_count <= args.count) or (args.count == 0):
    #         message = "{} [{}]".format(args.message, publish_count)
    #         print("Publishing message to topic '{}': {}".format(args.topic, message))
    #         mqtt_connection.publish(
    #             topic=args.topic,
    #             payload=message,
    #             qos=mqtt.QoS.AT_LEAST_ONCE)
    #         time.sleep(1)
    #         publish_count += 1

    # # Wait for all messages to be received.
    # # This waits forever if count was set to 0.
    # if args.count != 0 and not received_all_event.is_set():
    #     print("Waiting for all messages to be received...")

    # received_all_event.wait()
    # print("{} message(s) received.".format(received_count))

    # # Disconnect
    # print("Disconnecting...")
    # disconnect_future = mqtt_connection.disconnect()
    # disconnect_future.result()
    # print("Disconnected!")