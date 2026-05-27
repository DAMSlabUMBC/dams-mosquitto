#!/usr/bin/env python3
"""Regression test for the empty-topic PUBLISH SEGV in handle__publish.

Bug: under the DAP protection framework, handle__publish dereferenced
base_msg->data.topic (strcmp at handle_publish.c:308, and the sibling
strncmp/strlen calls in the other purpose-filter methods) BEFORE MQTT v5
topic-alias resolution ran. MQTT v5 permits a zero-length PUBLISH topic when a
Topic Alias is supplied, in which case the broker reads the topic as NULL until
it is resolved from the alias further down the function. An empty-topic PUBLISH
therefore crashed the broker with a NULL dereference (SIGSEGV / ASan SEGV on
address 0x0).

The MQTT-DAP benchmark triggered this constantly: its ConfigParser never loads
`reg_by_msg_reg_topic`, so GlobalDefs.REG_BY_MSG_REG_TOPIC stays "" and every
purpose registration is published to an empty topic.

This is an integration-level defect in the full packet path, so it is exercised
against a running broker rather than as a lib unit test. Build a broker with
AddressSanitizer, run it under the DAP framework, and point this script at it:

    # build-asan/ = cmake dir built with -O0 -g -fsanitize=address
    ASAN_OPTIONS=detect_leaks=0:abort_on_error=1:log_path=/tmp/asan \
      ./build-asan/src/mosquitto -c <framework-on .conf with `listener 1888`> &
    <benchmark venv>/bin/python test/dap/empty_topic_crash_repro.py

Before the fix: the broker aborts (ASan SEGV at handle_publish.c, exit 134).
After the fix: the broker logs "Empty PUBLISH topic with no valid topic alias
... rejecting", disconnects this client, and stays up. The script exits 0 only
if the broker is still reachable after the empty-topic publish.
"""
import sys
import time
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 1888


def publish_empty_topic():
    """Send the exact packet the benchmark sends for a purpose registration when
    the registration topic is empty: MQTT v5 PUBLISH, zero-length topic, no Topic
    Alias, DAP consent + MP user properties."""
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="empty_topic_pub",
                    protocol=mqtt.MQTTv5)
    c.connect(HOST, PORT, keepalive=60)
    c.loop_start()
    time.sleep(0.3)

    props = Properties(PacketTypes.PUBLISH)
    props.UserProperty = ("DAP-ClientID", "empty_topic_pub")
    props.UserProperty = ("DAP-MP", "p1:device")
    props.UserProperty = ("DAP-Allow", "1")
    c.publish("", payload=None, qos=0, properties=props)  # empty topic, no alias
    time.sleep(0.5)
    c.loop_stop()
    c.disconnect()


def broker_is_alive():
    """A fresh client can still connect == the broker survived the empty-topic publish."""
    probe = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="probe",
                        protocol=mqtt.MQTTv5)
    try:
        probe.connect(HOST, PORT, keepalive=60)
        probe.loop_start()
        time.sleep(0.3)
        probe.loop_stop()
        probe.disconnect()
        return True
    except Exception as e:  # noqa: BLE001
        print(f"probe failed to connect: {e}")
        return False


def main():
    publish_empty_topic()
    if broker_is_alive():
        print("empty_topic_crash_repro: PASS (broker survived empty-topic PUBLISH)")
        return 0
    print("empty_topic_crash_repro: FAIL (broker unreachable - likely crashed)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
