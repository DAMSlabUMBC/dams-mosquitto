#!/usr/bin/env python3
"""End-to-end routing test for multi-filter (|) message purposes.

A publisher registers an MP that carries two alternative filters joined by '|'
("quality/assurance|operations/forecast") for a topic, then publishes one data
message. Two subscribers are attached to that topic:

  subA  SP = "quality/assurance"    -> matches the FIRST  MP filter -> must receive
  subB  SP = "operations/forecast"  -> matches the SECOND MP filter -> must receive
  subC  SP = "vendor/maintenance"   -> matches NEITHER    MP filter -> must NOT receive

This exercises the §4 SP-vs-MP compatibility check (subs.c -> purpose_filter_mp_matches_sp)
under match-any semantics: a message reaches a subscription when any one of the MP's
'|'-separated filters equals one of the SP's filters.

Usage: the broker must already be running on HOST:PORT with allow_anonymous.
  python3 multi_filter_routing_test.py [host] [port]
"""
import sys
import time

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 18830

TOPIC = "sensors/temp"
REG_TOPIC = "$MP_REG"
MP = "quality/assurance|operations/forecast"   # multi-filter MP
PAYLOAD = b"hello"

received = {"subA": [], "subB": [], "subC": []}


def make_client(cid):
    return mqtt.Client(CallbackAPIVersion.VERSION2, client_id=cid, protocol=mqtt.MQTTv5)


def on_message_factory(name):
    def on_message(client, userdata, msg):
        received[name].append(msg.payload)
    return on_message


def sub_props(sp):
    p = Properties(PacketTypes.SUBSCRIBE)
    p.UserProperty = [("DAP-SP", sp)]
    return p


def pub_props(pairs):
    p = Properties(PacketTypes.PUBLISH)
    p.UserProperty = pairs
    return p


def main():
    subA = make_client("subA"); subA.on_message = on_message_factory("subA")
    subB = make_client("subB"); subB.on_message = on_message_factory("subB")
    subC = make_client("subC"); subC.on_message = on_message_factory("subC")
    subA.connect(HOST, PORT); subA.loop_start()
    subB.connect(HOST, PORT); subB.loop_start()
    subC.connect(HOST, PORT); subC.loop_start()
    time.sleep(0.3)

    subA.subscribe(TOPIC, qos=0, properties=sub_props("quality/assurance"))
    subB.subscribe(TOPIC, qos=0, properties=sub_props("operations/forecast"))
    subC.subscribe(TOPIC, qos=0, properties=sub_props("vendor/maintenance"))
    time.sleep(0.4)

    pub = make_client("pub1")
    pub.connect(HOST, PORT); pub.loop_start()
    time.sleep(0.2)

    # Register the multi-filter MP for TOPIC (consent required on every publish).
    pub.publish(REG_TOPIC, payload="", qos=0,
                properties=pub_props([("DAP-Allow", "1"), ("DAP-MP", f"{MP}:{TOPIC}")]))
    time.sleep(0.4)

    # Publish the data message.
    pub.publish(TOPIC, payload=PAYLOAD, qos=0, properties=pub_props([("DAP-Allow", "1")]))
    time.sleep(0.6)

    for c in (subA, subB, subC, pub):
        c.loop_stop(); c.disconnect()

    ok_a = received["subA"] == [PAYLOAD]
    ok_b = received["subB"] == [PAYLOAD]
    ok_c = received["subC"] == []
    print(f"subA (SP=quality/assurance,   matches MP filter 1) received: {received['subA']}")
    print(f"subB (SP=operations/forecast, matches MP filter 2) received: {received['subB']}")
    print(f"subC (SP=vendor/maintenance,  matches no MP filter) received: {received['subC']}")

    if ok_a and ok_b and ok_c:
        print("ROUTING TEST PASSED: match-any delivery to each matching SP; no delivery to the non-matching SP.")
        return 0
    print("ROUTING TEST FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
