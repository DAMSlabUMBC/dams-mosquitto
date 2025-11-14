# MQTT-DAP-Mosquitto

A modified implementation of Eclipse Mosquitto that implements **MQTT-DAP (MQTT for Data Protection)**, a framework extending the MQTT protocol to facilitate the protection of sensitive data in IoT systems.

## About MQTT-DAP

MQTT-DAP extends MQTT v5 to provide protocol-level data protection mechanisms, including:

- **Purpose-Based Access Control (PBAC)**: Ensures that sensitive data is only delivered to subscribers with explicitly permitted purposes for processing that data
- **Data Linkability**: Tracks data provenance and enables auditing of data flows through the system
- **Data Protection Operations**: Supports standardized operations for data access requests, corrections, deletions, and purpose updates
- **GDPR Compliance**: Facilitates compliance with privacy regulations like GDPR through structured handling of data subject requests

### Key Features

- Purpose-based message filtering to prevent unauthorized data access
- Support for hierarchical purpose definitions (e.g., "billing/electricity")
- Protocol-level enforcement of explicit consent for data processing
- Standardized request/response patterns for data protection operations
- Backward compatibility with standard MQTT topics and payloads
- Designed for resource-constrained IoT environments

### Use Cases

MQTT-DAP is designed for IoT deployments in privacy-sensitive domains:

- **Industrial IoT**: Protecting proprietary manufacturing data while allowing selective sharing with suppliers and partners
- **Healthcare**: Ensuring patient data is only accessed for legitimate medical purposes
- **Smart Cities**: Managing citizen data with transparent purpose tracking and consent management

## How MQTT-DAP Works

MQTT-DAP uses MQTT v5 user properties to embed data protection metadata in control packets:

1. **Purpose Filters**: Publishers specify allowed purposes using the `DAP-MP` (Message Purpose) property; subscribers declare their intended purposes using `DAP-SP` (Subscription Purpose)
2. **Purpose Matching**: The broker ensures messages are only delivered to subscribers whose stated purposes are explicitly permitted by the publisher
3. **Data Tracking**: The `DAP-ClientID` property links each message to its publisher, enabling auditing and data subject requests
4. **Explicit Consent**: The `DAP-Allow` property serves as an unambiguous indicator that the publisher consents to data collection

### Example

```bash
# Subscriber registers interest in temperature data for billing purposes
mosquitto_sub -t 'sensors/temperature' -D subscribe user-property DAP-SP "billing/electricity:sensors/temperature"

# Publisher sends temperature data allowed for billing purposes
mosquitto_pub -t 'sensors/temperature' -m '22.5' \
  -D publish user-property DAP-MP "billing/electricity" \
  -D publish user-property DAP-ClientID "sensor-01" \
  -D publish user-property DAP-Allow "1"
```

## Original Mosquitto Implementation

This implementation is based on Eclipse Mosquitto, an open source implementation of a server for version 5.0, 3.1.1,
and 3.1 of the MQTT protocol. It also includes a C and C++ client library, and
the `mosquitto_pub` and `mosquitto_sub` utilities for publishing and
subscribing.

## Testing MQTT-DAP-Mosquitto

For comprehensive testing and evaluation of MQTT-DAP-Mosquitto, use the [MQTT-DAP Benchmark](https://anonymous.4open.science/r/Submission-1074-MQTT-DAP-Benchmark/) framework.

The benchmark provides:
- Automated testing across all purpose management methods (PM0-PM4)
- Performance metrics (latency, throughput, CPU, memory)
- Correctness verification for purpose-based access control
- Operational request testing for GDPR compliance
- Realistic IoT workload simulation

**We strongly recommend using the benchmark framework to test MQTT-DAP-Mosquitto** rather than manual testing, as it ensures consistent and reproducible results.

## Links

### MQTT-DAP Resources

- Benchmark framework: [MQTT-DAP Benchmark](https://anonymous.4open.science/r/Submission-1074-MQTT-DAP-Benchmark/)

### MQTT Protocol Information

- Community page: <http://mqtt.org/>
- MQTT v5.0 standard: <https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html>

### Original Mosquitto Project

- Main homepage: <https://mosquitto.org/>
- Source code repository: <https://github.com/eclipse/mosquitto>
- Bug tracker: <https://github.com/eclipse/mosquitto/issues>

## Building and Installing

MQTT-DAP-Mosquitto must be built from source. See the [Building from source](#building-from-source) section below.

## Quick Start

After building, start the broker with a configuration file:

```bash
mosquitto -c /path/to/mosquitto.conf
```

### Manual Testing with MQTT-DAP User Properties

For quick manual testing, you can use `mosquitto_sub` and `mosquitto_pub`:

Subscribe with a purpose filter:
```bash
mosquitto_sub -t 'sensors/temperature' \
  -D subscribe user-property DAP-SP "billing/electricity:sensors/temperature" -v
```

Publish with purpose metadata:
```bash
mosquitto_pub -t 'sensors/temperature' -m '22.5' \
  -D publish user-property DAP-MP "billing/electricity" \
  -D publish user-property DAP-ClientID "sensor-01" \
  -D publish user-property DAP-Allow "1"
```

The subscriber will receive the message because their purpose (`billing/electricity`) matches the publisher's allowed purpose.

**For comprehensive testing**, use the [MQTT-DAP Benchmark](https://github.com/DAMSlabUMBC/Pub-Sub-Privacy) framework instead of manual testing.

**Note:** Configure appropriate authentication before deploying. Anonymous access should only be used for testing.

## Documentation

Documentation for the broker, clients and client library API can be found in
the man pages, which are available online at <https://mosquitto.org/man/>. There
are also pages with an introduction to the features of MQTT, the
`mosquitto_passwd` utility for dealing with username/passwords, and a
description of the configuration file options available for the broker.

Detailed client library API documentation can be found at <https://mosquitto.org/api/>

## Building from Source

Clone this repository:

```bash
git clone https://github.com/DAMSlabUMBC/dams-mosquitto.git
cd dams-mosquitto
```

### Option 1: Build with Docker (Recommended)

The simplest way to build and run MQTT-DAP-Mosquitto:

```bash
docker build -t mqtt-dap-mosquitto .
docker run -p 1883:1883 -p 9100:9100 mqtt-dap-mosquitto
```

This builds the broker with all dependencies and includes node_exporter for metrics collection on port 9100.

### Option 2: Build Locally

**Linux/Unix:**
```bash
make
sudo make install
```

**Windows and Mac:**
```bash
cmake .
make
```

**Note:** If building from git, documentation may not be included. Use `make binary` to skip man pages, or install `docbook-xsl` (Debian/Ubuntu) to build them.

### Build Dependencies

**MQTT-DAP-Mosquitto specific:**
* libargon2 (libargon2-dev on Debian/Ubuntu) - required for purpose management
* libsqlite3 (libsqlite3-dev on Debian/Ubuntu) - required for data storage

**Standard Mosquitto dependencies:**
* c-ares (libc-ares-dev) - only when compiled with `make WITH_SRV=yes`
* cJSON - required for dynsec plugin, broker control plugin, and client JSON output support
* libwebsockets (libwebsockets-dev) - enable with `make WITH_WEBSOCKETS=lws`
* openssl (libssl-dev) - disable with `make WITH_TLS=no`
* pthreads - for client library thread support
* uthash / utlist - bundled versions provided, disable with `make WITH_BUNDLED_DEPS=no`
* xsltproc and docbook-xsl - only needed when building from git sources, disable with `make WITH_DOCS=no`

Equivalent options for enabling/disabling features are available when using CMake.


## Credits

The original version of Mosquitto was written by Roger Light <roger@atchoo.org>
