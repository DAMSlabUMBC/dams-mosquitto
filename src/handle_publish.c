/*
Copyright (c) 2009-2021 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Roger Light - initial implementation and documentation.
*/

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mosquitto_broker_internal.h"
#include "alias_mosq.h"
#include "mosquitto/mqtt_protocol.h"
#include "packet_mosq.h"
#include "mp_registry.h" 
#include "ri_registry.h" 
#include "dr_registry.h" 
#include "rights_registry.h"
#include "rights_broker.h"
#include "dap_op_request.h"
#include "dap_deadline_tracker.h"
#include "dap_op_requester.h"
#include "dap_timestamp.h"
#include "dap_topics.h"
#include "property_common.h"
#include "property_mosq.h"
#include "read_handle.h"
#include "send_mosq.h"
#include "sys_tree.h"
#include "util_mosq.h"


/* Handle a subscriber's status notification for an operation (published to $OSYS with
 * DAP-Status set). The response is forwarded to the original requester's ONP - even
 * for an op that is no longer tracked, e.g. a late reply after the deadline - and a
 * terminal Success/Failure advances the deadline tracker. When the last expected
 * subscriber responds the broker settles the operation Successfully right away rather
 * than waiting for the deadline sweep. */
static void handle_dap_status_notification(struct mosquitto *context, const char *operation,
		uint64_t op_id_num, bool found_op_id_num, const char *op_status, const char *op_reason,
		const char *op_client_id, struct mosquitto__base_msg *stored,
		const char *correlation_data, uint16_t correlation_data_len)
{
	/* The responding subscriber is the connection that published the notification;
	 * trust context->id for tracker bookkeeping, relay the claimed id for display. */
	const char *responder = op_client_id ? op_client_id : context->id;

	const char *requester = NULL;
	if(db.dap_op_requester && found_op_id_num){
		requester = dap_op_requester_lookup(db.dap_op_requester, op_id_num);
	}
	if(requester){
		broker_forward_status_to_requester(requester, operation, op_id_num, op_status,
				op_reason, responder, stored->data.payload, stored->data.payloadlen,
				correlation_data, correlation_data_len);
	}

	/* Pending is relayed only; Success/Failure is a terminal response. */
	if(!found_op_id_num) return;
	if(strcmp(op_status, "Success") && strcmp(op_status, "Failure")) return;
	if(!db.dap_deadline_tracker) return;

	if(dap_deadline_tracker_mark_subscriber_responded(db.dap_deadline_tracker, op_id_num,
			context->id) != 0){
		return; /* untracked op or unexpected subscriber: relayed above, nothing to settle */
	}
	if(dap_deadline_tracker_all_responded(db.dap_deadline_tracker, op_id_num)){
		if(requester){
			broker_send_deadline_success(op_id_num, requester);
		}
		dap_deadline_tracker_remove(db.dap_deadline_tracker, op_id_num);
	}
}

int handle__publish(struct mosquitto *context)
{
	uint8_t dup;
	int rc = 0;
	int rc2;
	uint8_t header = context->in_packet.command;
	int res = 0;
	struct mosquitto__base_msg *base_msg, *stored = NULL;
	struct mosquitto__client_msg *cmsg_stored = NULL;
	size_t len;
	uint16_t slen;
	char *topic_mount;
	mosquitto_property *properties = NULL;
	uint32_t message_expiry_interval = MSG_EXPIRY_INFINITE;
	int topic_alias = -1;
	uint8_t reason_code = 0;
	uint16_t mid = 0;

	// For operations
	bool found_op = false;
	char *op_id = NULL;
	char *op_info = NULL;
	/* DAP-OpTFs / DAP-OpPFs / DAP-OpClients: the operation's comma-separated topic,
	 * purpose and client filter lists. */
	char *op_topic_filters = NULL;
	char *op_purpose_filters = NULL;
	char *op_client_filters = NULL;
	/* DAP-OpBefore / DAP-OpAfter: the operation's receipt-time bounds (decimal
	 * seconds; 0 = unbounded), used by the relevance query. */
	time_t op_before = 0;
	time_t op_after = 0;
	char *correlation_data = NULL;
	uint16_t correlation_data_len = 0;
	char *response_topic = NULL;

	/* Inbound operation status notification (subscriber -> broker). DAP-Status marks a
	 * publish on $OSYS as a notification rather than a request; DAP-OpId names the
	 * operation being responded to, DAP-Reason is optional, DAP-ClientID is the
	 * responding subscriber. */
	char *op_status = NULL;
	char *op_reason = NULL;
	char *op_client_id = NULL;
	uint64_t op_id_num = 0;
	bool found_op_id_num = false;

	if(context->state != mosq_cs_active){
		return MOSQ_ERR_PROTOCOL;
	}

	context->stats.messages_received++;

	base_msg = mosquitto_calloc(1, sizeof(struct mosquitto__base_msg));
	if(base_msg == NULL){
		return MOSQ_ERR_NOMEM;
	}

	/* Stamp the receipt time once, before any other DAP processing, so a single
	 * reference timestamp drives both queue ordering and pending-operation matchingn*/
	base_msg->dap_recv_time = time(NULL);

	dup = (header & 0x08)>>3;
	base_msg->data.qos = (header & 0x06)>>1;
	if(dup == 1 && base_msg->data.qos == 0){
		log__printf(NULL, MOSQ_LOG_INFO,
				"Invalid PUBLISH (QoS=0 and DUP=1) from %s, disconnecting.", context->id);
		db__msg_store_free(base_msg);
		return MOSQ_ERR_MALFORMED_PACKET;
	}
	if(base_msg->data.qos == 3){
		log__printf(NULL, MOSQ_LOG_INFO,
				"Invalid QoS in PUBLISH from %s, disconnecting.", context->id);
		db__msg_store_free(base_msg);
		return MOSQ_ERR_MALFORMED_PACKET;
	}
	if(base_msg->data.qos > context->max_qos){
		log__printf(NULL, MOSQ_LOG_INFO,
				"Too high QoS in PUBLISH from %s, disconnecting.", context->id);
		db__msg_store_free(base_msg);
		return MOSQ_ERR_QOS_NOT_SUPPORTED;
	}
	base_msg->data.retain = (header & 0x01);

	if(base_msg->data.retain && db.config->retain_available == false){
		db__msg_store_free(base_msg);
		return MOSQ_ERR_RETAIN_NOT_SUPPORTED;
	}

	if(packet__read_string(&context->in_packet, &base_msg->data.topic, &slen)){
		db__msg_store_free(base_msg);
		return MOSQ_ERR_MALFORMED_PACKET;
	}
	if(!slen && context->protocol != mosq_p_mqtt5){
		/* Invalid publish topic, disconnect client. */
		db__msg_store_free(base_msg);
		return MOSQ_ERR_MALFORMED_PACKET;
	}

	if(base_msg->data.qos > 0){
		if(packet__read_uint16(&context->in_packet, &mid)){
			db__msg_store_free(base_msg);
			return MOSQ_ERR_MALFORMED_PACKET;
		}
		if(mid == 0){
			db__msg_store_free(base_msg);
			return MOSQ_ERR_PROTOCOL;
		}
		/* It is important to have a separate copy of mid, because msg may be
		 * freed before we want to send a PUBACK/PUBREC. */
		base_msg->data.source_mid = mid;
	}

	/* Handle properties */
	if(context->protocol == mosq_p_mqtt5){
		rc = property__read_all(CMD_PUBLISH, &context->in_packet, &properties);
		if(rc){
			db__msg_store_free(base_msg);
			return rc;
		}

		/* MQTT v5 allows a zero-length PUBLISH topic when a Topic Alias is
			* supplied; the real topic is resolved from the alias. Upstream does that
			* resolution further down (after this block), but the DAP consent,
			* purpose-filtering and operation logic below all work on
			* base_msg->data.topic - so an as-yet-unresolved NULL topic here makes
			* strcmp()/strncmp() dereference NULL and crash the broker (the
			* handle_publish.c:308 SEGV). Resolve the alias now, or reject the publish
			* if the empty topic carries no usable alias, before any topic dereference. */
		if(base_msg->data.topic == NULL)
		{
			uint16_t resolved_alias = 0;
			const mosquitto_property *alias_prop = mosquitto_property_read_int16(
					properties, MQTT_PROP_TOPIC_ALIAS, &resolved_alias, false);
			if(alias_prop == NULL || resolved_alias == 0
					|| (context->listener && resolved_alias > context->listener->max_topic_alias))
			{
				log__printf(NULL, MOSQ_LOG_INFO,
					"Empty PUBLISH topic with no valid topic alias from %s, rejecting.",
					context->id);
				mosquitto_property_free_all(&properties);
				db__msg_store_free(base_msg);
				return MOSQ_ERR_TOPIC_ALIAS_INVALID;
			}
			if(alias__find_by_alias(context, ALIAS_DIR_R2L, resolved_alias, &base_msg->data.topic))
			{
				log__printf(NULL, MOSQ_LOG_INFO,
					"Unknown topic alias %u in PUBLISH from %s, rejecting.",
					resolved_alias, context->id);
				mosquitto_property_free_all(&properties);
				db__msg_store_free(base_msg);
				return MOSQ_ERR_PROTOCOL;
			}
		}

		/* Immediately check for consent and disallow if not given */
		const mosquitto_property *curr_prop_ptr = properties;
		bool consent_given = false;
		while(curr_prop_ptr)
		{
			/* Parse the current property name/value */
			char *name, *value;

			curr_prop_ptr = mosquitto_property_read_string_pair(curr_prop_ptr, MQTT_PROP_USER_PROPERTY, &name, &value, false );
			if(curr_prop_ptr)
			{
				/* Check if this is the consent property */
				if(!strcmp(name, MOSQ_DAP_CONSENT_KEY))
				{
					/* If value is "1", consent given, keep processing. Otherwise reject */
					if(!strcmp(value, "1"))
					{
						consent_given = true;
						break;
					}
					else
					{
						log__printf(NULL, MOSQ_LOG_INFO,
							"Consent not given for packet from %s, rejecting.",
							context->id);
						mosquitto_property_free_all(&properties);
						return MOSQ_ERR_MALFORMED_PACKET;
					}
				}
				/* Move to the next property */
				curr_prop_ptr = curr_prop_ptr->next;
			}
		}

		if(!consent_given)
		{
			log__printf(NULL, MOSQ_LOG_INFO,
				"Consent not given for packet from %s, rejecting.",
				context->id);
			mosquitto_property_free_all(&properties);
			return MOSQ_ERR_MALFORMED_PACKET;
		}

		/* Check if this is a registration message on the registration topic */
		if(!strcmp(base_msg->data.topic, MOSQ_DAP_MP_REG_TOPIC))
		{
			/* Since there can be multiple user properties, loop through them */
			const mosquitto_property *curr_prop_ptr = properties;
			while(curr_prop_ptr)
			{
				/* Parse the current property name/value */
				char *name, *value;
				curr_prop_ptr = mosquitto_property_read_string_pair(curr_prop_ptr, MQTT_PROP_USER_PROPERTY, &name, &value, false );
				if(curr_prop_ptr)
				{
					/* Check if this is a DAP-MP property */
					if(!strcmp(name, MOSQ_DAP_MP_KEY))
					{
						char *temp, *filter, *topic = NULL;

						/* In Registration by Message, MP is of the form '<MP>:<topic> */
						temp = strchr(value, ':');

						if (temp == NULL)
						{
							mosquitto_property_free_all(&properties);
							return MOSQ_ERR_MALFORMED_PACKET;
						}

						uint32_t index = (uint32_t)(temp - value);
						temp++; /* Skip the ':' */
						
						// Allocate memory for topic and purpose
						filter = mosquitto_malloc(index + 1);
						topic = mosquitto_malloc(strlen(temp) + 1);
						if(!filter || !topic)
						{
							mosquitto_property_free_all(&properties);
							return MOSQ_ERR_NOMEM;
						}

						// Copy
						filter = strncpy(filter, value, index);
						filter[index] = '\0';
						topic = strcpy(topic, temp);

						/* Register the topic to purpose filter mapping */
						mp__register_topic(context->id, topic, filter);
					}
					/* Move to the next property */
					curr_prop_ptr = curr_prop_ptr->next;
				}
			}

			/* Do not forward this registration message */
			mosquitto_property_free_all(&properties);
			return MOSQ_ERR_SUCCESS;
		}
		else
		{
			/* Normal data publish: the topic must have a registered MP. */
			struct mp_entry *stored = mp__lookup(context->id, base_msg->data.topic);
			if(stored && stored->purpose_filter)
			{
				base_msg->data.purpose_filter = mosquitto_strdup(stored->purpose_filter);
				base_msg->data.purpose_filter_version = stored->version;
				base_msg->data.has_purpose_filter = true;
			}
			else if(dap_is_op_system_topic(base_msg->data.topic))
			{
				/* Operation-system/control topic: no MP required. Leave
					* delivery ungated as before (deny-all filter). */
				base_msg->data.purpose_filter = mosquitto_strdup("");
				base_msg->data.purpose_filter_version = 0;
				base_msg->data.has_purpose_filter = true;
			}
			else
			{
				/* Paper 4.3: reject data messages on topics with no
					* registered MP, matching the PER_MSG path, rather than
					* silently applying a deny-all filter. */
				log__printf(NULL, MOSQ_LOG_INFO,
					"No message purpose (MP) registered for topic %s from %s, rejecting.",
					base_msg->data.topic, context->id);
				mosquitto_property_free_all(&properties);
				db__msg_store_free(base_msg);
				return MOSQ_ERR_MALFORMED_PACKET;
			}
		}

		/* Read all potential operational properties for later */
		if(db.config->metadata_operation_handling)
		{
			/* Look through the user properties for DAP-OpType */
			const mosquitto_property *p = properties;
			while(p){
				if(p->identifier == MQTT_PROP_USER_PROPERTY){
					char *name=NULL, *value=NULL;
					mosquitto_property_read_string_pair(p, MQTT_PROP_USER_PROPERTY, &name, &value, false);

					if(name && value){
						/* If we find DAP-OpType then note it. */
						if(!strcmp(name, MOSQ_DAP_OP_KEY)){
							found_op = true;
							op_id  = mosquitto_strdup(value);
						} else if(!strcmp(name, MOSQ_DAP_OP_TFS_KEY)){
							op_topic_filters = mosquitto_strdup(value);
						} else if(!strcmp(name, MOSQ_DAP_OP_PFS_KEY)){
							op_purpose_filters = mosquitto_strdup(value);
						} else if(!strcmp(name, MOSQ_DAP_OP_CLIENTS_KEY)){
							op_client_filters = mosquitto_strdup(value);
						} else if(!strcmp(name, MOSQ_DAP_OP_BEFORE_KEY)){
							op_before = (time_t)strtoll(value, NULL, 10);
						} else if(!strcmp(name, MOSQ_DAP_OP_AFTER_KEY)){
							op_after = (time_t)strtoll(value, NULL, 10);
						} else if(!strcmp(name, MOSQ_DAP_STATUS_KEY)){
							op_status = mosquitto_strdup(value);
						} else if(!strcmp(name, MOSQ_DAP_REASON_KEY)){
							op_reason = mosquitto_strdup(value);
						} else if(!strcmp(name, MOSQ_DAP_ID_KEY)){
							op_client_id = mosquitto_strdup(value);
						} else if(!strcmp(name, MOSQ_DAP_OP_ID_KEY)){
							op_id_num = (uint64_t)strtoull(value, NULL, 10);
							found_op_id_num = true;
						}
					}
				}
				else if (p->identifier == MQTT_PROP_CORRELATION_DATA)
				{
					/* A *present* CorrelationData of zero length reads back as a NULL
						* buffer with len 0 (mosquitto_property_read_binary), which is
						* indistinguishable from "absent" to the response builders'
						* `if(corr_data)` guard - so the property would be silently dropped
						* from the broker's Success/Failure/Pending response. A requester
						* that encodes a correlation id of 0 sends exactly this zero-length
						* value, so the response would come back with no CorrelationData and
						* could not be correlated. read_binary returns the property pointer
						* (non-NULL) whenever it is found; keep a non-NULL marker in that
						* case so the builders round-trip a zero-length CorrelationData
						* instead of omitting it. The 1-byte buffer is never dereferenced
						* (add_binary copies nothing when len is 0); it only flips the guard. */
					if(mosquitto_property_read_binary(p, MQTT_PROP_CORRELATION_DATA,
							(void **)&correlation_data, &correlation_data_len, false) != NULL
							&& correlation_data == NULL){
						correlation_data = mosquitto_calloc(1, 1);
					}
				}
				else if(p->identifier == MQTT_PROP_RESPONSE_TOPIC)
				{
					mosquitto_property_read_string(p, MQTT_PROP_RESPONSE_TOPIC, &response_topic, false);
				}

				/* Move to next property. */
				p = p->next;
			}
		}

		rc = property__process_publish(base_msg, &properties, &topic_alias, &message_expiry_interval);
		if(rc){
			mosquitto_property_free_all(&properties);
			db__msg_store_free(base_msg);
			return MOSQ_ERR_PROTOCOL;
		}

		/* Expose the receipt timestamp to subscribers as a user property, so they
		 * share the broker's reference time for ordering. */
		char ts_buf[32];
		if(dap_timestamp_format(base_msg->dap_recv_time, ts_buf, sizeof(ts_buf)) == 0){
			mosquitto_property_add_string_pair(&base_msg->data.properties,
					MQTT_PROP_USER_PROPERTY, MOSQ_DAP_TIMESTAMP_KEY, ts_buf);
		}
	}
	mosquitto_property_free_all(&properties);

	if(topic_alias == 0 || (context->listener && topic_alias > context->listener->max_topic_alias)){
		db__msg_store_free(base_msg);
		return MOSQ_ERR_TOPIC_ALIAS_INVALID;
	}else if(topic_alias > 0){
		if(base_msg->data.topic){
			rc = alias__add_r2l(context, base_msg->data.topic, (uint16_t)topic_alias);
			if(rc){
				db__msg_store_free(base_msg);
				return rc;
			}
		}else{
			rc = alias__find_by_alias(context, ALIAS_DIR_R2L, (uint16_t)topic_alias, &base_msg->data.topic);
			if(rc){
				db__msg_store_free(base_msg);
				return MOSQ_ERR_PROTOCOL;
			}
		}
	}

#ifdef WITH_BRIDGE
	rc = bridge__remap_topic_in(context, &base_msg->data.topic);
	if(rc){
		db__msg_store_free(base_msg);
		return rc;
	}

#endif
	if(mosquitto_pub_topic_check(base_msg->data.topic) != MOSQ_ERR_SUCCESS){
		/* Invalid publish topic, just swallow it. */
		db__msg_store_free(base_msg);
		return MOSQ_ERR_MALFORMED_PACKET;
	}

	base_msg->data.payloadlen = context->in_packet.remaining_length - context->in_packet.pos;
	metrics__int_inc(mosq_counter_pub_bytes_received, base_msg->data.payloadlen);
	if(context->listener && context->listener->mount_point){
		len = strlen(context->listener->mount_point) + strlen(base_msg->data.topic) + 1;
		topic_mount = mosquitto_malloc(len+1);
		if(!topic_mount){
			db__msg_store_free(base_msg);
			return MOSQ_ERR_NOMEM;
		}
		snprintf(topic_mount, len, "%s%s", context->listener->mount_point, base_msg->data.topic);
		topic_mount[len] = '\0';

		mosquitto_FREE(base_msg->data.topic);
		base_msg->data.topic = topic_mount;
	}

	if(base_msg->data.payloadlen){
		if(db.config->message_size_limit && base_msg->data.payloadlen > db.config->message_size_limit){
			log__printf(NULL, MOSQ_LOG_DEBUG, "Dropped too large PUBLISH from %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))", context->id, dup, base_msg->data.qos, base_msg->data.retain, base_msg->data.source_mid, base_msg->data.topic, (long)base_msg->data.payloadlen);
			reason_code = MQTT_RC_PACKET_TOO_LARGE;
			goto process_bad_message;
		}
		base_msg->data.payload = mosquitto_malloc(base_msg->data.payloadlen+1);
		if(base_msg->data.payload == NULL){
			db__msg_store_free(base_msg);
			return MOSQ_ERR_NOMEM;
		}
		/* Ensure payload is always zero terminated, this is the reason for the extra byte above */
		((uint8_t *)base_msg->data.payload)[base_msg->data.payloadlen] = 0;

		if(packet__read_bytes(&context->in_packet, base_msg->data.payload, base_msg->data.payloadlen)){
			db__msg_store_free(base_msg);
			return MOSQ_ERR_MALFORMED_PACKET;
		}
	}

	/* Check for topic access */
	rc = mosquitto_acl_check(context, base_msg->data.topic, base_msg->data.payloadlen, base_msg->data.payload, base_msg->data.qos, base_msg->data.retain, MOSQ_ACL_WRITE);
	if(rc == MOSQ_ERR_ACL_DENIED){
		log__printf(NULL, MOSQ_LOG_DEBUG,
				"Denied PUBLISH from %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))",
				context->id, dup, base_msg->data.qos, base_msg->data.retain, base_msg->data.source_mid, base_msg->data.topic,
				(long)base_msg->data.payloadlen);
		reason_code = MQTT_RC_NOT_AUTHORIZED;
		goto process_bad_message;
	}else if(rc != MOSQ_ERR_SUCCESS){
		db__msg_store_free(base_msg);
		return rc;
	}

	log__printf(NULL, MOSQ_LOG_DEBUG, "Received PUBLISH from %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))", context->id, dup, base_msg->data.qos, base_msg->data.retain, base_msg->data.source_mid, base_msg->data.topic, (long)base_msg->data.payloadlen);

	if(!strncmp(base_msg->data.topic, "$CONTROL/", 9)){
#ifdef WITH_CONTROL
		rc = control__process(context, base_msg);
		db__msg_store_free(base_msg);
		return rc;
#else
		reason_code = MQTT_RC_IMPLEMENTATION_SPECIFIC;
		goto process_bad_message;
#endif
	}

	{
		rc = plugin__handle_message_in(context, &base_msg->data);
		if(rc == MOSQ_ERR_ACL_DENIED){
			log__printf(NULL, MOSQ_LOG_DEBUG,
					"Denied PUBLISH from %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))",
					context->id, dup, base_msg->data.qos, base_msg->data.retain, base_msg->data.source_mid, base_msg->data.topic,
					(long)base_msg->data.payloadlen);

			reason_code = MQTT_RC_NOT_AUTHORIZED;
			goto process_bad_message;
		}else if(rc == MOSQ_ERR_QUOTA_EXCEEDED){
			log__printf(NULL, MOSQ_LOG_DEBUG,
					"Rejected PUBLISH from %s, quota exceeded.", context->id);

			reason_code = MQTT_RC_QUOTA_EXCEEDED;
			goto process_bad_message;
		}else if(rc != MOSQ_ERR_SUCCESS){
			db__msg_store_free(base_msg);
			return rc;
		}
	}

	if(base_msg->data.qos > 0){
		db__message_store_find(context, base_msg->data.source_mid, &cmsg_stored);
	}

	if(cmsg_stored && cmsg_stored->base_msg && base_msg->data.source_mid != 0 &&
			(cmsg_stored->base_msg->data.qos != base_msg->data.qos
			 || cmsg_stored->base_msg->data.payloadlen != base_msg->data.payloadlen
			 || strcmp(cmsg_stored->base_msg->data.topic, base_msg->data.topic)
			 || memcmp(cmsg_stored->base_msg->data.payload, base_msg->data.payload, base_msg->data.payloadlen) )){

		log__printf(NULL, MOSQ_LOG_WARNING, "Reused message ID %u from %s detected. Clearing from storage.", base_msg->data.source_mid, context->id);
		db__message_remove_incoming(context, base_msg->data.source_mid);
		cmsg_stored = NULL;
	}

	if(!cmsg_stored){
		if(base_msg->data.qos > 0 && context->msgs_in.inflight_quota == 0){
			/* Client isn't allowed any more incoming messages, so fail early */
			db__msg_store_free(base_msg);
			return MOSQ_ERR_RECEIVE_MAXIMUM_EXCEEDED;
		}

		if(base_msg->data.qos == 0
				|| db__ready_for_flight(context, mosq_md_in, base_msg->data.qos)
				){

			dup = 0;
			rc = db__message_store(context, base_msg, &message_expiry_interval, mosq_mo_client);
			if(rc) return rc;
		}else{
			/* Client isn't allowed any more incoming messages, so fail early */
			reason_code = MQTT_RC_QUOTA_EXCEEDED;
			goto process_bad_message;
		}
		stored = base_msg;
		base_msg = NULL;
		dup = 0;
	}else{
		db__msg_store_free(base_msg);
		base_msg = NULL;
		stored = cmsg_stored->base_msg;
		cmsg_stored->data.dup++;
		dup = cmsg_stored->data.dup;
	}

	if(stored->data.retain)
	{
		dr__record_retained_publisher(context->id, stored->data.topic);
	}

	/* Read all potential operational properties for later. A request carries
		* DAP-OpType (found_op); a subscriber status notification carries DAP-Status. */
	if(db.config->metadata_operation_handling && (found_op || op_status))
	{
		if(!strncmp(stored->data.topic, MOSQ_DAP_TOPIC_OSYS, 5))
		{
			if(op_status)
			{
				/* Inbound status notification: relay it to the requester and, on a
					* terminal status, advance the deadline tracker. */
				handle_dap_status_notification(context, found_op ? op_id : NULL,
						op_id_num, found_op_id_num, op_status, op_reason,
						op_client_id, stored, correlation_data, correlation_data_len);
			}
			/* C1 Operations */
			else if(!strcmp(op_id, MOSQ_DAP_OP_AUDIT))
			{
				subscription_list *subs = find_subscriptions_for_publisher(context->id);
				while(subs){
					const char *info = ri__lookup_info(subs->subscriber_id);
					if(info){
						broker_send_response_success(context->id, op_id, correlation_data, correlation_data_len, info, response_topic);
						ri__mark_sent_to_pub(context->id, subs->subscriber_id);
					}
					subs = subs->next;
				}
			}

			/* REGISTER-INFO: store the requester's info for later auto-fulfilment.
				* "Informed-Reg" is the original name and is still accepted. */
			else if(!strcmp(op_id, MOSQ_DAP_OP_REGISTER_INFO))
			{
				ri__register_info(context->id, stored->data.payload);
			}

			/* C2/C3 Operations */
			else if (!strcmp(op_id, MOSQ_DAP_OP_HISTORY)
			|| !strcmp(op_id, MOSQ_DAP_OP_DELETE) || !strcmp(op_id, MOSQ_DAP_OP_RESTRICT))
			{
				/* DELETE/RESTRICT become pending operations in the broker-wide map;
					* dap_op_request_insert succeeds for exactly those two and assigns the
					* numeric op id. Every other right (Access/Portability/Rectification/
					* Object/AutoDecision) falls through to the unchanged immediate path. */
				uint64_t pending_op_id = 0;
				bool is_pending_op = (dap_op_request_insert(db.dap_pending_ops, context->id, op_id,
							op_topic_filters, op_purpose_filters, op_client_filters,
							stored->dap_recv_time, &pending_op_id) == 0);

				if(is_pending_op)
				{
					/* Deadline workflow. Relevant subscribers are those that received
						* data from this publisher matching the operation's topic/purpose/
						* client filters within the DAP-OpBefore/OpAfter receipt-time
						* bounds (0 = unbounded on that side). */
					log__printf(NULL, MOSQ_LOG_DEBUG,
							"DAP pending operation %llu (%s) registered for %s; bounds after=%lld before=%lld",
							(unsigned long long)pending_op_id, op_id, context->id,
							(long long)op_after, (long long)op_before);

					struct dr_sublist *relevant = dr__find_relevant_subscribers(context->id,
							op_topic_filters, op_purpose_filters, op_client_filters,
							op_before, op_after);

					/* Assign a deadline relative to the receipt timestamp, forward to the
						* relevant subs on their ORS, register the op with the deadline
						* tracker, and echo a Pending ack (op id + deadline) to the requester.
						* The final Success/Failure is settled by the deadline sweep (loop.c)
						* and the status path, not synchronously here. */
					time_t deadline = stored->dap_recv_time + MOSQ_DAP_DEFAULT_DEADLINE_SECS;
					broker_dispatch_pending_operation(context->id, op_id, pending_op_id,
							relevant, &stored->data, response_topic, op_info,
							correlation_data, correlation_data_len, deadline);
					dr__free_sublist(relevant);

					/* DELETE additionally drops the publisher's stored will/retained data. */
					if(!strcmp(op_id, MOSQ_DAP_OP_DELETE))
					{
						handle_remove_stored_messages(context->id);
					}
				}
				else
				{
					/* Non-pending rights: immediate forward + Success/Failure. */
					subscriber_list *sub_list = find_subscribers_with_data(context->id, op_info);
					subscriber_list *offline = forward_request_to_connected(sub_list, &stored->data, response_topic, op_id, op_info, correlation_data, correlation_data_len, 0);
					if(offline){
						broker_send_response_failure(context->id, op_id, correlation_data, correlation_data_len, "Subscriber not connected", offline, response_topic);
					}
					else
					{
						broker_send_response_success(context->id, op_id, correlation_data, correlation_data_len, NULL, response_topic);
					}
				}
			}

			/* AUDIT: report which subscribers received the requester's matching
				* data. The broker fulfils this directly without consulting anyone;
				* the success payload is the comma-separated subscriber ids. No
				* relevant subscribers is a failure. */
			else if(!strcmp(op_id, MOSQ_DAP_OP_AUDIT))
			{
				struct dr_sublist *relevant = dr__find_relevant_subscribers(context->id,
						op_topic_filters, op_purpose_filters, op_client_filters,
						op_before, op_after);
				if(!relevant)
				{
					broker_send_response_failure(context->id, op_id, correlation_data,
							correlation_data_len, "No relevant subscribers", NULL, response_topic);
				}
				else
				{
					size_t len = 0;
					for(struct dr_sublist *s = relevant; s; s = s->next){
						len += strlen(s->sub_id) + 1; /* id plus a separator/terminator */
					}
					char *payload = mosquitto_calloc(1, len + 1);
					if(payload)
					{
						for(struct dr_sublist *s = relevant; s; s = s->next){
							strcat(payload, s->sub_id);
							if(s->next) strcat(payload, ",");
						}
						broker_send_response_success(context->id, op_id, correlation_data,
								correlation_data_len, payload, response_topic);
						mosquitto_FREE(payload);
					}
					dr__free_sublist(relevant);
				}
			}

			/* HISTORY and UPDATE are subscriber-involving like DELETE/RESTRICT but
				* do not apply to in-flight messages, so they get no pending-ops entry.
				* Allocate an op id, forward to the relevant subscribers and track the
				* deadline. UPDATE's replacement payload rides along in the forwarded
				* request (stored->data.payload). */
			else if(!strcmp(op_id, MOSQ_DAP_OP_HISTORY) || !strcmp(op_id, MOSQ_DAP_OP_UPDATE))
			{
				uint64_t hu_op_id = dap_pending_ops_allocate_op_id(db.dap_pending_ops);
				struct dr_sublist *relevant = dr__find_relevant_subscribers(context->id,
						op_topic_filters, op_purpose_filters, op_client_filters,
						op_before, op_after);
				time_t deadline = stored->dap_recv_time + MOSQ_DAP_DEFAULT_DEADLINE_SECS;
				broker_dispatch_pending_operation(context->id, op_id, hu_op_id,
						relevant, &stored->data, response_topic, op_info,
						correlation_data, correlation_data_len, deadline);
				dr__free_sublist(relevant);
			}

			/* Generic operator-defined operation ("O:" prefix), also not implemented. */
			else if(!strncmp(op_id, MOSQ_DAP_OP_PREFIX, strlen(MOSQ_DAP_OP_PREFIX)))
			{
				log__printf(NULL, MOSQ_LOG_INFO,
						"DAP generic operation %s from %s not yet implemented", op_id, context->id);
			}

			else
			{
				/* Unrecognized right. */
				broker_send_response_failure(context->id, op_id, correlation_data, correlation_data_len, "Unknown right", NULL, response_topic);
			}
		}
	}

	/* DAP-OpTFs/OpPFs/OpClients were copied by the pending-ops insert; release the
	 * parsed copies (mosquitto_FREE no-ops on the NULLs of a non-operation publish). */
	mosquitto_FREE(op_topic_filters);
	mosquitto_FREE(op_purpose_filters);
	mosquitto_FREE(op_client_filters);
	mosquitto_FREE(op_status);
	mosquitto_FREE(op_reason);
	mosquitto_FREE(op_client_id);
	/* Allocated by mosquitto_property_read_binary (or the zero-length marker above);
	 * the response builders copy what they need, so it is safe to release here. */
	mosquitto_FREE(correlation_data);

	switch(stored->data.qos){
		case 0:
			rc2 = sub__messages_queue(context->id, stored->data.topic, stored->data.qos, stored->data.retain, &stored);
			if(rc2 > 0) rc = 1;
			break;
		case 1:
			util__decrement_receive_quota(context);
			rc2 = sub__messages_queue(context->id, stored->data.topic, stored->data.qos, stored->data.retain, &stored);
			/* stored may now be free, so don't refer to it */
			if(rc2 == MOSQ_ERR_SUCCESS || context->protocol != mosq_p_mqtt5){
				if(send__puback(context, mid, 0, NULL)) rc = 1;
			}else if(rc2 == MOSQ_ERR_NO_SUBSCRIBERS){
				if(send__puback(context, mid, MQTT_RC_NO_MATCHING_SUBSCRIBERS, NULL)) rc = 1;
			}else{
				rc = rc2;
			}
			break;
		case 2:
			if(dup == 0){
				res = db__message_insert_incoming(context, 0, stored, true);
			}else{
				res = 0;
			}

			/* db__message_insert() returns 2 to indicate dropped message
			 * due to queue. This isn't an error so don't disconnect them. */
			/* FIXME - this is no longer necessary due to failing early above */
			if(!res){
				if(dup == 0 || dup == 1){
					rc2 = send__pubrec(context, stored->data.source_mid, 0, NULL);
					if(rc2) rc = rc2;
				}else{
					return MOSQ_ERR_PROTOCOL;
				}
			}else if(res == 1){
				rc = 1;
			}
			break;
	}

	db__message_write_queued_in(context);
	return rc;
process_bad_message:
	rc = 1;
	if(base_msg){
		switch(base_msg->data.qos){
			case 0:
				rc = MOSQ_ERR_SUCCESS;
				break;
			case 1:
				rc = send__puback(context, base_msg->data.source_mid, reason_code, NULL);
				break;
			case 2:
				rc = send__pubrec(context, base_msg->data.source_mid, reason_code, NULL);
				break;
		}
		db__msg_store_free(base_msg);
	}
	if(context->out_packet_count >= db.config->max_queued_messages){
		rc = MQTT_RC_QUOTA_EXCEEDED;
	}
	return rc;
}
