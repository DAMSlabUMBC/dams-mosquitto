#ifndef RIGHTS_BROKER_H
#define RIGHTS_BROKER_H

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <time.h>

/* Forward-declare mosquitto struct if needed. */
struct mosquitto;
struct subscriber_list;
struct mosquitto_base_msg;
struct subscription_list;
struct dr_sublist; /* lib/dr_registry.h: relevant-subscriber list node */

/* A function to lookup a client context by ID and a function to check if a subscriber is online. */
struct mosquitto *broker_find_context_by_id(const char *client_id);
bool is_sub_online(const char *sub_id);

/* Removes Will or retained messages. */
void handle_remove_stored_messages(const char *publisher_id);

/* Responses back to a publisher on RNP/<publisher_id>. */
void broker_send_response_success(const char *publisher_id, const char *operation, const char *corr_data, uint16_t correlation_data_len, const char *payload, char* response_topic);
/* Acknowledge a validated pending op to the requester's ONP, carrying the
 * broker-assigned numeric op id and the absolute deadline (epoch seconds). */
void broker_send_response_pending(const char *publisher_id, const char *operation, uint64_t op_id, const char *corr_data, uint16_t correlation_data_len, time_t deadline);
void broker_send_response_failure(const char *publisher_id, const char *operation, const char *corr_data, uint16_t correlation_data_len, const char *reason,
    struct subscriber_list *unreached_subs);

/* Pending-op dispatch (DELETE/RESTRICT): forward the request to the relevant
 * subscribers on their ORS (carrying op_id), register the op with the deadline tracker
 * so unresponded subscribers can be reported at expiry, and echo a Pending ack with the
 * op id + deadline back to the requester. The relevant list is borrowed, not freed. */
void broker_dispatch_pending_operation(const char *publisher_id, const char *operation,
    uint64_t op_id, struct dr_sublist *relevant, struct mosquitto_base_msg *msg_data,
    char *response_topic, char *op_info, char *correlation_data, uint16_t correlation_data_len, time_t deadline);

/* Deadline-expiry notification: tell the requester on ONP that op_id expired with
 * the given unresponded subscriber ids (DAP-Status=Failure, DAP-OpId, DAP-UnreachedClients). */
void broker_send_deadline_failure(uint64_t op_id, const char *publisher_id,
    char **unresponded_subs, size_t num_unresponded);

/* All-responded notification: tell the requester on ONP that op_id completed because
 * every relevant subscriber responded before the deadline (DAP-Status=Success, DAP-OpId). */
void broker_send_deadline_success(uint64_t op_id, const char *publisher_id);

/* Forward a subscriber's status notification (success/failure/pending) to the
 * requester on ONP/<requester_id>, preserving DAP-OpId, DAP-Status, DAP-Reason and
 * the responding subscriber id (DAP-ClientID), plus any payload/correlation data. */
void broker_forward_status_to_requester(const char *requester_id, const char *operation,
    uint64_t op_id, const char *status, const char *reason, const char *responder_id,
    const void *payload, uint32_t payloadlen, const char *corr_data, uint16_t corr_len);

/* For enumerating who got the publisher's data (C1). */
struct subscription_list *find_subscriptions_for_publisher(const char *publisher_id);

/* For enumerating who has data matching a filter (C2/C3). */
struct subscriber_list *find_subscribers_with_data(const char *publisher_id, const char *data_filter);

/* Forward a right request to RRS/<sub_id> if online, else add to an offline list. */
struct subscriber_list *forward_request_to_connected(struct subscriber_list *sub_list, struct mosquitto_base_msg *msg_data, char* response_topic, char* op_id, char* op_info, char* correlation_data, uint16_t correlation_data_len, uint64_t op_id_num);

/* Data structures for returning lists of subscribers. */
typedef struct subscription_list {
    char *subscriber_id;
    char *topic;
    struct subscription_list *next;
} subscription_list;

typedef struct subscriber_list {
    char *sub_id;
    struct subscriber_list *next;
} subscriber_list;

#endif
