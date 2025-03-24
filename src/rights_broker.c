#include <string.h>
#include <stdio.h>
#include "rights_broker.h"
#include "mosquitto_broker_internal.h" 
#include "util_mosq.h"
#include "property_mosq.h"
#include "send_mosq.h"
#include "dr_registry.h" 

/* Finds a client context by ID by calling db__find_context_by_id(). */
struct mosquitto *broker_find_context_by_id(const char *client_id)
{
    return db__find_context_by_id(client_id);
}

/* Checks if a subscriber is online (active state). */
bool is_sub_online(const char *sub_id)
{
    struct mosquitto *ctx = broker_find_context_by_id(sub_id);
    if(!ctx) return false;
    return (ctx->state == mosq_cs_active);
}

/* Removes Wills or retained messages when 'PF-RemoveStoredMessages' is set. */
void handle_remove_stored_messages(const char *publisher_id, const char *remove_stored)
{
    if(!remove_stored) return;

    if(!strcmp(remove_stored, "WILL")){
        struct mosquitto *pub_ctx = broker_find_context_by_id(publisher_id);
        if(pub_ctx && pub_ctx->will){
            mosquitto_FREE(pub_ctx->will->msg.topic);
            mosquitto_FREE(pub_ctx->will->msg.payload);
            mosquitto_FREE(pub_ctx->will);
            pub_ctx->will = NULL;
        }
    }
    else if(!strncmp(remove_stored, "RETAINED:", 9)){
        const char *topic = remove_stored + 9;
        /* Zero-length retained => remove old retained msg. */
        db__messages_easy_queue(NULL, topic, 0, 0, NULL, true, 0, NULL);
    }
    else if(!strncmp(remove_stored, "WILL,RETAINED:", 14)){
        const char *topic = remove_stored + 14;
        struct mosquitto *pub_ctx = broker_find_context_by_id(publisher_id);
        if(pub_ctx && pub_ctx->will){
            mosquitto_FREE(pub_ctx->will->msg.topic);
            mosquitto_FREE(pub_ctx->will->msg.payload);
            mosquitto_FREE(pub_ctx->will);
            pub_ctx->will = NULL;
        }
        db__messages_easy_queue(NULL, topic, 0, 0, NULL, true, 0, NULL);
    }
}

/* Sends a basic status response to RNP/<publisher_id>. */
void broker_send_response_status(const char *publisher_id, const char *corr_data, const char *payload)
{
    if(!publisher_id) return;
    char rnp_topic[256];
    snprintf(rnp_topic, sizeof(rnp_topic), "%s/%s", MOSQ_PF_TOPIC_RNP, publisher_id);

    mosquitto_property *props = NULL;
    if(payload){
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_PF_STATUS_KEY, payload);
    }
    if(corr_data){
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_PF_CORRELATION_DATA_KEY, corr_data);
    }

    db__messages_easy_queue(NULL, rnp_topic, 0, 0, NULL, false, 0, &props);
    mosquitto_property_free_all(&props);
}

/* Sends data to RNP/<publisher_id> (e.g. for access or be_informed). */
void broker_send_response_data(const char *publisher_id, const char *corr_data, const char *payload)
{
    if(!publisher_id) return;
    char rnp_topic[256];
    snprintf(rnp_topic, sizeof(rnp_topic), "%s/%s", MOSQ_PF_TOPIC_RNP, publisher_id);

    mosquitto_property *props = NULL;
    if(payload){
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_PF_DATA_KEY, payload);
    }
    if(corr_data){
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_PF_CORRELATION_DATA_KEY, corr_data);
    }

    db__messages_easy_queue(NULL, rnp_topic, 0, 0, NULL, false, 0, &props);
    mosquitto_property_free_all(&props);
}

/* Notifies publisher of offline subs, setting PF-Deadline & PF-SubscribersToContact. */
void broker_send_response_pending(const char *publisher_id, const char *corr_data,
                                  struct subscriber_list *offline, int deadline_sec)
{
    if(!publisher_id) return;
    char rnp_topic[256];
    snprintf(rnp_topic, sizeof(rnp_topic), "%s/%s", MOSQ_PF_TOPIC_RNP, publisher_id);

    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_PF_GDPR_REASON_KEY, "Subscriber not connected");

    char deadline_buf[32];
    snprintf(deadline_buf, sizeof(deadline_buf), "%d", deadline_sec);
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_PF_DEADLINE_KEY, deadline_buf);

    char contact_buf[256];
    contact_buf[0] = '\0';
    while(offline){
        strncat(contact_buf, offline->sub_id, sizeof(contact_buf)-1);
        strncat(contact_buf, " ", sizeof(contact_buf)-1);
        offline = offline->next;
    }
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_PF_SUBSCRIBERS_TO_CONTACT_KEY, contact_buf);

    if(corr_data){
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_PF_CORRELATION_DATA_KEY, corr_data);
    }

    db__messages_easy_queue(NULL, rnp_topic, 0, 0, NULL, false, 0, &props);
    mosquitto_property_free_all(&props);
}

/* Sends a final failure to RNP/<publisher_id> (e.g. unknown right or offline never reconnected). */
void broker_send_response_failure(const char *publisher_id, const char *corr_data, const char *reason)
{
    if(!publisher_id) return;
    char rnp_topic[256];
    snprintf(rnp_topic, sizeof(rnp_topic), "%s/%s", MOSQ_PF_TOPIC_RNP, publisher_id);

    mosquitto_property *props = NULL;
    if(reason){
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_PF_GDPR_REASON_KEY, reason);
    }
    if(corr_data){
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_PF_CORRELATION_DATA_KEY, corr_data);
    }

    db__messages_easy_queue(NULL, rnp_topic, 0, 0, NULL, false, 0, &props);
    mosquitto_property_free_all(&props);
}

/* Finds (sub_id, topic) for all subscriptions that got data from publisher_id. */
struct subscription_list *find_subscriptions_for_publisher(const char *publisher_id)
{
    struct subscription_list *head = NULL;
    extern struct dr_entry *dr_head;
    struct dr_entry *cur = dr_head;

    while(cur){
        if(!strcmp(cur->pub_id, publisher_id)){
            struct dr_sublist *s = cur->sub_list;
            while(s){
                struct subscription_list *node = mosquitto_calloc(1, sizeof(*node));
                if(!node) return head;
                node->subscriber_id = mosquitto_strdup(s->sub_id);
                node->topic        = mosquitto_strdup(cur->topic);
                node->next         = head;
                head = node;
                s = s->next;
            }
        }
        cur = cur->next;
    }
    return head;
}

/* Finds subs with matching topic if data_filter is in the topic name. */
struct subscriber_list *find_subscribers_with_data(const char *publisher_id, const char *data_filter)
{
    struct subscriber_list *head = NULL;
    extern struct dr_entry *dr_head;
    struct dr_entry *cur = dr_head;

    while(cur){
        if(!strcmp(cur->pub_id, publisher_id)){
            if(data_filter && strstr(cur->topic, data_filter)){
                struct dr_sublist *s = cur->sub_list;
                while(s){
                    struct subscriber_list *node = mosquitto_calloc(1, sizeof(*node));
                    if(!node) return head;
                    node->sub_id = mosquitto_strdup(s->sub_id);
                    node->next   = head;
                    head         = node;
                    s = s->next;
                }
            }
        }
        cur = cur->next;
    }
    return head;
}

/* Publishes a right request to RRS/<sub> if online, else collects them offline. */
struct subscriber_list *forward_request_to_connected(struct subscriber_list *sub_list,
    const char *corr_data, const char *invoked_right, const char *data_filter)
{
    struct subscriber_list *offline_head = NULL;

    while(sub_list){
        if(is_sub_online(sub_list->sub_id)){
            char rrs_topic[256];
            snprintf(rrs_topic, sizeof(rrs_topic), "%s/%s", MOSQ_PF_TOPIC_RRS, sub_list->sub_id);

            mosquitto_property *props = NULL;
            if(invoked_right){
                mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
                    MOSQ_PF_RIGHT_KEY, invoked_right);
            }
            if(data_filter){
                mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
                    MOSQ_PF_DATA_FILTER_KEY, data_filter);
            }
            if(corr_data){
                mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
                    MOSQ_PF_CORRELATION_DATA_KEY, corr_data);
            }

            db__messages_easy_queue(NULL, rrs_topic, 0, 0, NULL, false, 0, &props);
            mosquitto_property_free_all(&props);
        } else {
            struct subscriber_list *off = mosquitto_calloc(1, sizeof(*off));
            off->sub_id = mosquitto_strdup(sub_list->sub_id);
            off->next   = offline_head;
            offline_head= off;
        }
        sub_list = sub_list->next;
    }
    return offline_head;
}