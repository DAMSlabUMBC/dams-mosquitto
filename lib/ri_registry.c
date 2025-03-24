/* ri_registry.c */
#include <string.h>
#include <stdlib.h>

#include "ri_registry.h"
#include "mosquitto_internal.h"
#include "util_mosq.h"

/* Linked-list node */
struct ri_entry {
    char *sub_id;
    char *topic;
    char *info;
    struct ri_entry *next;
};

static struct ri_entry *ri_head = NULL;

void ri_registry_init(void)
{
    ri_head = NULL;
}

void ri_registry_cleanup(void)
{
    struct ri_entry *cur = ri_head;
    while(cur){
        struct ri_entry *temp = cur;
        cur = cur->next;
        mosquitto_FREE(temp->sub_id);
        mosquitto_FREE(temp->topic);
        mosquitto_FREE(temp->info);
        mosquitto_FREE(temp);
    }
    ri_head = NULL;
}

/* If sub_id and topic exists, overwrite its info otherwise create a new entry. */
void ri__register_info(const char *sub_id, const char *topic, const char *info)
{
    struct ri_entry *curr = ri_head;
    while(curr){
        if(!strcmp(curr->sub_id, sub_id) && !strcmp(curr->topic, topic)){
            mosquitto_FREE(curr->info);
            curr->info = mosquitto_strdup(info);
            return;
        }
        curr = curr->next;
    }

    /* Not found so allocate a new entry */
    struct ri_entry *e = mosquitto_calloc(1, sizeof(*e));
    if(!e) return;
    e->sub_id = mosquitto_strdup(sub_id);
    e->topic  = mosquitto_strdup(topic);
    e->info   = mosquitto_strdup(info);
    e->next   = ri_head;
    ri_head   = e;
}

/* Returns the stored info, or a NULL if not found. */
char *ri__lookup_info(const char *sub_id, const char *topic)
{
    struct ri_entry *curr = ri_head;
    while(curr){
        if(!strcmp(curr->sub_id, sub_id) && !strcmp(curr->topic, topic)){
            return curr->info;
        }
        curr = curr->next;
    }
    return NULL;
}
