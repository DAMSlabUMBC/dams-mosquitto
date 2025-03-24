/* ri_registry.h */
#ifndef RI_REGISTRY_H
#define RI_REGISTRY_H

/* Initializes the registry. */
void ri_registry_init(void);

/* Frees all stored entries. */
void ri_registry_cleanup(void);

/* Stores or overwrites info for the sub_id and topic. */
void ri__register_info(const char *sub_id, const char *topic, const char *info);

/* Retrieves info for sub_id and topic, or returns NULL if none. */
char *ri__lookup_info(const char *sub_id, const char *topic);

#endif
