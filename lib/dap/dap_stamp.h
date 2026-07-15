/* dap_stamp.h */
#ifndef DAP_STAMP_H
#define DAP_STAMP_H

#include <stdint.h>
#include <time.h>

/* enum dap_op_action is used as an out-parameter, so it must be complete here. */
#include "dap_pending_ops.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Borrowed types, held by pointer. Forward declared. */
struct dap_subscription_queues;
struct mosquitto__base_msg;

/*
 * Stamp a matched data message for one subscription and append it to that
 * subscription's topic queue, alongside the broker's existing per-client queue.
 *
 * The MP version is looked up by (publisher_id, topic). The SP version is supplied
 * by the caller from the subscription leaf. mid is the per-client message id
 * subs__send assigned; it is stamped on so the send-path hook can correlate the
 * two queue entries when subscriptions overlap (0 for QoS 0, which carries no mid).
 *
 * pending_ops (may be NULL) is consulted with dap_pending_ops_match:
 *   - DROP     -> the message is not enqueued (a DELETE applies);
 *   - RESTRICT -> the message is enqueued stamped with the deciding op id;
 *   - NONE     -> the message is enqueued with no applied op ids.
 * The decided action is written to action_out when non-NULL, so the caller can
 * skip recording the recipient on a DROP. A NULL pending_ops behaves as NONE.
 *
 * msg is borrowed. Returns 0 when the message was handled (enqueued or dropped),
 * non-zero on a bad argument or allocation failure.
 */
int dap_stamp_and_enqueue(struct dap_subscription_queues *queues,
                          struct dap_pending_ops *pending_ops,
                          const char *publisher_id,
                          const char *subscriber_id,
                          const char *topic,
                          uint16_t mid,
                          uint32_t sp_version,
                          const char *purpose,
                          struct mosquitto__base_msg *msg,
                          time_t enqueue_time,
                          enum dap_op_action *action_out);

#ifdef __cplusplus
}
#endif

#endif /* DAP_STAMP_H */
