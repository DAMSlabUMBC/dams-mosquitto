/* Standalone isolation test for the MQTT-DAP priority queue (lib/dap_pqueue.c).
 *
 * The queue only ever stores packet pointers, so this test feeds it dummy
 * (never-dereferenced) addresses and checks ordering, counting and lifecycle.
 * Build and run on its own, no broker objects required:
 *
 *   cc -I../../../lib -I../../../deps dap_pqueue_test.c ../../../lib/dap_pqueue.c -o dap_pqueue_test && ./dap_pqueue_test
 */

#include <assert.h>
#include <stdio.h>

#include "dap_pqueue.h"

/* Distinct, non-NULL stand-ins for real packets. Their contents are never read. */
static char packet_slots[8];
#define PKT(i) ((struct mosquitto__packet_in *)&packet_slots[(i)])

static void test_empty_queue(void)
{
    struct dap_pqueue q;
    assert(dap_pqueue__init(&q) == 0);
    assert(dap_pqueue__size(&q) == 0);
    assert(dap_pqueue__peek(&q) == NULL);
    assert(dap_pqueue__dequeue(&q) == NULL);
    dap_pqueue__destroy(&q);
    printf("ok - empty queue reports size 0 and yields NULL\n");
}

static void test_single_packet_roundtrip(void)
{
    struct dap_pqueue q;
    dap_pqueue__init(&q);

    assert(dap_pqueue__enqueue(&q, PKT(0), DAP_PQ_PRIORITY_LOW) == 0);
    assert(dap_pqueue__size(&q) == 1);
    /* peek does not remove */
    assert(dap_pqueue__peek(&q) == PKT(0));
    assert(dap_pqueue__size(&q) == 1);
    /* dequeue does */
    assert(dap_pqueue__dequeue(&q) == PKT(0));
    assert(dap_pqueue__size(&q) == 0);

    dap_pqueue__destroy(&q);
    printf("ok - single packet enqueue/peek/dequeue roundtrip\n");
}

static void test_high_drains_before_low(void)
{
    struct dap_pqueue q;
    dap_pqueue__init(&q);

    /* Interleave the priorities on the way in. */
    dap_pqueue__enqueue(&q, PKT(0), DAP_PQ_PRIORITY_LOW);
    dap_pqueue__enqueue(&q, PKT(1), DAP_PQ_PRIORITY_HIGH);
    dap_pqueue__enqueue(&q, PKT(2), DAP_PQ_PRIORITY_LOW);
    dap_pqueue__enqueue(&q, PKT(3), DAP_PQ_PRIORITY_HIGH);
    assert(dap_pqueue__size(&q) == 4);

    /* High first, in FIFO order, then low in FIFO order. */
    assert(dap_pqueue__dequeue(&q) == PKT(1));
    assert(dap_pqueue__dequeue(&q) == PKT(3));
    assert(dap_pqueue__dequeue(&q) == PKT(0));
    assert(dap_pqueue__dequeue(&q) == PKT(2));
    assert(dap_pqueue__dequeue(&q) == NULL);
    assert(dap_pqueue__size(&q) == 0);

    dap_pqueue__destroy(&q);
    printf("ok - high priority drains before low, FIFO within each level\n");
}

static void test_peek_tracks_priority(void)
{
    struct dap_pqueue q;
    dap_pqueue__init(&q);

    dap_pqueue__enqueue(&q, PKT(0), DAP_PQ_PRIORITY_LOW);
    /* A later high-priority packet should jump ahead of the waiting low one. */
    assert(dap_pqueue__peek(&q) == PKT(0));
    dap_pqueue__enqueue(&q, PKT(1), DAP_PQ_PRIORITY_HIGH);
    assert(dap_pqueue__peek(&q) == PKT(1));

    dap_pqueue__destroy(&q);
    printf("ok - peek follows the high-before-low draining order\n");
}

static void test_destroy_nonempty_resets(void)
{
    struct dap_pqueue q;
    dap_pqueue__init(&q);

    dap_pqueue__enqueue(&q, PKT(0), DAP_PQ_PRIORITY_HIGH);
    dap_pqueue__enqueue(&q, PKT(1), DAP_PQ_PRIORITY_LOW);
    dap_pqueue__destroy(&q);

    /* After destroy the queue is empty again and safe to reuse. */
    assert(dap_pqueue__size(&q) == 0);
    assert(dap_pqueue__peek(&q) == NULL);
    printf("ok - destroy on a non-empty queue frees nodes and resets to empty\n");
}

static void test_rejects_bad_arguments(void)
{
    struct dap_pqueue q;
    dap_pqueue__init(&q);

    assert(dap_pqueue__init(NULL) != 0);
    assert(dap_pqueue__enqueue(NULL, PKT(0), DAP_PQ_PRIORITY_LOW) != 0);
    assert(dap_pqueue__enqueue(&q, NULL, DAP_PQ_PRIORITY_LOW) != 0);
    assert(dap_pqueue__size(&q) == 0);

    dap_pqueue__destroy(&q);
    printf("ok - NULL queue and NULL packet arguments are rejected\n");
}

int main(void)
{
    test_empty_queue();
    test_single_packet_roundtrip();
    test_high_drains_before_low();
    test_peek_tracks_priority();
    test_destroy_nonempty_resets();
    test_rejects_bad_arguments();
    printf("\nAll dap_pqueue tests passed.\n");
    return 0;
}
