/* Standalone isolation test for the op-id -> requester map
 * (lib/dap_op_requester.c).
 *
 * Covers recording, lookup, overwrite, miss, and bad-argument handling. Build and
 * run on its own (CUnit is not required here):
 *
 *   cc -I../../.. -I../../../lib -I../../../include -I../../../libcommon \
 *      -I../../../src -I../../../common -I../../../deps -I/opt/homebrew/include \
 *      dap_op_requester_test.c ../../../lib/dap_op_requester.c \
 *      ../../../libcommon/memory_common.c -o dap_op_requester_test \
 *      && ./dap_op_requester_test
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "dap_op_requester.h"

static void test_record_and_lookup(void)
{
    struct dap_op_requester m;
    assert(dap_op_requester_init(&m) == 0);

    assert(dap_op_requester_record(&m, 1, "pub/a") == 0);
    assert(dap_op_requester_record(&m, 2, "pub/b") == 0);

    assert(!strcmp(dap_op_requester_lookup(&m, 1), "pub/a"));
    assert(!strcmp(dap_op_requester_lookup(&m, 2), "pub/b"));

    /* An unrecorded op id has no requester. */
    assert(dap_op_requester_lookup(&m, 99) == NULL);

    dap_op_requester_destroy(&m);
    printf("ok - record then lookup returns the requester per op id\n");
}

static void test_overwrite(void)
{
    struct dap_op_requester m;
    dap_op_requester_init(&m);

    assert(dap_op_requester_record(&m, 1, "pub/a") == 0);
    assert(dap_op_requester_record(&m, 1, "pub/b") == 0);
    assert(!strcmp(dap_op_requester_lookup(&m, 1), "pub/b"));

    dap_op_requester_destroy(&m);
    printf("ok - re-recording an op id overwrites the requester\n");
}

static void test_late_lookup_survives(void)
{
    struct dap_op_requester m;
    dap_op_requester_init(&m);

    /* The map is independent of the deadline tracker: an entry stays available so a
     * late status notification can still be routed to the requester. */
    dap_op_requester_record(&m, 7, "pub/late");
    assert(!strcmp(dap_op_requester_lookup(&m, 7), "pub/late"));

    dap_op_requester_destroy(&m);
    printf("ok - entries persist for late notifications\n");
}

static void test_bad_args(void)
{
    struct dap_op_requester m;
    dap_op_requester_init(&m);

    assert(dap_op_requester_init(NULL) != 0);
    assert(dap_op_requester_record(NULL, 1, "pub/a") != 0);
    assert(dap_op_requester_record(&m, 1, NULL) != 0);
    assert(dap_op_requester_lookup(NULL, 1) == NULL);
    dap_op_requester_destroy(NULL); /* must not crash */

    dap_op_requester_destroy(&m);
    printf("ok - bad arguments are rejected without crashing\n");
}

int main(void)
{
    test_record_and_lookup();
    test_overwrite();
    test_late_lookup_survives();
    test_bad_args();
    printf("\nAll dap_op_requester tests passed.\n");
    return 0;
}
