/* Unit tests for lib/dap_timestamp.c.
 *
 * dap_timestamp_format renders a message's receipt time_t as the DAP-Timestamp
 * property value (decimal seconds since the epoch). Build and run on its own:
 *
 *   cc -Wall -Wextra -Werror -I../../.. -I../../../lib -I../../../include \
 *      -I../../../libcommon -I../../../src -I../../../common -I../../../deps \
 *      -I/opt/homebrew/include \
 *      dap_timestamp_test.c ../../../lib/dap_timestamp.c -o dap_timestamp_test \
 *      && ./dap_timestamp_test
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "dap_timestamp.h"

/* A normal receipt time is rendered as its decimal seconds-since-epoch. */
static void test_format_basic(void)
{
    char buf[32];
    assert(dap_timestamp_format((time_t)1716700000, buf, sizeof(buf)) == 0);
    assert(!strcmp(buf, "1716700000"));
    printf("ok - receipt time rendered as decimal seconds\n");
}

/* The epoch renders as "0", not empty. */
static void test_format_zero(void)
{
    char buf[32];
    assert(dap_timestamp_format((time_t)0, buf, sizeof(buf)) == 0);
    assert(!strcmp(buf, "0"));
    printf("ok - epoch rendered as \"0\"\n");
}

/* A buffer too small for the value fails rather than writing past the end. */
static void test_format_buffer_too_small(void)
{
    char buf[4]; /* not enough for "1716700000" + NUL */
    assert(dap_timestamp_format((time_t)1716700000, buf, sizeof(buf)) != 0);
    printf("ok - too-small buffer rejected\n");
}

/* Bad arguments are rejected. */
static void test_format_bad_args(void)
{
    char buf[32];
    assert(dap_timestamp_format((time_t)1716700000, NULL, sizeof(buf)) != 0);
    assert(dap_timestamp_format((time_t)1716700000, buf, 0) != 0);
    printf("ok - NULL buffer / zero length rejected\n");
}

int main(void)
{
    test_format_basic();
    test_format_zero();
    test_format_buffer_too_small();
    test_format_bad_args();
    printf("\nAll dap_timestamp tests passed.\n");
    return 0;
}
