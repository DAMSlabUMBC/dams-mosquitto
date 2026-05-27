/* Regression test for the SUBSCRIBE DAP-SP purpose-filter copy (handle_subscribe.c).
 *
 * A benchmark subscriber sending DAP-SP "DAP_OP" (6 chars) drove a 1-byte
 * heap-buffer-overflow: the broker stored each parsed subscription purpose in a
 * buffer sized strlen() instead of strlen()+1, then strcpy'd into it, writing the
 * NUL terminator one byte past the allocation. parse_purpose_filter already returns
 * NUL-terminated strings, so the defect lived purely in the store step. That step is
 * now factored into purpose_filter_store_dup() so it can be exercised here under
 * AddressSanitizer - which is what catches the overflow.
 *
 * Build and run on its own (ASan required to catch the regression):
 *   cc -I../../.. -I../../../lib -I../../../include -I../../../libcommon \
 *      -I/opt/homebrew/include -include string.h -fsanitize=address -g -O0 \
 *      purpose_filter_copy_test.c ../../../lib/purpose_filters.c \
 *      ../../../libcommon/memory_common.c -o purpose_filter_copy_test \
 *      && ./purpose_filter_copy_test
 */
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "purpose_filters.h"

/* Declared here rather than via the export-annotated libcommon header so the test
 * compiles standalone; satisfied by linking libcommon/memory_common.c. */
extern void *mosquitto_malloc(size_t size);
extern void mosquitto_free(void *mem);

/* Storing a parsed purpose yields an independent, NUL-terminated copy equal to the
 * input - and writing it must not run past the allocation (ASan enforces the latter).
 * "DAP_OP" is the exact value the benchmark subscriber sent when the broker crashed. */
static void test_store_dup_roundtrip(const char *purpose)
{
    char *copy = purpose_filter_store_dup(purpose);
    assert(copy != NULL);
    assert(copy != purpose);
    assert(strlen(copy) == strlen(purpose));
    assert(strcmp(copy, purpose) == 0);
    mosquitto_free(copy);
}

/* The full path the broker uses for a DAP-SP user property: parse the SP value into
 * its constituent purposes, then store each one. Every parsed purpose must be storable
 * without overflow. */
static void test_parse_then_store(const char *sp_value, uint32_t expected_count)
{
    uint32_t n = 0;
    char **purposes = parse_purpose_filter(sp_value, &n);
    assert(purposes != NULL);
    assert(n == expected_count);
    for(uint32_t i = 0; i < n; i++)
    {
        char *stored = purpose_filter_store_dup(purposes[i]);
        assert(stored != NULL);
        assert(strcmp(stored, purposes[i]) == 0);
        mosquitto_free(stored);
        mosquitto_free(purposes[i]);
    }
    mosquitto_free(purposes);
}

int main(void)
{
    /* Single-purpose values, including the exact one that crashed the broker. */
    test_store_dup_roundtrip("DAP_OP");
    test_store_dup_roundtrip("*");
    test_store_dup_roundtrip("");
    test_store_dup_roundtrip("a/longer/hierarchical/purpose");

    /* End-to-end with the real parser, including a brace expansion that yields
     * several purposes, each of which must be stored safely. */
    test_parse_then_store("DAP_OP", 1);
    test_parse_then_store("*", 1);
    test_parse_then_store("billing/electricity", 1);
    test_parse_then_store("{read,write}", 2);

    printf("purpose_filter_copy_test: all assertions passed\n");
    return 0;
}
