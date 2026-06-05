/* Unit test for multi-filter purpose compatibility (lib/purpose_filters.c).
 *
 * The §4 SP-vs-MP compatibility check used to assume a single filter per MP and
 * compared each subscription SP entry to that one MP string for equality. An MP
 * (and an SP) may now carry several alternative filters joined by '|', with
 * match-any semantics: the message is deliverable to a subscription when some
 * '|'-separated filter of the MP equals some '|'-separated filter of any SP entry.
 * A '*' SP entry matches any MP.
 *
 * Standalone build/run:
 *   cc -I../../.. -I../../../lib -I../../../include -I../../../libcommon \
 *      -include string.h -g -O0 \
 *      purpose_filter_match_test.c ../../../lib/purpose_filters.c \
 *      ../../../libcommon/memory_common.c -o purpose_filter_match_test \
 *      && ./purpose_filter_match_test
 */
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "purpose_filters.h"

extern void *mosquitto_malloc(size_t size);
extern void mosquitto_free(void *mem);

int main(void)
{
    /* --- single-filter MP: existing behaviour preserved --- */
    char *sp_a[]  = {"quality/assurance"};
    char *sp_c[]  = {"operations/forecast"};
    char *sp_star[] = {"*"};

    assert(purpose_filter_mp_matches_sp("quality/assurance", sp_a, 1) == true);
    assert(purpose_filter_mp_matches_sp("quality/assurance", sp_c, 1) == false);
    assert(purpose_filter_mp_matches_sp("anything/at/all", sp_star, 1) == true);

    /* --- multi-filter MP, match-any --- */
    const char *mp_multi = "quality/assurance|operations/forecast";
    assert(purpose_filter_mp_matches_sp(mp_multi, sp_a, 1) == true);   /* matches first  */
    assert(purpose_filter_mp_matches_sp(mp_multi, sp_c, 1) == true);   /* matches second */

    char *sp_none[] = {"vendor/maintenance"};
    assert(purpose_filter_mp_matches_sp(mp_multi, sp_none, 1) == false); /* matches none */

    /* --- multi-filter SP entry (SP may also carry '|') --- */
    char *sp_pipe[] = {"vendor/maintenance|operations/forecast"};
    assert(purpose_filter_mp_matches_sp("operations/forecast", sp_pipe, 1) == true);
    assert(purpose_filter_mp_matches_sp("quality/assurance", sp_pipe, 1) == false);

    /* --- SP array with several entries, one of which matches an MP filter --- */
    char *sp_array[] = {"vendor/maintenance", "operations/forecast"};
    assert(purpose_filter_mp_matches_sp(mp_multi, sp_array, 2) == true);

    /* --- degenerate inputs --- */
    assert(purpose_filter_mp_matches_sp("", sp_a, 1) == false);
    assert(purpose_filter_mp_matches_sp("quality/assurance", sp_a, 0) == false);
    assert(purpose_filter_mp_matches_sp(NULL, sp_a, 1) == false);

    printf("purpose_filter_match_test: all assertions passed\n");
    return 0;
}
