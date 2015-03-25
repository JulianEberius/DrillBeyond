#include "postgres.h"
#include "drillbeyond/drillbeyond.h"
#include "utils/hsearch.h"

/* this is actually necessary when using dynahash.c
    as there is no other way to use external data in its hash
    or eq functions (execGrouping.c for example is doing
    the same)*/
static DrillBeyondExpansion *currentExpansion = NULL;

static uint32 drb_key_hash(const void *key, Size keysize);
static int drb_key_match(const void *key1, const void *key2, Size keysize);

extern HTAB* drb_setupHashTable(DrillBeyondExpansion *exp, int nrows) {
    HASHCTL hash_ctl;
    HTAB *results_hashtable;
    MemSet(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Datum);
    hash_ctl.entrysize = sizeof(DrillBeyondValues);
    hash_ctl.hash = drb_key_hash;
    hash_ctl.match = drb_key_match;
    results_hashtable = hash_create("DrillBeyondHashTable", nrows,
                                     &hash_ctl,
                    HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

    return results_hashtable;
}

extern void drb_addToHashTable(DrillBeyondExpansion *exp, Datum *keys, Datum *values, int numValues)
{
    bool found;
    DrillBeyondValues *entry;

    // set global var! non-reentrant code
    currentExpansion = exp;

    entry = (DrillBeyondValues *) hash_search(exp->results_hashtable,
                                         (const void *) keys,
                                         HASH_ENTER,
                                         &found);
    if (!found) {
        entry->requested = false;
        entry->joinValues = keys;
    }
    // value 0 means just allocate entry, do not add anything
    if (values == NULL) {
        return;
    }
    entry->values = values;
    entry->numValues = numValues;
    entry->requested = true;
    entry->inUnion = true;
}

extern DrillBeyondValues* drb_retrieveFromHashTable(DrillBeyondExpansion *exp, Datum *keys)
{
    bool found;
    DrillBeyondValues *entry;

    // sets global state, non-reentrant!
    currentExpansion = exp;

    entry = (DrillBeyondValues *) hash_search(exp->results_hashtable,
                                         (const void *) keys,
                                         HASH_FIND,
                                         &found);
    return entry;
}

static uint32
drb_key_hash(const void *key, Size keysize)
{
    int i;
    Datum       *values = (Datum *) key;
    uint32      hashkey = 0;
    int num_join_cols = list_length(currentExpansion->join_cols);

    for (i = 0; i < num_join_cols; i++) {
        Datum d = values[i];
        hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);
        if (d != 0) {
            uint32 h;
            h = DatumGetUInt32(FunctionCall1(&(currentExpansion->hashFunctions)[i], d));
            hashkey ^= h;
        }
    }
    return hashkey;
}

/*
 * Matching function for elements, to be used in hashtable lookups.
 */
static int
drb_key_match(const void *key1, const void *key2, Size keysize)
{
    int i;
    DrillBeyondValues      *values1 = ((DrillBeyondValues *) key1);
    Datum       *values2 = ((Datum *) key2);
    int num_join_cols = list_length(currentExpansion->join_cols);

    for (i = 0; i < num_join_cols; i++) {
        Datum d1 = values1->joinValues[i];
        Datum d2 = values2[i];
        bool d1null = d1 == 0;
        bool d2null = d2 == 0;

        if (d1null != d2null)
        {
            return 1; // > 0 is no match
        }

        if (d1null)
            continue;           /* both are null, treat as equal */

        if (!DatumGetBool(FunctionCall2(
                &(currentExpansion->eqFunctions)[i],
                d1, d2)))
        {
            return 1;
        }
    }
    return 0; // 0 is match
}
