#include "postgres.h"
#include "postgres.h"
#include "postgres_ext.h"
#include "commands/vacuum.h"
#include "parser/parse_coerce.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/rel.h"
#include "drillbeyond/drillbeyond.h"


int drillbeyond_sample_rel(Oid relid, int targrows, HeapTuple **rows, TupleDesc *tupDesc) {
    int numrows;
    double      totalrows,
                totaldeadrows;
    Relation onerel;
    BlockNumber relpages = 0;

    onerel = try_relation_open(relid, ShareUpdateExclusiveLock);
    if (!onerel)
        return 0;
    /* Also get regular table's size */
    relpages = RelationGetNumberOfBlocks(onerel);

    *rows = (HeapTuple *) palloc(targrows * sizeof(HeapTuple));
    *tupDesc = onerel->rd_att;
    numrows = acquire_sample_rows(onerel, DEBUG2,
                                  *rows, targrows,
                                  &totalrows, &totaldeadrows);

    relation_close(onerel, NoLock);
    return numrows;
}
