#include "postgres.h"

#include <ctype.h>

#include "lib/stringinfo.h"
#include "drillbeyond/drillbeyond.h"
#include "nodes/plannodes.h"
#include "nodes/print.h"
#include "nodes/execnodes.h"
#include "nodes/relation.h"
#include "utils/datum.h"

static void
_printDepth(int depth) {
    int i;
    for (i=0;i<depth;i++) {
        printf("\t");
    }
}

static void
_outNode(Plan *plan, List *rtable, int depth) {
    ListCell *tl;
    List *tlist;

    _printDepth(depth);
    tlist = plan->targetlist;
    printf("(%s): ", nodeName(plan));
    foreach(tl, tlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(tl);
        // printf("\t%d %s\t", tle->resno,
        //        tle->resname ? tle->resname : "<null>");
        // if (tle->ressortgroupref != 0)
        //     printf("(%u):\t", tle->ressortgroupref);
        // else
        //     printf("    :\t");
        print_expr((Node *) tle->expr, rtable);
        printf(" | ");
    }
    if (plan->lefttree != NULL) {
        printf("\n");
        _outNode(plan->lefttree, rtable, depth+1);
    }
    if (plan->righttree != NULL) {
        printf("\n");
        _outNode(plan->righttree, rtable, depth+1);
    }
}

/*
 * nodeToString -
 *     returns the ascii representation of the Node as a palloc'd string
 */
extern void
drb_printTL(Plan *plan, List *rtable)
{
    /* see stringinfo.h for an explanation of this maneuver */
    _outNode(plan, rtable, 0);
    printf("\n\n");
}
