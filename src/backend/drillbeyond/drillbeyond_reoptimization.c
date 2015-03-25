    /*-------------------------------------------------------------------------
 *
 * drillbeyond_reoptimization.c
 *
 * IDENTIFICATION
 *    /drillbeyond/drillbeyond_reoptimization.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "drillbeyond/drillbeyond.h"
#include "optimizer/planner.h"
#include "optimizer/planmain.h"
#include "nodes/print.h"
#include "nodes/makefuncs.h"
#include "parser/parsetree.h"
#include "commands/explain.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"

static bool check_tl_subset(List *rt, Plan *plan, Plan *orig_plan);
static void fix_join_cols(DrillBeyond *drb);

extern PlannedStmt* reoptimize(Query *query) {
    PlannedStmt *pstmt = planner(query, 0, NULL);
    Plan *plan = pstmt->planTree;
    if (! (nodeTag(plan) == T_DrillBeyondExpand && ((DrillBeyondExpand*)plan)->drb_strategy == DRB_TOP )) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("Reoptimized query has no DRB_TOP node at top.")
            ));
    }
    // printf("news plan::::\n");
    // print_explanation((PlannedStmt*)copyObject(pstmt));
    // printf("------------------------\n");
    // return outerPlan(plan);
    return pstmt;
}

extern void execute_drb_fragment(DrillBeyondState *drb) {


}

void switch_plans(DrillBeyondExpandState *node) {
    DrillBeyondExpand *plan;
    List *original_subplanstates;
    List *original_operator_states;
    int i;

    plan = (DrillBeyondExpand *) node->ps.plan;

      // a reoptimization was triggered, switch nodes
    ListCell *l;
    EState* estate = node->ps.state;

    // print_explanation(estate->es_plannedstmt);

    PlanState *original_plan_state = outerPlanState(node);
    PlannedStmt *pstmt = plan->reoptimized_plan;
    plan->reoptimized_plan = NULL;
    estate->es_plannedstmt = pstmt;// correct??


    if (pstmt->nParamExec > 0)
        estate->es_param_exec_vals = (ParamExecData *)
        palloc0(pstmt->nParamExec * sizeof(ParamExecData));

    original_subplanstates = estate->es_subplanstates;
    estate->es_subplanstates = NIL;
    i = 1;
    foreach(l, pstmt->subplans)
    {
        Plan       *subplan = (Plan *) lfirst(l);
        PlanState  *subplanstate;
        int         sp_eflags;

        /*
         * A subplan will never need to do BACKWARD scan nor MARK/RESTORE. If
         * it is a parameterless subplan (not initplan), we suggest that it be
         * prepared to handle REWIND efficiently; otherwise there is no need.
         */
        sp_eflags = estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY; // correct?
        if (bms_is_member(i, pstmt->rewindPlanIDs))
            sp_eflags |= EXEC_FLAG_REWIND;

        subplanstate = ExecInitNode(subplan, estate, sp_eflags);

        estate->es_subplanstates = lappend(estate->es_subplanstates,
                                           subplanstate);

        i++;
    }

    outerPlan(plan) =  outerPlan(pstmt->planTree);
    outerPlanState(node) = ExecInitNode(outerPlan(plan), estate, node->original_eflags);

    /* ---------------------------------------------------------------- */
    /* REFACTOR with Init */
    original_operator_states = node->drb_operator_states;
    node->drb_operator_states = NIL;
    drb_collect_drb_operator_states(outerPlanState(node), &node->drb_operator_states);
    foreach(l, node->drb_operator_states) {
        DrillBeyondState *dbs = (DrillBeyondState*)lfirst(l);
        DrillBeyond *drb = (DrillBeyond*)dbs->js.ps.plan;
        drb->drb_topNode = plan;

        DrillBeyondState *orig_dbs = stateForExpansion(drb->drb_expansion, original_operator_states);
        dbs->db_num_cands = orig_dbs->db_num_cands;
        DrillBeyond *orig_plan = (DrillBeyond *)orig_dbs->js.ps.plan;

        bool tl_is_subset = check_tl_subset(estate->es_range_table, (Plan*)drb, (Plan*)orig_plan);
        if (tl_is_subset) {
            // we can reuse the tupstore
            dbs->db_fetchedResult = true;
            dbs->tuplestorestate = orig_dbs->tuplestorestate;
            orig_dbs->tuplestorestate = NULL;
            // drb->drb_join_cols = (List*)copyObject(orig_plan->drb_join_cols);
            drb->drb_join_cols = (List*)copyObject(drb->drb_expansion->join_cols);
            fix_join_cols(drb);
        } else {
            // scan below new DRB oper
            dbs->db_fetchedResult = false;
            dbs->tuplestorestate = NULL;
        }


        drb->drb_expansion->reoptimized = true;
        reconsiderStrategy(dbs);
    }

    /* cleanup old plan */
    foreach(l, original_subplanstates)
    {
        PlanState  *subplanstate = (PlanState *) lfirst(l);
        ExecEndNode(subplanstate);
    }
    ExecEndNode(original_plan_state);


    node->current_origin = 0;
    node->total_permutations = pow(drb_max_num_cands, list_length(node->drb_operator_states));
}

static void fix_join_cols(DrillBeyond *drb) {
    ListCell   *c, *c2;
    List *sub_tl = outerPlan(drb)->targetlist;
    foreach(c, drb->drb_join_cols) {
        Var *v = (Var *) lfirst(c);

        bool found = false;
        int t = 1;
        foreach(c2, sub_tl) {
            TargetEntry *sub_tle = (TargetEntry *) lfirst(c2);
            Var *sv = (Var *) sub_tle->expr;
            if (v->varoattno == sv->varoattno && v->varnoold == sv->varnoold) {
                v->varno = OUTER_VAR; //  always outer side for DrillBeyond
                v->varattno = t;
                found = true;
            }
            t++;
        }

        if (!found) {
            ereport(ERROR,
             (errcode(ERRCODE_DRILLBEYOND_RELNAME_PREFIX_NEEDED),
                errmsg("did not find Var %d-%d in new plan TL\n", v->varnoold, v->varoattno)
            ));
        }
    }
}

static bool check_tl_subset(List *rt, Plan *plan, Plan *orig_plan) {
    ListCell   *c, *c2;
    List *tl = plan->targetlist;
    List *orig_tl = orig_plan->targetlist;

    foreach(c, tl) {
        TargetEntry *tle = (TargetEntry *) lfirst(c);
        // TODO: hack for Q7
        if (nodeTag(tle->expr) != T_Var)
            continue;
        Var *v = (Var *)tle->expr;

        bool found = false;
        foreach(c2, orig_tl) {
            TargetEntry *orig_tle = (TargetEntry *) lfirst(c2);
            Var *ov = (Var *) orig_tle->expr;
            if (v->varoattno == ov->varoattno && v->varnoold == ov->varnoold) {
                found = true;
            }
        }

        if (!found) {
            return false;
        }
    }
    return true;
}

extern void print_explanation(PlannedStmt *pstmt) {
    QueryDesc *queryDesc = CreateQueryDesc(pstmt, "tempQueryString",
                                GetActiveSnapshot(), InvalidSnapshot,
                                NULL, NULL, 0);

    int eflags = EXEC_FLAG_EXPLAIN_ONLY;

    /* call ExecutorStart to prepare the plan for execution */
    ExecutorStart(queryDesc, eflags);

    ExplainState es;
    ExplainInitState(&es);
    es.pstmt = pstmt;
    es.rtable = pstmt->rtable;

    /* Create textual dump of plan tree */
    ExplainPrintPlan(&es, queryDesc);
    ExecutorEnd(queryDesc);
    FreeQueryDesc(queryDesc);

    printf("--------------------------------------------------\n");
    printf("%s\n",es.str->data);
    printf("--------------------------------------------------\n");
}
