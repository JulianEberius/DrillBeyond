/*-------------------------------------------------------------------------
 *
 * drillbeyond_compress.c
 *
 * IDENTIFICATION
 *    /drillbeyond/drillbeyond_compress.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "drillbeyond/drillbeyond.h"
#include "catalog/pg_type.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "nodes/print.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_oper.h"
#include "executor/executor.h"
#include "executor/executor.h"
#include "optimizer/var.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "json/json.h"
#include "access/tupmacs.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/typcache.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/tuplestore.h"
#include "utils/tuplesort.h"


static TupleTableSlot *drb_top(DrillBeyondExpandState *node);
static TupleTableSlot *drb_expand2(DrillBeyondExpandState *node);

static void drb_collect_drb_mat_states(PlanState *planState, List *mat_plans, List **mat_states);
static bool drb_is_variable(PlanState *planState);
static bool drb_set_chg_param(PlanState *planState, Bitmapset *resetOps);
static bool drb_has_variable_subplanstate(PlanState *planState);
static void drb_ensure_rewind_enabled(PlanState *planState);
static void drb_enable_rewind(PlanState *planState);

extern DrillBeyondExpandState *ExecInitDrillBeyondExpand(DrillBeyondExpand *node, EState *estate, int eflags) {
    DrillBeyondExpandState *dbstate;
    Plan       *outerNode;

    /*
     * create state structure
     */
    dbstate = makeNode(DrillBeyondExpandState);
    dbstate->ps.plan = (Plan *) node;
    dbstate->ps.state = estate;

    outerNode = outerPlan(node);

    // drillbeyond need to scan the underlying plan twice
    eflags |= EXEC_FLAG_REWIND;
    dbstate->original_eflags = eflags;
    outerPlanState(dbstate) = ExecInitNode(outerNode, estate, eflags);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &dbstate->ps);

    /*
     * initialize child expressions
     */
    dbstate->ps.targetlist = (List *)
        ExecInitExpr((Expr *) node->plan.targetlist,
                     (PlanState *) dbstate);
    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &dbstate->ps);

    /*
     * initialize tuple type and projection info
     */
    ExecAssignResultTypeFromTL(&dbstate->ps);
    ExecAssignProjectionInfo(&dbstate->ps, NULL);

    dbstate->drb_operator_states = NIL;
    drb_collect_drb_operator_states(outerPlanState(dbstate), &dbstate->drb_operator_states);

    if (node->drb_strategy == DRB_TOP) {
        ListCell *lc;

        foreach(lc, dbstate->drb_operator_states) {
            DrillBeyondState *dbs = (DrillBeyondState*)lfirst(lc);
            ((DrillBeyond*)dbs->js.ps.plan)->drb_topNode = node;
        }

        dbstate->current_origin = 0;
        // if (list_length(dbstate->drb_operator_states) == 1)
            // dbstate->total_permutations = drb_max_num_cands;
        // else if (drb_max_num_cands == 1)
        //     dbstate->total_permutations = 1;
        // else
        dbstate->total_permutations = pow(drb_max_num_cands, list_length(dbstate->drb_operator_states));
        dbstate->fragments_executed = false;

        if (drb_enable_rewind_cache) {
            drb_ensure_rewind_enabled(outerPlanState(dbstate));
        }
    }
    else if (node->drb_strategy == DRB_EXPAND2) {
        //materialization
        DrillBeyond *drbPlan;
        List *matplanStates = NIL;
        dbstate->tupstore = tuplestore_begin_heap(false, false, work_mem);
        dbstate->materialized = false;
        dbstate->needNewContext = true;
        dbstate->intermediate_slot = ExecInitExtraTupleSlot(estate);
        ExecSetSlotDescriptor(dbstate->intermediate_slot,
                          ExecGetResultType((PlanState *) dbstate));

        // set pointers between small and big omega
        DrillBeyondState *drbState = stateForExpansion(node->drb_expansion, dbstate->drb_operator_states);
        dbstate->drb_operator_state = drbState;
        drbState->drb_expand_operator_state = dbstate;


        // TODO EVIL HACK!!
        drbPlan = (DrillBeyond*)dbstate->drb_operator_state->js.ps.plan;
        drb_collect_drb_mat_states((PlanState*)dbstate, drbPlan->drb_addedMatNodes, &matplanStates);
        drbPlan->drb_addedMatNodes = matplanStates;

        dbstate->drb_quals = (List *)
            ExecInitExpr((Expr *)
                    node->plan.qual,
                    (PlanState *) dbstate);
    }

    dbstate->needNewOuter = true;
    dbstate->ps.ps_TupFromTlist = false;
    return dbstate;
}

extern TupleTableSlot *ExecDrillBeyondExpand(DrillBeyondExpandState *node) {
    TupleTableSlot *result;

    switch (((DrillBeyondExpand *)node->ps.plan)->drb_strategy) {
        case DRB_TOP:
            result = drb_top(node);
            break;
        case DRB_EXPAND2:
            result = drb_expand2(node);
            break;
        case DRB_DEFAULT:
            {
                PlanState *outerNode = outerPlanState(node);
                result = ExecProcNode(outerNode);
            }
            break;
        case DRB_EXPAND:
        case DRB_PLACEHOLDER:
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("DRB_PLACEHOLDER/DRB_EXPAND not allowed here.")
                    ));
            result = NULL;
            break;
    }

    return result;
}


extern void ExecReScanDrillBeyondExpand(DrillBeyondExpandState *node) {
    DrillBeyondExpand *plan = (DrillBeyondExpand *) node->ps.plan;
    PlanState  *outerPlan = outerPlanState(node);

    node->needNewOuter = true;
    node->ps.ps_TupFromTlist = false;
    if (plan->drb_strategy == DRB_EXPAND2) {
        // TODO do we need to handle "normal" rescans here? maybe
        if (bms_is_member(128, node->ps.chgParam)) {
            // printf("DrillbeyondExpand (%s) Rescan by DRB mechanism (now candiate %d)!\n", plan->drb_expansion->keyword, node->drb_operator_state->db_current_origin);
            node->current_origin += 1;
            if (bms_is_member(128, outerPlan->chgParam)) {
                tuplestore_clear(node->tupstore);
            } else {
                // printf("  Deeper plans are invariant!\n");
                node->materialized = true;
                tuplestore_rescan(node->tupstore);
            }
        }
        else {
            // printf("DrillbeyondEXPAND (%s) Rescan by normal mechanism\n" , plan->drb_expansion->keyword);
            tuplestore_clear(node->tupstore);
        }
    }
    if (plan->drb_strategy == DRB_DEFAULT) {
        node->current_origin += 1;
    }
}

extern void ExecEndDrillBeyondExpand(DrillBeyondExpandState *node){
    ExecFreeExprContext(&node->ps);
    ExecClearTuple(node->ps.ps_ResultTupleSlot);
    ExecEndNode(outerPlanState(node));

    if (node->tupstore != NULL)
        tuplestore_end(node->tupstore);
}

extern DrillBeyondState* stateForExpansion(DrillBeyondExpansion *dbe, List *states) {
    ListCell *c;
    DrillBeyondState *dbs;
    foreach(c, states) {
        dbs = (DrillBeyondState *) lfirst(c);
        DrillBeyond *plan = (DrillBeyond*)dbs->js.ps.plan;
        if (plan->drb_expansion == dbe)
            break;
    }
    return dbs;
}

static TupleTableSlot *drb_top(DrillBeyondExpandState *node) {
    PlanState  *outerNode;
    DrillBeyondExpand *plan;
    TupleTableSlot *outerslot;
    TupleTableSlot *resultslot;
    Datum      *values;
    bool       *isnulls;
    bool       isnull;
    int i;

    plan = (DrillBeyondExpand *) node->ps.plan;

    if (drb_enable_reoptimization && !node->fragments_executed) {
        ListCell *c;
        Query *q;
        foreach(c, node->drb_operator_states) {
            DrillBeyondState *dbstate = (DrillBeyondState *) lfirst(c);
            DrillBeyond *drbplan = (DrillBeyond *)dbstate->js.ps.plan;
            // printf("before running fragment sel was: %f\n",drbplan->drb_expansion->selectivity);
            ExecProcNode((PlanState*)dbstate);

            //"manual" rescan
            dbstate->db_NeedNewOuter = true;
            tuplestore_rescan(dbstate->tuplestorestate);
            // dbstate->js.ps.chgParam = bms_add_member(dbstate->js.ps.chgParam, 128); //T
            // ExecReScan((PlanState *)dbstate);

            // printf("after running fragment sel is: %f\n",drbplan->drb_expansion->selectivity);
            q = drbplan->drb_expansion->query; // should be same for all expansions
        }
        node->fragments_executed = true;

        // now, we can consider switching plans
        plan->reoptimized_plan = reoptimize(q);
        outerslot = NULL;
    //     if (reoptimized_plan) {
    //         // Cost original_cost = plan->drb_topNode->plan.total_cost;
    //         // Cost new_cost = reoptimized_plan->planTree->total_cost;
    //         // if (new_cost * ARBITRARY_CORRECTION_FACTOR < original_cost)
    //             return reoptimized_plan;
    }
    else {
        outerNode = outerPlanState(node);
        outerslot = ExecProcNode(outerNode);
    }

    // TODO: big refactoring
    if (TupIsNull(outerslot) && plan->reoptimized_plan) {
        switch_plans(node);
        if (drb_enable_rewind_cache) {
            drb_ensure_rewind_enabled(outerPlanState(node));
        }

        // start new plan
        outerNode = outerPlanState(node);
        outerslot = ExecProcNode(outerNode);
    }

    // while (TupIsNull(outerslot) && (--node->total_permutations > 0) && (node->current_origin < drb_max_num_cands))
    while (TupIsNull(outerslot) && (--node->total_permutations > 0))
    {


        // printf("======================================================================\n");

        // expand more than one attribute
        for (i = 0; i < plan->drb_numExpansions; i++)
         {
            DrillBeyondExpansion *dbex = (DrillBeyondExpansion*)list_nth(plan->drb_all_expansions, i);
            DrillBeyondState *state = stateForExpansion(dbex, node->drb_operator_states);
            // printf("Finished: %d%c", state->db_current_origin, i == list_length(node->drb_operator_states) - 1 ? '\n' : ' ');
        }
        Bitmapset *resetOps = NULL;
        for (i = 0; i < list_length(node->drb_operator_states); i++) {
            DrillBeyondState* state = (DrillBeyondState *)list_nth(node->drb_operator_states, i);
            DrillBeyond *dbplan = (DrillBeyond*)state->js.ps.plan;
            if (state->db_current_origin >= drb_max_num_cands - 1) {
                state->db_current_origin = 0;
                resetOps = bms_add_member(resetOps, dbplan->drb_expansion->rti);
            } else {
                state->db_current_origin++;
                resetOps = bms_add_member(resetOps, dbplan->drb_expansion->rti);
                break;
            }
        }
        node->current_origin += 1; // TODO: individual operators
        for (i = 0; i < plan->drb_numExpansions; i++)
         {
            DrillBeyondExpansion *dbex = (DrillBeyondExpansion*)list_nth(plan->drb_all_expansions, i);
            DrillBeyondState *state = stateForExpansion(dbex, node->drb_operator_states);
            // printf("Next: %d%c", state->db_current_origin, i == list_length(node->drb_operator_states) - 1 ? '\n' : ' ');
        }
        // printf("======================================================================\n");
        drb_set_chg_param(outerNode, resetOps);
        ExecReScan(outerNode);
        outerslot = ExecProcNode(outerNode);
    }

    if (TupIsNull(outerslot))
        return NULL;

    // resultslot = outerslot;
    resultslot = node->ps.ps_ResultTupleSlot;
    values = resultslot->tts_values;
    isnulls = resultslot->tts_isnull;

    ExecClearTuple(resultslot);
    for (i=0;i<outerslot->tts_tupleDescriptor->natts;i++) {
        values[i] = slot_getattr(outerslot, i+1, &isnull);
        isnulls[i] = isnull;
    }

    for (i=0; i<list_length(plan->drb_all_expansions);i++) {
        DrillBeyondExpansion *dbex = (DrillBeyondExpansion *)list_nth(plan->drb_all_expansions, i);
        DrillBeyondState *dbe = stateForExpansion(dbex, node->drb_operator_states);
        int to = plan->drb_expandTo[i];
        values[to] = Int64GetDatum(dbe->db_current_origin);
        isnulls[to] = false;
    }
    ExecStoreVirtualTuple(resultslot);

    return resultslot;
}

static TupleTableSlot *drb_expand2(DrillBeyondExpandState *node) {
    PlanState  *outerNode;
    DrillBeyondExpand *plan;
    DrillBeyondState *dbe;
    TupleTableSlot *outerslot;
    TupleTableSlot *resultslot;
    ProjectionInfo *projInfo;
    List *drb_quals;
    ExprContext *econtext;
    Datum      *values;
    bool       *isnulls;
    bool       isnull;
    int i;

    projInfo = node->ps.ps_ProjInfo;

    plan = (DrillBeyondExpand *) node->ps.plan;
    outerNode = outerPlanState(node);
    dbe = node->drb_operator_state;
    drb_quals = node->drb_quals;
    econtext = node->ps.ps_ExprContext;
    outerslot = node->intermediate_slot;

    for (;;)
    {
        if (node->materialized && plan->drb_strategy == DRB_EXPAND2) {
            outerslot = node->intermediate_slot;
            ExecClearTuple(outerslot);
            tuplestore_gettupleslot(node->tupstore, true, true, outerslot);
        }
        else {
            outerslot = ExecProcNode(outerNode);
            if (!TupIsNull(outerslot))
                tuplestore_puttupleslot(node->tupstore, outerslot);
        }

        ResetExprContext(econtext);
        if (TupIsNull(outerslot))
        {
            node->materialized = true;
            return NULL;
        }

        // strategy may change at runtime!
        if (plan->drb_strategy == DRB_DEFAULT) {
            return outerslot;
        }

        resultslot = node->ps.ps_ResultTupleSlot;
        values = resultslot->tts_values;
        isnulls = resultslot->tts_isnull;

        ExecClearTuple(resultslot);
        for (i=0;i<outerslot->tts_tupleDescriptor->natts;i++)
        {
            values[i] = slot_getattr(outerslot, i+1, &isnull);
            isnulls[i] = isnull;
        }
        // expand more than one attribute
        //  {
        int from = plan->drb_expandFrom[0];
        int to = plan->drb_expandTo[0];
        Datum ptrDatum = values[from];
        //hack: convert NUMERIC into pointer
        DrillBeyondValues *vals = (DrillBeyondValues *) DirectFunctionCall1(numeric_int8, ptrDatum);
        values[to] = vals->values[dbe->db_current_origin];
        isnulls[to] = false;
         // }
        ExecStoreVirtualTuple(resultslot);

        econtext->ecxt_outertuple = resultslot;
        if (!drb_enable_pull_up_selection || drb_quals == NIL || ExecQual(drb_quals, econtext, false)) {
            return resultslot;
        }
    }
    // return resultslot;
}

static const char *drb_strategy_strings[] = { "ω expand","top","default","placeholder","Ω expand2","none"};
extern const char *drb_strategyname(enum DrillBeyondStrategy f)
{
    return drb_strategy_strings[f];
}

extern void drb_collect_drb_operator_states(PlanState *planState, List **drb_ops)
{
    ListCell *c;
    if (planState == NULL)
        return;

    if (nodeTag(planState) == T_DrillBeyondState) {
        DrillBeyondState *expand = (DrillBeyondState *)planState;
        // *drb_ops = lcons(expand, *drb_ops);
        *drb_ops = lappend(*drb_ops, expand);
    }

    drb_collect_drb_operator_states(outerPlanState(planState), drb_ops);
    drb_collect_drb_operator_states(innerPlanState(planState), drb_ops);
    foreach(c, planState->subPlan) {
        SubPlanState *spState = (SubPlanState *) lfirst(c);
        drb_collect_drb_operator_states(spState->planstate, drb_ops);
    }
}

static bool drb_is_variable(PlanState *planState)
{
    if (planState == NULL)
        return false;

    if (nodeTag(planState) == T_DrillBeyondState) {
        return true;
    }

    bool result = false;
    result = result || drb_is_variable(outerPlanState(planState));
    result = result || drb_is_variable(innerPlanState(planState));
    result = result || drb_has_variable_subplanstate(planState);
    return result;
}

static bool drb_has_variable_subplanstate(PlanState *planState)
{
    ListCell *c;
    bool result = false;
    foreach(c, planState->subPlan) {
        SubPlanState *spState = (SubPlanState *) lfirst(c);
        result = result || drb_is_variable(spState->planstate);
    }
    return result;
}

static bool _drb_set_chg_param(PlanState *planState, Bitmapset *resetOps, bool expandFound) {
    ListCell *c;
    bool drb_operator_found = false;

    if (planState == NULL)
        return false;
    // printf("Considerig state: %s\n", nodeName(planState->plan));

    if (nodeTag(planState) == T_DrillBeyondState) {
    // } &&
            // !expandFound) { // TODO: not correct
        // always set chgParam for DrillBeyondNodes!
        // planState->chgParam = bms_add_member(planState->chgParam, 128); //TODO: evil constant
        DrillBeyond *dbplan = (DrillBeyond*)planState->plan;
        if (bms_is_member(dbplan->drb_expansion->rti, resetOps))
        {
            resetOps = bms_del_member(resetOps, dbplan->drb_expansion->rti);
            drb_operator_found = true;
        }
        // return true;
    }
    if (nodeTag(planState) == T_DrillBeyondExpandState) {
        // always set chgParam for DrillBeyondNodes!
        // planState->chgParam = bms_add_member(planState->chgParam, 128); //TODO: evil constant
        DrillBeyondExpand *dbplan = (DrillBeyondExpand*)planState->plan;
        if (dbplan->drb_strategy!=DRB_DEFAULT && // default just passed through
             bms_is_member(dbplan->drb_expansion->rti, resetOps))
        {
            resetOps = bms_del_member(resetOps, dbplan->drb_expansion->rti);
            drb_operator_found = true;
        }
        // expandFound = true;
        // return true;
    }

    drb_operator_found = _drb_set_chg_param(outerPlanState(planState), resetOps, expandFound) || drb_operator_found;
    drb_operator_found = _drb_set_chg_param(innerPlanState(planState), resetOps, expandFound) || drb_operator_found;
    foreach(c, planState->subPlan) {
        SubPlanState *spState = (SubPlanState *) lfirst(c);
        // printf("checking subplan\n");
        bool foundInSubplan = _drb_set_chg_param(spState->planstate, resetOps, expandFound);
        drb_operator_found = drb_operator_found || foundInSubplan;
        // if (foundInSubplan)
            // printf("found in subplan!\n");
    }

    if (drb_operator_found) {
        planState->chgParam = bms_add_member(planState->chgParam, 128); //TODO: evil constant
        // printf("setting chg_param in %s\n", nodeName(planState->plan));
    }
    return drb_operator_found;
}

static bool drb_set_chg_param(PlanState *planState, Bitmapset *resetOps) {
    return _drb_set_chg_param(planState, resetOps, false);
}

static void drb_ensure_rewind_enabled(PlanState *planState) {
    if (planState == NULL)
        return;
    // printf("EnsureRewind: Considerig state: %s\n", nodeName(planState->plan));

    // if (drb_has_variable_subplanstate(planState)) {
        // printf("EnsureRewind: Setting rewind to: %s and %s\n",
        //     nodeName(innerPlanState(planState)->plan),
        //     nodeName(outerPlanState(planState)->plan));
        // drb_enable_rewind(innerPlanState(planState));
        // drb_enable_rewind(outerPlanState(planState));
    // }
    drb_enable_rewind(planState);

    drb_ensure_rewind_enabled(innerPlanState(planState));
    drb_ensure_rewind_enabled(outerPlanState(planState));
}

static void drb_enable_rewind(PlanState *planState)
{
   switch (nodeTag(planState->plan))
    {
        case T_Material:
            {
                MaterialState *ms = (MaterialState *)planState;
                ms->eflags |= EXEC_FLAG_REWIND;
            }
            break;
        case T_FunctionScan:
            {
                FunctionScanState *fs = (FunctionScanState *)planState;
                fs->eflags |= EXEC_FLAG_REWIND;
            }
            break;
        case T_CteScan:
            // always REWINDs
            break;
        case T_WorkTableScan:
            //unclear atm
            break;
        case T_Sort:
            {
                SortState *ss = (SortState *)planState;
                ss->randomAccess = true;
            }

        default:
            break;
    }
}


static void drb_collect_drb_mat_states(PlanState *planState, List *mat_plans, List **mat_states) {
    ListCell *c;
    if (planState == NULL)
        return;

    if (nodeTag(planState) == T_MaterialState) {
        Plan *matplan = planState->plan;
        if (list_member_ptr(mat_plans, matplan)) {
           *mat_states = lappend(*mat_states, planState);
        }
    }

    drb_collect_drb_mat_states(outerPlanState(planState), mat_plans, mat_states);
    drb_collect_drb_mat_states(innerPlanState(planState), mat_plans, mat_states);
    foreach(c, planState->subPlan) {
        SubPlanState *spState = (SubPlanState *) lfirst(c);
        drb_collect_drb_mat_states(spState->planstate, mat_plans, mat_states);
    }
}
