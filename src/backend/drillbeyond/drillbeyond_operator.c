/*-------------------------------------------------------------------------
 *
 * drillbeyond_operator.c
 *
 * IDENTIFICATION
 *    /drillbeyond/drillbeyond_operator.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "drillbeyond/drillbeyond.h"
#include "nodes/execnodes.h"
#include "miscadmin.h"
#include "nodes/plannodes.h"
#include "nodes/print.h"
#include "executor/executor.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/selfuncs.h"
#include "json/json.h"
#include "access/tupmacs.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/typcache.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"

static bool remove_mat_nodes(PlanState *state, List *addedMatNodes);

DrillBeyondState *ExecInitDrillBeyond(DrillBeyond *node, EState *estate, int eflags)
{
    DrillBeyondState *dbstate;
    Plan       *outerNode;
    ProjectionInfo *projInfo;
    DrillBeyondDummy *dummyNode;
    int k, i;

    /*
     * create state structure
     */
    dbstate = makeNode(DrillBeyondState);
    dbstate->js.ps.plan = (Plan *) node;
    dbstate->js.ps.state = estate;

    outerNode = outerPlan(node);
    dummyNode = (DrillBeyondDummy *) innerPlan(node);

    // drillbeyond need to scan the underlying plan twice
    // eflags |= EXEC_FLAG_REWIND;
    // underlying blabla
    eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);
    outerPlanState(dbstate) = ExecInitNode(outerNode, estate, eflags);
    innerPlanState(dbstate) = ExecInitNode((Plan *) dummyNode, estate, eflags);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &dbstate->js.ps);

    /*
     * initialize child expressions
     */
    dbstate->js.ps.targetlist = (List *)
        ExecInitExpr((Expr *) node->join.plan.targetlist,
                     (PlanState *) dbstate);
    dbstate->js.ps.qual = (List *)
        ExecInitExpr((Expr *) node->join.plan.qual,
                     (PlanState *) dbstate);
    dbstate->js.jointype = node->join.jointype;
    dbstate->js.joinqual = (List *)
        ExecInitExpr((Expr *) node->join.joinqual,
                     (PlanState *) dbstate);
    dbstate->db_qual = (List *)
        ExecInitExpr((Expr *) node->drb_expansion->drb_qual,
                     (PlanState *) dbstate);


    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &dbstate->js.ps);

    dbstate->db_InnerTupleSlot = innerPlanState(dbstate)->ps_ResultTupleSlot;
    for (k=0; k<dbstate->db_InnerTupleSlot->tts_tupleDescriptor->natts; k++) {
        dbstate->db_InnerTupleSlot->tts_isnull[k] = true;
    }
    dbstate->db_OuterTupleSlot = ExecInitExtraTupleSlot(estate);
    ExecSetSlotDescriptor(dbstate->db_OuterTupleSlot, ExecGetResultType(outerPlanState(dbstate)));

    /*
     * initialize tuple type and projection info
     */
    ExecAssignResultTypeFromTL(&dbstate->js.ps);
    ExecAssignProjectionInfo(&dbstate->js.ps, NULL);

    dbstate->js.ps.ps_TupFromTlist = false;
    dbstate->db_NeedNewOuter = true;
    dbstate->db_current_origin = 0;

    // find slot to put open value in from projection info
    projInfo = innerPlanState(dbstate)->ps_ProjInfo;
    for (i = 0; i < projInfo->pi_numSimpleVars; i++)
    {
        if (projInfo->pi_varNumbers[i] == 1) {
            dbstate->db_valueColIdx = i;
            break;
        }
    }


    dbstate->num_join_cols = list_length(node->drb_join_cols);
    dbstate->keys = (Datum *)palloc(sizeof(Datum) * dbstate->num_join_cols);

    //CONTEXT STUFF
    // reset_context();

    // materialization
    dbstate->tuplestorestate = NULL;

    return dbstate;
}

DrillBeyondDummyState *ExecInitDrillBeyondDummy(SeqScan *node, EState *estate, int eflags)
{
    DrillBeyondDummyState *dummyState;

    Assert(outerPlan(node) == NULL);
    Assert(innerPlan(node) == NULL);

    /*
     * create state structure
     */
    dummyState = makeNode(DrillBeyondDummyState);
    dummyState->ps.plan = (Plan *) node;
    dummyState->ps.state = estate;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &dummyState->ps);

    /*
     * initialize child expressions
     */
    dummyState->ps.targetlist = (List *)
        ExecInitExpr((Expr *) node->plan.targetlist,
                     (PlanState *) dummyState);
    dummyState->ps.qual = (List *)
        ExecInitExpr((Expr *) node->plan.qual,
                     (PlanState *) dummyState);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &dummyState->ps);

    dummyState->ps.ps_TupFromTlist = false;

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(&dummyState->ps);
    ExecAssignProjectionInfo(&dummyState->ps, NULL);

    return dummyState;

}

extern PlannedStmt* reconsiderStrategy(DrillBeyondState *node) {
    DrillBeyond *plan;
    DrillBeyondExpand *expandPlan;
    DrillBeyondExpansion *expansion;
    double cost, sel, score, big_omega_cost, avg_sel;
    double ARBITRARY_CORRECTION_FACTOR = 0.8;
    int i;


    plan = (DrillBeyond*) node->js.ps.plan;
    expandPlan = plan->drb_expandNode;
    expansion = plan->drb_expansion;

    if (expandPlan == NULL)
        return NULL;

    cost = sum_costs((Plan*)expandPlan, (Plan*)plan);
    // double startup_cost = 0.0;
    // double run_cost = 0.0;
    // sum_both_costs((Plan*)expandPlan, (Plan*)plan, &startup_cost, &run_cost);
    if (expansion->selective) {
        // cost for one full execution of the partial plan, so no selectivity
        if (drb_cost_model == DRB_COST_ONLY_S || drb_cost_model == DRB_COST_BOTH) {
            cost /= expansion->selectivity;
            // startup_cost /= expansion->selectivity;
            // run_cost /= expansion->selectivity;
        }
    }
    if (drb_cost_model == DRB_COST_ONLY_K || drb_cost_model == DRB_COST_BOTH) {
        cost /= drb_max_num_cands;
        // startup_cost /= drb_max_num_cands;
        // run_cost /= drb_max_num_cands;
    }

    // big_omega_cost = startup_cost + run_cost;// cost is now normalized to one run, with no selection
    // double def_cost = (startup_cost + drb_max_num_cands * run_cost)*expansion->selectivity;
    // printf("exp->sel %f\n", expansion->selectivity);
    // printf("startup_cost: %f, run_cost: %f\n", startup_cost, run_cost);
    // printf("est. def_cost %f\n", def_cost);
    // printf("est. bigomega cost: %f\n", big_omega_cost);
    // printf("single_costs:");
    big_omega_cost = cost ;//* (1/expansion->selectivity); // cost is now normalized to one run, with no selection
    // printf("cost: %f\n", cost);
    // printf("est. def_cost %f (cost * k)\n", cost*drb_max_num_cands*expansion->selectivity);
    // printf("est. bigomega cost: %f (cost * (1/sel))\n", cost);
    // printf("single_costs:");
    avg_sel = 0.0;
    score = 0.0;
    for (i = 0; i < node->db_num_cands; ++i)
    {
        sel = expansion->selectivities[i];
        // remove estimatead sel, add real sel
        score += cost * sel;
        // score += run_cost * sel; // OH NOES
        avg_sel += sel;
        // printf("%f[%f] ", run_cost * sel, sel);
        // printf("%f[%f] ", cost * sel, sel);
    }
    avg_sel /= node->db_num_cands;
    // printf("\nreal default mode total cost: %f. \n", score);

    // REAL DYNAMIC REOPTIMIZATION; JUST FOR FUN
    // ---------------------------------------------------------------------//
    bool shouldSwitch = score < big_omega_cost * ARBITRARY_CORRECTION_FACTOR;
    printf("score: %f, bigomega: %f -> %f should switch strategy: %s\n", score, big_omega_cost, score/big_omega_cost, shouldSwitch ? "yes" : "no");
    if (drb_enable_static_reoptimization) {
        // "static" means static at runtime, but dynamic at plantime
        if (shouldSwitch)
            expansion->selectivity = avg_sel;
        else if (drb_enable_preselection)
            expansion->selectivity = expansion->union_selectivity;
        else {
            expansion->selectivity = 1.0;
            expansion->selective = false;
        }
    } else {
        // more primitive replanning
        expansion->selectivity = avg_sel;
    }

    // if (drb_enable_reoptimization && !expansion->reoptimized) {
    //     PlannedStmt *reoptimized_plan = reoptimize(plan);
    //     if (reoptimized_plan) {
    //         // Cost original_cost = plan->drb_topNode->plan.total_cost;
    //         // Cost new_cost = reoptimized_plan->planTree->total_cost;
    //         // if (new_cost * ARBITRARY_CORRECTION_FACTOR < original_cost)
    //             return reoptimized_plan;
    //     }
    // }
    // ---------------------------------------------------------------------//


    if (!drb_enable_dynamic_omega)
        return NULL;

    if (shouldSwitch) {
        plan->drb_strategy = DRB_DEFAULT;
        expandPlan->drb_strategy = DRB_DEFAULT;
    }
    // if selection is pulled up, or just one candidate -> no mat
    if (!shouldSwitch || drb_max_num_cands == 1) {
        remove_mat_nodes((PlanState*)node->drb_expand_operator_state, plan->drb_addedMatNodes);
    }
    return NULL;
}

static bool remove_mat_nodes(PlanState *state, List *addedMatNodes) {
    if (state == NULL)
        return false;

    if (nodeTag(state) == T_MaterialState) {
        if (list_member_ptr(addedMatNodes, state)) {
            return true;
        }
    }
    if (remove_mat_nodes(innerPlanState(state), addedMatNodes)) {
        printf("REMOVING MAT STATE\n");
        innerPlanState(state) = outerPlanState(innerPlanState(state));
    }
    if (remove_mat_nodes(outerPlanState(state), addedMatNodes)) {
        printf("REMOVING MAT STATE\n");
        outerPlanState(state) = outerPlanState(outerPlanState(state));
    }
    return false;
}


TupleTableSlot *ExecDrillBeyond(DrillBeyondState *node)
{
    DrillBeyond *plan;
    PlanState   *innerPlan;
    PlanState   *outerPlan;
    TupleTableSlot *outerTupleSlot;
    TupleTableSlot *innerTupleSlot;
    List       *joinqual;
    List       *otherqual;
    List       *drb_quals;
    ExprContext *econtext, *inner_econtext;
    DrillBeyondExpansion *expansion;
    int i;
    int attno;
    Datum origattr;
    Datum value;
    bool isnull;

    /*
     * get information from the node
     */
    joinqual = node->js.joinqual;
    otherqual = node->js.ps.qual;
    plan = (DrillBeyond *) node->js.ps.plan;
    expansion = plan->drb_expansion;
    outerPlan = outerPlanState(node);
    innerPlan = innerPlanState(node);
    econtext = node->js.ps.ps_ExprContext;
    inner_econtext = innerPlan->ps_ExprContext;
    innerTupleSlot = node->db_InnerTupleSlot;
    drb_quals = node->db_qual;

    if (!node->db_fetchedResult) {
        json_object *msg = NULL;                                        //request data
        const char *msg_str;
        int request_err;
        bool req_necessary;
        // phase 1: collect all tuples
		if (msg == NULL) {
			msg = initDrillBeyondRequest(plan->drb_expansion);
            add_restrictions_to_msg(msg, plan->drb_expansion->drb_qual);                        //add qualisfiers (?)
        }

        if (node->tuplestorestate == NULL) {
            node->tuplestorestate = tuplestore_begin_heap(false, false, work_mem);
            tuplestore_set_eflags(node->tuplestorestate, EXEC_FLAG_REWIND);
        }

        for (;;)
        {
            // printf("scanning outer plan, filling tuplestore\n");
            outerTupleSlot = ExecProcNode(outerPlan);                   //exec node to get tuple
            if (TupIsNull(outerTupleSlot))
                break;
            tuplestore_puttupleslot(node->tuplestorestate, outerTupleSlot);
            tup_to_json(plan->drb_expansion, plan->drb_join_cols, outerTupleSlot, msg);      //build request data
        }

        req_necessary = drillbeyond_fill_msg(plan->drb_expansion, msg); //look for open attributes (?)

        if (req_necessary) {                                            //open attributes available -> request
            msg_str = json_object_to_json_string_ext(msg, JSON_C_TO_STRING_PLAIN);  //convert request data to string
            request_err = drillbeyond_request(msg_str, node);//drillbeyond_request(msg_str, node);           //do the request
            if (request_err) {
                 ereport(ERROR,
                    (errcode(ERRCODE_DRILLBEYOND_REQUEST_FAILED),
                        errmsg("Can't get a response from DrillBeyond server")
                    ));
            }
            json_object_put(msg); // refcounting: "put" is the strange name for "release" that libjson uses
        }
        node->db_fetchedResult = true;

        tuplestore_rescan(node->tuplestorestate);

        // now that selectivities are known, reconsider the strategy (PLACEHOLDER VS. DEFAULT)
        PlannedStmt *reoptimized_plan = reconsiderStrategy(node);
        if (reoptimized_plan) {
            // plan->drb_topNode->reoptimized_plan = reoptimized_plan;
            return NULL; // end execution of this plan
        }
    }


    // phase two: add new field to input tuples
    /*
     * Check to see if we're still projecting out tuples from a previous join
     * tuple (because there is a function-returning-set in the projection
     * expressions).  If so, try to project another one.
     */
    if (node->js.ps.ps_TupFromTlist)
    {
        TupleTableSlot *result;
        ExprDoneCond isDone;

        result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);
        if (isDone == ExprMultipleResult)
            return result;
        /* Done with that source tuple... */
        node->js.ps.ps_TupFromTlist = false;
    }
    ResetExprContext(econtext);

    for (;;)
    {
        /*
         * If we don't have an outer tuple, get the next one and reset the
         * inner scan.
         */
        if (node->db_NeedNewOuter)
        {
            // outerTupleSlot = ExecProcNode(outerPlan);
            // ExecClearTuple(outerTupleSlot);
            outerTupleSlot = node->db_OuterTupleSlot;
            ExecClearTuple(outerTupleSlot);
            tuplestore_gettupleslot(node->tuplestorestate, true, false, outerTupleSlot);

            /*
             * if there are no more outer tuples, then the join is complete..
             */
            if (TupIsNull(outerTupleSlot))
            {
                return NULL;
            }
            econtext->ecxt_outertuple = outerTupleSlot;

            // printf("new outer tuple\n");
            // print_slot(outerTupleSlot);
            /*
             * we have an outerTuple, try to get the next inner tuple.
             * first hash the outertuples join attributes
             */
            for (i = 0; i < node->num_join_cols; i++) {
                Var *join_col_var = (Var*) list_nth(plan->drb_join_cols, i);
                attno = join_col_var->varattno;
                origattr = slot_getattr(outerTupleSlot, attno, &isnull);
                if (isnull)
                    node->keys[i] = 0;
                else
                    node->keys[i] = origattr;
            }
            node->db_current_values = drb_retrieveFromHashTable(expansion, node->keys);
            // preselection of those values that are outside the union of candidates
            if (drb_enable_preselection) {
                if (!node->db_current_values->inUnion) {
                    continue; // get next outer tuple
                }
            }
            node->db_NeedNewOuter = false;
        }

        econtext->ecxt_innertuple = innerTupleSlot;
        inner_econtext->ecxt_innertuple = innerTupleSlot;

        ExecClearTuple(innerTupleSlot);
        if (plan->drb_strategy == DRB_DEFAULT) {
            isnull = node->db_current_values->is_null[node->db_current_origin];
            value = node->db_current_values->values[node->db_current_origin];
        } else if (plan->drb_strategy == DRB_PLACEHOLDER) {
            isnull = false;
            // value = PointerGetDatum(node->db_current_values); // EVIL HACK
            // value = node->db_current_values->values[node->db_current_origin];
            // value = node->db_current_values->context;
            Datum ptr = PointerGetDatum(node->db_current_values);
            value = DirectFunctionCall1(int8_numeric, ptr);
        }
        innerTupleSlot->tts_isnull[node->db_valueColIdx] = isnull; // ltype is never null
        innerTupleSlot->tts_values[node->db_valueColIdx] = value;
        ExecStoreVirtualTuple(innerTupleSlot);

        // node->db_current_origin++;
        // if (node->db_current_origin == node->db_num_cands) {
        //     node->db_current_origin = 0;
        //     node->db_NeedNewOuter = true;
        // }

        node->db_NeedNewOuter = true;

        // printf("new inner tuple\n");
        // print_slot(innerTupleSlot);

        if (otherqual == NIL || ExecQual(otherqual, econtext, false))
        {
            if ((drb_enable_pull_up_selection && plan->drb_strategy == DRB_PLACEHOLDER) || drb_quals == NIL || ExecQual(drb_quals, inner_econtext, false)) {
                /*
                 * qualification was satisfied so we project and return the
                 * slot containing the result tuple using ExecProject().
                 */
                TupleTableSlot *result;
                ExprDoneCond isDone;

                result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);
                // printf("new result tuple in %s\n", plan->drb_expansion->keyword);
                // print_slot(result);

                if (isDone != ExprEndResult)
                {
                    node->js.ps.ps_TupFromTlist =
                        (isDone == ExprMultipleResult);
                    return result;
                }
            }
            ResetExprContext(inner_econtext);
        }
        ResetExprContext(econtext);
    }
}

void ExecEndDrillBeyond(DrillBeyondState *node)
{
    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * clean out the tuple table
     */
    ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

    if (node->tuplestorestate != NULL)
        tuplestore_end(node->tuplestorestate);
    node->tuplestorestate = NULL;


    /*
     * close down subplans
     */
    ExecEndNode(outerPlanState(node));
    ExecEndNode(innerPlanState(node));
}

void ExecEndDrillBeyondDummy(DrillBeyondDummyState *node) {
    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->ps);

    /*
     * clean out the tuple table
     */
    ExecClearTuple(node->ps.ps_ResultTupleSlot);
}

void ExecReScanDrillBeyond(DrillBeyondState *node)
{
    PlanState  *outerPlan = outerPlanState(node);

    node->db_NeedNewOuter = true;
    // a drb_reset leaves subnodes untouched and does not fetch new results
    // printf("Drillbeyond (%s) Rescan!\n", db->drb_expansion->keyword);
    if (bms_is_member(128, node->js.ps.chgParam)) {
        // printf("Drillbeyond (%s) Rescan by DRB mechanism (now candiate %d)!\n", db->drb_expansion->keyword, node->db_current_origin);
        if (bms_is_member(128, outerPlan->chgParam)) {
            // printf("  Deeper plan rescans!\n");
            tuplestore_clear(node->tuplestorestate);
            node->db_fetchedResult = false;
        } else {
            // printf("  Deeper plans are invariant!\n");
            tuplestore_rescan(node->tuplestorestate);
        }
        return;
    }
    // printf("Drillbeyond (%s) Rescan by normal mechanism!\n", db->drb_expansion->keyword);
    node->db_fetchedResult = false;
    if (node->tuplestorestate != NULL)
        tuplestore_clear(node->tuplestorestate);

    // normal rescan, e.g. in subquery
    // What does the above mean? Don't know anymore
    if (outerPlan->chgParam == NULL) {
        ExecReScan(outerPlan);
    }
}
