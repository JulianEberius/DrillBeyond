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
#include "miscadmin.h"
#include "access/printtup.h"
#include "access/tupdesc.h"
#include "drillbeyond/drillbeyond.h"
#include "nodes/relation.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "parser/parsetree.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"

/*
 * final_cost_drillbeyond
 *    Final estimate of the cost and result size of a hashjoin pathnode.
 *
 * Note: the numbatches estimate is also saved into 'path' for use later
 *
 * 'path' is already filled in except for the rows and cost fields and
 *      num_batches
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'semifactors' contains valid data if path->jointype is SEMI or ANTI
 */
void
final_cost_drillbeyond(PlannerInfo *root, DrillBeyondPath *path,
                    SpecialJoinInfo *sjinfo,
                    SemiAntiJoinFactors *semifactors)
{
    Path * outer_path = path->jpath.outerjoinpath;
    Cost        startup_cost = outer_path->startup_cost;
    Cost        run_cost = outer_path->total_cost - outer_path->startup_cost;
    Cost        cpu_per_tuple = 0;
    QualCost    restrict_qual_cost;

    int i;
    int num_join_cols;
    DrillBeyondExpansion *expansion;
    VariableStatData vardata;
    Oid relId;
    bool isDefault;
    double distinct;
    double nrows;

    /* Mark the path with the correct row estimate */
    path->jpath.path.rows = path->jpath.path.rows;
    nrows = path->jpath.path.rows;

    expansion = sjinfo->drb_expansion;
    num_join_cols = list_length(expansion->join_cols);
    relId = planner_rt_fetch(expansion->extended_rti, root)->relid;
    distinct = 0;

    /* we also need to pay the price for the restrictions on the drillbeyond baserel,
    which is never evaluated on its own, but during execution of the DrbJoin */
    cost_qual_eval(&restrict_qual_cost, path->jpath.innerjoinpath->parent->baserestrictinfo, root);
    startup_cost += restrict_qual_cost.startup;
    cpu_per_tuple += restrict_qual_cost.per_tuple;

    for (i = 0; i < num_join_cols; i++) {
        Var *join_col = (Var*)list_nth(expansion->join_cols, i);
        examine_variable(root, (Node*)join_col, 0, &vardata);
        double d = get_variable_numdistinct(&vardata, &isDefault);
        distinct = distinct > d ? distinct : d;
        ReleaseVariableStats(vardata);
    }
    // printf("guessed number of distinct tuples for %s.%s: %f \n", expansion->extended_relname, expansion->keyword, distinct);

    double original_rows = root->simple_rel_array[expansion->extended_rti]->tuples;
    double rows_without_sel = outer_path->rows;
    double tupFrac = rows_without_sel / original_rows;
    if (tupFrac < 1.0)
        distinct *= tupFrac;
    // RelOptInfo *rel = path->jpath.path.parent;
    // Bitmapset *tmpset = bms_copy(rel->relids);
    // int index;
    // printf("sel at (");
    // while ((index = bms_first_member(tmpset)) > 0)
    // {
    //     printf(" %d",index);
    // }
    // printf(") is assumed to be %f (tupfrac %f, rows_without_sel %f / orig_rows %f)\n", distinct, tupFrac, rows_without_sel, original_rows);
    // bms_free(tmpset);
    // printf("calculated number of distinct tuples for %s.%s: %f \n", expansion->extended_relname, expansion->keyword, distinct);

    // if (nrows < distinct)
    //     distinct = nrows;


    startup_cost += nrows * cpu_operator_cost + 100*cpu_tuple_cost*distinct; // arbitrary constant for drb index lookup
    run_cost += cpu_per_tuple * nrows;


    if(drb_startup_cost != -1)
        startup_cost = outer_path->startup_cost + drb_startup_cost;      //set the costs with the SET operator
    if(drb_run_cost != -1)
        run_cost = outer_path->total_cost + drb_run_cost;

    // settable fixed factor for manually manipulating plans
    if (drb_fixed_cost != -1) {
        startup_cost = outer_path->startup_cost * drb_fixed_cost;      //set the costs with the SET operator
        run_cost = outer_path->total_cost * drb_fixed_cost;
    }

    path->jpath.path.startup_cost = startup_cost;
    path->jpath.path.total_cost = startup_cost + run_cost;
}

void
final_cost_drillbeyond_expand(PlannerInfo *root, DrillBeyondExpand *plan)
{
    // Path * outer_path = path->jpath.outerjoinpath;
    Plan *outerPlan = outerPlan(plan);
    Cost        startup_cost = outerPlan->startup_cost;
    Cost        run_cost = outerPlan->total_cost;
    Cost        cpu_per_tuple = cpu_tuple_cost;
    QualCost    restrict_qual_cost;
    DrillBeyondExpansion *expansion;
    double nrows;

    /* Mark the path with the correct row estimate */
    plan->plan.plan_rows = outerPlan->plan_rows;
    nrows = plan->plan.plan_rows;

    expansion = plan->drb_expansion;

    if (drb_enable_pull_up_selection) {
        cost_qual_eval(&restrict_qual_cost, expansion->drb_qual, root);
        startup_cost += restrict_qual_cost.startup;
        cpu_per_tuple += restrict_qual_cost.per_tuple;
    }

    run_cost += cpu_per_tuple * nrows;
    plan->plan.total_cost = startup_cost + run_cost;
    plan->plan.startup_cost = startup_cost;
}

extern double drillbeyond_estimate_join_size(PlannerInfo *root,
                           double outer_rows,
                           SpecialJoinInfo *sjinfo,
                           List *restrictlist) {
    double result = outer_rows;

    //TODO: COST MODEL IMPORTANT
    // printf("is_selective: %d\n", sjinfo->drb_expansion->selective);
    double sel;
    if (sjinfo->drb_expansion->selective) {
        if (sjinfo->drb_expansion->selectivity != -1.0) {
            // already determined in previous execution
            sel = sjinfo->drb_expansion->selectivity;
        }
        else {
            if (drb_enable_selectivity_estimation) {
                printf("guessing selectivity for %s.%s: ", sjinfo->drb_expansion->extended_relname, sjinfo->drb_expansion->keyword);
                RangeTblEntry *exrte = planner_rt_fetch(sjinfo->drb_expansion->extended_rti, root);
                RelOptInfo *ri = root->simple_rel_array[sjinfo->drb_expansion->rti];
                if (list_length(ri->baserestrictinfo) != 1) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("only one restriction allowed on open attribute for selectivity estimation")
                        ));
                }
                sel = estimateSelectivity(sjinfo->drb_expansion, exrte->relid, ri->baserestrictinfo);
                printf("%f\n", sel);
            }
            else {
                if (drb_selectivity == DBL_MAX) {
                    sel = DRB_DEFAULT_SEL;
                } else {
                    sel = drb_selectivity;
                }
                sjinfo->drb_expansion->selectivity = sel;
            }
        }
    }

    if (!drb_enable_big_omega || drb_enable_dynamic_omega || drb_enable_reoptimization) {
        if (sjinfo->drb_expansion->selective) {
            if (drb_cost_model == DRB_COST_ONLY_S || drb_cost_model == DRB_COST_BOTH) {
                result *= sel;
            }
        }

        if (drb_cost_model == DRB_COST_ONLY_K || drb_cost_model == DRB_COST_BOTH) {
            result *= drb_max_num_cands;
        }
    }

    // TODO: COST MODEL IMPORTANT INDEED?!
    // if (drb_cost_model == DRB_COST_ONLY_S || drb_cost_model == DRB_COST_BOTH) {
    //     if (sjinfo->drb_expansion->selective) {
    //         result *= sel;
    //         // printf("drb_sel: %f resultsize: %f\n", drb_sel, result);
    //     }
    // }
    if (drb_fixed_cost != -1)
        result = drb_fixed_cost;
    return clamp_row_est(result);
}

extern void cost_drillbeyond_expand(PlannerInfo *root, DrillBeyondExpand *expand) {
    Plan *outer_plan = expand->plan.lefttree;

    expand->plan.startup_cost = outer_plan->startup_cost;
    expand->plan.total_cost = outer_plan->total_cost;
    expand->plan.plan_rows = outer_plan->plan_rows;
    expand->plan.plan_width = outer_plan->plan_width;

    switch (expand->drb_strategy) {
        case DRB_EXPAND2:
            expand->plan.total_cost += (outer_plan->plan_rows * cpu_tuple_cost);
            break;
        case DRB_TOP:
            expand->plan.total_cost += outer_plan->plan_rows * cpu_tuple_cost;
            break;
        default:{
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("Compress/Decompress/Final/Expand1 not implemented anymore.")
                ));
            break;
        }
    }
}


