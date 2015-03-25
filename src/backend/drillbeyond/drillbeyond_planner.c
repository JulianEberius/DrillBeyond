#include "postgres.h"
#include "miscadmin.h"
#include "drillbeyond/drillbeyond.h"
#include "catalog/pg_type.h"
#include "optimizer/pathnode.h"
#include "optimizer/var.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "optimizer/cost.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "parser/parse_oper.h"
#include "nodes/relation.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "nodes/print.h"
#include "nodes/nodeFuncs.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/hsearch.h"


// optimizations
bool drb_enable_basic_cache = true;
bool drb_enable_rewind_cache = true;
bool drb_enable_additional_cache = true;
bool drb_enable_big_omega = true;
bool drb_enable_send_isnumeric_constraint = false;
bool drb_enable_pull_up_selection = false;
bool drb_enable_send_predicates = true;
bool drb_enable_pull_up_projection = true;
bool drb_enable_force_big_omega_before_agg = false;
bool drb_enable_selectivity_estimation = false;
double drb_selectivity = 0.33;
bool drb_enable_rea = false;
bool drb_enable_dynamic_omega = false;
int drb_cost_model = 1;
bool drb_enable_reoptimization = false;
bool drb_enable_preselection = false;
bool drb_enable_static_reoptimization = false;


bool drb_enable_compress = true;
bool drb_enable_hashbundle = false;
double drb_run_cost = -1;
double drb_startup_cost = -1;
double drb_fixed_cost = -1;

// real global state
static Query *savedTopLevelQuery;

typedef struct
{
    List *sub_tlist;
    List *upper_tlist;
} two_tls;

typedef struct
{
    List *expansions;
    Bitmapset *drb_varnos;
    Bitmapset *sortRefs;
    Bitmapset *groupRefs;
    bool in_agg;
    bool in_sort_tle;
    bool in_group_tle;
} drb_walk_context;

typedef struct
{
    List       *varlist;
} pull_drb_var_clause_context;

static void setupExpansionExecution(PlannerInfo *root, DrillBeyondExpansion *exp);
static void set_sorting(List *expansions, Index varno);
static void set_aggregative(List *expansions, Index varno);
static void set_groupedby(List *expansions, Index varno);
static bool drb_walker(Node *node, drb_walk_context *context);
static void drb_pull_drb_operator(PlannerInfo *root, Plan **planAddr, Plan *drbOperator);
static two_tls *drb_make_subplanTargetList(PlannerInfo *root, List *tlist);
static List *pull_drb_var_clause(Node *node);
static bool pull_drb_var_clause_walker(Node *node, pull_drb_var_clause_context *context);
static Plan* drb_pull_first_node(PlannerInfo *root, Plan *plan, NodeTag tag);
extern DrillBeyondExpansion* find_expansion(List *expansions, int varno);


static void save_query(PlannerInfo *root);

extern void drillbeyond_planner_phase_zero(Query *q) {
    savedTopLevelQuery = (Query *) copyObject(q);
}

extern bool drillbeyond_planner_phase_one(PlannerInfo *root, List *tlist) {
    ListCell *sl, *gl, *lr, *c;
    Bitmapset *groupRefs = NULL;
    Bitmapset *sortRefs = NULL;
    Bitmapset *drb_varnos = NULL;
    bool is_drillbeyond = false;
    drb_walk_context context;
    Index rti = 0;

    /*iterate through the range table*/
    foreach(lr, root->parse->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lr);                      //get an entry
        rti ++;                                                                 // rangetable is one-based

        if (rte->rtekind == RTE_DRILLBEYOND)
        {
            DrillBeyondExpansion *expansion = rte->drb_expansion;               //get an expansion
            // DrillBeyondExpansion *expansion = palloc(sizeof(DrillBeyondExpansion));
            is_drillbeyond = true;
            expansion->rti = rti;
            expansion->selective = false;                                       //list_length(drb_rel->baserestrictinfo) > 0;
            expansion->aggregative = false;
            expansion->sorting = false;

            root->drb_expansions = lappend(root->drb_expansions, expansion);    //append current drb expansion to root
            drb_varnos = bms_add_member(drb_varnos, rti);                       //add rti to drb_varnos
        }
    }

    // attach subplans' expansions
    root->drb_all_expansions = list_copy(root->drb_expansions);
    foreach(c, root->glob->subroots) {
        PlannerInfo *subplanInfo = (PlannerInfo *) lfirst(c);
        root->drb_all_expansions = list_concat(root->drb_all_expansions, subplanInfo->drb_expansions);
    }

    //retrieve sort and group tleSortGroupRefs
    foreach(sl, root->parse->sortClause)
    {
        SortGroupClause *sc = (SortGroupClause *) lfirst(sl);
        sortRefs = bms_add_member(sortRefs, sc->tleSortGroupRef);
    }
    foreach(gl, root->parse->groupClause)
    {
        SortGroupClause *sc = (SortGroupClause *) lfirst(gl);
        groupRefs = bms_add_member(groupRefs, sc->tleSortGroupRef);
    }

    context.sortRefs = sortRefs;
    context.groupRefs = groupRefs;
    context.expansions = root->drb_expansions;
    context.drb_varnos = drb_varnos;
    context.in_sort_tle = false;
    context.in_group_tle = false;
    context.in_agg = false;
    // sets aggregative and sorting, selective is set later
    (void) drb_walker((Node *) tlist, &context);

    return is_drillbeyond;
}

static void setupExpansionExecution(PlannerInfo *root, DrillBeyondExpansion *exp) {
    ListCell *c;
    int i = 0;
    if (exp->results_hashtable != NULL) {
        // Setup was already done once
        return;
    }
    int num_join_cols = list_length(exp->join_cols);
    RelOptInfo *rel = find_base_rel(root, exp->rti);

    exp->outFunctions = (FmgrInfo *)palloc(sizeof(FmgrInfo) * num_join_cols);
    exp->eqFunctions = (FmgrInfo *)palloc(sizeof(FmgrInfo) * num_join_cols);
    exp->hashFunctions = (FmgrInfo *)palloc(sizeof(FmgrInfo) * num_join_cols);

    foreach(c, exp->join_cols) {
        Oid         eq_function;
        Oid         left_hash_function;
        Oid         right_hash_function;
        Oid         eqop;
        Oid         typeOut;
        bool        hashable, isvarlena;

        Var *var = (Var *) lfirst(c);

        // find eq and hash funcs
        get_sort_group_operators(var->vartype,
                                 false, true, false,
                                 NULL, &eqop, NULL,
                                 &hashable);
        Assert(hashable);
        eq_function = get_opcode(eqop);
        if (!get_op_hash_functions(eqop,
                                   &left_hash_function, &right_hash_function))
            elog(ERROR, "could not find hash function for hash operator %u",
                 eqop);
        Assert(left_hash_function == right_hash_function);

        // find out func, for printing to json
        getTypeOutputInfo(var->vartype, &typeOut, &isvarlena);

        // retrieve function regproc whatevers
        fmgr_info(typeOut, &(exp->outFunctions)[i]);
        fmgr_info(eq_function, &(exp->eqFunctions)[i]);
        fmgr_info(right_hash_function, &(exp->hashFunctions)[i]);

        i++;

    }
    exp->results_hashtable = drb_setupHashTable(exp, (int)rel->rows);
}

/*used in planmain.c/query_planner()*/
extern void drillbeyond_planner_phase_two(PlannerInfo *root) {
    ListCell *ec, *c;
    foreach(ec, root->drb_expansions) {
        DrillBeyondExpansion *exp = (DrillBeyondExpansion *) lfirst(ec);
        RelOptInfo *rel = find_base_rel(root, exp->rti);
        exp->drb_qual = NIL;
        foreach(c, rel->baserestrictinfo) {
            RestrictInfo *restr = (RestrictInfo *) lfirst(c);

            Bitmapset  *attrs_used = NULL;
            pull_varattnos((Node *) restr->clause, rel->relid, &attrs_used);
            // drb quals are only those defined on attribute 1
            bms_del_member(attrs_used, 9); // attr 1 + 8 (=FirstLowInvalidHeapAttributeNumber)
            if (bms_is_empty(attrs_used)) {
                exp->drb_qual = lappend(exp->drb_qual, restr->clause);
            }
        }
        exp->selective = list_length(rel->baserestrictinfo) > 0;
        setupExpansionExecution(root, exp);
    }
}

static DrillBeyond* find_drb_operator(List* drb_ops, DrillBeyondExpansion *needle) {
    ListCell *c;
    foreach(c, drb_ops) {
        DrillBeyond *op = (DrillBeyond *) lfirst(c);
        if (op->drb_expansion == needle)
            return op;
    }
    return NULL;
}

static bool
contain_variable_subplans_walker(Node *node, void *context)
{
    if (node == NULL)
        return false;
    if (IsA(node, SubPlan)) { // ||
        // IsA(node, AlternativeSubPlan) ||
        // IsA(node, SubLink))
        SubPlan *subplan = (SubPlan*)node;
        Plan *sPlan = planner_subplan_get_plan((PlannerInfo*)context, subplan);
        return drb_is_variable_plan((PlannerInfo*)context, (Plan*)sPlan);
    }
    return expression_tree_walker(node, contain_variable_subplans_walker, context);
}

static bool
contain_variable_subplans(PlannerInfo *root, Node *clause)
{
    return contain_variable_subplans_walker(clause, (void*)root);
}

static double
relation_byte_size(double tuples, int width)
{
    return tuples * (MAXALIGN(width) + MAXALIGN(sizeof(HeapTupleHeaderData)));
}

static bool should_materialize(PlannerInfo *root, Plan *plan) {
    switch (nodeTag(plan))
    {
        case T_Material:
        case T_IndexOnlyScan:
        //case T_Scan:
        case T_FunctionScan:
        case T_CteScan:
        case T_WorkTableScan:
        case T_Sort:
        case T_Hash:
            return false;
        case T_IndexScan: {
            IndexScan *ixs = (IndexScan *)plan;
            if (list_length(ixs->indexqualorig) == 0)
                return true;
            else
                return false;
        }
        case T_SeqScan: {
            // never materialize full scans
            if (list_length(plan->qual) == 0)
                return false;
            return true;

            Scan *scan = (Scan*)plan;
            RelOptInfo *rel = root->simple_rel_array[scan->scanrelid];
            // random threshold for selectivity
            // TODO: should use real cost_mat_path machinery
            if ((plan->plan_rows / rel->tuples) > 0.25)
                return false;

            // // random criterion: we do not spill to disk
            // double      nbytes = relation_byte_size(scan->plan.plan_rows, scan->plan.plan_width);
            // long        work_mem_bytes = work_mem * 1024L;
            // if (nbytes > work_mem_bytes)
            //     return false;

            return true;
        }
        default:
            {
                if (plan->plan_rows == 1) {
                    return false;
                }
                return true;
            }
    }
}

static bool add_mat_nodes(PlannerInfo *root, Plan *plan, List ** addedMatNodes) {
    if (!drb_enable_additional_cache)
        return false;
    if (drb_enable_big_omega && !drb_enable_dynamic_omega) {
        // makes no sense to cache if there is only one run
        return false;
    }
    if (plan == NULL)
        return false;

    if (nodeTag(plan) == T_DrillBeyond) {
        return true;
    }

    bool result = false;
    bool innerVariable = drb_is_variable_plan(root, innerPlan(plan));
    bool outerVariable = drb_is_variable_plan(root, outerPlan(plan));
    result = result || outerVariable;
    result = result || innerVariable;
    result = result || drb_has_variable_subplan(root, plan);
    result = result || contain_variable_subplans(root, (Node*)plan->qual);

    switch (nodeTag(plan)) {
        case T_NestLoop:
        case T_MergeJoin:
        case T_HashJoin:
            {
                Join *j = (Join*)plan;
                result = result || contain_variable_subplans(root, (Node*)j->joinqual);
            }
            break;
        default:
            break;
    }
    // no variable nodes beneath this one
    if (result == true) {
        if (innerPlan(plan) != NULL) {
            if (!innerVariable && should_materialize(root, innerPlan(plan))) {
                Material   *node = makeNode(Material);
                Plan       *mat_plan = &node->plan;
                mat_plan->targetlist = (List*)copyObject(innerPlan(plan)->targetlist);
                mat_plan->qual = NIL;
                mat_plan->lefttree = innerPlan(plan);
                mat_plan->righttree = NULL;
                plan->righttree = mat_plan;
                *addedMatNodes = lappend(*addedMatNodes, mat_plan);
            } else {
                add_mat_nodes(root, innerPlan(plan), addedMatNodes);
            }
        }
        if (outerPlan(plan) != NULL) {
            if (!outerVariable && should_materialize(root, outerPlan(plan))) {
                Material   *node = makeNode(Material);
                Plan       *mat_plan = &node->plan;
                mat_plan->targetlist = (List*)copyObject(outerPlan(plan)->targetlist);
                mat_plan->qual = NIL;
                mat_plan->lefttree = outerPlan(plan);
                mat_plan->righttree = NULL;
                plan->lefttree = mat_plan;
                *addedMatNodes = lappend(*addedMatNodes, mat_plan);
            } else {
                add_mat_nodes(root, outerPlan(plan), addedMatNodes);
            }
        }

    }

    return result;
}

// TODO: what about more than one drb operator?
static bool _sum_costs(Plan *plan, Plan *drbPlan, double* cost) {
    if (plan == NULL) {
        return false;
    }

    if (plan == drbPlan) {
        *cost += plan->total_cost - plan->startup_cost;
        // printf("adding costs at %s -> new sum: %f\n", nodeName(plan), *cost);
        return true;
    }

    bool innerTreeVariable = _sum_costs(innerPlan(plan), drbPlan, cost);
    bool outerTreeVariable = _sum_costs(outerPlan(plan), drbPlan, cost);

    bool result = innerTreeVariable || outerTreeVariable;

    if (result)  {
        *cost += plan->total_cost - plan->startup_cost;
        // printf("adding costs at %s -> new sum: %f\n", nodeName(plan), *cost);
    }
    return result;
}

extern bool sum_both_costs(Plan *plan, Plan *drbPlan, double *startup_cost, double* run_cost) {
    if (plan == NULL) {
        return false;
    }

    // if (nodeTag(plan) == T_DrillBeyond) {
    if (plan == drbPlan) {
        // *cost += plan->total_cost;
        *run_cost += plan->total_cost - plan->startup_cost;
        *startup_cost += plan->startup_cost;
        // printf("adding costs at %s -> new sums: %f and %f\n", nodeName(plan), *startup_cost, *run_cost);
        return true;
    }

    bool innerTreeVariable = sum_both_costs(innerPlan(plan), drbPlan, startup_cost, run_cost);
    bool outerTreeVariable = sum_both_costs(outerPlan(plan), drbPlan, startup_cost, run_cost);

    bool result = innerTreeVariable || outerTreeVariable;

    if (result)  {
        *run_cost += plan->total_cost - plan->startup_cost;
        *startup_cost += plan->startup_cost;
        // printf("adding costs at %s -> new sums: %f and %f\n", nodeName(plan), *startup_cost, *run_cost);
    }
    return result;
}


extern double sum_costs(Plan *plan, Plan *drbPlan) {
    double cost = 0.0;
    _sum_costs(plan, drbPlan, &cost);
    return cost;
}

extern bool drb_is_variable_plan(PlannerInfo *root, Plan *plan)
{
    if (plan == NULL)
        return false;

    if (nodeTag(plan) == T_DrillBeyond) {
        return true;
    }

    bool result = false;
    result = result || drb_is_variable_plan(root, outerPlan(plan));
    result = result || drb_is_variable_plan(root, innerPlan(plan));
    result = result || drb_has_variable_subplan(root, plan);
    result = result || contain_variable_subplans(root, (Node*)plan->qual);
    if (nodeTag(plan) == T_SubqueryScan) {
        SubqueryScan *sqs = (SubqueryScan*)plan;
        result = result || drb_is_variable_plan(root, sqs->subplan);
    }

    switch (nodeTag(plan)) {
        case T_NestLoop:
        case T_MergeJoin:
        case T_HashJoin:
            {
                Join *j = (Join*)plan;
                result = result || contain_variable_subplans(root, (Node*)j->joinqual);
            }
            break;
        default:
            break;
    }
    return result;
}

extern bool drb_has_variable_subplan(PlannerInfo *root, Plan *plan)
{
    ListCell *l;
    bool result = false;
    foreach(l, plan->initPlan)
    {
        SubPlan    *initsubplan = (SubPlan *) lfirst(l);
        Plan       *initplan = planner_subplan_get_plan(root, initsubplan);
        result = result || drb_is_variable_plan(root, initplan);
    }
    return result;
}

static bool should_add_tl(Plan *plan) {
    switch (nodeTag(plan))
    {
        case T_Hash:
            return false;
        // TODO: why did I add it? made sense at the time
//        case T_Sort:
//            return false;
        default:
            return true;
    }
}

static void add_targetlist_entry(Plan *plan, DrillBeyondExpansion *exp) {
    char *new_name;
    new_name = (char*)palloc(strlen(exp->keyword)+1);
    sprintf(new_name, "%s", exp->keyword);

    Var *col_expr = makeVar(exp->rti, 1, 1700, 1700, 0, 0);
    plan->targetlist = lappend(plan->targetlist, makeTargetEntry(
           (Expr *) col_expr,
           list_length(plan->targetlist) + 1,
           new_name,
           false));
}

static bool _add_targetlist_entries(PlannerInfo *root, Plan *plan, DrillBeyondExpansion *exp) {
    if (plan == NULL)
        return false;


    if (nodeTag(plan) == T_DrillBeyond) {
        DrillBeyond *drb = (DrillBeyond*)plan;
        if (exp == drb->drb_expansion) {
            add_targetlist_entry(plan, drb->drb_expansion);
            return true;
        }
    }

    bool result = false;
    result = result || _add_targetlist_entries(root, outerPlan(plan), exp);
    result = result || _add_targetlist_entries(root, innerPlan(plan), exp);
    if (result && should_add_tl(plan)) {
        add_targetlist_entry(plan, exp);
    }
    return result;
}

static bool contains_var(List *tlist, Index rti) {
    ListCell   *c;
    foreach(c, tlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(c);
        if (IsA(tle->expr, Var))
        {
            Var * v = (Var *)tle->expr;
            if (v->varno == rti && v->varattno == 1)
                return true;
        }
    }
    return false;
}

static bool add_targetlist_entries(PlannerInfo *root, Plan *plan, DrillBeyondExpansion *exp) {
    if (contains_var(plan->targetlist, exp->rti))
        return false;
    _add_targetlist_entries(root, plan, exp);
}


extern Plan *drillbeyond_planner_phase_three(PlannerInfo *root, Plan *result_plan) {
    ListCell *c, *c2;

    foreach(c, root->drb_expansions) {
        DrillBeyondExpansion *expansion = (DrillBeyondExpansion *) lfirst(c);
        expansion->was_planned = true;
    }
    // additional materialization nodes on always invariable subtrees
    List *addedMatNodes = NIL;
    add_mat_nodes(root, result_plan, &addedMatNodes);

    Plan *topPlan, *outputPlan;
    List *drb_ops = NIL;

    if (root->query_level == 1) {
        outputPlan = (Plan *) make_drillbeyondexpand(root,
            result_plan, DRB_TOP, NULL, NULL, false, drb_ops, NULL);
        save_query(root);
    } else {
        outputPlan = result_plan;
    }

    if (root->drb_all_expansions != NULL) {// && root->parent_root == NULL) {
        topPlan = outputPlan;

        // two_tls *ttl;
        // printf("--------------------targetlist\nORIGINAL:\n");
        // print_tl(result_plan->targetlist, root->parse->rtable);
        // if (drb_enable_pull_up_projection) {
            // ttl = drb_make_subplanTargetList(root, result_plan->targetlist);
            // result_plan->targetlist = ttl->sub_tlist;
            // printf("------------------------------\nNEW:\n");
            // print_tl(ttl->upper_tlist, root->parse->rtable);
            // printf("------------------------------\nSUB:\n");
            // print_tl(ttl->sub_tlist, root->parse->rtable);
            // printf("------------------------------\nFINAl:\n");
        // }


        if (drb_enable_big_omega) {
            drb_collect_drb_operators(result_plan, &drb_ops);
            foreach(c, root->drb_expansions) {
                DrillBeyondExpansion *expansion = (DrillBeyondExpansion *) lfirst(c);
                DrillBeyond *op = find_drb_operator(drb_ops, expansion);
                op->drb_addedMatNodes = addedMatNodes;
                // printf("expansion keyword: %s\n", expansion->keyword);

                if (expansion->selective && !drb_enable_pull_up_selection)
                    continue;

                if (nodeTag(result_plan) == T_Limit) {
                    Plan *child = outerPlan(result_plan);
                    topPlan = result_plan;
                    result_plan = child;
                }

                if (expansion->sorting) {
                    if (nodeTag(result_plan) == T_Sort) {
                        Plan *child = outerPlan(result_plan);
                        topPlan = result_plan;
                        result_plan = child;
                    }
                }
                /// AGGREGATIVE
                Plan *aggNode = drb_pull_first_node(root, result_plan, T_Agg);
                if (aggNode != NULL
                    && ((expansion->aggregative && !drb_enable_pull_up_projection) || drb_enable_force_big_omega_before_agg)) {
                    op->drb_strategy = DRB_PLACEHOLDER;

                    // double cost = sum_costs(result_plan, (Plan*)op);
                    // printf("1) cost: %f def_cost: %f (cost*k) big_omega_cost (cost * (1/sel)): %f\n", cost, (cost*drb_max_num_cands), (cost  *(1 / expansion->selectivity)));

                    // Plan *aggNode = drb_pull_first_node(root, result_plan, T_Agg);
                    Plan *aggChild = outerPlan(aggNode);

                    Bitmapset *drb_cols = NULL;
                    foreach(c2, aggChild->targetlist) {
                        TargetEntry *te = (TargetEntry *) lfirst(c2);
                        if (IsA(te->expr, Var)) {
                            Var *v = (Var *)te->expr;
                            if (v->varno == expansion->rti) {
                                // printf("found col to expand at %d for %s\n", te->resno, te->resname);
                                drb_cols = bms_add_member(drb_cols, te->resno);
                            }
                        }
                    }

                    add_targetlist_entries(root, aggChild, expansion);
                    Plan *tmp = (Plan *) make_drillbeyondexpand(root,
                                        aggChild, DRB_EXPAND2, drb_cols, NULL, false, drb_ops, expansion);
                    // final_cost_drillbeyond_expand(root, (DrillBeyondExpand*)result_plan);
                    aggNode->lefttree = tmp;
                    result_plan = tmp;
                    topPlan = aggNode;
                    bms_free(drb_cols);
                    op->drb_expandNode = (DrillBeyondExpand*)tmp;
                }
                /// NON_AGG && NON_SEL
                else {
                    // double cost = sum_costs(result_plan, (Plan*)op);
                    // printf("2) cost: %f def_cost: %f (cost*k) big_omega_cost (cost * (1/sel)): %f\n", cost, (cost*drb_max_num_cands), (cost  *(1 / expansion->selectivity)));

                    op->drb_strategy = DRB_PLACEHOLDER;
                    Bitmapset *drb_cols = NULL;
                    foreach(c2, result_plan->targetlist) {
                        TargetEntry *te = (TargetEntry *) lfirst(c2);
                        if (IsA(te->expr, Var)) {
                            Var *v = (Var *)te->expr;
                            if (v->varno == expansion->rti) {
                                // printf("found col to expand at %d for %s\n", te->resno, te->resname);
                                drb_cols = bms_add_member(drb_cols, te->resno);
                            }
                        }
                    }
                    add_targetlist_entries(root, result_plan, expansion);
                    result_plan = (Plan *) make_drillbeyondexpand(root,
                                        result_plan, DRB_EXPAND2, drb_cols, NULL, false, drb_ops, expansion);
                    bms_free(drb_cols);
                    op->drb_expandNode = (DrillBeyondExpand*) result_plan;
                }
                if (drb_enable_pull_up_projection) {
                    two_tls *ttl;
                    ttl = drb_make_subplanTargetList(root, result_plan->targetlist);
                    result_plan->targetlist = ttl->sub_tlist;
                    aggNode->targetlist = ttl->sub_tlist;
                    outerPlan(result_plan)->targetlist = ttl->sub_tlist; // TODO: EVIL HACK, not always correct
                    result_plan = (Plan *) make_result(root, ttl->upper_tlist, NULL, result_plan);
                }
            }

            topPlan->lefttree = result_plan;
        }

        //     printf("split result targetlist\nONE BEFORE FINAL:\n");
        //     print_tl(result_plan->targetlist, root->parse->rtable);

        // result_plan = (Plan *) make_drillbeyondexpand(root,
        //     result_plan, DRB_TOP, NULL, NULL, false, drb_ops, NULL);
        // result_plan = topPlan;
        // printf("split result targetlist\nFINAL:\n");
        // print_tl(result_plan->targetlist, root->parse->rtable);
    }

    return outputPlan;
}

static bool
drb_walker(Node *node, drb_walk_context *context)
{
    if (node == NULL)
        return false;
    if (IsA(node, TargetEntry)) {
        TargetEntry *tle = (TargetEntry *) node;
        context->in_sort_tle = bms_is_member(tle->ressortgroupref, context->sortRefs);
        context->in_group_tle = bms_is_member(tle->ressortgroupref, context->groupRefs);
    }
    else if (IsA(node, Aggref))
    {
        context->in_agg = true;
        bool result = expression_tree_walker(node, drb_walker,
                                  (void *) context);
        context->in_agg = false;
        return result;
    }
    else if (IsA(node, Var)) {
        Var *var = (Var *) node;
        if (var->varattno != 1)
            return false;                                           // we only consider the first att
        if (bms_is_member(var->varno, context->drb_varnos)) {
            if (context->in_agg)
                set_aggregative(context->expansions, var->varno);   // is grouped
            if (context->in_group_tle)
                set_groupedby(context->expansions, var->varno);     // is grouped by
            if (context->in_sort_tle)
                set_sorting(context->expansions, var->varno);       // is sorted by
        }
    }
    return expression_tree_walker(node, drb_walker,
                                  (void *) context);
}


extern DrillBeyondExpansion* find_expansion(List *expansions, int varno) {
    ListCell *c;
    foreach (c, expansions) {
        DrillBeyondExpansion *expansion = (DrillBeyondExpansion *)lfirst(c);
        if (expansion->rti == varno) {
            return expansion;
        }
    }
    Assert(false); // shouldn happen
    return NULL;
}

static void set_sorting(List *expansions, Index varno) {
    ListCell *c;
    foreach (c, expansions) {
        DrillBeyondExpansion *expansion = (DrillBeyondExpansion *)lfirst(c);
        if (expansion->rti == varno) {
            expansion->sorting = true;
            return;
        }
    }
    Assert(false); // shouldn happen
}

static void set_aggregative(List *expansions, Index varno) {
    ListCell *c;
    foreach (c, expansions) {
        DrillBeyondExpansion *expansion = (DrillBeyondExpansion *)lfirst(c);
        if (expansion->rti == varno) {
            expansion->aggregative = true;
            return;
        }
    }
    Assert(false); // shouldn happen
}

static void set_groupedby(List *expansions, Index varno) {
    ListCell *c;
    foreach (c, expansions) {
        DrillBeyondExpansion *expansion = (DrillBeyondExpansion *)lfirst(c);
        if (expansion->rti == varno) {
            expansion->groupedby = true;
            return;
        }
    }
    Assert(false); // shouldn happen
}

extern void
drb_print_relids(Relids relids)
{
    Relids      tmprelids;
    int         x;
    bool        first = true;

    tmprelids = bms_copy(relids);
    while ((x = bms_first_member(tmprelids)) >= 0)
    {
        if (!first)
            printf(" ");
        printf("%d", x);
        first = false;
    }
    bms_free(tmprelids);
}

static bool
is_drb_tlist_entry(Expr *expr)
{
    ListCell *c;
    Oid ltype = 1700;

    List *vars = pull_var_clause((Node *) expr,
            PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
    foreach(c, vars) {
        Var *var = (Var *) lfirst(c);
        if (var->vartypmod == ltype) {
            return true;
        }
    }
    return false;
}

extern Bitmapset *
get_drb_tlist_entries(List *tlist)
{
    Bitmapset       *result = NULL;
    ListCell   *l;
    int i = 1;

    foreach(l, tlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(l);

        if (tle->resjunk)
            continue;
        if (is_drb_tlist_entry(tle->expr))
            result = bms_add_member(result, i);
        i++;
    }
    return result;
}



extern Bitmapset*
get_drb_tlist_idx_for_expansion(List *tlist, DrillBeyondExpansion* expansion) {
    ListCell   *l, *c;
    Index       idx = 1;
    foreach(l, tlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(l);
        List *vars = pull_var_clause((Node *) tle->expr,
            PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
        foreach(c, vars) {
            Var *var = (Var *) lfirst(c);
            if (var->varno == expansion->rti && var->varoattno == 1) {
                return bms_make_singleton(idx);
            }
        }
        idx++;
    }
    return NULL;
}

extern void drb_collect_drb_operators(Plan *plan, List **drb_ops)
{
    Plan *result_plan = NULL;

    if (plan == NULL)
        return;

    if (nodeTag(plan) == T_DrillBeyond) {
        DrillBeyond *expand = (DrillBeyond *)plan;
        *drb_ops = lappend(*drb_ops, expand);
    }

    drb_collect_drb_operators(plan->lefttree, drb_ops);
    drb_collect_drb_operators(plan->righttree, drb_ops);

    if (result_plan == NULL)
        result_plan = plan;
}

static void
drb_pull_drb_operator(PlannerInfo *root, Plan **planAddr, Plan *drbOperator)
{
    Plan *plan = *planAddr;

    if (plan == NULL)
        return;

    if (plan == drbOperator) {
        Plan *outerplan = outerPlan(drbOperator);
        *planAddr = outerplan;
        return;
    }
    drb_pull_drb_operator(root, &(outerPlan(plan)), drbOperator);
    drb_pull_drb_operator(root, &(innerPlan(plan)), drbOperator);
}

static Plan*
drb_pull_first_node(PlannerInfo *root, Plan *plan, NodeTag tag)
{
    if (plan == NULL)
        return NULL;

    if (nodeTag(plan) == tag) {
        return plan;
    }
    Plan *result = NULL;
    result = drb_pull_first_node(root, outerPlan(plan), tag);
    if (result != NULL)
        return result;
    result = drb_pull_first_node(root, innerPlan(plan), tag);
    return result;

}

static two_tls*
drb_make_subplanTargetList(PlannerInfo *root,
                       List *tlist)
{
    List       *sub_tlist;
    List       *upper_tlist;
    List       *tl_vars;
    ListCell   *c, *c2;
    sub_tlist = NIL;
    upper_tlist = NIL;
    List *otherTLEs = NIL;
    List *drbTLEs = NIL;
    two_tls *result = (two_tls*)palloc(sizeof(two_tls));

    foreach(c, tlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(c);
        if (is_drb_tlist_entry(tle->expr)) {
            drbTLEs = lappend(drbTLEs, tle->expr);
        }
        else {
            otherTLEs = lappend(otherTLEs, tle->expr);
        }
    }
    sub_tlist = add_to_flat_tlist(sub_tlist, otherTLEs);
    upper_tlist = (List*)copyObject(tlist);

    foreach(c, drbTLEs) {
        Expr *ex = (Expr *) lfirst(c);
        tl_vars = pull_var_clause((Node *) ex,
                                            PVC_INCLUDE_AGGREGATES,
                                            PVC_INCLUDE_PLACEHOLDERS);
        foreach(c2, tl_vars) {
            Var *var = (Var *) lfirst(c2);
            sub_tlist = add_to_flat_tlist(sub_tlist, list_make1(var));
        }
    }

    result->sub_tlist = sub_tlist;
    result->upper_tlist = upper_tlist;
    return result;
}

List *
pull_drb_var_clause(Node *node)
{
    pull_drb_var_clause_context context;

    context.varlist = NIL;

    pull_drb_var_clause_walker(node, &context);
    return context.varlist;
}

static bool
pull_drb_var_clause_walker(Node *node, pull_drb_var_clause_context *context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var))
    {
        if (((Var *) node)->varlevelsup != 0)
            elog(ERROR, "Upper-level Var found where not expected");
        context->varlist = lappend(context->varlist, node);
        return false;
    }
    else if (IsA(node, Aggref))
    {
        context->varlist = lappend(context->varlist, node);
        /* we do NOT descend into the contained expression */
        return false;
    }
    else if (IsA(node, PlaceHolderVar))
    {
        elog(ERROR, "Upper-level PlaceHolderVar found where not expected");
    }
    return expression_tree_walker(node, pull_drb_var_clause_walker,
                                  (void *) context);
}

static void save_query(PlannerInfo *root) {
    ListCell *lr;
    foreach(lr, root->parse->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lr);                      //get an entry
        if (rte->rtekind == RTE_DRILLBEYOND) {
            DrillBeyondExpansion *expansion = rte->drb_expansion;
            expansion->query = savedTopLevelQuery;
        }
    }

}

