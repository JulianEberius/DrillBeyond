/*-------------------------------------------------------------------------
 *
 * drillbeyond.h
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 *
 * src/include/drillbeyond/drillbeyond.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DRILLBEYOND_H
#define DRILLBEYOND_H

#include "parser/parse_node.h"
#include "nodes/execnodes.h"
#include "nodes/relation.h"
#include "nodes/plannodes.h"
#include "json/json.h"

 /*
  * Explaining
  */
#define do_text_output_drillbeyond_explain(tstate, str_to_emit, str_to_emit2) \
    do { \
        Datum   values_[2]; \
        bool    isnull_[2]; \
        values_[0] = PointerGetDatum(cstring_to_text(str_to_emit)); \
        isnull_[0] = false; \
        values_[1] = PointerGetDatum(cstring_to_text(str_to_emit2)); \
        isnull_[1] = false; \
        do_tup_output(tstate, values_, isnull_); \
        pfree(DatumGetPointer(values_[0])); \
        pfree(DatumGetPointer(values_[1])); \
    } while (0)

extern void merge_explain_data(json_object *expl);
extern bool is_drillbeyond();
extern void clear_drillbeyond_explain();
extern const char *drillbeyond_explain_string();

//optimizations
extern bool drb_enable_basic_cache;
extern bool drb_enable_big_omega;
extern bool drb_enable_rewind_cache;
extern bool drb_enable_additional_cache;
extern bool drb_enable_send_isnumeric_constraint;
extern bool drb_enable_pull_up_selection;
extern bool drb_enable_send_predicates;
extern bool drb_enable_pull_up_projection;
extern double drb_selectivity;
extern double drb_selectivity;
extern bool drb_enable_force_big_omega_before_agg;
extern bool drb_enable_selectivity_estimation;
extern bool drb_enable_rea;
extern bool drb_enable_dynamic_omega;
extern bool drb_enable_reoptimization;
extern bool drb_enable_preselection;
extern bool drb_enable_static_reoptimization;

extern int drb_cost_model;
extern int drb_max_num_cands;
extern double drb_run_cost;
extern double drb_startup_cost;
extern double drb_fixed_cost;

/* drb cost models */
#define DRB_COST_ONLY_S 1
#define DRB_COST_NONE 2
#define DRB_COST_ONLY_K 3
#define DRB_COST_BOTH 4

/*
 * global constants
 */
#define NUM_CANDS 10
#define DRB_VALUE_ATTR 1  // first attr of drb_relation is value
#define DRB_ID_ATTR 2 // second is id


#define DRB_DEFAULT_SEL 0.33

/*
 * One DrillBeyondExpansion object is created for each open attribute found
 * by the query rewritter.
 */
typedef struct DrillBeyondExpansion {

    Index rti; // entry in query rangetable for the fake relation
    Index extended_rti; // entry in query rangetable for the extened relation e.g. Nation
    char *keyword; // original keywod queried by the user
    /* the following attributes are used to generate the explain info */
    char *extended_relname; // name of relation that was extended e.g. Nation
    List *extended_attrNames;
    List *extended_strAttrNames;
    /* flags that indicate in which way the attribute is used in the query */
    bool selective;
    bool aggregative;
    bool groupedby;
    bool sorting;

    /* List of Var* -> columns on which the fake relation is "joined" with the extended relation
     * this effectively determines the "identity" of a tuple from the view of drillbeyond
     * its currently includes all string-type columns of the extended relation
     */
    List *join_cols;
    /* list of RestrictInfo -> contains all the clauses in which the open attribute is used */
    List *drb_qual;

    /* TODO temporary solution: pull up from subqueries mutates join_cols and drb_qual, do not repeat on reoptimization */
    bool was_planned;

    /* cache for functions on the join columns */
    FmgrInfo *outFunctions;
    FmgrInfo *eqFunctions;
    FmgrInfo *hashFunctions;

    /* filled by the external entity augmentation system in drillbeyond_requests.c
     * contains DrillBeyondValues objects (see below) as values and Datum arrays as key
     * these Datums are joining values from native tuples (e.g. n_name)
     */
    HTAB *results_hashtable;
    double *selectivities; // actual selectivities found (one predicate only)

    /* selectivity estimation, at the moment for one predicate only */
    double selectivity;
    double union_selectivity;

    /* save the original query to enable reoptimization */
   Query *query;
   bool reoptimized; // only reoptimize once


} DrillBeyondExpansion;


/*
 * container for drilled values for one combinations of join values
 */
typedef struct DrillBeyondValues {
    Datum *joinValues; // array of join values, e.g. n_names
    Datum *values; // array of drilled values e.g. 23.0, 12.0
    bool *is_null; // array of bools, true if the value is null
    int numValues; // number of different variants (values) available
    bool requested; // request for these joinValues was sent
    bool inUnion; // true if it passes the predicate(s) in at least one candidate
} DrillBeyondValues;


/*
 * Rewriting
 */
typedef struct FakeRelationEntry
{
    char*       rel_name;
    char*       field_name;
    ParseState* pstate; // to which subquery level does the fake_rel belong?
    RangeTblRef *rtr; /* entry in range table */
    Oid relid;
} FakeRelationEntry;

extern Node *drillbeyond_column_transform(ParseState *pstate, ColumnRef *cref, Node *var);
extern void drillbeyond_extend_query(ParseState *pstate, Query *query);

/*
 * Operator
 * these are all the standard interface methods of an operator in Postgres
 */
extern DrillBeyondState *ExecInitDrillBeyond(DrillBeyond *node, EState *estate, int eflags);
extern DrillBeyondDummyState *ExecInitDrillBeyondDummy(DrillBeyondDummy *node, EState *estate, int eflags);
extern TupleTableSlot *ExecDrillBeyond(DrillBeyondState *node);
extern Node *MultiExecDrillBeyond(DrillBeyondState *node);
extern void ExecEndDrillBeyond(DrillBeyondState *node);
extern void ExecEndDrillBeyondDummy(DrillBeyondDummyState *node);
extern void ExecReScanDrillBeyond(DrillBeyondState *node);

extern bool drb_resetting_query;

/* Expand/Compress/Decompress */
extern DrillBeyondExpandState *ExecInitDrillBeyondExpand(DrillBeyondExpand *node, EState *estate, int eflags);
extern TupleTableSlot *ExecDrillBeyondExpand(DrillBeyondExpandState *node);
extern void ExecEndDrillBeyondExpand(DrillBeyondExpandState *node);

extern void ExecReScanDrillBeyondExpand(DrillBeyondExpandState *node);
extern const char *drb_strategyname(enum DrillBeyondStrategy f);
extern void reset_context();

/*
 * Request
 */

#define KEYWORD "keyword"
#define LOCAL_TABLE "local_table"
#define COLUMNS "columns"
#define COL_NAMES "col_names"
#define STR_COL_NAMES "str_col_names"
#define COLUMN_NUMBER "column_number"
#define VALUES "values"
#define EXPLANATION "explanation"
#define CANDIDATES "candidates"
#define IN_UNION "inUnion"
#define RESTRICTIONS "restrictions"
#define REQUEST_HULL "request_hull"
#define CANDIDATES_FOR "candidatesFor"
#define OPEN_COLUMNS "openColumns"
#define LOCAL_TABLES "localTables"
#define INFOS "infos"
#define MAX_CANDS "max_cands"
#define SELECTIVITY "selectivity"
#define SELECTIVITIES "selectivities"


extern int drillbeyond_request(const char *msg_str, DrillBeyondState *dbstate);
extern void heap_tup_to_json(HeapTuple tup, TupleDesc tupdesc, json_object *msg);                   //not used
extern json_object *initDrillBeyondRequest(DrillBeyondExpansion *expansion);
extern void tup_to_json(DrillBeyondExpansion *expansion, List *join_cols, TupleTableSlot *slot, json_object *msg);
extern void add_restrictions_to_msg(json_object *msg, List* restrictions);
extern bool drillbeyond_fill_msg(DrillBeyondExpansion *expansion, json_object *msg);
extern double estimateSelectivity(DrillBeyondExpansion *expansion, Oid extended_relid, List *restrictlist);

/*
 * Costs
 */
extern int drillbeyond_sample_rel(Oid relid, int targrows, HeapTuple **rows, TupleDesc *tupDesc);
extern void final_cost_drillbeyond(PlannerInfo *root, DrillBeyondPath *path,
                    SpecialJoinInfo *sjinfo, SemiAntiJoinFactors *semifactors);
extern void final_cost_drillbeyond_expand(PlannerInfo *root, DrillBeyondExpand *plan);
extern double drillbeyond_estimate_join_size(PlannerInfo *root, double outer_rows,
                    SpecialJoinInfo *sjinfo, List *restrictlist);
extern void cost_drillbeyond_expand(PlannerInfo *root, DrillBeyondExpand *expand);

/*
 * Planner
 */

extern void drillbeyond_planner_phase_zero(Query *parse);
extern bool drillbeyond_planner_phase_one(PlannerInfo *root, List *tlist);
extern void drillbeyond_planner_phase_two(PlannerInfo *root);
extern Plan *drillbeyond_planner_phase_three(PlannerInfo *root, Plan *result_plan);
extern Bitmapset *get_drb_tlist_entries(List *tlist);
extern Bitmapset *get_drb_tlist_idx_for_expansion(List *tlist, DrillBeyondExpansion* expansion);
extern bool drb_has_variable_subplan(PlannerInfo *root, Plan *plan);
extern bool drb_is_variable_plan(PlannerInfo *root, Plan *plan);
extern double sum_costs(Plan *plan, Plan *drbPlan);
extern bool sum_both_costs(Plan *plan, Plan *drbPlan, double *startup_cost, double* run_cost);
extern DrillBeyondState* stateForExpansion(DrillBeyondExpansion *dbe, List *states);
extern void drb_collect_drb_operator_states(PlanState *planState, List **drb_ops);
extern void drb_collect_drb_operators(Plan *plan, List **drb_ops);

/*
 * Reoptimization
 */
// extern PlannedStmt *reoptimize(DrillBeyond *drb);
extern PlannedStmt* reoptimize(Query *query);
extern void print_explanation(PlannedStmt *pstmt);
extern void switch_plans(DrillBeyondExpandState *node);
extern PlannedStmt* reconsiderStrategy(DrillBeyondState *node);
extern void execute_drb_fragment(DrillBeyondState *drb);

/*
 * Util
 */
extern HTAB* drb_setupHashTable(DrillBeyondExpansion *exp, int nrows);
extern void drb_addToHashTable(DrillBeyondExpansion *exp, Datum *keys, Datum *values, int numValues);
extern DrillBeyondValues* drb_retrieveFromHashTable(DrillBeyondExpansion *exp, Datum *keys);

extern void drb_reset_query();
extern void drb_finished_reset_query();
/*debug*/

extern void drb_print_relids(Relids relids);
extern void drb_printTL(Plan *plan, List *rtable);
char* nodeName(const void *obj);

#endif   /* DRILLBEYOND_H */
