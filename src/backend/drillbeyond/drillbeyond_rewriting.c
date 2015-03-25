    /*-------------------------------------------------------------------------
 *
 * drillbeyond_rewriting.c
 *
 * IDENTIFICATION
 *    /drillbeyond/drillbeyond_rewriting.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "drillbeyond/drillbeyond.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "parser/parse_clause.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/print.h"
#include "fmgr.h"

static Node * conj_clause(ParseState *pstate, Node *lexpr, Node *rexpr);
static List * drillbeyond_find_string_attrs(ParseState *pstate, RangeTblEntry *original_rte);
static void drillbeyond_find_attr_names(ParseState *pstate, RangeTblEntry *original_rte, List **attrNames, List **strAttrNames);
static RangeTblEntry* create_fake_relation(ParseState* pstate, RangeTblEntry *original_rte, List *string_attrs, char* field_name);
static Node * create_fake_quals(ParseState* pstate, RangeTblEntry *original_rte, RangeTblEntry *rte, List *string_attrs);

static Node * conj_clause(ParseState *pstate, Node *lexpr, Node *rexpr) {
    if (!lexpr)
        return rexpr;
    if (!rexpr)
        return lexpr;
    lexpr = coerce_to_boolean(pstate, lexpr, "AND");
    rexpr = coerce_to_boolean(pstate, rexpr, "AND");

    return (Node *) makeBoolExpr(AND_EXPR,
        list_make2(lexpr, rexpr), -1);
}

void drillbeyond_extend_query(ParseState *pstate, Query *query)
{
    if (pstate->drb_quals && query->jointree) {
        query->jointree->quals = conj_clause(pstate, query->jointree->quals, pstate->drb_quals);
        pstate->drb_quals = NULL;
    }
}

static List * drillbeyond_find_string_attrs(ParseState *pstate, RangeTblEntry *original_rte) {
    int         rtindex;
    List        *colnames,
                *colvars;
    ListCell    *c;
    List        *result = NIL;

    rtindex = RTERangeTablePosn(pstate, original_rte, 0);
    expandRTE(original_rte, rtindex, 0, -1, false,
                &colnames, &colvars);


    foreach(c, colvars) {
        Var *v = (Var*) lfirst(c);
        if (TypeCategory(v->vartype) == TYPCATEGORY_STRING) {
            result = lappend(result, v);
        }
    }

    return result;
}

static void drillbeyond_find_attr_names(ParseState *pstate, RangeTblEntry *original_rte, List **attrNames, List **strAttrNames) {
    int         rtindex;
    List        *colnames,
                *colvars;
    ListCell    *c;

    rtindex = RTERangeTablePosn(pstate, original_rte, 0);
    expandRTE(original_rte, rtindex, 0, -1, false,
                &colnames, &colvars);


    foreach(c, colvars) {
        Var *v = (Var*) lfirst(c);
        Value *val = (Value*) list_nth(original_rte->eref->colnames, v->varattno-1);
        if (TypeCategory(v->vartype) == TYPCATEGORY_STRING) {
            *strAttrNames = lappend(*strAttrNames, copyObject(val));
        }
        *attrNames = lappend(*attrNames, copyObject(val));
    }
}

static Oid next_drb_relid(ParseState* pstate) {
    ListCell *c;
    Oid min_id = 0;
    foreach(c, pstate->drb_relations) {
        FakeRelationEntry *fre = (FakeRelationEntry *) lfirst(c);
        min_id = fre->relid < min_id ? fre->relid : min_id;
    }
    return min_id - 1;
}

static RangeTblEntry* create_fake_relation(ParseState* pstate, RangeTblEntry *original_rte, List *string_attrs, char* field_name) {
    ListCell *c;
    RangeTblRef *rtr;
    int original_vnum;
    List *fake_col_names;
    char *rel_name = original_rte->eref->aliasname;
    char *fake_rel_name, *cand_identifier_name;
    int fake_rel_name_size = 5 + strlen(rel_name) + 1 + strlen(field_name);
    int cand_identifier_name_size = strlen(field_name) + 1 + 2; // e.g. "gdp_id"
    RangeTblEntry *rte = makeNode(RangeTblEntry);
    DrillBeyondExpansion *expansion;

    // create fake rel_name
    fake_rel_name = (char*) palloc(fake_rel_name_size + 1);
    sprintf(fake_rel_name, "fake_%s_%s", rel_name, field_name);
    cand_identifier_name = (char*) palloc(cand_identifier_name_size + 1);
    sprintf(cand_identifier_name, "%s_id", field_name);

    fake_col_names = list_make2(makeString(field_name), makeString(cand_identifier_name));
    // fake_col_names = list_make1(makeString(field_name));
    foreach(c, string_attrs){
        Var *var = (Var*) lfirst(c);
        Value *val = (Value*) list_nth(original_rte->eref->colnames, var->varattno-1);
        char *attr_name = strVal(val);
        char * fake_attr_name = (char*) palloc(5 + strlen(attr_name) + 1);
        sprintf(fake_attr_name, "fake_%s", attr_name);
        Value *val_fake = makeString(fake_attr_name);
        fake_col_names = lappend(fake_col_names, val_fake);
    }

    rte->rtekind = RTE_DRILLBEYOND;
    rte->alias = NULL;

    rte->relid = next_drb_relid(pstate);
    rte->relkind = RELKIND_RELATION;
    rte->eref = makeAlias(fake_rel_name, fake_col_names);

    rte->inh = false;
    rte->inFromCl = false;
    rte->requiredPerms = ACL_SELECT;
    rte->checkAsUser = InvalidOid;      /* not set-uid by default, either */
    rte->selectedCols = NULL;
    rte->modifiedCols = NULL;

    original_vnum = RTERangeTablePosn(pstate, original_rte, NULL);
    rtr = makeNode(RangeTblRef);
    rtr->rtindex = original_vnum;

    expansion = (DrillBeyondExpansion *) palloc(sizeof(DrillBeyondExpansion));
    expansion->keyword = field_name;
    expansion->extended_rti = original_vnum;
    expansion->extended_relname = pstrdup(rel_name);
    expansion->selective = false; //list_length(drb_rel->baserestrictinfo) > 0;
    expansion->aggregative = false;
    expansion->sorting = false;
    expansion->join_cols = string_attrs;
    expansion->drb_qual = NIL;
    expansion->extended_attrNames = NIL;
    expansion->extended_strAttrNames = NIL;
    expansion->selectivity = -1.0;
    expansion->selectivities = NULL;
    expansion->results_hashtable = NULL;
    expansion->was_planned = false;
    expansion->query = NULL;
    expansion->reoptimized = false;

    drillbeyond_find_attr_names(pstate, original_rte,
        &(expansion->extended_attrNames), &(expansion->extended_strAttrNames));

    rte->drb_expansion = expansion;


    if (pstate != NULL)
        pstate->p_rtable = lappend(pstate->p_rtable, rte);

    return rte;
}

static Node * create_fake_quals(ParseState* pstate, RangeTblEntry *original_rte,
                            RangeTblEntry *rte, List *string_attrs) {
    ListCell *c;
    Var *lexpr, *rexpr;
    Node *qual, *result = NULL;

    foreach(c, string_attrs) {
        Var *var = (Var*) lfirst(c);
        // copy original relations string var
        lexpr = (Var*) copyObject(var);
        // create fake attr_name
        Value *val = (Value*) list_nth(original_rte->eref->colnames, var->varattno-1);
        char *attr_name = strVal(val);
        char *fake_attr_name = (char *) palloc(5 + strlen(attr_name) + 1);
        sprintf(fake_attr_name, "fake_%s", attr_name);
        // find var of same name from new fake rte
        rexpr = (Var*)scanRTEForColumn(pstate, rte, fake_attr_name, -1);

        rexpr->vartype = lexpr->vartype;
        rexpr->vartypmod = lexpr->vartypmod;
        rexpr->varcollid = lexpr->varcollid;

        qual = (Node *) make_op(pstate, list_make1(makeString("=")), (Node*)lexpr, (Node*)rexpr ,-1);

        result = conj_clause(pstate, result, qual);
        pstate->drb_quals = conj_clause(pstate,
            pstate->drb_quals,
            qual);
    }

    return result;
}

Node *
drillbeyond_column_transform(ParseState *pstate, ColumnRef *cref, Node *var)
{
    ListCell   *lc;
    Node        *result, *field1, *field2;
    int         vnum,
                sublevels_up = 0;
    Oid         vartypeid;
    int32       type_mod = 1700; //"open"
    Oid         varcollid = 0; // encoding something
    int attnum = 1;
    RangeTblEntry* fake_rte;
    RangeTblRef *rtr = NULL;
    List* relnamespace;
    char *rel_name, *field_name;
    FakeRelationEntry *entry;

    // no ltype anymore
    // vartypeid = TypenameGetTypid("ltype");
    vartypeid = 1700;

    if (var != NULL) {
        return NULL;
    }
    if (list_length(cref->fields) < 2) {
        Node       *field2 = (Node *) linitial(cref->fields);
        ereport(ERROR,
            (errcode(ERRCODE_DRILLBEYOND_RELNAME_PREFIX_NEEDED),
            errmsg("Open attribute %s needs to be prefixed by a valid relation name", strVal(field2))
            ));
    }

    field1 = (Node *) linitial(cref->fields);
    field2 = (Node *) lsecond(cref->fields);
    rel_name = strVal(field1);
    field_name = strVal(field2);

    // check whether a fake relation for this field was already created
    foreach(lc, pstate->drb_relations) {
        FakeRelationEntry *entry = (FakeRelationEntry *) lfirst(lc);
        if (strcmp(entry->field_name, field_name) == 0
        		&& strcmp(entry->rel_name, rel_name) == 0
                && entry->pstate == pstate) {
                rtr = entry->rtr;
                break;
            }
    }
    if (rtr != NULL) {
        fake_rte = GetRTEByRangeTablePosn(pstate, rtr->rtindex, 0);
        result = scanRTEForColumn(pstate, fake_rte, field_name, cref->location);
    }
    else {
        RangeTblEntry *original_rte = refnameRangeTblEntry(pstate, NULL,rel_name, -1, 0);
        List *string_attrs = drillbeyond_find_string_attrs(pstate, original_rte);
        fake_rte = create_fake_relation(pstate, original_rte, string_attrs, field_name);
        // pstate->drb_quals = conj_clause(pstate,
        //     create_fake_quals(pstate, original_rte, fake_rte, string_attrs),
        //     pstate->drb_quals);
        create_fake_quals(pstate, original_rte, fake_rte, string_attrs);


        vnum = RTERangeTablePosn(pstate, fake_rte, &sublevels_up);
        rtr = makeNode(RangeTblRef);
        rtr->rtindex = vnum;

        fake_rte->drb_expansion->rti = vnum;

        relnamespace = list_make1(fake_rte);
        pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);
        pstate->p_relnamespace = list_concat(pstate->p_relnamespace, relnamespace);
        pstate->p_varnamespace = lappend(pstate->p_varnamespace, fake_rte);
        result = (Node*)makeVar(vnum, attnum, vartypeid, type_mod, varcollid, sublevels_up);

        // save the new fake relation
        entry = (FakeRelationEntry*)palloc(sizeof(FakeRelationEntry));
        entry->rel_name = rel_name;
        entry->field_name = field_name;
        entry->rtr = rtr;
        entry->pstate = pstate;
        entry->relid = fake_rte->relid;
        pstate->drb_relations = lappend(pstate->drb_relations, entry);
    }

    return (Node*)result;
}
