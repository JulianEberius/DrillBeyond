#include "postgres.h"

#include "drillbeyond/drillbeyond.h"
#include "catalog/pg_type.h"
#include "curl/curl.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "optimizer/clauses.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "lib/stringinfo.h"

#include <stdlib.h>
#include <time.h>

int drb_max_num_cands = 1;

static int next_context = 0;

extern void reset_context() {
    next_context = 0;
}

static int get_context() {
    return next_context++;
}

// LOCAL DUMMY SERVER
#define URL_BASE "http://127.0.0.1:8765"
#define DRILLBEYOND_PATH "/"
#define DRILLBEYOND_ARTIFICIAL_PATH "/artificial"
#define SELECTIVITY_PATH "/drb_estimatedSelectivity"

// REA SERVER
// #define URL_BASE "http://141.76.47.133:8765"
// #define DRILLBEYOND_PATH "/"
// #define DRILLBEYOND_ARTIFICIAL_PATH "/artificial"
// #define SELECTIVITY_PATH "/drb_estimatedSelectivity"

static size_t write_data_to_buffer(void *buffer, size_t size, size_t nmemb, void *userp);
static json_object* send_request(const char *path, const char *msg_str);

static json_object* serialize_restrictlist(List *restrictlist);
static bool is_simple_restriction(List *argumentList);
static char* simple_restriction_to_string(const Node *expr);

extern int drillbeyond_request(const char *msg_str, DrillBeyondState *dbstate) {
    void *ptr;
    int j, i, t;
    int cand_length, num_tuples, result_length;
    json_object *obj;
    json_object *values, *candidates, *cand, *explanation, *sel, *inUnion;
    HASH_SEQ_STATUS seq_status;
    DrillBeyond *plan = (DrillBeyond *) dbstate->js.ps.plan;
    DrillBeyondExpansion *expansion = plan->drb_expansion;


    if (drb_enable_rea)
        obj = send_request(URL_BASE DRILLBEYOND_PATH, msg_str);         //send the JSON request and get a JSON object returned
    else
        obj = send_request(URL_BASE DRILLBEYOND_ARTIFICIAL_PATH, msg_str);

    /* process parsed results */
    candidates = json_object_object_get(obj, CANDIDATES);           //get the candidates with the values
    inUnion = json_object_object_get(obj, IN_UNION);           //get the candidates with the values
    if(candidates)cand_length = json_object_array_length(candidates);
    else{printf("Empty response\n"); cand_length = 0;}
    dbstate->db_num_cands = cand_length;


    num_tuples = 0;
    // calculate total number of tuples across candidates and extract selectivities per candidate
    expansion->selectivities = (double*) palloc(sizeof(double) * dbstate->db_num_cands);
    for (i=0; i<cand_length; i++) {
        cand = json_object_array_get_idx(candidates, i);            //get one candidate
        values = json_object_object_get(cand, VALUES);              //get the values out of the candidate
        result_length = json_object_array_length(values);
        num_tuples += result_length;
        sel = json_object_object_get(cand, SELECTIVITY);
        expansion->selectivities[i] = json_object_get_double(sel);
    }


    t = 0;
    double sumInUnion = 0;
    hash_seq_init(&seq_status, expansion->results_hashtable);
    while((ptr = hash_seq_search(&seq_status)) != NULL) {
        Datum *new_values;
        bool *is_null;
        DrillBeyondValues *drb_values = (DrillBeyondValues *)ptr;
        if (drb_values->requested)
            continue;

        if (dbstate->db_num_cands == 0) {                                   //no candidates
            new_values = (Datum*)palloc(sizeof(Datum) * 1);
            is_null = (bool*)palloc(sizeof(bool) * 1);
            is_null[0] = true;
            new_values[0] = 0;
            drb_values->numValues = 1;
        } else {
            new_values = (Datum*)palloc(sizeof(Datum) * dbstate->db_num_cands);     //create empty new values
            is_null = (bool*)palloc(sizeof(bool) * dbstate->db_num_cands);         //a is_null array

            for (j = 0; j < dbstate->db_num_cands; j++) {                   //iterate through the candidates
                json_object *val;
                cand = json_object_array_get_idx(candidates, j);
                values = json_object_object_get(cand, VALUES);              //get values again of the current candidate
                val = json_object_array_get_idx(values, t);                 //get data at index k for each candidate (k is equal for all candidates)
                if (json_object_get_type(val) == json_type_null){           //if empty
                    is_null[j] = true;                                      //set corresponding is_null to true
                    new_values[j] = 0;
                }
                else {
                    Datum v = Float8GetDatum(json_object_get_double(val));  //always a double value?
                    new_values[j] = DirectFunctionCall1(float8_numeric, v); //convert value to general numeric
                    is_null[j] = false;                                     //there was a non null value -> is_null = false
                }
            }
            drb_values->numValues = dbstate->db_num_cands;
        }
        drb_values->values = new_values;                                    //requested drillbeyond values drb_values->is_null = is_null;                                      //is_null bit vektor
        drb_values->is_null = is_null;                                    //requested drillbeyond values drb_values->is_null = is_null;                                      //is_null bit vektor
        drb_values->requested = true;
        drb_values->inUnion = json_object_get_boolean(json_object_array_get_idx(inUnion, t));
        if (drb_values->inUnion)
            sumInUnion += 1;
        // Datum attr = drb_values->joinValues[0];
        // const char *c = DatumGetCString(FunctionCall1(&(expansion->outFunctions)[0], attr));
        // printf("value %s is in union: %d\n", c, drb_values->inUnion);
        t++;
    }
    if (dbstate->db_num_cands == 0) {
    	dbstate->db_num_cands = 1; // we added one NULL candidate
    }

    expansion->union_selectivity = sumInUnion / t;

    explanation = json_object_object_get(obj, EXPLANATION);
    merge_explain_data(explanation);
    json_object_put(obj);
    return 0;
}

extern json_object* serialize_restrictlist(List *restrictlist) {
    ListCell *c;
    // List *simpleRestrictions = NIL;
    json_object *result = json_object_new_array();
    foreach(c, restrictlist) {
        RestrictInfo *rinfo;
        OpExpr *opExpression = NULL;
        char *operatorName = NULL;
        NodeTag expressionType = T_Invalid;
        Expr *expression;

        if (nodeTag(lfirst(c)) == T_RestrictInfo) {
        	rinfo = (RestrictInfo *) lfirst(c);
        	expression = rinfo->clause;
        } else {
        	expression = (Expr *) lfirst(c);
        }

        /* we only support operator expressions */
        expressionType = nodeTag(expression);
        if (expressionType != T_OpExpr)
        {
            continue;
        }
        opExpression = (OpExpr *) expression;
        operatorName = get_opname(opExpression->opno);
        if (is_simple_restriction(opExpression->args)) {
            json_object *rest_str = json_object_new_string(simple_restriction_to_string((Node *) expression));
            json_object_array_add(result, rest_str);
        }
            // simpleRestrictions = lappend(simpleRestrictions, expression);
    }
    // if (list_length(simpleRestrictions) > 0) {
        // return simple_restriction_to_string((Node *) simpleRestrictions);
    // }
    return result;
}

static bool is_simple_restriction(List *argumentList)
{
    ListCell *c;
    int foundConst = 0;
    int foundVar = 0;
    int foundOther = 0;

    foreach(c, argumentList)
    {
        Expr *argument = (Expr *) lfirst(c);
        switch (nodeTag(argument)) {
            case T_Var:
                foundVar++;
                break;
            case T_Const:
                foundConst++;
                break;
            default:
                foundOther++;
                break;
        }
    }

    return (foundVar == 1) && (foundConst == 1) && (foundOther == 0);
}

static char* simple_restriction_to_string(const Node *expr)
{
    if (expr == NULL)
    {
        return NULL;
    }

    if (IsA(expr, Var)) // there can be only one var
    {
        return NULL;
    }
    else if (IsA(expr, Const))
    {
        const Const *c = (const Const *) expr;
        Oid         typoutput;
        bool        typIsVarlena;
        char       *outputstr;

        if (c->constisnull)
        {
            return "NULL";
        }

        getTypeOutputInfo(c->consttype,
                          &typoutput, &typIsVarlena);
        outputstr = OidOutputFunctionCall(typoutput, c->constvalue);
        return outputstr;
    }
    else if (IsA(expr, OpExpr))
    {
        const OpExpr *e = (const OpExpr *) expr;
        char       *opname, *left, *right, *result;
        Assert(list_length(e->args) > 1);

        opname = get_opname(e->opno);
        left = simple_restriction_to_string(get_leftop((const Expr *) e));
        right = simple_restriction_to_string(get_rightop((const Expr *) e));
        if (right == NULL) // one of them has to be the constant, switch if necessary
            right = left;
        result = palloc(strlen(right) + 2 + 1);
        sprintf(result, "%s %s", opname, right);
        return result;
    }
    else if (IsA(expr, RestrictInfo))
    {
        const RestrictInfo *rinfo = (const RestrictInfo *) expr;
        return simple_restriction_to_string((Node *) rinfo->clause);
    }
    // else if (IsA(expr, List)) {
    //     ListCell *c;
    //     List *list = (List *) expr;
    //     Node *node;
    //     StringInfo s = makeStringInfo();
    //     appendStringInfoChar(s, '[');
    //     foreach(c, list) {
    //         node = lfirst(c);
    //         appendStringInfoString(s, simple_restriction_to_string(node));
    //         if (lnext(c))
    //             appendStringInfoChar(s, ',');
    //     }
    //     appendStringInfoChar(s, ']');
    //     char *result = s->data;
    //     pfree(s);
    //     return result;
    // }
    else
        Assert(false);
    return NULL;
}


static json_object* send_request(const char *url, const char *msg_str) {
    CURL            *curl;
    CURLcode        ret;
    char            curl_error_buffer[CURL_ERROR_SIZE+1]    = {0};
    struct curl_slist   *curl_opts = NULL;
    json_object *obj;
    StringInfo buffer = makeStringInfo();

    curl_global_init(CURL_GLOBAL_ALL);
    initStringInfo(buffer);

    curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, curl_error_buffer);
    curl_easy_setopt(curl, CURLOPT_POST, 1);
    curl_opts = curl_slist_append(curl_opts, "Content-type:");
    curl_opts = curl_slist_append(curl_opts, "application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, curl_opts);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, msg_str);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data_to_buffer);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, buffer);

    // struct timeval  tv;
    // gettimeofday(&tv, NULL);
    // double time_in_mill =
    //      (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000;
    ret = curl_easy_perform(curl);
    //     gettimeofday(&tv, NULL);
    // double time_in_mill2 =
    //      (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000;
    // printf("time for req: %f", time_in_mill2-time_in_mill);

    curl_easy_cleanup(curl);
    if(ret) {
        ereport(ERROR,
            (errcode(ERRCODE_DRILLBEYOND_REQUEST_FAILED),
            errmsg("Can't get a response from server: %s", curl_error_buffer)
            ));
    }
    if(curl_opts)
        curl_slist_free_all(curl_opts);

    obj = json_tokener_parse(buffer->data);
    if(obj == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_DRILLBEYOND_REQUEST_FAILED),
            errmsg("Could not parse server response: %s", buffer->data)
            ));
    }
    return obj;
}

extern void add_restrictions_to_msg(json_object *msg, List* restrictions)
{
    json_object *restriction_array;
    if (drb_enable_send_predicates || drb_enable_selectivity_estimation) {
        restriction_array = serialize_restrictlist(restrictions);
    } else {
        restriction_array = json_object_new_array();
    }
    if (drb_enable_send_isnumeric_constraint) {
        json_object_array_add(restriction_array, json_object_new_string("isNumeric()"));
    }

	json_object_object_add(msg, RESTRICTIONS, restriction_array);
}

extern void tup_to_json(DrillBeyondExpansion *expansion, List *join_cols, TupleTableSlot *slot, json_object *msg)
{
    Datum       origattr;                                                                   //Datum is an unsigned int
    int i;
    bool isnull;
    int num_join_cols = list_length(join_cols);

    Datum *keys = (Datum *)palloc(sizeof(Datum) * num_join_cols);
    HeapTuple htup = ExecCopySlotTuple(slot);
    for (i = 0; i < num_join_cols; ++i)
    {
        Var *var = (Var *)list_nth(join_cols, i);                                       //get the varattno
        origattr = heap_getattr(htup, var->varattno, slot->tts_tupleDescriptor, &isnull);   //get the attribute with varattno out of the slot
        if (isnull) {                                                                       //no original attribute? Can this happen?
            keys[i] = 0;
        } else {
            keys[i] = origattr;                                                             //set the key
        }
    }
    drb_addToHashTable(expansion, keys, NULL, 0);                                           //add the expansion to hash table with keys (?)
}

extern bool drillbeyond_fill_msg(DrillBeyondExpansion *expansion, json_object *msg) {
    int num_join_cols = list_length(expansion->join_cols);
    int         i;
    void        *ptr;
    Datum       attr;
    char       *value;
    json_object *col_array, *col;
    HASH_SEQ_STATUS seq_status;
    int num_tuples = 0;
    bool       unrequestedEntries = false;

    col_array = json_object_object_get(msg, COLUMNS);
    hash_seq_init(&seq_status, expansion->results_hashtable);
    while((ptr = hash_seq_search(&seq_status)) != NULL) {
        DrillBeyondValues *values = (DrillBeyondValues *)ptr;

        if (values->requested)
            continue;

        for (i = 0; i < num_join_cols; i++) {
            col = json_object_array_get_idx(col_array, i);
            attr = values->joinValues[i];
            if (attr == 0) {
                json_object_array_add(col, json_type_null);
            }
            else {
                attr = PointerGetDatum(PG_DETOAST_DATUM(attr));
                value = DatumGetCString(FunctionCall1(&(expansion->outFunctions)[i], attr));
                json_object_array_add(col, json_object_new_string(value));
                unrequestedEntries = true;
                pfree(value);
            }


            if (DatumGetPointer(attr) != DatumGetPointer(values->joinValues[i]))
                pfree(DatumGetPointer(attr));
        }
        num_tuples++;
    }
    return unrequestedEntries;
}

//! not used
//TODO: REFACTOR  is almost same as tup_to_json
extern void heap_tup_to_json(HeapTuple tuple, TupleDesc tupDesc, json_object *msg)
{
    int         natts = tupDesc->natts;
    int         i;
    int         attno = 0;
    Datum       origattr,
                attr;
    char       *value;
    bool        isnull;
    Oid         typoutput;
    bool        typisvarlena;
    json_object *col_array, *col;

    col_array = json_object_object_get(msg, COLUMNS);
    for (i = 0; i < natts; ++i)
    {
        if (TypeCategory(tupDesc->attrs[i]->atttypid) != TYPCATEGORY_STRING)
            continue;
        origattr = heap_getattr(tuple, i + 1, tupDesc, &isnull);
        if (isnull)
            continue;
        getTypeOutputInfo(tupDesc->attrs[i]->atttypid,
                          &typoutput, &typisvarlena);

        col = json_object_array_get_idx(col_array, attno);
        attr = PointerGetDatum(PG_DETOAST_DATUM(origattr));
        value = OidOutputFunctionCall(typoutput, attr);
        json_object_array_add(col, json_object_new_string(value));
        attno += 1;

        pfree(value);
        if (DatumGetPointer(attr) != DatumGetPointer(origattr))
            pfree(DatumGetPointer(attr));
    }
}

extern double estimateSelectivity(DrillBeyondExpansion *expansion, Oid extended_relid, List *restrictlist) {
    json_object *req;
    json_object *obj;
    HeapTuple *samples;
    const char *msg_str;
    TupleDesc tupDesc;
    int numSampleRows;
    int i;
    const int numSamples = 25;

    if (expansion->selectivity != -1.0)
        return expansion->selectivity;

    req = initDrillBeyondRequest(expansion);
    json_object_object_add(req, MAX_CANDS, json_object_new_int(numSamples));
    numSampleRows = drillbeyond_sample_rel(extended_relid, numSamples, &samples, &tupDesc);
    for (i = 0; i < numSampleRows; ++i)
    {
        heap_tup_to_json(samples[i], tupDesc, req);
    }

    add_restrictions_to_msg(req, restrictlist);

    msg_str = json_object_to_json_string_ext(req, JSON_C_TO_STRING_PLAIN);  //convert request data to string
    obj = send_request(URL_BASE SELECTIVITY_PATH, msg_str);         //send the JSON request and get a JSON object return

    double sel = json_object_get_double(json_object_object_get(obj, SELECTIVITY));
    expansion->selectivity = sel;
    return sel;
}

extern json_object *initDrillBeyondRequest(DrillBeyondExpansion *expansion) {
    int i, i_max;
    ListCell *c;
    json_object *msg = json_object_new_object();
    json_object *kw = json_object_new_string(expansion->keyword);
    json_object *max_num_cands = json_object_new_int(drb_max_num_cands);
    json_object *local_table = json_object_new_string(expansion->extended_relname);
    json_object *col_array = json_object_new_array();
    json_object *col_names_array = json_object_new_array();
    json_object *str_col_names_array = json_object_new_array();

    json_object_object_add(msg, KEYWORD, kw);
    json_object_object_add(msg, MAX_CANDS, max_num_cands);
    json_object_object_add(msg, LOCAL_TABLE, local_table);

    i_max = list_length(expansion->join_cols);
    for (i = 0; i < i_max; i++)
    {
        json_object_array_add(col_array, json_object_new_array());
    }
    json_object_object_add(msg, COLUMNS, col_array);

    foreach(c, expansion->extended_attrNames) {
        Value *val = (Value *) lfirst(c);
        json_object_array_add(col_names_array, json_object_new_string(strVal(val)));
    }
    json_object_object_add(msg, COL_NAMES, col_names_array);

    foreach(c, expansion->extended_strAttrNames) {
        Value *val = (Value *) lfirst(c);
        json_object_array_add(str_col_names_array, json_object_new_string(strVal(val)));
    }
    json_object_object_add(msg, STR_COL_NAMES, str_col_names_array);

    return msg;
}

static size_t
write_data_to_buffer(void *buffer, size_t size, size_t nmemb, void *userp)
{
    StringInfo      b  = (StringInfo)userp;
    int             s   = size*nmemb;
    appendBinaryStringInfo(b, buffer, s);

    return s;
}
