/*-------------------------------------------------------------------------
 *
 * drillbeyond_explain.c
 *
 * IDENTIFICATION
 *    /drillbeyond/drillbeyond_explain.c
 *
 *  One big hack to enable the Demo-Frontend.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "string.h"
#include "drillbeyond/drillbeyond.h"

// global, manually managed state
static json_object *drb_ed = NULL;

static int num_expl_objs = 0;
static json_object* expl_objs[30];


extern const char *drillbeyond_explain_string() {
    json_object *tmp = drb_ed;
    const char *exp_str = json_object_get_string(tmp);
    return exp_str;
}

extern bool is_drillbeyond() {
    // return false;
    json_object *tmp = drb_ed;
    return tmp != NULL;
}

extern void clear_drillbeyond_explain() {
    int i;
    if (drb_ed != NULL) {
    	for	(i = 0; i<num_expl_objs;i++) {
    		json_object_put(expl_objs[i]);
    		expl_objs[i] = NULL;
    	}
    	num_expl_objs = 0;

        json_object_put(drb_ed);
        drb_ed = NULL;
    }
}

extern void merge_explain_data(json_object *expl) {
    json_object_iter iter;

    if (drb_ed == NULL) {
        drb_ed = expl;
        json_object_get(drb_ed);  // increase reference count
    }
    else {
        // first, memory management
        json_object_get(expl);
        expl_objs[num_expl_objs] = expl;
        num_expl_objs++;

        json_object *infos = json_object_object_get(drb_ed, INFOS);
        json_object *new_infos = json_object_object_get(expl, INFOS);

        json_object *cfn = json_object_object_get(new_infos, CANDIDATES_FOR);
        json_object *cfmerged = json_object_object_get(infos, CANDIDATES_FOR);
        json_object_object_foreachC(cfn, iter) {
            json_object_object_add(cfmerged, iter.key, iter.val);
        }
        json_object *ocn = json_object_object_get(new_infos, OPEN_COLUMNS);
        json_object *ocmerged = json_object_object_get(infos, OPEN_COLUMNS);
        json_object_object_foreachC(ocn, iter) {
            json_object_object_add(ocmerged, iter.key, iter.val);
        }

        json_object *ltn = json_object_object_get(new_infos, LOCAL_TABLES);
        json_object *ltmerged = json_object_object_get(infos, LOCAL_TABLES);
        json_object *l = json_object_array_get_idx(ltn, 0);
        const char* new_str = (json_object_get_string(json_object_object_get(l, "tableName")));

        int i;
        bool found = false;
        int arraylen = json_object_array_length(ltmerged);
        json_object *jvalue;

        for (i=0; i < arraylen; i++){
            jvalue = json_object_array_get_idx(ltmerged, i);
            const char* existing_str = (json_object_get_string(json_object_object_get(jvalue, "tableName")));
            if (strcmp(new_str, existing_str) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            json_object_array_add(ltmerged, l);
        }
    }
}
