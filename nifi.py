import requests
import json
import time
 
# NiFi API base
NIFI_API = "http://<host>:<port>/nifi-api"
 
# Source Process Group (the one you want to copy)
SOURCE_PG_ID = "8fe23053-0ef2-15a5-84c2-825b8ba293e4"
 
# Parent Process Group where new PGs should be created
PARENT_PG_ID = "153d62f9-0199-1000-0000-000045082093"
 
spacing_x = 430
spacing_y = 230
# Tables you want to create PGs for
tables = [
       
        {"schema": "schema1", "table": "table1"},
        {"schema": "schema2", "table": "table2"}
]
 
 
def url(path):
    return "{}/{}".format(NIFI_API.rstrip('/'), path.lstrip('/'))
 
def create_snippet(pg_id):
    """Create a snippet from the source PG and return snippetId"""
    r = requests.get(url("process-groups/{}".format(pg_id)))
    r.raise_for_status()
    pg = r.json()
 
    rev = pg["revision"]["version"]
    parent = pg["component"]["parentGroupId"]
 
    payload = {
            "snippet": {
                "parentGroupId": parent,
            "processGroups": {
                    pg_id: {
                        "id": pg_id,
                    "version": rev
                }
            }
        }
    }
 
    # print("Snippet payload:", json.dumps(payload, indent=2))  # debug
 
    r = requests.post(url("snippets"), json=payload)
    # print("Snippet response:", r.status_code, r.text)  # debug
    r.raise_for_status()
    return r.json()["snippet"]["id"]
 
def get_pg(pg_id):
    r = requests.get(url("process-groups/{}".format(pg_id)))
    r.raise_for_status()
    return r.json()
 
def instantiate_snippet(parent_id, snippet_id, schema, table, counter):
    """Instantiate snippet into target PG and update schema_name & table_name"""
    payload = {
            "originX": -1160.0 + (counter%7)*spacing_x,
        "originY": 648.0 + (counter//7)*spacing_y,
        "snippetId": snippet_id
    }
 
    # print("Instantiate payload:", json.dumps(payload, indent=2))
    r = requests.post(
            url("process-groups/{}/snippet-instance".format(parent_id)),
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"}
    )
    # print("Instantiate response:", r.status_code, r.text)
    r.raise_for_status()
    result = r.json()
 
    new_pg_id = result["flow"]["processGroups"][0]["id"]
 
    # --- Rename PG ---
    pg = get_pg(new_pg_id)
    rev = pg["revision"]["version"]
 
    rename_payload = {
            "revision": {"version": rev},
        "component": {"id": new_pg_id, "name": "{}_{}".format(schema, table)}
    }
    r = requests.put(url("process-groups/{}".format(new_pg_id)),
                     data=json.dumps(rename_payload),
                     headers={"Content-Type": "application/json"})
    # print("Rename response:", r.status_code, r.text)
    r.raise_for_status()
 
    # --- Update Variables ---
    # Small delay to let NiFi register the new PG
    time.sleep(1)
 
    r = requests.get(url("process-groups/{}/variable-registry".format(new_pg_id)))
    r.raise_for_status()
    current_vars = r.json()
 
    rev = current_vars["processGroupRevision"]["version"]
 
    # Build new variable list but only overwrite schema_name & table_name
    updated_vars = []
    for var in current_vars["variableRegistry"]["variables"]:
        name = var["variable"]["name"]
        if name == "schema_name":
            updated_vars.append({"variable": {"name": name, "value": schema}})
        elif name == "table_name":
            updated_vars.append({"variable": {"name": name, "value": table}})
        else:
            updated_vars.append(var)  # keep others unchanged
 
    var_payload = {
            "processGroupRevision": {
                "clientId": "update-script",
            "version": rev
        },
        "variableRegistry": {
                "processGroupId": new_pg_id,
            "variables": updated_vars
        }
    }
 
    r = requests.put(
            url("process-groups/{}/variable-registry".format(new_pg_id)),
        data=json.dumps(var_payload),
        headers={"Content-Type": "application/json"}
    )
    # print("Variable update response:", r.status_code, r.text)
    r.raise_for_status()
 
    return new_pg_id
 
 
# === Main ===-
for counter, item in enumerate(tables):
    snippet_id = create_snippet(SOURCE_PG_ID)  # create fresh snippet for each table
    new_pg_id = instantiate_snippet(PARENT_PG_ID, snippet_id, item["schema"], item["table"], counter)
    print("created pg-------------", counter, item)  
