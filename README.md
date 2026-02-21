# monitoring-kafka-service


⏺ ServiceNow Table API — Basic Examples                                                                                                                    
                                                                                                                                                           
  Base URL: https://YOUR_INSTANCE.service-now.com/api/now/table/{table_name}                                                                               
                  
  ---                                                                                                                                                      
  Create an Incident (POST)                                                                                                                                

  curl -X POST \
    https://YOUR_INSTANCE.service-now.com/api/now/table/incident \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    -u "admin:password" \
    -d '{
      "short_description": "Server is down",
      "urgency": "1",
      "impact": "1"
    }'

  ---
  Get a Record (GET)

  curl -X GET \
    https://YOUR_INSTANCE.service-now.com/api/now/table/incident/RECORD_SYS_ID \
    -H "Accept: application/json" \
    -u "admin:password"

  ---
  Query Records (GET with filters)

  curl -X GET \
    "https://YOUR_INSTANCE.service-now.com/api/now/table/incident?sysparm_query=state=1&sysparm_limit=10" \
    -H "Accept: application/json" \
    -u "admin:password"

  ---
  Update a Record (PATCH)

  curl -X PATCH \
    https://YOUR_INSTANCE.service-now.com/api/now/table/incident/RECORD_SYS_ID \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    -u "admin:password" \
    -d '{
      "state": "2",
      "work_notes": "Investigating the issue"
    }'

  ---
  Delete a Record (DELETE)

  curl -X DELETE \
    https://YOUR_INSTANCE.service-now.com/api/now/table/incident/RECORD_SYS_ID \
    -H "Accept: application/json" \
    -u "admin:password"

  ---
  Tips

  - Replace admin:password with a service account or use an OAuth Bearer token via -H "Authorization: Bearer TOKEN" for production
  - The sys_id of the created/fetched record is in the response result.sys_id
  - Common tables: incident, problem, change_request, sc_request, sys_user
  - Use sysparm_fields to limit returned fields: ?sysparm_fields=sys_id,short_description,state

──