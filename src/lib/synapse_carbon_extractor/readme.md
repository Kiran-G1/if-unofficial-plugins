

Prerequisite:

Synapse cluster needs to be configured with azure log anlytics for storing the metrics to be queried.
Follow this documentation for the same : https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-azure-log-analytics?source=recommendations


Run the synapse_carbon_extractor by providng the workspace id as parameter


ts-node-cwd.cmd .\synapse_carbon_extractor.ts --workspaceid=<workspaceid_here>
