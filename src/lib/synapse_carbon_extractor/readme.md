

Prerequisite:

Synapse cluster needs to be configured with azure log anlytics for storing the metrics to be queried.
Follow this documentation for the same : https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-azure-log-analytics?source=recommendations


Run the synapse_carbon_extractor by providng the workspace id as parameter


ts-node-cwd.cmd .\synapse_carbon_extractor.ts --workspaceid=<workspaceid_here>

sample output:

workspace_id provided is  da7cc61c-bba4-4efa-aef5-04ab14e8358d
Results for query
{
  name: 'PrimaryResult',
  columns: [
    { name: 'applicationId_s', type: 'string' },
    { name: 'workspaceName_s', type: 'string' },
    { name: 'applicationName_s', type: 'string' },
    { name: 'name_s', type: 'string' },
    { name: 'clusterName_s', type: 'string' },
    { name: 'TotalE', type: 'real' }
  ],
  rows: [
    [
      'application_1712032165218_0001',
      'synapsepockiran',
      'Notebook 2_testpool_1712032073_',
      'SynapseDiagnostic.cores',
      'testpool',
      93.39002486854534
    ],
    [
      'application_1711965691599_0001',
      'synapsepockiran',
      'Notebook 1_testpool_1711965610_',
      'SynapseDiagnostic.cores',
      'testpool',
      9.498730052773087
    ]
  ]
}
