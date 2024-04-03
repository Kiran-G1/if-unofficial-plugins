// class to query the log analytics workspace for GSF

import { DefaultAzureCredential } from "@azure/identity";
import {
  Durations,
  LogsQueryClient,
} from "@azure/monitor-query";


class SynapseCarbonExtractor {

    // constructor to initialize the class with the workspace id and the query
    private logsQueryClient: LogsQueryClient;
    private azureLogAnalyticsWorkspaceId: string;
    private query: string;
    
    constructor(workspaceId: string, query: string) {
        this.logsQueryClient = new LogsQueryClient(new DefaultAzureCredential());
        this.azureLogAnalyticsWorkspaceId = workspaceId;
        this.query = query;
    }

    // function which runs the query on the azure log analytics and prints the results
    async  run(required_duration: string | undefined) {


      let duration: string;

      if (required_duration === "last24Hours") {
          duration = Durations.last24Hours;
      } else if (required_duration === "last3Days") {
          duration = Durations.last3Days;
      } else {
          duration = Durations.last7Days;
      }

        const kustoQuery = this.query;
        const result = await this.logsQueryClient.queryLogs(
          this.azureLogAnalyticsWorkspaceId,
          kustoQuery,
          duration
        );
        const tablesFromResult = result.tables;
      
        if (tablesFromResult == null) {
          console.log(`No results for query`);
          return;
        }
      
        console.log(`Results for query`);

        for (const table of tablesFromResult)
        {
           console.log(table)
        }
      }
}


const query = `
let job_completions = (){
    SparkListenerEvent_CL
    | summarize arg_max(TimeGenerated, *) by Completion_Time_d,workspaceName_s, applicationName_s, applicationId_s, Job_ID_d
    | where Completion_Time_d>0
    | project TimeGenerated, Completion_Time_d,workspaceName_s, applicationName_s, applicationId_s, Job_ID_d
    };
    let job_submissions = (){
    SparkListenerEvent_CL
    | summarize arg_min(TimeGenerated, *) by Submission_Time_d,workspaceName_s, applicationName_s, applicationId_s, Job_ID_d
    | where Submission_Time_d>0
    | project TimeGenerated, Submission_Time_d,workspaceName_s, applicationName_s, applicationId_s, Job_ID_d
    };
    
    let job_details = (){
    job_completions()
    | join kind=inner job_submissions() on applicationId_s, Job_ID_d
    | project-rename TimeGenerated_Completions = TimeGenerated1, Completion_Time = Completion_Time_d, Submission_Time = Submission_Time_d, workspaceName = workspaceName_s, applicationName = applicationName_s, applicationId = applicationId_s, Job_ID = Job_ID_d
    | project TimeGenerated, Completion_Time, Submission_Time, workspaceName,applicationName,applicationId,Job_ID
    };
    
    let application_cores = () {
    SparkMetrics_CL
    | where name_s contains "cores"
    | summarize arg_max(TimeGenerated, *) by applicationId_s, workspaceName_s, applicationName_s, executorId_s
    | summarize TotalCores = sum(value_d) by applicationId_s, workspaceName_s, applicationName_s,name_s,clusterName_s,executorId_s
    };
    
    let full_application_details = (){
    application_cores()
    | join kind=inner job_details() on $left.applicationId_s == $right.applicationId
    };
    
    let maxCpuLoadPerNotebook = () {
    SparkMetrics_CL
    | where name_s contains "processCpuLoad"
    | summarize arg_max(value_d, *) by applicationId_s, workspaceName_s, applicationName_s, executorId_s
    | summarize maxCpuUtil = sum(value_d) by applicationId_s, workspaceName_s, applicationName_s,name_s,clusterName_s,executorId_s
    | project maxCpuUtil, applicationId_s, workspaceName_s, applicationName_s, clusterName_s,executorId_s
    };
    
    
    let e_calculated = () { 
    full_application_details()
    | join kind=inner maxCpuLoadPerNotebook() on applicationId_s, workspaceName_s, applicationName_s,clusterName_s,executorId_s
    | extend totaljobTime = Completion_Time-Submission_Time
    | extend E = TotalCores * ((totaljobTime/1000)/3600) * (maxCpuUtil*100) * 270.00
    | project  applicationId_s, workspaceName_s, applicationName_s,name_s,clusterName_s,executorId_s, TotalCores, maxCpuUtil,Job_ID, totaljobTime, E
    };
    
    e_calculated()
    | summarize TotalE = sum(E) by applicationId_s, workspaceName_s, applicationName_s,name_s,clusterName_s    
`;


// read command line arguments and run the query

console.log(process.argv.slice(2));


const workspace_id = process.argv.slice(2).at(0)?.replace("--workspaceid=", "");

const required_duration = process.argv.slice(2).at(1)?.replace("--duration=", "");

console.log("workspace_id provided is ", workspace_id);

if (!workspace_id) {
    console.log("workspace_id is required");
    process.exit(1);
}


const SCE = new SynapseCarbonExtractor(workspace_id, query);
SCE.run(required_duration);
