-- Load BCLConvert session data
INSERT INTO appsessions (
  SessionId, SessionName, DateCreated, ExecutionStatus,
  ICA_Link, ICA_ProjectId, WorkflowReference,
  RunId, RunName, PercentGtQ30,
  FlowcellBarcode, ReagentBarcode, Status,
  ExperimentName, RunDateCreated
) VALUES (
  '%s', '%s', '%s', '%s',
  '%s', '%s', '%s',
  '%s', '%s', %s,
  '%s', '%s', '%s',
  '%s', '%s'
)
ON DUPLICATE KEY UPDATE
  SessionName = VALUES(SessionName),
  DateCreated = VALUES(DateCreated),
  ExecutionStatus = VALUES(ExecutionStatus),
  ICA_Link = VALUES(ICA_Link),
  ICA_ProjectId = VALUES(ICA_ProjectId),
  WorkflowReference = VALUES(WorkflowReference),
  RunName = VALUES(RunName),
  PercentGtQ30 = VALUES(PercentGtQ30),
  FlowcellBarcode = VALUES(FlowcellBarcode),
  ReagentBarcode = VALUES(ReagentBarcode),
  Status = VALUES(Status),
  ExperimentName = VALUES(ExperimentName),
  RunDateCreated = VALUES(RunDateCreated),
  updated_at = NOW();
