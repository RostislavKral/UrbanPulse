export type DataQualityStatus = "fresh" | "stale" | "missing" | "error";

export type TableQualitySource = {
  status: DataQualityStatus;
  reason: string;
  latest_time: string | null;
  latest_age_seconds: number | null;
  freshness_threshold_seconds: number;
  window_seconds: number;
  rows_near_latest: number;
  vehicles_near_latest: number;
  rows_recent: number;
  vehicles_recent: number;
};

export type AlertQualitySource = {
  status: DataQualityStatus;
  reason: string;
  artifact_path: string;
  artifact_exists: boolean;
  artifact_mtime: string | null;
  artifact_age_seconds: number | null;
  latest_alert_time: string | null;
  latest_alert_age_seconds: number | null;
  total_count: number;
  alert_count: number;
  source?: "realtime" | "artifact" | string;
  model_path?: string | null;
  threshold?: number | null;
  context_minutes?: number | null;
  output_freshness_seconds?: number | null;
  sampling_seconds?: number | null;
  alert_max_per_run?: number | null;
  alert_min_risk?: number | null;
  raw_alert_count?: number | null;
  eligible_alert_count?: number | null;
  suppressed_alert_count?: number | null;
  persisted_prediction_count?: number | null;
  prediction_retention_hours?: number | null;
};

export type PipelineQualityCheck = {
  name: string;
  status: "failed" | "warning";
  message: string;
};

export type PipelineQualitySource = {
  status: DataQualityStatus;
  reason: string;
  report_path: string;
  report_exists: boolean;
  report_mtime: string | null;
  generated_at: string | null;
  report_age_seconds: number | null;
  freshness_threshold_seconds: number;
  summary: {
    failed: number;
    warnings: number;
    passed: number;
  };
  checks: PipelineQualityCheck[];
};

export type DataQualityResponse = {
  meta: {
    generated_at: string;
    query_time_ms: number;
    thresholds_seconds: {
      live: number;
      replay: number;
      alert_artifact: number;
      alert_rows: number;
      pipeline_report?: number;
      quality_window: number;
    };
  };
  overall_status: DataQualityStatus;
  sources: {
    live_positions: TableQualitySource;
    replay_trajectory: TableQualitySource;
    delay_alerts: AlertQualitySource;
    pipeline_quality?: PipelineQualitySource;
  };
};
