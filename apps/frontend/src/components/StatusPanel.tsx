import type { ReplayFilters, ReplayMeta, ReplayStats, ViewMode } from "../types/replay";
import type {
  DataQualityResponse,
  DataQualityStatus,
  PipelineQualityCheck,
  PipelineQualitySource,
} from "../types/dataQuality";
import type { VehicleMode } from "../types/vehicle";

function formatQualityAge(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  if (value < 60) return `${Math.round(value)}s`;
  if (value < 3600) return `${Math.round(value / 60)}m`;
  if (value < 86400) return `${(value / 3600).toFixed(1)}h`;
  return `${(value / 86400).toFixed(1)}d`;
}

function formatQualityStatus(status: DataQualityStatus | undefined): string {
  return status ? status.toUpperCase() : "UNKNOWN";
}

function formatQualityPercent(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `${Math.round(value * 100)}%`;
}

const MISSING_PIPELINE_QUALITY: PipelineQualitySource = {
  status: "missing",
  reason: "Pipeline quality is unavailable from the current API response.",
  report_path: "",
  report_exists: false,
  report_mtime: null,
  generated_at: null,
  report_age_seconds: null,
  freshness_threshold_seconds: 0,
  summary: {
    failed: 0,
    warnings: 0,
    passed: 0,
  },
  checks: [],
};

function formatPipelineChecks(checks: PipelineQualityCheck[]): string {
  if (!checks.length) return "";
  return checks
    .map((check) => `${check.status}: ${check.name}`)
    .join(" | ");
}

type StatusPanelProps = {
  viewMode: ViewMode;
  isConnected: boolean;
  activeCount: number;
  waitingCount: number;
  isReplayLoading: boolean;
  isReplayPlaying: boolean;
  replaySpeed: number;
  replayWindowDraftMinutes: number;
  replayWindowDirty: boolean;
  replayWindowOptions: number[];
  replayFilters: ReplayFilters;
  replayModes: Array<VehicleMode | "all">;
  replayMeta: ReplayMeta | null;
  replayProgressPct: number;
  activeReplayWindowMinutes: number;
  playbackTime: number;
  replayDurationMs: number;
  replayStats: ReplayStats;
  replayError: string | null;
  dataQuality: DataQualityResponse | null;
  dataQualityError: string | null;
  isDataQualityLoading: boolean;
  playbackDelaySeconds: number;
  trailLengthSeconds: number;
  onLiveClick: () => void;
  onReplayClick: () => void;
  onApplyReplayWindow: () => void;
  onReplayWindowChange: (minutes: number) => void;
  onToggleReplayPlaying: () => void;
  onReplayReset: () => void;
  onReplaySpeedChange: (speed: number) => void;
  onReplayFiltersChange: (filters: ReplayFilters) => void;
};

export function StatusPanel({
  viewMode,
  isConnected,
  activeCount,
  waitingCount,
  isReplayLoading,
  isReplayPlaying,
  replaySpeed,
  replayWindowDraftMinutes,
  replayWindowDirty,
  replayWindowOptions,
  replayFilters,
  replayModes,
  replayMeta,
  replayProgressPct,
  activeReplayWindowMinutes,
  playbackTime,
  replayDurationMs,
  replayStats,
  replayError,
  dataQuality,
  dataQualityError,
  isDataQualityLoading,
  playbackDelaySeconds,
  trailLengthSeconds,
  onLiveClick,
  onReplayClick,
  onApplyReplayWindow,
  onReplayWindowChange,
  onToggleReplayPlaying,
  onReplayReset,
  onReplaySpeedChange,
  onReplayFiltersChange,
}: StatusPanelProps) {
  const statusLabel =
    viewMode === "live"
      ? isConnected
        ? "PRAGUE LIVE"
        : "DISCONNECTED"
      : "PRAGUE REPLAY";
  const pipelineQuality =
    dataQuality?.sources.pipeline_quality ?? MISSING_PIPELINE_QUALITY;
  const displayedOverallStatus =
    dataQuality && !dataQuality.sources.pipeline_quality
      ? "missing"
      : dataQuality?.overall_status;

  return (
    <div className="status-panel">
      <div className="status-panel__header">
        <div
          className={`status-panel__dot status-panel__dot--${viewMode} ${
            viewMode === "live" && isConnected ? "status-panel__dot--connected" : ""
          }`}
        />
        <span className="status-panel__title">{statusLabel}</span>
      </div>

      <div className="status-panel__grid">
        <div>
          Active: <span className="status-panel__value">{activeCount}</span>
        </div>
        <div>
          Waiting: <span className="status-panel__value">{waitingCount}</span>
        </div>

        <div className="status-panel__buttons">
          <button
            className="status-panel__button"
            onClick={onLiveClick}
            style={{ opacity: viewMode === "live" ? 1 : 0.6 }}
          >
            Live
          </button>
          <button
            className="status-panel__button"
            onClick={onReplayClick}
            disabled={isReplayLoading}
            style={{ opacity: viewMode === "replay" ? 1 : 0.6 }}
          >
            {isReplayLoading ? "Loading..." : "Replay"}
          </button>
        </div>

        <label className="status-panel__control">
          Window
          <select
            value={replayWindowDraftMinutes}
            onChange={(event) => onReplayWindowChange(Number(event.target.value))}
            disabled={isReplayLoading}
          >
            {replayWindowOptions.map((minutes) => (
              <option key={minutes} value={minutes}>
                {minutes}m
              </option>
            ))}
          </select>
        </label>

        <button
          className="status-panel__button"
          onClick={onApplyReplayWindow}
          disabled={isReplayLoading || !replayWindowDirty}
          style={{ opacity: replayWindowDirty ? 1 : 0.6 }}
        >
          Apply
        </button>
        <div>
          Mode: {viewMode.charAt(0).toUpperCase()}
          {viewMode.substring(1)}
        </div>
      </div>

      {viewMode === "replay" && (
        <div className="status-panel__row">
          <button className="status-panel__button" onClick={onToggleReplayPlaying}>
            {isReplayPlaying ? "Pause" : "Play"}
          </button>
          <button className="status-panel__button" onClick={onReplayReset}>
            Reset
          </button>
          <label className="status-panel__control">
            Speed
            <select
              value={replaySpeed}
              onChange={(event) => onReplaySpeedChange(Number(event.target.value))}
            >
              <option value={0.5}>0.5x</option>
              <option value={1}>1x</option>
              <option value={2}>2x</option>
              <option value={4}>4x</option>
            </select>
          </label>
        </div>
      )}

      {viewMode === "replay" && (
        <div className="status-panel__replay-filters">
          <label className="status-panel__control">
            Mode
            <select
              value={replayFilters.mode}
              onChange={(event) =>
                onReplayFiltersChange({
                  ...replayFilters,
                  mode: event.target.value as ReplayFilters["mode"],
                })
              }
            >
              {replayModes.map((mode) => (
                <option key={mode} value={mode}>
                  {mode}
                </option>
              ))}
            </select>
          </label>
          <label className="status-panel__control">
            Line
            <input
              value={replayFilters.lineQuery}
              onChange={(event) =>
                onReplayFiltersChange({
                  ...replayFilters,
                  lineQuery: event.target.value,
                })
              }
              placeholder="all"
            />
          </label>
          <label className="status-panel__checkbox">
            <input
              type="checkbox"
              checked={replayFilters.includeInterpolated}
              onChange={(event) =>
                onReplayFiltersChange({
                  ...replayFilters,
                  includeInterpolated: event.target.checked,
                })
              }
            />
            Include interpolated
          </label>
          <label className="status-panel__checkbox">
            <input
              type="checkbox"
              checked={replayFilters.hideVehiclesWithLatestInvalidGap}
              onChange={(event) =>
                onReplayFiltersChange({
                  ...replayFilters,
                  hideVehiclesWithLatestInvalidGap: event.target.checked,
                })
              }
            />
            Hide latest invalid-gap
          </label>
        </div>
      )}

      <div className="status-panel__footer">
        Buffer: {playbackDelaySeconds}s | Trail: {trailLengthSeconds}s
      </div>

      <div className="status-panel__quality">
        <div className="status-panel__quality-header">
          <span>Data quality</span>
          <span
            className={`status-panel__quality-status status-panel__quality-status--${
              displayedOverallStatus ?? "missing"
            }`}
          >
            {isDataQualityLoading
              ? "CHECKING"
              : formatQualityStatus(displayedOverallStatus)}
          </span>
        </div>

        {dataQualityError && (
          <div className="status-panel__quality-error">{dataQualityError}</div>
        )}

        {dataQuality && (
          <div className="status-panel__quality-list">
            <div className="status-panel__quality-row">
              <span
                className={`status-panel__quality-status status-panel__quality-status--${dataQuality.sources.live_positions.status}`}
              >
                {formatQualityStatus(dataQuality.sources.live_positions.status)}
              </span>
              <div className="status-panel__quality-copy">
                <strong>Live DB</strong>
                <span>
                  age {formatQualityAge(dataQuality.sources.live_positions.latest_age_seconds)}
                  {" | "}recent {dataQuality.sources.live_positions.rows_recent}/
                  {dataQuality.sources.live_positions.vehicles_recent}
                  {" | "}latest {dataQuality.sources.live_positions.rows_near_latest}/
                  {dataQuality.sources.live_positions.vehicles_near_latest}
                </span>
                <small>{dataQuality.sources.live_positions.reason}</small>
              </div>
            </div>

            <div className="status-panel__quality-row">
              <span
                className={`status-panel__quality-status status-panel__quality-status--${dataQuality.sources.replay_trajectory.status}`}
              >
                {formatQualityStatus(dataQuality.sources.replay_trajectory.status)}
              </span>
              <div className="status-panel__quality-copy">
                <strong>Replay DB</strong>
                <span>
                  age{" "}
                  {formatQualityAge(dataQuality.sources.replay_trajectory.latest_age_seconds)}
                  {" | "}recent {dataQuality.sources.replay_trajectory.rows_recent}/
                  {dataQuality.sources.replay_trajectory.vehicles_recent}
                  {" | "}latest {dataQuality.sources.replay_trajectory.rows_near_latest}/
                  {dataQuality.sources.replay_trajectory.vehicles_near_latest}
                </span>
                <small>{dataQuality.sources.replay_trajectory.reason}</small>
              </div>
            </div>

            <div className="status-panel__quality-row">
              <span
                className={`status-panel__quality-status status-panel__quality-status--${dataQuality.sources.delay_alerts.status}`}
              >
                {formatQualityStatus(dataQuality.sources.delay_alerts.status)}
              </span>
              <div className="status-panel__quality-copy">
                <strong>ML signal</strong>
                <span>
                  source {dataQuality.sources.delay_alerts.source ?? "artifact"}
                  {" | "}scored{" "}
                  {formatQualityAge(dataQuality.sources.delay_alerts.artifact_age_seconds)}
                  {" | "}latest row{" "}
                  {formatQualityAge(
                    dataQuality.sources.delay_alerts.latest_alert_age_seconds,
                  )}
                  {" | "}alerts {dataQuality.sources.delay_alerts.alert_count}/
                  {dataQuality.sources.delay_alerts.total_count}
                </span>
                <small>
                  threshold {formatQualityPercent(dataQuality.sources.delay_alerts.threshold)}
                  {" | "}raw {dataQuality.sources.delay_alerts.raw_alert_count ?? "--"}
                  {" | "}suppressed{" "}
                  {dataQuality.sources.delay_alerts.suppressed_alert_count ?? "--"}
                  {" | "}saved{" "}
                  {dataQuality.sources.delay_alerts.persisted_prediction_count ?? "--"}
                </small>
                <small>{dataQuality.sources.delay_alerts.reason}</small>
              </div>
            </div>

            <div className="status-panel__quality-row">
              <span
                className={`status-panel__quality-status status-panel__quality-status--${pipelineQuality.status}`}
              >
                {formatQualityStatus(pipelineQuality.status)}
              </span>
              <div className="status-panel__quality-copy">
                <strong>Pipeline</strong>
                <span>
                  report{" "}
                  {formatQualityAge(
                    pipelineQuality.report_age_seconds,
                  )}
                  {" | "}pass {pipelineQuality.summary.passed}
                  {" | "}warn {pipelineQuality.summary.warnings}
                  {" | "}fail {pipelineQuality.summary.failed}
                </span>
                <small>{pipelineQuality.reason}</small>
                {pipelineQuality.checks.length > 0 && (
                  <small className="status-panel__quality-checks">
                    {formatPipelineChecks(pipelineQuality.checks)}
                  </small>
                )}
              </div>
            </div>
          </div>
        )}
      </div>

      {viewMode === "replay" && replayMeta && (
        <div className="status-panel__meta">
          Load: {isReplayLoading ? "fetching" : "ready"} | progress:{" "}
          {replayMeta.total_count ? replayProgressPct.toFixed(1) : "--"}% |
          window: {activeReplayWindowMinutes}m | Replay: {replayMeta.loaded_count}/
          {replayMeta.total_count ?? "?"} | has_more: {String(replayMeta.has_more)} |
          db: {replayMeta.query_time_ms}ms | t: {(playbackTime / 1000).toFixed(1)}
          s / {(replayDurationMs / 1000).toFixed(1)}s | speed: {replaySpeed}x |
          shown: {replayStats.shownVehicles} | obs: {replayStats.observedPoints} int:{" "}
          {replayStats.interpolatedPoints} | invalid: {replayStats.invalidGapPoints}
        </div>
      )}

      {viewMode === "replay" && replayMeta && replayStats.shownVehicles === 0 && (
        <div className="status-panel__warning">
          Replay loaded rows, but none are visible with the current filters and map
          bounds.
        </div>
      )}

      {viewMode === "replay" && replayError && (
        <div className="status-panel__error">Replay error: {replayError}</div>
      )}

      {viewMode === "replay" && replayMeta && (
        <div className="status-panel__progress">
          <div
            className="status-panel__progress-fill"
            style={{ width: `${replayProgressPct}%` }}
          />
        </div>
      )}
    </div>
  );
}
