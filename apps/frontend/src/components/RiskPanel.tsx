import type {
  DelayAlertMeta,
  DelayAlertRecord,
  RiskListFilter,
} from "../types/alerts";
import { RiskDetailPanel } from "./RiskDetailPanel";

type RiskPanelProps = {
  isOpen: boolean;
  alerts: DelayAlertRecord[];
  visibleAlerts: DelayAlertRecord[];
  meta: DelayAlertMeta | null;
  topAlert: DelayAlertRecord | null;
  onMapCount: number;
  activeVehicleIds: Set<string>;
  selectedVehicleId: string | null;
  selectedAlert: DelayAlertRecord | null;
  filter: RiskListFilter;
  isLoading: boolean;
  error: string | null;
  formatPercent: (value: number) => string;
  formatSeconds: (value: number | null | undefined) => string;
  formatAlertTime: (value: string) => string;
  onRefresh: () => void;
  onToggleOpen: () => void;
  onFilterChange: (filter: RiskListFilter) => void;
  onSelectAlert: (alert: DelayAlertRecord) => void;
  onClearSelection: () => void;
};

function formatMaybeNumber(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? String(value) : "--";
}

function statusDotClass(
  status: DelayAlertMeta["status"] | undefined,
  error: string | null,
): string {
  if (error) return "risk-panel__dot--error";
  if (status === "fresh") return "risk-panel__dot--fresh";
  if (status === "stale" || status === "missing") return "risk-panel__dot--warning";
  if (status === "error") return "risk-panel__dot--error";
  return "";
}

export function RiskPanel({
  isOpen,
  alerts,
  visibleAlerts,
  meta,
  topAlert,
  onMapCount,
  activeVehicleIds,
  selectedVehicleId,
  selectedAlert,
  filter,
  isLoading,
  error,
  formatPercent,
  formatSeconds,
  formatAlertTime,
  onRefresh,
  onToggleOpen,
  onFilterChange,
  onSelectAlert,
  onClearSelection,
}: RiskPanelProps) {
  const scoredCount = meta?.scored_count ?? meta?.total_count ?? alerts.length;
  const alertCount =
    meta?.alert_count ?? alerts.filter((alert) => alert.delay_increase_alert).length;
  const rawAlertCount = meta?.raw_alert_count ?? alertCount;
  const suppressedAlertCount = meta?.suppressed_alert_count ?? 0;
  const sourceLabel = meta?.source ?? "artifact";
  const thresholdLabel =
    typeof meta?.threshold === "number" && Number.isFinite(meta.threshold)
      ? formatPercent(meta.threshold)
      : "--";
  const minRiskLabel =
    typeof meta?.alert_min_risk === "number" && Number.isFinite(meta.alert_min_risk)
      ? formatPercent(meta.alert_min_risk)
      : "--";
  const contextLabel =
    typeof meta?.context_minutes === "number" && Number.isFinite(meta.context_minutes)
      ? `${meta.context_minutes}m`
      : "--";
  const freshnessLabel =
    typeof meta?.output_freshness_seconds === "number" &&
    Number.isFinite(meta.output_freshness_seconds)
      ? `${meta.output_freshness_seconds}s`
      : "--";
  const emptyMessage =
    filter === "on_map"
      ? "No map-visible delay alerts right now."
      : meta?.status === "missing"
        ? "No model scores are available yet."
        : "No scored delay-risk rows in this view.";

  return (
    <div className={`risk-panel ${isOpen ? "" : "risk-panel--collapsed"}`}>
      <div className="risk-panel__header">
        <div className="risk-panel__title">
          <div className={`risk-panel__dot ${statusDotClass(meta?.status, error)}`} />
          <div>
            <div className="risk-panel__eyebrow">MODEL SIGNAL</div>
            <div className="risk-panel__heading">Delay Increase Risk</div>
          </div>
        </div>
        <div className="risk-panel__actions">
          <button
            className="risk-panel__button"
            onClick={onRefresh}
            disabled={isLoading}
            title="Refresh alerts"
          >
            {isLoading ? "..." : "Refresh"}
          </button>
          <button
            className="risk-panel__button"
            onClick={onToggleOpen}
            title={isOpen ? "Collapse alerts" : "Expand alerts"}
          >
            {isOpen ? "-" : "+"}
          </button>
        </div>
      </div>

      {isOpen && (
        <div className="risk-panel__body">
          <div className="risk-panel__summary">
            <div className="risk-panel__metric">
              <div className="risk-panel__metric-label">Scored</div>
              <div className="risk-panel__metric-value">{scoredCount}</div>
            </div>
            <div className="risk-panel__metric">
              <div className="risk-panel__metric-label">Map Alerts</div>
              <div className="risk-panel__metric-value">
                {suppressedAlertCount > 0 ? `${alertCount}/${rawAlertCount}` : alertCount}
              </div>
            </div>
            <div className="risk-panel__metric">
              <div className="risk-panel__metric-label">Visible</div>
              <div className="risk-panel__metric-value">{onMapCount}</div>
            </div>
            <div className="risk-panel__metric">
              <div className="risk-panel__metric-label">Top Risk</div>
              <div className="risk-panel__metric-value">
                {topAlert ? formatPercent(topAlert.delay_increase_risk) : "--"}
              </div>
            </div>
          </div>

          <div className="risk-panel__filters">
            <button
              className={`risk-panel__filter ${
                filter === "all" ? "risk-panel__filter--active" : ""
              }`}
              onClick={() => onFilterChange("all")}
            >
              All Scores
            </button>
            <button
              className={`risk-panel__filter ${
                filter === "on_map" ? "risk-panel__filter--active" : ""
              }`}
              onClick={() => onFilterChange("on_map")}
            >
              Visible Alerts ({onMapCount})
            </button>
          </div>

          {meta && (
            <div className="risk-panel__signal">
              <span>source {sourceLabel}</span>
              <span>threshold {thresholdLabel}</span>
              <span>min {minRiskLabel}</span>
              <span>context {contextLabel}</span>
              <span>fresh {freshnessLabel}</span>
              <span>cap {formatMaybeNumber(meta.alert_max_per_run)}</span>
              <span>raw {formatMaybeNumber(meta.raw_alert_count)}</span>
              <span>suppressed {formatMaybeNumber(meta.suppressed_alert_count)}</span>
              <span>saved {formatMaybeNumber(meta.persisted_prediction_count)}</span>
            </div>
          )}

          {meta?.status && meta.status !== "fresh" && meta.reason && (
            <div className="risk-panel__status risk-panel__status--warning">
              {meta.reason}
            </div>
          )}

          {error && <div className="risk-panel__status risk-panel__status--error">{error}</div>}

          {selectedAlert && (
            <RiskDetailPanel
              alert={selectedAlert}
              routeAlerts={alerts}
              activeVehicleIds={activeVehicleIds}
              isOnMap={activeVehicleIds.has(selectedAlert.vehicle_id)}
              formatPercent={formatPercent}
              formatSeconds={formatSeconds}
              formatAlertTime={formatAlertTime}
              onSelectAlert={onSelectAlert}
              onClose={onClearSelection}
            />
          )}

          {!error && visibleAlerts.length === 0 && (
            <div className="risk-panel__status">{emptyMessage}</div>
          )}

          {!error && visibleAlerts.length > 0 && (
            <div className="risk-panel__list">
              {visibleAlerts.map((alert) => {
                const isOnMap = activeVehicleIds.has(alert.vehicle_id);
                const isSelected = selectedVehicleId === alert.vehicle_id;
                const isMapAlert = alert.delay_increase_alert;
                const isSuppressed = Boolean(
                  alert.raw_delay_increase_alert && !alert.delay_increase_alert,
                );

                return (
                  <button
                    className={`risk-alert ${
                      isMapAlert ? "risk-alert--map-alert" : "risk-alert--score-only"
                    } ${isOnMap ? "risk-alert--clickable" : ""} ${
                      isSelected ? "risk-alert--selected" : ""
                    }`}
                    key={`${alert.vehicle_id}-${alert.time}`}
                    onClick={() => {
                      if (isOnMap) onSelectAlert(alert);
                    }}
                    disabled={!isOnMap}
                    title={
                      isOnMap
                        ? "Show this vehicle on the map"
                      : "This alert is not in the current map view"
                    }
                  >
                    <div
                      className={`risk-alert__score ${
                        isMapAlert ? "risk-alert__score--hot" : "risk-alert__score--watch"
                      }`}
                    >
                      {formatPercent(alert.delay_increase_risk)}
                    </div>
                    <div className="risk-alert__main">
                      <div className="risk-alert__topline">
                        <div className="risk-alert__name">
                          {alert.line ?? alert.route_id ?? "unknown"} / {alert.vehicle_id}
                        </div>
                        <div className="risk-alert__time">
                          {formatAlertTime(alert.time)}
                        </div>
                      </div>
                      <div className="risk-alert__meta">
                        {alert.state_position ?? "unknown"} | route{" "}
                        {alert.route_id ?? "--"} | {isOnMap ? "on map" : "not on map"}
                      </div>
                      <div className="risk-alert__chips">
                        <span
                          className={`risk-alert__chip ${
                            isMapAlert
                              ? "risk-alert__chip--hot"
                              : isSuppressed
                                ? "risk-alert__chip--warning"
                                : ""
                          }`}
                        >
                          {isMapAlert
                            ? "map alert"
                            : isSuppressed
                              ? "suppressed"
                              : "score only"}
                        </span>
                        <span className="risk-alert__chip">
                          delay vs schedule {formatSeconds(alert.delay)}
                        </span>
                        <span className="risk-alert__chip">
                          schedule trend {formatSeconds(alert.delay_delta_1)}
                        </span>
                        <span className="risk-alert__chip">
                          speed{" "}
                          {typeof alert.speed === "number" &&
                          Number.isFinite(alert.speed)
                            ? Math.round(alert.speed)
                            : "--"}
                        </span>
                      </div>
                    </div>
                  </button>
                );
              })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
