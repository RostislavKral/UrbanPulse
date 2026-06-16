import type { DelayAlertRecord } from "../types/alerts";

type RiskDetailPanelProps = {
  alert: DelayAlertRecord;
  routeAlerts: DelayAlertRecord[];
  activeVehicleIds: Set<string>;
  isOnMap: boolean;
  formatPercent: (value: number) => string;
  formatSeconds: (value: number | null | undefined) => string;
  formatAlertTime: (value: string) => string;
  onSelectAlert: (alert: DelayAlertRecord) => void;
  onClose: () => void;
};

function formatNumber(value: number | null | undefined, suffix = ""): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `${Math.round(value)}${suffix}`;
}

function formatScheduleUntil(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const absolute = Math.abs(value);
  const formatted =
    absolute < 60 ? `${Math.round(absolute)}s` : `${(absolute / 60).toFixed(1)}m`;
  if (value < 0) return `${formatted} ago`;
  return `in ${formatted}`;
}

function formatScheduleSince(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const absolute = Math.abs(value);
  const formatted =
    absolute < 60 ? `${Math.round(absolute)}s` : `${(absolute / 60).toFixed(1)}m`;
  if (value < 0) return `${formatted} before`;
  return `${formatted} ago`;
}

function stopSequence(alert: DelayAlertRecord): string {
  if (
    typeof alert.last_stop_sequence === "number" &&
    typeof alert.next_stop_sequence === "number"
  ) {
    return `${alert.last_stop_sequence} -> ${alert.next_stop_sequence}`;
  }
  return "--";
}

function stopIds(alert: DelayAlertRecord): string {
  if (alert.last_stop_id && alert.next_stop_id) {
    return `${alert.last_stop_id} -> ${alert.next_stop_id}`;
  }
  return alert.next_stop_id ?? alert.last_stop_id ?? "--";
}

function routeKey(alert: DelayAlertRecord): string {
  return alert.route_id?.trim() || alert.line?.trim() || "";
}

function sequenceCenter(alert: DelayAlertRecord): number | null {
  if (
    typeof alert.last_stop_sequence === "number" &&
    typeof alert.next_stop_sequence === "number"
  ) {
    return (alert.last_stop_sequence + alert.next_stop_sequence) / 2;
  }
  if (typeof alert.next_stop_sequence === "number") return alert.next_stop_sequence;
  if (typeof alert.last_stop_sequence === "number") return alert.last_stop_sequence;
  return null;
}

function averageDelay(alerts: DelayAlertRecord[]): number | null {
  const values = alerts
    .map((item) => item.delay)
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  if (!values.length) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function risingCount(alerts: DelayAlertRecord[]): number {
  return alerts.filter(
    (item) => typeof item.delay_delta_1 === "number" && item.delay_delta_1 > 0,
  ).length;
}

function sameRouteAlerts(
  selected: DelayAlertRecord,
  alerts: DelayAlertRecord[],
): DelayAlertRecord[] {
  const selectedRoute = routeKey(selected);
  if (!selectedRoute) return [selected];

  const byVehicle = new Map<string, DelayAlertRecord>();
  for (const item of [selected, ...alerts]) {
    if (routeKey(item) !== selectedRoute) continue;
    if (sequenceCenter(item) === null) continue;
    byVehicle.set(item.vehicle_id, item);
  }
  return [...byVehicle.values()];
}

function sequenceLabel(alert: DelayAlertRecord): string {
  const center = sequenceCenter(alert);
  if (center === null) return "--";
  return Number.isInteger(center) ? String(center) : center.toFixed(1);
}

type DetailMetricProps = {
  label: string;
  value: string;
  tone?: "hot" | "muted" | "warning";
};

function DetailMetric({ label, value, tone }: DetailMetricProps) {
  return (
    <div className={`risk-detail__metric ${tone ? `risk-detail__metric--${tone}` : ""}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

type PropagationBandProps = {
  label: string;
  alerts: DelayAlertRecord[];
  formatSeconds: (value: number | null | undefined) => string;
};

function PropagationBand({ label, alerts, formatSeconds }: PropagationBandProps) {
  return (
    <div className="risk-detail__band">
      <span>{label}</span>
      <strong>{formatSeconds(averageDelay(alerts))}</strong>
      <small>
        {alerts.length} veh | rising {risingCount(alerts)}
      </small>
    </div>
  );
}

function servingState(alert: DelayAlertRecord): string {
  if (alert.delay_increase_alert) return "map alert";
  if (alert.raw_delay_increase_alert) return "suppressed";
  return "score only";
}

function servingTone(alert: DelayAlertRecord): DetailMetricProps["tone"] {
  if (alert.delay_increase_alert) return "hot";
  if (alert.raw_delay_increase_alert) return "warning";
  return "muted";
}

export function RiskDetailPanel({
  alert,
  routeAlerts,
  activeVehicleIds,
  isOnMap,
  formatPercent,
  formatSeconds,
  formatAlertTime,
  onSelectAlert,
  onClose,
}: RiskDetailPanelProps) {
  const futureLabel =
    typeof alert.target_delay_delta === "number"
      ? formatSeconds(alert.target_delay_delta)
      : "not observed";
  const targetDelay =
    typeof alert.target_delay === "number"
      ? formatSeconds(alert.target_delay)
      : "not observed";
  const alertRank =
    typeof alert.alert_rank === "number" && Number.isFinite(alert.alert_rank)
      ? `#${alert.alert_rank}`
      : "--";
  const selectedSequence = sequenceCenter(alert);
  const routeItems = sameRouteAlerts(alert, routeAlerts)
    .map((item) => ({ item, sequence: sequenceCenter(item) }))
    .filter(
      (entry): entry is { item: DelayAlertRecord; sequence: number } =>
        entry.sequence !== null,
    );
  const upstream =
    selectedSequence === null
      ? []
      : routeItems
          .filter((entry) => entry.sequence < selectedSequence)
          .map((entry) => entry.item);
  const sameSegment =
    selectedSequence === null
      ? [alert]
      : routeItems
          .filter((entry) => entry.sequence === selectedSequence)
          .map((entry) => entry.item);
  const downstream =
    selectedSequence === null
      ? []
      : routeItems
          .filter((entry) => entry.sequence > selectedSequence)
          .map((entry) => entry.item);
  const routeWindow =
    selectedSequence === null
      ? [alert]
      : routeItems
          .sort((left, right) => {
            const distance =
              Math.abs(left.sequence - selectedSequence) -
              Math.abs(right.sequence - selectedSequence);
            if (distance !== 0) return distance;
            return right.item.delay_increase_risk - left.item.delay_increase_risk;
          })
          .slice(0, 7)
          .sort((left, right) => left.sequence - right.sequence)
          .map((entry) => entry.item);

  return (
    <aside className="risk-detail">
      <div className="risk-detail__header">
        <div className="risk-detail__title">
          <span>{alert.line ?? alert.route_id ?? "unknown"}</span>
          <strong>{alert.vehicle_id}</strong>
        </div>
        <button className="risk-detail__close" onClick={onClose} title="Close detail">
          x
        </button>
      </div>

      <div className="risk-detail__hero">
        <DetailMetric
          label="Risk"
          value={formatPercent(alert.delay_increase_risk)}
          tone={alert.delay_increase_alert ? "hot" : undefined}
        />
        <DetailMetric
          label="Current delay vs schedule"
          value={formatSeconds(alert.delay)}
        />
        <DetailMetric
          label="Prediction time"
          value={formatAlertTime(alert.time)}
        />
        <DetailMetric
          label="Map state"
          value={isOnMap ? "on map" : "not visible"}
          tone={isOnMap ? undefined : "muted"}
        />
      </div>

      <div className="risk-detail__section">
        <div className="risk-detail__section-title">Serving Policy</div>
        <div className="risk-detail__grid">
          <DetailMetric
            label="State"
            value={servingState(alert)}
            tone={servingTone(alert)}
          />
          <DetailMetric
            label="Rank"
            value={alertRank}
            tone={alert.delay_increase_alert ? "hot" : "muted"}
          />
          <DetailMetric
            label="Raw threshold"
            value={alert.raw_delay_increase_alert ? "crossed" : "below"}
          />
          <DetailMetric
            label="Policy"
            value={alert.alert_policy_reason ?? "--"}
          />
        </div>
      </div>

      <div className="risk-detail__section">
        <div className="risk-detail__section-title">Route Propagation</div>
        <div className="risk-detail__bands">
          <PropagationBand
            label="Upstream"
            alerts={upstream}
            formatSeconds={formatSeconds}
          />
          <PropagationBand
            label="This segment"
            alerts={sameSegment}
            formatSeconds={formatSeconds}
          />
          <PropagationBand
            label="Downstream"
            alerts={downstream}
            formatSeconds={formatSeconds}
          />
        </div>
        <div className="risk-detail__route-list">
          {routeWindow.map((item) => {
            const isSelected = item.vehicle_id === alert.vehicle_id;
            const itemIsOnMap = activeVehicleIds.has(item.vehicle_id);
            return (
              <button
                key={`${item.vehicle_id}-${item.time}`}
                className={`risk-detail__route-row ${
                  isSelected ? "risk-detail__route-row--selected" : ""
                }`}
                disabled={!itemIsOnMap}
                onClick={() => onSelectAlert(item)}
                title={itemIsOnMap ? "Show this vehicle on the map" : "Not visible on map"}
              >
                <span>#{sequenceLabel(item)}</span>
                <strong>{item.vehicle_id}</strong>
                <em>{formatSeconds(item.delay)}</em>
                <small>
                  {item.delay_increase_alert ? "A " : ""}
                  {formatPercent(item.delay_increase_risk)}
                </small>
              </button>
            );
          })}
        </div>
        <div className="risk-detail__muted">
          Same route rows are compared by stop sequence, using schedule-relative delay.
        </div>
      </div>

      <div className="risk-detail__section">
        <div className="risk-detail__section-title">Delay Trend</div>
        <div className="risk-detail__grid">
          <DetailMetric
            label="Previous sample"
            value={formatSeconds(alert.delay_lag_1)}
          />
          <DetailMetric
            label="Change vs previous"
            value={formatSeconds(alert.delay_delta_1)}
          />
          <DetailMetric
            label="3-sample average"
            value={formatSeconds(alert.delay_mean_3)}
          />
          <DetailMetric
            label="Speed change"
            value={formatNumber(alert.speed_delta_1)}
          />
        </div>
      </div>

      <div className="risk-detail__section">
        <div className="risk-detail__section-title">Schedule Context</div>
        <div className="risk-detail__grid">
          <DetailMetric
            label="Next scheduled arrival"
            value={formatScheduleUntil(alert.seconds_until_next_stop_arrival)}
          />
          <DetailMetric
            label="Next scheduled departure"
            value={formatScheduleUntil(alert.seconds_until_next_stop_departure)}
          />
          <DetailMetric
            label="Last scheduled arrival"
            value={formatScheduleSince(alert.seconds_since_last_stop_arrival)}
          />
          <DetailMetric
            label="Stop sequence"
            value={stopSequence(alert)}
          />
        </div>
        <div className="risk-detail__muted">{stopIds(alert)}</div>
      </div>

      <div className="risk-detail__section">
        <div className="risk-detail__section-title">Model Evidence</div>
        <div className="risk-detail__grid">
          <DetailMetric
            label="Observed next-5m change"
            value={futureLabel}
            tone={futureLabel === "not observed" ? "muted" : undefined}
          />
          <DetailMetric
            label="Observed future delay"
            value={targetDelay}
            tone={targetDelay === "not observed" ? "muted" : undefined}
          />
          <DetailMetric
            label="Current speed"
            value={formatNumber(alert.speed)}
          />
          <DetailMetric
            label="3-sample speed"
            value={formatNumber(alert.speed_mean_3)}
          />
        </div>
      </div>
    </aside>
  );
}
