import { useEffect, useMemo, useState } from "react";
import type { EvidenceResponse } from "./api";

export type OpportunityClockPhase =
  | "LOCKED"
  | "ENTRY_WINDOW"
  | "DECAYING"
  | "EXIT_WINDOW"
  | "CLOSED";

export interface OpportunityClockProjection {
  phase: OpportunityClockPhase;
  label: string;
  validated: boolean;
  remainingSeconds: number | null;
  progress: number;
  windowEndsAt: string | null;
  entryEndsAt: string | null;
  exitStartsAt: string | null;
  exitEndsAt: string | null;
  canDisplayCountdown: boolean;
}

function finiteNumber(value: unknown): number | null {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function epochMs(value: string | null | undefined): number | null {
  if (!value) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function clamp(value: number, minimum: number, maximum: number): number {
  return Math.min(maximum, Math.max(minimum, value));
}

export function formatOpportunityDuration(seconds: number | null): string {
  if (seconds == null || !Number.isFinite(seconds)) return "—";
  const rounded = Math.max(0, Math.ceil(seconds));
  const hours = Math.floor(rounded / 3600);
  const minutes = Math.floor((rounded % 3600) / 60);
  const secs = rounded % 60;
  if (hours > 0) return `${hours}:${String(minutes).padStart(2, "0")}:${String(secs).padStart(2, "0")}`;
  return `${String(minutes).padStart(2, "0")}:${String(secs).padStart(2, "0")}`;
}

/**
 * Projects only an authoritative server-side Opportunity Clock snapshot.
 * It never creates or promotes an opportunity on the client.
 */
export function projectOpportunityClock(
  clock: EvidenceResponse["opportunity_clock"] | null | undefined,
  generatedAt: string | null | undefined,
  nowMs = Date.now(),
): OpportunityClockProjection {
  if (!clock) {
    return {
      phase: "LOCKED",
      label: "LOCKED",
      validated: false,
      remainingSeconds: null,
      progress: 0,
      windowEndsAt: null,
      entryEndsAt: null,
      exitStartsAt: null,
      exitEndsAt: null,
      canDisplayCountdown: false,
    };
  }

  const rawState = String(clock.state || "").toUpperCase();
  const rawPhase = String(clock.phase || "").toUpperCase();
  const validated = Boolean(clock.validated && rawState === "ACTIVE");
  const snapshotMs = epochMs(generatedAt);
  const ageSeconds = snapshotMs == null ? 0 : Math.max(0, (nowMs - snapshotMs) / 1000);
  const serverRemaining = finiteNumber(clock.remaining_seconds);

  let remainingSeconds = serverRemaining == null
    ? null
    : Math.max(0, serverRemaining - ageSeconds);

  const explicitEndMs = epochMs(clock.window_ends_at);
  if (explicitEndMs != null) {
    remainingSeconds = Math.max(0, (explicitEndMs - nowMs) / 1000);
  }

  const horizonMs = finiteNumber(clock.horizon_ms);
  const horizonSeconds = horizonMs == null ? null : Math.max(0, horizonMs / 1000);
  const elapsedSeconds = horizonSeconds != null && remainingSeconds != null
    ? clamp(horizonSeconds - remainingSeconds, 0, horizonSeconds)
    : 0;
  const progress = horizonSeconds && horizonSeconds > 0
    ? clamp(elapsedSeconds / horizonSeconds, 0, 1)
    : 0;

  let phase: OpportunityClockPhase;
  if (!validated) {
    phase = rawPhase === "CLOSED" || (remainingSeconds != null && remainingSeconds <= 0)
      ? "CLOSED"
      : "LOCKED";
  } else if (
    rawPhase === "ENTRY_WINDOW" ||
    rawPhase === "DECAYING" ||
    rawPhase === "EXIT_WINDOW" ||
    rawPhase === "CLOSED"
  ) {
    phase = rawPhase;
  } else if (remainingSeconds != null && remainingSeconds <= 0) {
    phase = "CLOSED";
  } else if (progress < 0.5) {
    phase = "ENTRY_WINDOW";
  } else if (progress < 0.75) {
    phase = "DECAYING";
  } else {
    phase = "EXIT_WINDOW";
  }

  if (validated && remainingSeconds != null && remainingSeconds <= 0) phase = "CLOSED";

  const label =
    phase === "ENTRY_WINDOW" ? "ENTRY WINDOW" :
    phase === "DECAYING" ? "DECAYING" :
    phase === "EXIT_WINDOW" ? "EXIT WINDOW" :
    phase === "CLOSED" ? "CLOSED" :
    "LOCKED";

  return {
    phase,
    label,
    validated,
    remainingSeconds,
    progress,
    windowEndsAt: clock.window_ends_at ?? null,
    entryEndsAt: clock.entry_window_end_at ?? null,
    exitStartsAt: clock.exit_window_start_at ?? null,
    exitEndsAt: clock.exit_window_end_at ?? null,
    canDisplayCountdown: validated && remainingSeconds != null && remainingSeconds > 0,
  };
}

export function useOpportunityClock(
  clock: EvidenceResponse["opportunity_clock"] | null | undefined,
  generatedAt: string | null | undefined,
): OpportunityClockProjection {
  const [nowMs, setNowMs] = useState(() => Date.now());

  useEffect(() => {
    if (!clock?.validated) return;
    const id = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [clock?.validated, clock?.state, clock?.phase, clock?.window_ends_at]);

  return useMemo(
    () => projectOpportunityClock(clock, generatedAt, nowMs),
    [clock, generatedAt, nowMs],
  );
}
