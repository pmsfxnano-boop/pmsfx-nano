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

/**
 * Project an authoritative server-side Opportunity Clock snapshot onto the
 * current client time. The client interpolates only; it never creates or
 * promotes a new opportunity.
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

  const serverRemaining = finiteNumber(clock.remaining_seconds);
  const snapshotMs = epochMs(generatedAt);
  const ageSeconds = snapshotMs == null ? 0 : Math.max(0, (nowMs - snapshotMs) / 1000);

  let remainingSeconds = serverRemaining == null
    ? null
    : Math.max(0, serverRemaining - ageSeconds);

  const explicitEndMs = epochMs(clock.window_ends_at);
  if (explicitEndMs != null) {
    remainingSeconds = Math.max(0, (explicitEndMs - nowMs) / 1000);
  }

  let phase: OpportunityClockPhase;
  if (!validated) {
    phase = remainingSeconds != null && remainingSeconds <= 0 ? "CLOSED" : "LOCKED";
  } else if (rawPhase === "ENTRY_WINDOW" || rawPhase === "EXIT_WINDOW" || rawPhase === "DECAYING") {
    phase = rawPhase;
  } else if (rawPhase === "CLOSED" || (remainingSeconds != null && remainingSeconds <= 0)) {
    phase = "CLOSED";
  } else {
    phase = "ENTRY_WINDOW";
  }

  if (phase !== "LOCKED" && phase !== "CLOSED" && remainingSeconds != null && remainingSeconds <= 0) {
    phase = "CLOSED";
  }

  const horizonValue = finiteNumber(clock.horizon_ms);
  const horizonSeconds = horizonValue == null ? null : Math.max(0, horizonValue / 1000);
  const progress = horizonSeconds && remainingSeconds != null
    ? clamp(1 - remainingSeconds / horizonSeconds, 0, 1)
    : 0;

  const label =
    phase === "ENTRY_WINDOW" ? "ENTRY WINDOW" :
    phase === "DECAYING" ? "DECAYING" :
    phase === "EXIT_WINDOW" ? "EXIT WINDOW" :
    phase === "CLOSED" ? "CLOSED" :
    "LOCKED";

  return {
    phase,
    label,
    remainingSeconds,
    progress,
    windowEndsAt: clock.window_ends_at ?? null,
    entryEndsAt: clock.entry_window_end_at ?? null,
    exitStartsAt: clock.exit_window_start_at ?? null,
    exitEndsAt: clock.exit_window_end_at ?? null,
    canDisplayCountdown: validated && remainingSeconds != null && remainingSeconds > 0,
  };
}
