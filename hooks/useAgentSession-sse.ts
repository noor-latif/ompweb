"use client";

// SSE-streaming core extracted from useAgentSession (extraction only — no
// logic changes). Owns the EventSource connection lifecycle, the send-blocking
// connect gate, the no-stream terminal fallbacks (prompt/bash settlement),
// and the server-reconcile recovery net for missed SSE events.

import { useCallback, useEffect, useRef } from "react";
import type { Dispatch, SetStateAction } from "react";
import type { ExtensionStatusItem, ExtensionWidgetItem } from "@/lib/types";
import type { TodoPhase } from "@/lib/pi-types";
import type { SubagentInfo } from "@/lib/subagent-types";
import type { MessageUpdateCoalescer } from "@/lib/message-update-coalescer";
import { createReconcileGuard, type ReconcileGuard } from "@/lib/reconcile-guard";
import { translate } from "@/lib/i18n";
import { toast } from "@/components/ui/toast";
import { subscribeSessionsChanged } from "@/lib/session-change-bus";
import { EMPTY_QUEUE, type QueuedMessages } from "./useAgentSession-queue";
import type { NoticeType } from "./useAgentSession-notices";
import {
  AGENT_STATE_RECONCILE_MS,
  BASH_STATE_RECONCILE_MS,
  EVENT_STREAM_CONNECT_TIMEOUT_MS,
  EVENT_STREAM_SLOW_CONNECT_MS,
  PROMPT_SETTLE_INITIAL_DELAY_MS,
  PROMPT_SETTLE_MAX_MS,
  PROMPT_SETTLE_POLL_MS,
  EventStreamConnectionError,
  delay,
  isQuotaLikeError,
} from "./useAgentSession-stream";
import type {
  AgentEvent,
  AgentPhase,
  AgentStateResponse,
  EventStreamConnectionResult,
  EventStreamConnectionStatus,
  StreamAction,
} from "./useAgentSession-stream";

export interface UseAgentSessionSseParams {
  sessionIdRef: { current: string | null };
  agentRunningRef: { current: boolean };
  bashRunningRef: { current: boolean };
  promptRunIdRef: { current: number };
  hookAliveRef: { current: boolean };
  queueMutatedAtRef: { current: number };
  bashRecoveryIdRef: { current: number };
  isCompactingRef: { current: boolean };
  subagentRosterGenerationRef: { current: number };
  runHadContentRef: { current: boolean };
  lastQuotaErrorRef: { current: string | null };
  lastRunErrorRef: { current: string | null };
  slashCommandRunRef: { current: boolean };
  optimisticUserMessageKeyRef: { current: string | null };
  eventCoalescer: MessageUpdateCoalescer;
  agentRunning: boolean;
  onAgentEnd: (() => void) | undefined;
  loadSession: (sid: string, showLoading?: boolean, includeState?: boolean, fenceRunId?: number) => Promise<{ running: boolean; state?: AgentStateResponse } | null>;
  refreshSubagentRoster: (sid: string) => Promise<void>;
  refreshSubagentHistory: (sid: string) => Promise<void>;
  addNotice: (notice: { id?: string; message: string; type?: NoticeType }) => void;
  clearTerminalReconcileTimer: () => void;
  resetSubagentActivityState: () => void;
  surfaceQuotaOnStream: (errorMessage: string) => void;
  dispatch: Dispatch<StreamAction>;
  setAgentRunning: Dispatch<SetStateAction<boolean>>;
  setAgentPhase: Dispatch<SetStateAction<AgentPhase>>;
  setRetryInfo: Dispatch<SetStateAction<{ attempt: number; maxAttempts: number; errorMessage?: string } | null>>;
  setSubagents: Dispatch<SetStateAction<SubagentInfo[]>>;
  setAdvisorActiveAt: Dispatch<SetStateAction<number>>;
  setIsCompacting: Dispatch<SetStateAction<boolean>>;
  setTodoPhases: Dispatch<SetStateAction<TodoPhase[]>>;
  setQueuedMessages: Dispatch<SetStateAction<QueuedMessages>>;
  setContextUsage: Dispatch<SetStateAction<{ percent: number | null; contextWindow: number; tokens: number | null } | null>>;
  setSystemPrompt: Dispatch<SetStateAction<string | null>>;
  setExtensionStatuses: Dispatch<SetStateAction<ExtensionStatusItem[]>>;
  setExtensionWidgets: Dispatch<SetStateAction<ExtensionWidgetItem[]>>;
  setBashRunning: Dispatch<SetStateAction<boolean>>;
  setPendingBash: Dispatch<SetStateAction<{ command: string; excludeFromContext: boolean } | null>>;
}

export interface UseAgentSessionSseResult {
  eventSourceRef: { current: EventSource | null };
  reconnectTimerRef: { current: ReturnType<typeof setTimeout> | null };
  reconnectActionsRef: { current: ((sid: string) => void) | null };
  reconcileGuardRef: { current: ReconcileGuard | null };
  connectEvents: (sid: string) => Promise<EventStreamConnectionResult>;
  ensureEventsConnected: (sid: string) => Promise<void>;
  finishPromptWithoutStream: (sid?: string | null, runId?: number) => Promise<void>;
  waitForPromptSettlement: (sid: string, runId?: number) => Promise<void>;
  waitForBashSettlement: (sid: string) => Promise<void>;
  reconcileAgentState: (sid: string) => Promise<void>;
}

export function useAgentSessionSse({
  sessionIdRef,
  agentRunningRef,
  bashRunningRef,
  promptRunIdRef,
  hookAliveRef,
  queueMutatedAtRef,
  bashRecoveryIdRef,
  isCompactingRef,
  subagentRosterGenerationRef,
  runHadContentRef,
  lastQuotaErrorRef,
  lastRunErrorRef,
  slashCommandRunRef,
  optimisticUserMessageKeyRef,
  eventCoalescer,
  agentRunning,
  onAgentEnd,
  loadSession,
  refreshSubagentRoster,
  refreshSubagentHistory,
  addNotice,
  clearTerminalReconcileTimer,
  resetSubagentActivityState,
  surfaceQuotaOnStream,
  dispatch,
  setAgentRunning,
  setAgentPhase,
  setRetryInfo,
  setSubagents,
  setAdvisorActiveAt,
  setIsCompacting,
  setTodoPhases,
  setQueuedMessages,
  setContextUsage,
  setSystemPrompt,
  setExtensionStatuses,
  setExtensionWidgets,
  setBashRunning,
  setPendingBash,
}: UseAgentSessionSseParams): UseAgentSessionSseResult {
  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Coalesces concurrent reconcileAgentState triggers (15s interval,
  // visibilitychange, online, todo events) into one in-flight request per
  // run — a slow /api/agent/[id] response must not stack stale polls that
  // then overwrite newer state out of order.
  const reconcileGuardRef = useRef<ReconcileGuard | null>(null);
  // timeoutMs: a reconcile GET that never settles (hung connection, lost
  // response) must not block future calibration forever — the guard
  // auto-releases the lock and the next trigger re-issues.
  if (reconcileGuardRef.current === null) reconcileGuardRef.current = createReconcileGuard({ timeoutMs: 30_000 });

  // A session omp is writing outside the web UI has no RPC stream to deliver
  // its turns, so reload the transcript when the watcher reports that this
  // session's file grew. Skipped while an event stream is attached: that
  // stream is already the authority and a reload would fight it.
  useEffect(() => {
    return subscribeSessionsChanged((sessionIds) => {
      const sid = sessionIdRef.current;
      if (!sid || eventSourceRef.current || !sessionIds.includes(sid)) return;
      void loadSession(sid);
    });
  }, [loadSession, sessionIdRef]);

  // Reconnect actions captured after their definitions (host-tool and URI
  // registrations are per-wrapper and are not persisted by omp, and the
  // roster needs a fresh get_subagents snapshot) so the fatal-error reconnect
  // below can restore everything the mount flow sets up — not just the stream.
  const reconnectActionsRef = useRef<((sid: string) => void) | null>(null);

  const connectEvents = useCallback((sid: string): Promise<EventStreamConnectionResult> => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    // A pending coalesced update belongs to the stream being replaced.
    eventCoalescer.reset();
    const es = new EventSource(`/api/agent/${encodeURIComponent(sid)}/events`);
    eventSourceRef.current = es;

    return new Promise((resolve) => {
      let settled = false;
      const settle = (status: EventStreamConnectionStatus) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        resolve({ status, source: es });
      };
      const timeout = setTimeout(() => settle("timeout"), EVENT_STREAM_CONNECT_TIMEOUT_MS);

      // The stream is live as soon as the response headers land, whether or not
      // the server also sends an explicit `connected` frame.
      es.onopen = () => settle("connected");

      es.onmessage = (e) => {
        try {
          const event = JSON.parse(e.data) as AgentEvent;
          if (event.type === "connected") settle("connected");
          // message_update frames arrive at network rate (often 30-100+/s);
          // the coalescer buffers the latest one and dispatches at display
          // rate, flushing synchronously before any other event type.
          eventCoalescer.push(event);
        } catch {
          // ignore
        }
      };
      es.onerror = () => {
        if (es.readyState === EventSource.CLOSED) {
          // Fatal error (404/500/content-type mismatch): browser won't
          // auto-reconnect. Settle the Promise and manually reconnect for
          // already-running sessions. Keep the timer in a ref so unmount or a
          // session switch cancels it — otherwise an orphaned stream respawns
          // (and can 404-loop) after the hook is torn down.
          settle("closed");
          if (eventSourceRef.current === es && agentRunningRef.current) {
            eventSourceRef.current = null;
            if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
            reconnectTimerRef.current = setTimeout(() => {
              reconnectTimerRef.current = null;
              if (agentRunningRef.current && sessionIdRef.current === sid) {
                void connectEvents(sid);
                // The reconnect restores the event stream, but host tools, URI
                // schemes, and the subagent roster were registered on the old
                // connection — re-register them so the agent keeps working.
                reconnectActionsRef.current?.(sid);
              }
            }, 1000);
          }
        }
        // Recoverable errors (CONNECTING): let EventSource auto-reconnect.
        // The timeout above resolves only to let callers decide whether this
        // connection must be ready before they continue.
      };
    });
  }, [eventCoalescer, agentRunningRef, sessionIdRef]);

  // Declared after addNotice: the dependency array below is evaluated during
  // render, so addNotice must already be initialized.
  const ensureEventsConnected = useCallback(async (sid: string) => {
    // Only this (send-blocking) path announces a slow connect; the mount and
    // auto-reconnect paths call connectEvents directly and stay silent.
    const slowNotice = setTimeout(() => {
      addNotice({ type: "info", message: translate("agentSession.startingAgent") });
    }, EVENT_STREAM_SLOW_CONNECT_MS);
    let result: EventStreamConnectionResult;
    try {
      result = await connectEvents(sid);
    } finally {
      clearTimeout(slowNotice);
    }
    if (result.status === "connected" || result.source.readyState === EventSource.OPEN) return;
    if (eventSourceRef.current === result.source) eventSourceRef.current = null;
    result.source.close();
    throw new EventStreamConnectionError(result.status);
  }, [addNotice, connectEvents]);

  const finishPromptWithoutStream = useCallback(async (sid: string | null = sessionIdRef.current, runId?: number) => {
    clearTerminalReconcileTimer();
    // Bail out before loadSession too: a stale finish for a previous run
    // must not overwrite the messages of the run currently streaming.
    if (runId !== undefined && promptRunIdRef.current !== runId) return;
    const hadContent = runHadContentRef.current;
    const quotaMessage = lastQuotaErrorRef.current;
    const runError = lastRunErrorRef.current;
    const allowEmptyResponse = slashCommandRunRef.current;
    try {
      // Pass the fence into loadSession: the pre-check above only guards the
      // start — a next prompt that begins while the reload is in flight must
      // not be overwritten by the finished run's snapshot.
      if (sid) await loadSession(sid, false, true, runId);
    } finally {
      if (runId !== undefined && promptRunIdRef.current !== runId) return;
      optimisticUserMessageKeyRef.current = null;
      if (!agentRunningRef.current) return;
      if (runError) {
        addNotice({ type: "error", message: runError });
        if (!isQuotaLikeError(runError)) {
          toast.error("Request failed", runError, { timeout: 12000 });
        } else {
          surfaceQuotaOnStream(runError);
        }
      } else if (quotaMessage && isQuotaLikeError(quotaMessage) && !hadContent) {
        // Silent stop: no assistant bubble to stamp — the shelf notice is the
        // in-chat message. Mid-run quota already toasted via the notice path.
        addNotice({ type: "error", message: quotaMessage });
      } else if (!hadContent && !allowEmptyResponse) {
        // Fallback for silent stops with no visible content and no explicit
        // error — never leave the user with a disappeared spinner and no
        // explanation. Builtin slash commands are allowed to complete without
        // an assistant message, hence allowEmptyResponse above.
        const message = translate("agentSession.responseFailed");
        addNotice({ type: "error", message });
        toast.error("Request failed", message, { timeout: 10000 });
      }
      agentRunningRef.current = false;
      setAgentRunning(false);
      setAgentPhase(null);
      setRetryInfo(null);
      setSubagents([]);
      setAdvisorActiveAt(0);
      subagentRosterGenerationRef.current += 1;
      // Bound per-run activity state: without this, subagentEvents and the
      // transcript-version map retain one entry per subagent id forever.
      resetSubagentActivityState();
      // loadSession above already hydrated on-disk history, but it may have
      // resolved BEFORE this clear — re-issue so finished runs repopulate the
      // roster (merge is idempotent).
      if (sid) void refreshSubagentHistory(sid);
      dispatch({ type: "end" });
      runHadContentRef.current = false;
      lastQuotaErrorRef.current = null;
      lastRunErrorRef.current = null;
      slashCommandRunRef.current = false;
      onAgentEnd?.();
    }
  }, [addNotice, clearTerminalReconcileTimer, loadSession, onAgentEnd, refreshSubagentHistory, resetSubagentActivityState, surfaceQuotaOnStream]);

  const waitForPromptSettlement = useCallback(async (sid: string, runId?: number) => {
    await delay(PROMPT_SETTLE_INITIAL_DELAY_MS);
    const startedAt = Date.now();

    while (
      hookAliveRef.current
      && sessionIdRef.current === sid
      && agentRunningRef.current
      && Date.now() - startedAt < PROMPT_SETTLE_MAX_MS
    ) {
      if (runId !== undefined && promptRunIdRef.current !== runId) return;
      try {
        const res = await fetch(`/api/agent/${encodeURIComponent(sid)}`);
        if (res.ok) {
          const data = await res.json() as { running?: boolean; state?: AgentStateResponse };
          const state = data.state;
          if (!data.running || !state || (!state.isStreaming && !state.isPromptRunning)) {
            await finishPromptWithoutStream(sid, runId);
            return;
          }
        }
      } catch {
        // SSE remains the primary completion path.
      }
      await delay(PROMPT_SETTLE_POLL_MS);
    }
  }, [finishPromptWithoutStream]);

  const waitForBashSettlement = useCallback(async (sid: string) => {
    const recoveryId = bashRecoveryIdRef.current + 1;
    bashRecoveryIdRef.current = recoveryId;

    while (
      bashRunningRef.current
      && bashRecoveryIdRef.current === recoveryId
      && sessionIdRef.current === sid
    ) {
      await delay(BASH_STATE_RECONCILE_MS);
      try {
        const res = await fetch(`/api/agent/${encodeURIComponent(sid)}`);
        if (!res.ok) continue;
        const data = await res.json() as { state?: AgentStateResponse };
        if (data.state?.isBashRunning) continue;

        await loadSession(sid);
        if (bashRecoveryIdRef.current !== recoveryId || sessionIdRef.current !== sid) return;
        bashRunningRef.current = false;
        setBashRunning(false);
        setPendingBash(null);
        return;
      } catch {
        // Keep polling while the page is mounted; network recovery is transparent.
      }
    }
  }, [loadSession]);

  // Reconcile client streaming state with the server. When SSE events are
  // missed (network drop, mobile tab backgrounded, half-open connection),
  // agent_end never arrives and the UI stays in streaming state forever.
  // If the server reports idle while we still think it's running, finish
  // through the same path as prompt_done.
  const reconcileAgentState = useCallback(async (sid: string) => {
    if (!agentRunningRef.current) return;
    const runId = promptRunIdRef.current;
    // One request at a time per run: concurrent triggers coalesce into the
    // in-flight request and re-issue on its completion (see release below).
    const guard = reconcileGuardRef.current;
    if (!guard) return;
    const token = guard.tryAcquire();
    if (token === null) return;
    try {
      const res = await fetch(`/api/agent/${encodeURIComponent(sid)}`);
      if (!res.ok) return;
      const data = await res.json() as { running?: boolean; state?: AgentStateResponse };
      // A slow response can straddle a run boundary (previous run finished
      // and the user already started the next one while this request was in
      // flight) — everything in it is stale, drop it.
      if (promptRunIdRef.current !== runId) return;
      const state = data.state;
      // Mirror compaction state unconditionally: a missed compaction_end
      // would otherwise leave the "Stop compaction" UI stuck. No state
      // (wrapper destroyed) means nothing is compacting.
      isCompactingRef.current = state?.isCompacting ?? false;
      setIsCompacting(state?.isCompacting ?? false);
      // Also mid-run: this poll is the only todo-phase refresh while streaming.
      if (state?.todoPhases !== undefined) setTodoPhases(state.todoPhases ?? []);
      // And the only reliable re-sync for a missed subagent lifecycle frame.
      void refreshSubagentRoster(sid);
      if ((!state || state.queuedMessageCount === 0) && Date.now() - queueMutatedAtRef.current >= 5000) setQueuedMessages(EMPTY_QUEUE);
      const busy = data.running && state
        && (state.isStreaming || state.isPromptRunning || state.isCompacting);
      if (busy || !agentRunningRef.current) return;
      if (state) {
        if (state.contextUsage !== undefined) setContextUsage(state.contextUsage ?? null);
        if (state.systemPrompt !== undefined) setSystemPrompt(state.systemPrompt ?? null);
        if (state.extensionStatuses !== undefined) setExtensionStatuses(state.extensionStatuses ?? []);
        if (state.extensionWidgets !== undefined) setExtensionWidgets(state.extensionWidgets ?? []);
      }
      await finishPromptWithoutStream(sid, runId);
    } catch {
      // Network still down — the next poll / visibility / online tick retries.
    } finally {
      // A trigger that landed while this request was in flight must not be
      // lost: re-issue one reconcile immediately (only if still relevant).
      // The token scopes the release: if a new run reset the guard (or this
      // request lost ownership to a newer acquire), the stale owner's
      // release() is a no-op — it must not clear the new run's lock.
      const reissue = guard.release(token);
      if (reissue && agentRunningRef.current && promptRunIdRef.current === runId && sessionIdRef.current === sid) {
        void reconcileAgentState(sid);
      }
    }
  }, [finishPromptWithoutStream, refreshSubagentRoster]);

  // Recovery net for missed SSE events: while the agent is running, verify
  // against the server periodically and whenever the tab returns to the
  // foreground or the network comes back.
  useEffect(() => {
    if (!agentRunning) return;
    const reconcile = () => {
      // Read the ref on every tick: for brand-new sessions the id is
      // assigned only after ensure_session returns.
      const sid = sessionIdRef.current;
      if (sid) void reconcileAgentState(sid);
    };
    const onVisible = () => {
      if (document.visibilityState === "visible") reconcile();
    };
    const interval = setInterval(reconcile, AGENT_STATE_RECONCILE_MS);
    document.addEventListener("visibilitychange", onVisible);
    window.addEventListener("online", reconcile);
    return () => {
      clearInterval(interval);
      document.removeEventListener("visibilitychange", onVisible);
      window.removeEventListener("online", reconcile);
    };
  }, [agentRunning, reconcileAgentState, sessionIdRef]);

  return {
    eventSourceRef,
    reconnectTimerRef,
    reconnectActionsRef,
    reconcileGuardRef,
    connectEvents,
    ensureEventsConnected,
    finishPromptWithoutStream,
    waitForPromptSettlement,
    waitForBashSettlement,
    reconcileAgentState,
  };
}
