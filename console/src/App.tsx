import { useCallback, useEffect, useState } from 'react'
import {
  listFailedMessages,
  listOperatorActions,
  replay,
  type Credentials,
  type FailedMessage,
  type OperatorAction,
  type ReplayResult,
} from './api/client'
import './App.css'

function SignIn({ onSignIn }: { onSignIn: (credentials: Credentials) => void }) {
  const [username, setUsername] = useState('operator')
  const [password, setPassword] = useState('')

  return (
    <form
      className="signin"
      onSubmit={(event) => {
        event.preventDefault()
        onSignIn({ username, password })
      }}
    >
      <h1>EventForge console</h1>
      <label>
        User
        <input value={username} onChange={(e) => setUsername(e.target.value)} autoComplete="username" />
      </label>
      <label>
        Password
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          autoComplete="current-password"
        />
      </label>
      <button type="submit">Sign in</button>
      <p className="hint">
        Credentials are held in memory for this tab only — never written to localStorage, which any
        script on the page can read.
      </p>
    </form>
  )
}

function StatusPill({ status }: { status: string | undefined }) {
  return <span className={`pill pill-${(status ?? 'unknown').toLowerCase()}`}>{status ?? 'unknown'}</span>
}

/**
 * Every field on the generated types is optional, because the Java records do not declare which
 * fields are required in the OpenAPI description. Rather than assert non-null and hope, the
 * console renders a visible placeholder — a missing value shows as missing instead of as an empty
 * cell that looks like data.
 */
function value(text: string | undefined): string {
  return text === undefined || text === '' ? '—' : text
}

function FailedMessages({
  credentials,
  onActed,
}: {
  credentials: Credentials
  onActed: () => void
}) {
  const [rows, setRows] = useState<FailedMessage[]>([])
  const [openOnly, setOpenOnly] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<ReplayResult | null>(null)
  const [busyId, setBusyId] = useState<string | null>(null)

  // Takes a staleness check rather than just fetching: toggling the filter twice quickly starts
  // two requests, and without this the slower one can land last and overwrite the newer result
  // with rows the user is no longer looking at.
  const fetchRows = useCallback(
    async (isStale: () => boolean) => {
      try {
        const next = await listFailedMessages(credentials, openOnly)
        if (isStale()) return
        setRows(next)
        setError(null)
      } catch (e) {
        if (isStale()) return
        setError(e instanceof Error ? e.message : String(e))
      }
    },
    [credentials, openOnly],
  )

  useEffect(() => {
    let cancelled = false
    // False positive on set-state-in-effect: fetchRows is async and every setState inside it runs
    // after an await, so nothing is set synchronously during this effect. Fetching on mount is the
    // "synchronizing with an external system" case the rule's own guidance names as legitimate.
    // Suppressed with the reason recorded rather than left as a standing warning, which would
    // invite someone to "fix" it by removing the fetch.
    // eslint-disable-next-line react/set-state-in-effect
    void fetchRows(() => cancelled)
    return () => {
      cancelled = true
    }
  }, [fetchRows])

  const refresh = useCallback(() => fetchRows(() => false), [fetchRows])

  async function onReplay(id: string) {
    setBusyId(id)
    const outcome = await replay(credentials, id)
    setResult(outcome)
    setBusyId(null)
    await refresh()
    onActed()
  }

  return (
    <section>
      <header className="section-header">
        <h2>Failed messages</h2>
        <label className="toggle">
          <input type="checkbox" checked={openOnly} onChange={(e) => setOpenOnly(e.target.checked)} />
          Outstanding only
        </label>
        <button onClick={() => void refresh()}>Refresh</button>
      </header>

      {error !== null && <p className="error">Could not load failed messages: {error}</p>}
      {result !== null && <p className={`result result-${result.kind}`}>{result.message}</p>}

      {rows.length === 0 && error === null ? (
        <p className="empty">Nothing captured. That is the good case.</p>
      ) : (
        <div className="scroll">
          <table>
            <thead>
              <tr>
                <th>Captured</th>
                <th>Topic</th>
                <th>Key</th>
                <th>Partition/Offset</th>
                <th>Event type</th>
                <th>Status</th>
                <th>Attempts</th>
                <th>Reason</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.failedMessageId}>
                  <td>{value(row.capturedAt)}</td>
                  <td>{value(row.topic)}</td>
                  {/* The Kafka message key — for these topics, the order id. It is what an
                      operator uses to tie a failure back to the thing it was about. */}
                  <td className="reason" title={row.messageKey}>
                    {value(row.messageKey)}
                  </td>
                  <td>
                    {row.partition ?? '—'}/{row.offset ?? '—'}
                  </td>
                  <td>{value(row.eventType)}</td>
                  <td>
                    <StatusPill status={row.status} />
                  </td>
                  <td>{row.replayAttempts ?? 0}</td>
                  <td className="reason" title={row.failureReason}>
                    {value(row.failureReason)}
                  </td>
                  <td>
                    {/* Rendered for everyone, deliberately. Hiding it from a VIEWER would make the
                        console the thing enforcing authorization; the server refuses the request
                        and the console shows that refusal. */}
                    <button
                      disabled={row.failedMessageId === undefined || busyId === row.failedMessageId}
                      onClick={() => row.failedMessageId !== undefined && void onReplay(row.failedMessageId)}
                    >
                      {busyId === row.failedMessageId ? 'Replaying…' : 'Replay'}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}

function AuditTrail({ credentials, reloadToken }: { credentials: Credentials; reloadToken: number }) {
  const [rows, setRows] = useState<OperatorAction[]>([])
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    listOperatorActions(credentials)
      .then((actions) => {
        if (!cancelled) {
          setRows(actions)
          setError(null)
        }
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : String(e))
        }
      })
    return () => {
      cancelled = true
    }
  }, [credentials, reloadToken])

  return (
    <section>
      <header className="section-header">
        <h2>Audit trail</h2>
      </header>
      {error !== null && <p className="error">Could not load the audit trail: {error}</p>}
      {rows.length === 0 && error === null ? (
        <p className="empty">No operator actions recorded.</p>
      ) : (
        <div className="scroll">
          <table>
            <thead>
              <tr>
                <th>Dispatched</th>
                <th>Actor</th>
                <th>Action</th>
                <th>Target</th>
                <th>Status</th>
                <th>Completed</th>
                <th>Detail</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.operatorActionId}>
                  <td>{value(row.dispatchedAt)}</td>
                  <td>{value(row.actor)}</td>
                  <td>{value(row.actionType)}</td>
                  <td className="reason" title={row.targetId}>
                    {value(row.targetId)}
                  </td>
                  <td>
                    <StatusPill status={row.status} />
                  </td>
                  {/* An action with no completion is not a failure — it is one nothing recorded
                      the outcome of. Labelling it "unresolved" rather than leaving the cell blank
                      is the difference between the trail being read correctly and not. */}
                  <td>{row.completedAt === undefined ? <em>unresolved</em> : row.completedAt}</td>
                  <td>{value(row.detail)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}

export default function App() {
  const [credentials, setCredentials] = useState<Credentials | null>(null)
  const [reloadToken, setReloadToken] = useState(0)

  if (credentials === null) {
    return <SignIn onSignIn={setCredentials} />
  }

  return (
    <main>
      <header className="top">
        <h1>EventForge console</h1>
        <span>
          signed in as <strong>{credentials.username}</strong>
        </span>
        <button onClick={() => setCredentials(null)}>Sign out</button>
      </header>
      <FailedMessages credentials={credentials} onActed={() => setReloadToken((n) => n + 1)} />
      <AuditTrail credentials={credentials} reloadToken={reloadToken} />
    </main>
  )
}
