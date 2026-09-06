import type { components } from './schema'

// Aliased from the generated types rather than redeclared. Redeclaring them would recreate exactly
// the drift that generating from the spec exists to prevent — a second definition that compiles
// fine while describing a different API.
export type FailedMessage = components['schemas']['FailedMessageRow']
export type OperatorAction = components['schemas']['OperatorAction']

export interface Credentials {
  username: string
  password: string
}

/**
 * The outcome of a replay, as the console understands it.
 *
 * `forbidden` is a distinct case rather than folded into `error` because it is the one the
 * authorization model is actually about: a VIEWER's replay is refused by the server, and the
 * console has to show that plainly instead of as a generic failure.
 */
export type ReplayResult =
  | { kind: 'accepted'; message: string }
  | { kind: 'conflict'; message: string }
  | { kind: 'forbidden'; message: string }
  | { kind: 'error'; message: string }

const BASE = '/api'

function authHeader({ username, password }: Credentials): string {
  return `Basic ${btoa(`${username}:${password}`)}`
}

async function get<T>(path: string, credentials: Credentials): Promise<T> {
  const response = await fetch(`${BASE}${path}`, {
    headers: { Authorization: authHeader(credentials) },
  })
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`)
  }
  return (await response.json()) as T
}

export function listFailedMessages(credentials: Credentials, openOnly: boolean): Promise<FailedMessage[]> {
  return get<FailedMessage[]>(`/failed-messages?openOnly=${openOnly}`, credentials)
}

export function listOperatorActions(credentials: Credentials): Promise<OperatorAction[]> {
  return get<OperatorAction[]>('/operator-actions?limit=50', credentials)
}

export async function replay(credentials: Credentials, failedMessageId: string): Promise<ReplayResult> {
  const response = await fetch(`${BASE}/failed-messages/${failedMessageId}/replay`, {
    method: 'POST',
    headers: { Authorization: authHeader(credentials) },
  })
  const body = await response.text()

  // Mapped from the status codes the server actually returns (see FailedMessageController), not
  // from a guess at what "failure" means. 403 in particular must survive to the UI intact.
  switch (response.status) {
    case 202:
      return { kind: 'accepted', message: body }
    case 409:
      return { kind: 'conflict', message: body }
    case 401:
    case 403:
      return { kind: 'forbidden', message: 'Your account is not permitted to replay messages.' }
    default:
      return { kind: 'error', message: body || `${response.status} ${response.statusText}` }
  }
}
