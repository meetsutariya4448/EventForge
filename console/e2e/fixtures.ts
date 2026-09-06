import { Client } from 'pg'
import { randomUUID } from 'node:crypto'

/**
 * Seeds a captured failure directly into order-service's own database.
 *
 * There is deliberately no HTTP endpoint that creates one — rows are written only by the code path
 * that captures a genuine consumer failure — so a test fixture has to go to the database, the same
 * way the Java tests call FailedMessageStore directly. Driving a real poison message through Kafka
 * would test the capture path, which already has its own tests; what needs a fixture here is the
 * console's behaviour given a row that exists.
 */
const CONNECTION = {
  host: 'localhost',
  port: 5433,
  database: 'order',
  user: 'order',
  password: 'order',
}

export interface SeededFailure {
  failedMessageId: string
  messageKey: string
}

async function withClient<T>(fn: (client: Client) => Promise<T>): Promise<T> {
  const client = new Client(CONNECTION)
  await client.connect()
  try {
    return await fn(client)
  } finally {
    await client.end()
  }
}

export function seedFailedMessage(): Promise<SeededFailure> {
  const failedMessageId = randomUUID()
  const messageKey = `e2e-${randomUUID()}`
  // A random offset keeps this row from colliding with the capture table's unique index on
  // (consumer_group, topic, partition_id, record_offset) across repeated runs.
  const offset = Math.floor(Math.random() * 1_000_000_000) + 1_000_000

  return withClient(async (client) => {
    await client.query(
      `INSERT INTO failed_messages (
         failed_message_id, consumer_group, topic, partition_id, record_offset,
         message_key, payload, headers, event_id, event_type,
         failure_reason, status, captured_at
       ) VALUES ($1, 'order-service', 'payments.events', 0, $2, $3, $4, NULL, NULL, NULL, $5, 'CAPTURED', now())`,
      [
        failedMessageId,
        offset,
        messageKey,
        '{not-valid-json-at-all',
        'com.fasterxml.jackson.core.JsonParseException: seeded by the console e2e suite',
      ],
    )
    return { failedMessageId, messageKey }
  })
}

export function readFailedMessageStatus(failedMessageId: string): Promise<string | undefined> {
  return withClient(async (client) => {
    const result = await client.query<{ status: string }>(
      'SELECT status FROM failed_messages WHERE failed_message_id = $1',
      [failedMessageId],
    )
    return result.rows[0]?.status
  })
}

export function countOperatorActionsFor(failedMessageId: string): Promise<number> {
  return withClient(async (client) => {
    const result = await client.query<{ count: string }>(
      "SELECT count(*) AS count FROM operator_action WHERE target_id = $1 AND target_type = 'FAILED_MESSAGE'",
      [failedMessageId],
    )
    return Number(result.rows[0]?.count ?? '0')
  })
}

export function deleteFailedMessage(failedMessageId: string): Promise<void> {
  return withClient(async (client) => {
    await client.query("DELETE FROM operator_action WHERE target_id = $1 AND target_type = 'FAILED_MESSAGE'", [
      failedMessageId,
    ])
    await client.query('DELETE FROM failed_messages WHERE failed_message_id = $1', [failedMessageId])
  })
}
