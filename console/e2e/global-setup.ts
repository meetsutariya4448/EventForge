/**
 * Fails fast and legibly if the stack the tests need is not up.
 *
 * Without this, a missing order-service surfaces as a browser-level timeout on an empty table,
 * which reads like a console bug and sends whoever hit it looking in the wrong place entirely.
 */
export default async function globalSetup(): Promise<void> {
  const health = 'http://localhost:8081/actuator/health'
  try {
    const response = await fetch(health)
    if (!response.ok) {
      throw new Error(`${response.status} ${response.statusText}`)
    }
  } catch (cause) {
    throw new Error(
      `order-service is not reachable at ${health}.\n` +
        `These tests run against the real service on purpose — a mocked API cannot prove that the ` +
        `SERVER denies an unauthorized replay.\n` +
        `Start the stack first:  make up      (from the repository root)\n` +
        `Underlying error: ${cause instanceof Error ? cause.message : String(cause)}`,
    )
  }
}
