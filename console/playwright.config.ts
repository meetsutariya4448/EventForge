import { defineConfig, devices } from '@playwright/test'

/**
 * These tests run against the REAL order-service, not a mocked API, and that is the whole point.
 *
 * The claim under test is "an unauthorized replay is denied". A test with `page.route` stubbing
 * the API would assert that the stub returns 403 — it would prove something about the test's own
 * mock and nothing about the server's authorization. The same reasoning is why this project's
 * Java tests use real Postgres and real Kafka rather than mocks.
 *
 * Prerequisite: `make up` (Docker infra + the four services). Tests fail with an explicit message
 * rather than a confusing timeout if order-service is not reachable — see global-setup.ts.
 */
export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: [['list']],
  globalSetup: './e2e/global-setup.ts',
  use: {
    baseURL: 'http://localhost:5173',
    trace: 'retain-on-failure',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: {
    command: 'npm run dev',
    url: 'http://localhost:5173',
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
})
