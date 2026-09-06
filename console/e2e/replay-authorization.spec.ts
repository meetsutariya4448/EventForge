import { test, expect, type Page } from '@playwright/test'
import {
  countOperatorActionsFor,
  deleteFailedMessage,
  readFailedMessageStatus,
  seedFailedMessage,
  type SeededFailure,
} from './fixtures'

/**
 * The console's authorization behaviour, asserted end to end against the real order-service.
 *
 * The claim being tested is a SERVER claim — a VIEWER's replay is refused — so every assertion
 * below is anchored to something the server owns: the HTTP status it returned, the row it did or
 * did not modify, the audit row it did or did not write. A test that only checked whether a button
 * was visible would pass just as happily against a server with no authorization at all.
 */

let seeded: SeededFailure

test.beforeEach(async () => {
  seeded = await seedFailedMessage()
})

test.afterEach(async () => {
  await deleteFailedMessage(seeded.failedMessageId)
})

async function signIn(page: Page, username: string, password: string): Promise<void> {
  await page.goto('/')
  await page.getByLabel('User').fill(username)
  await page.getByLabel('Password').fill(password)
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page.getByRole('heading', { name: 'Failed messages' })).toBeVisible()
}

/**
 * Located by this run's own seeded message key, not by the failure text. Filtering on something
 * generic like the exception name would match any real captured failure that happens to be in the
 * table, and the test would then replay someone else's row.
 */
function rowFor(page: Page, seededFailure: SeededFailure) {
  return page.getByRole('row').filter({ hasText: seededFailure.messageKey })
}

test('a viewer is denied the replay by the server, and the console says so', async ({ page }) => {
  await signIn(page, 'viewer', 'viewer')

  const row = rowFor(page, seeded)
  await expect(row).toBeVisible()

  // The button is rendered for a VIEWER on purpose. Hiding it would make the CONSOLE the thing
  // enforcing authorization, and this test would then prove only that the console hides buttons.
  const replayButton = row.getByRole('button', { name: 'Replay' })
  await expect(replayButton).toBeVisible()

  const response = page.waitForResponse(
    (r) => r.url().includes(`/failed-messages/${seeded.failedMessageId}/replay`) && r.request().method() === 'POST',
  )
  await replayButton.click()

  // The server refused it. This is the assertion the whole test exists for.
  expect((await response).status()).toBe(403)
  await expect(page.getByText('not permitted to replay')).toBeVisible()

  // And it refused before doing anything: the message is untouched and nothing was audited.
  expect(await readFailedMessageStatus(seeded.failedMessageId)).toBe('CAPTURED')
  expect(await countOperatorActionsFor(seeded.failedMessageId)).toBe(0)
})

test('an operator can replay, and the action is recorded against their name', async ({ page }) => {
  await signIn(page, 'operator', 'operator')

  const row = rowFor(page, seeded)
  await expect(row).toBeVisible()

  const response = page.waitForResponse(
    (r) => r.url().includes(`/failed-messages/${seeded.failedMessageId}/replay`) && r.request().method() === 'POST',
  )
  await row.getByRole('button', { name: 'Replay' }).click()
  expect((await response).status()).toBe(202)

  expect(await readFailedMessageStatus(seeded.failedMessageId)).toBe('REPLAYED')

  // The audit trail is the visible half of what the server recorded.
  const auditRow = page.getByRole('row').filter({ hasText: seeded.failedMessageId }).first()
  await expect(auditRow).toContainText('operator')
  await expect(auditRow).toContainText('REPLAY_FAILED_MESSAGE')
  await expect(auditRow).toContainText('SUCCEEDED')
})

test('a viewer can read the audit trail even though they cannot act', async ({ page }) => {
  await signIn(page, 'viewer', 'viewer')
  // Present and readable, not an error — a record of who did what is worth less if only the people
  // who can act are allowed to see it.
  await expect(page.getByRole('heading', { name: 'Audit trail' })).toBeVisible()
  await expect(page.getByText('Could not load the audit trail')).toHaveCount(0)
})
