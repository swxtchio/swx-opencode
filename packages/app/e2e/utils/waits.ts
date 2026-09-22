import { expect, type Locator, type Page } from "@playwright/test"
import { deadline } from "./deadline"

/**
 * How long to wait for the app shell to render.
 *
 * Scaled, not fixed: a run on a loaded box failed here at 30s while asserting
 * nothing about the app under test, which is the failure class #10 is about.
 * This constant is used across the e2e suite, so the scaling reaches every
 * spec rather than the one that happened to expose it.
 */
export const APP_READY_TIMEOUT = deadline(30_000)

export async function expectAppVisible(locator: Locator) {
  await expect(locator).toBeVisible({ timeout: APP_READY_TIMEOUT })
}

export async function expectSessionTitle(page: Page, title: string) {
  await expectAppVisible(page.getByRole("heading", { name: title }))
}
