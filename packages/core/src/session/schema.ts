export * as SessionSchema from "./schema"

import { Session } from "@opencode-ai/schema/session"

export const ID = Session.ID
export type ID = typeof ID.Type

export const SmallModel = Session.SmallModel
export type SmallModel = Session.SmallModel

export const Info = Session.Info
export type Info = Session.Info

export const WaitResult = Session.WaitResult
export type WaitResult = typeof WaitResult.Type
