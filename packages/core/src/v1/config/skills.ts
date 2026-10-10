export * as ConfigSkillsV1 from "./skills"

import { Schema } from "effect"

export const Info = Schema.Struct({
  paths: Schema.optional(Schema.Array(Schema.String)).annotate({
    description: "Additional paths to skill folders",
  }),
  urls: Schema.optional(Schema.Array(Schema.String)).annotate({
    description: "URLs to fetch skills from (e.g., https://example.com/.well-known/skills/)",
  }),
  exclude: Schema.optional(Schema.Array(Schema.String)).annotate({
    description:
      "Glob patterns of SKILL.md files to skip in every skill root, matched relative to that root (e.g., **/.trash/**)",
  }),
})
export type Info = Schema.Schema.Type<typeof Info>
