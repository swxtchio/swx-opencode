# Small-model scope implementation

Authority: swxtchio/swx-opencode#338 and the assigned task brief.

- Trace primary-model configuration, agent normalization, session persistence and HTTP controls before choosing the corresponding small-model representation and precedence.
- Resolve configured auxiliary models independently of the primary provider. Preserve a distinction between absent configuration and failed resolution, and thread effective session/agent selection into title and compaction requests.
- Verify real configuration and session controls through production auxiliary orchestration with a controlled LLM boundary. Cover competing scopes, isolated sessions, both Azure provider identifiers, auxiliary-agent overrides, and failed configured resolution without substitution.
- Demonstrate bounded regression failures with the guarded behavior removed, restore the implementation, and run focused tests and changed-package typechecks.
- Regenerate affected client contracts, reconcile operator documentation and the fork changeset, then push the single draft PR for captain-owned review.
