export * as DatabaseMessageAdmission from "./message-admission"

import { sql } from "drizzle-orm"
import type { Migration } from "./migration"

type Transaction = Parameters<Migration["up"]>[0]

export function installLegacyMessageAdmissionTrigger(tx: Transaction) {
  return tx.run(sql`
    CREATE TRIGGER IF NOT EXISTS message_admission_seq_legacy_insert
    AFTER INSERT ON message
    WHEN NEW.admission_seq = 0
    BEGIN
      UPDATE message
      SET admission_seq = (
        SELECT COALESCE(MAX(admission_seq), 0) + 1
        FROM message
        WHERE session_id = NEW.session_id AND admission_seq > 0
      )
      WHERE id = NEW.id AND session_id = NEW.session_id;
    END;
  `)
}
