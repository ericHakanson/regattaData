# Mailchimp Pre-Send Checklist

Use this checklist before any campaign that uses `mailchimp_event_activation` output.

## 1. Preconditions

- [ ] Confirm you are on the correct environment and DB (`echo "$DB_DSN"` points to intended target).
- [ ] Confirm latest schema and migrations are applied (activation audit tables exist).
- [ ] Confirm `mailchimp_contact_state` is current enough for suppression (same-day refresh preferred).
- [ ] Confirm campaign owner and reviewer are assigned (two-person check).

## 2. Data Build (No External Side Effects)

- [ ] Run activation in dry-run mode first:

```bash
.venv/bin/regatta-import \
  --mode mailchimp_event_activation \
  --db-dsn "$DB_DSN" \
  --event-window-days 45 \
  --segment-type all \
  --delivery-mode csv \
  --output-path "artifacts/exports/mailchimp_event_activation.csv" \
  --dry-run
```

- [ ] Verify dry-run report shows:
  - `db_errors = 0`
  - expected `rows_eligible`
  - expected suppression counts
  - no unexpected warnings

## 3. Review Export Candidate Set

- [ ] Run non-dry CSV export (still no Mailchimp API writes):

```bash
.venv/bin/regatta-import \
  --mode mailchimp_event_activation \
  --db-dsn "$DB_DSN" \
  --event-window-days 45 \
  --segment-type all \
  --delivery-mode csv \
  --output-path "artifacts/exports/mailchimp_event_activation.csv"
```

- [ ] Validate CSV spot sample (at least 20 rows):
  - names/emails look correct
  - segment tags make sense
  - no obvious stale or wrong contacts
- [ ] Validate there are no duplicate emails.
- [ ] Validate campaign audience size is in expected range for this event cycle.

## 4. Mailchimp Send Guardrails

- [ ] Do not send to "All contacts"/entire audience.
- [ ] Send only to a gated segment built from activation tags (for example, `source:regattadata_cdp` + campaign tag).
- [ ] Exclude suppressed states (`unsubscribed`, `cleaned`) in segment logic.
- [ ] First send goes to internal seed/test recipients only.
- [ ] Verify seed send content and merge fields before broader send.

## 5. Go/No-Go

- [ ] Reviewer approves dry-run report and CSV sample.
- [ ] Reviewer approves final recipient count.
- [ ] Campaign owner records run ID and report path in release notes.
- [ ] If any mismatch is found, stop and rerun from dry-run after correction.

## 6. Evidence to Save

- [ ] Activation run ID(s) and report JSON path.
- [ ] Final CSV artifact path and timestamp.
- [ ] Reviewer name + approval timestamp.
- [ ] Mailchimp campaign ID used for seed send and production send.
