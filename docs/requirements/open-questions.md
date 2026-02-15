# Open Questions Status

## Current Status
All previously open decisions are now resolved.

## Confirmed Decisions
1. Registration source: `regattaman.com`.
2. Waiver source: `Jotform`.
3. Scraped sources: `regattaman.com` and `yachtscoring.com`.
4. Entity resolution is required for participants, clubs, events, and yachts.
5. Outlier owner/participant combinations must be supported.
6. No paid third-party address validation in initial release.
7. Auto-merge thresholds accepted as recommended defaults:
- participants: `>= 0.95`
- yachts: `>= 0.97`
- clubs: `>= 0.98`
- events: `>= 0.98`
8. Retention policy:
- raw artifacts: retain 3 years
- PII: retain 10 years
9. Campaign export contract:
- versioned CSV + JSON exports to GCS
- primary destination assumption: Google Workspace workflows (Google Sheets/Docs/Slides)
10. Club vitality transition rule:
- status enum: `active`, `inactive`, `unknown`
- transition to `inactive` after 24 months with no observed event instance, with manual override.

## Future Revisit Topics
1. Address validation integration (optional future paid provider).
2. Auto-merge threshold tuning by measured precision/recall.
3. Additional destination connectors beyond Google Workspace.
