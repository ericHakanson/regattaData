# Decision Log

## 2026-02-15 Confirmed Decisions
1. Platform is Google Cloud centered, with Cloud SQL + GCS.
2. Unified person model is accepted: owners and participants are both represented by `participant` with relationship/role tables.
3. Registration truth is derived from `regattaman.com` ingestion.
4. Waiver truth is derived from `Jotform` ingestion.
5. Scraped source set includes `regattaman.com` and `yachtscoring.com`.
6. Entity resolution and confidence-scored auto-merge are required for participants, clubs, events, and yachts.
7. Outlier role scenarios must be supported:
- owner not participating on owned yacht entry,
- owner participating on a different yacht entry in same event.
8. No paid third-party address validation is used in phase 1.
9. Auto-merge threshold defaults accepted:
- participants `>= 0.95`, yachts `>= 0.97`, clubs `>= 0.98`, events `>= 0.98`.
10. Data retention policy accepted:
- raw artifacts retained for 3 years,
- PII retained for 10 years.
11. Campaign export contract accepted:
- versioned CSV + JSON to GCS with Google Workspace as initial consumer target.
12. Club vitality rule accepted:
- mark `inactive` after 24 months with no observed event instance; allow manual override.
13. Legacy local scaffolding has been removed from the repo.
