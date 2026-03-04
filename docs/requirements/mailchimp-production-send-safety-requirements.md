# Requirements: Mailchimp Production Send Safety Guardrails

## 1. Objective

Prevent incorrect recipients and suppression violations when activating audiences from the CDP into Mailchimp.

This document defines production send controls in addition to technical pipeline requirements.

## 2. Scope

In scope:
- pre-send process controls
- audience gating controls
- suppression freshness controls
- approval and evidence requirements

Out of scope:
- campaign copy/creative quality
- deliverability strategy

## 3. Mandatory Guardrails

1. No broad sends:
   - Campaigns sourced from CDP activation must never target "All contacts."
   - Campaigns must target a gated segment/tag set created by activation workflow.

2. Suppression safety:
   - Activation must exclude `unsubscribed` and `cleaned` statuses.
   - Suppression source data must be refreshed before production send (same-day preferred).

3. Two-step execution:
   - Dry-run is required before every production send cycle.
   - CSV export review is required before API activation or Mailchimp campaign launch.

4. Human approval:
   - Two-person check is required: campaign owner + reviewer.
   - Reviewer must approve recipient count and row sample quality.

5. Seed send requirement:
   - First send for each campaign must go to internal seed recipients only.
   - Production audience send occurs only after seed validation.

6. Failure policy:
   - Any activation run with DB errors or API delivery failures is a failed run.
   - Failed runs cannot be used as campaign source without rerun and re-approval.

## 4. Required Evidence

For each production send cycle, store:

1. Activation run ID and report path.
2. Export artifact path and timestamp.
3. Reviewer identity and approval timestamp.
4. Mailchimp campaign ID for seed send and production send.

## 5. Acceptance Criteria

1. Every production campaign has traceable activation evidence.
2. No production campaign is sent to ungated audiences.
3. No suppressed contacts are present in delivery set.
4. Failed activation runs are not promoted to send.
5. Seed send is always performed and verified first.
