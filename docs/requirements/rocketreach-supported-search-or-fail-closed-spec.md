# Requirements: RocketReach Must Use a Supported Search Method or Fail Closed

## 1. Objective
Prevent further wasted RocketReach credits on unsupported participant hold-pool lookups.

The integration must do one of the following:
1. use a RocketReach API method and request shape that is explicitly documented and supported for the available subscription/API surface, or
2. fail closed and refuse live hold-pool RocketReach calls when no supported search method exists for the available candidate data.

## 2. Background (Observed Production Evidence)
Current production results after multiple iterations:
1. candidate preparation is working:
   - hold pool prepared and geo-classified
   - Northeast USA subset identified
   - person-name gate working
2. outbound request payload now includes:
   - `name`
   - `city`
   - `state`
   - `country`
3. RocketReach live run outcomes remain:
   - `api_error`
   - no matched/applied rows
4. observed API error response:
   - `Specify current_employer to narrow search results`
5. sample outbound payloads are of the form:
   - `{"city":"Old Greenwich","name":"william geiger","state":"CT","country":"USA"}`
   - `{"city":"Mattapoisett","name":"charlie van voorhis","state":"MA","country":"USA"}`
6. no credits appear to have been consumed productively; no enrichment was applied.

Interpretation:
1. the internal selection and gating pipeline is now reasonably strong,
2. the current RocketReach endpoint/query shape is still not valid for this use case,
3. continued live runs on the current method are not justified.

## 3. Non-Negotiable Constraint
Do not continue live RocketReach hold-pool calls unless the implementation is using a documented, supported RocketReach search mode for the available data.

Required safety rule:
1. if the supported search mode cannot be confirmed, live calls for this hold-pool path must be disabled or skipped with an explicit reason.

Prohibited behavior:
1. continuing to issue name-plus-location live calls to an endpoint that returns HTTP 400 with a documented narrowing requirement,
2. making paid calls based on guesswork about RocketReach parameter semantics,
3. silently falling back to unsupported request shapes.

## 4. Scope
In scope:
1. RocketReach integration mode selection
2. request-shape validation against official docs
3. fail-closed safety behavior for unsupported hold-pool searches
4. tests
5. operator-facing warnings and skip reasons

Out of scope:
1. participant scoring changes
2. hold-pool geo preparation
3. Mailchimp ingestion
4. replacing RocketReach with another provider in this change
5. employer-inference heuristics unless they are explicitly chosen and documented as a follow-up phase

## 5. Functional Requirements

### R1. Verify Supported RocketReach Search Method Against Official Documentation
Claude must verify, using official RocketReach documentation only, which search methods are supported for the current API integration path.

Minimum required verification questions:
1. Is there a documented endpoint or request shape that supports lookup by `name + location`?
2. Is `current_employer` required for name-based search on the endpoint currently in use?
3. Is there a different documented endpoint for person search that supports:
   - `name + current_employer`
   - `name + location`
   - `linkedin_url`
   - or another supported non-email anchor?
4. Are the current HTTP method and path correct for the intended search operation?

Claude must use official RocketReach docs as the source of truth.

### R2. If Supported Method Exists, Implement It Explicitly
If official docs confirm a supported search method for the hold-pool data you actually have, Claude may implement it.

Requirements:
1. switch the integration to that documented method,
2. align request payload keys exactly to the documented parameter names,
3. update audit logging so `request_payload` mirrors the real request shape,
4. add tests proving the correct method/path/payload is used.

### R3. If No Supported Method Exists, Fail Closed
If official docs do not confirm a supported method for `name + location` hold-pool enrichment, the implementation must fail closed.

Required behavior:
1. do not make live API calls for these candidates,
2. write `rocketreach_enrichment_row` with:
   - `status = 'skipped'`
   - a distinct `error_code`
3. increment a dedicated counter,
4. emit a clear warning/report line explaining why live calls were skipped.

Recommended error code:
1. `rocketreach_unsupported_search_mode`

Recommended warning text:
1. current RocketReach endpoint requires search narrowing not available for this candidate class

### R4. Separate Endpoint/Method Validation from Candidate Quality
The integration must distinguish:
1. candidate skipped because the candidate itself is poor quality (`rocketreach_non_person_name`, no geo readiness, etc.), versus
2. candidate skipped because RocketReach does not support the intended search shape (`rocketreach_unsupported_search_mode`).

These are different operational problems and must not share the same reason code.

### R5. Preserve Existing Candidate and Geo Filters
This change must not remove the work already done.

Keep:
1. Northeast geo filters
2. `require_geo_ready`
3. person-name quality gate
4. audit tables
5. dry-run behavior

The only change should be:
1. use a documented supported RocketReach search method, or
2. stop live calls cleanly.

## 6. Implementation Requirements

### I1. Use Official RocketReach Docs Only for Search-Mode Decision
Claude must browse official RocketReach documentation and restrict source attribution to official RocketReach docs.

This is required because the current blocker is API contract uncertainty, not local code uncertainty.

### I2. Make Search Mode Explicit in Code
File: `src/regatta_etl/import_rocketreach_enrichment.py`

Implementation should make the chosen search mode explicit, for example:
1. `email_lookup`
2. `name_geo_lookup`
3. `name_employer_lookup`
4. `unsupported`

This can be implemented as:
1. a helper that determines search mode from candidate evidence and validated API capability, or
2. a strict branch in `_process_candidate(...)`.

### I3. Fail Closed Before Billable Call
If the search mode is unsupported for a candidate:
1. insert enrichment row,
2. increment counter,
3. return without calling the RocketReach client.

This must happen before any network request.

### I4. Add Counters and Report Lines
Minimum new counter if failing closed:
1. `rocketreach_unsupported_search_mode`

If a supported alternate method is implemented, add a counter for the chosen method, for example:
1. `rocketreach_name_geo_called`
2. or `rocketreach_name_employer_called`

The report must make it obvious which path is being used.

## 7. Testing Requirements

### 7.1 Unit
1. search-mode decision returns the expected mode for:
   - email-bearing candidate
   - geo-ready hold candidate with no email
   - unsupported hold candidate
2. unsupported mode produces skipped row with `rocketreach_unsupported_search_mode`
3. unsupported mode does not call the client
4. if a supported alternate method is implemented, request builder uses documented payload keys exactly

### 7.2 Integration
1. hold candidate on unsupported search mode is skipped before API call
2. `rocketreach_enrichment_row` records the new skip reason
3. report/counters reflect unsupported search mode
4. existing Northeast geo filter and person-name gate still work
5. if a supported alternate method is implemented, integration test proves the live client/stub is invoked with the documented request shape

## 8. Acceptance Criteria
1. The code no longer issues live RocketReach hold-pool calls using an unsupported request shape.
2. The supported method, if any, is grounded in official RocketReach documentation.
3. If no supported method exists, hold-pool RocketReach runs fail closed with explicit skip reasons.
4. Operators can distinguish unsupported-search skips from candidate-quality skips and API transport errors.
5. Tests pass.

## 9. Recommended Rollout Plan
1. verify RocketReach docs and determine whether a supported hold-pool search method exists
2. implement either:
   - supported documented method, or
   - fail-closed skip behavior
3. run dry-run on Northeast hold candidates
4. inspect counters and request payloads
5. only then consider a live pilot

## 10. Explicit Non-Goals
Do not:
1. keep experimenting with undocumented RocketReach payload permutations in live runs,
2. spend more live calls on the current name-plus-location path,
3. infer `current_employer` heuristically in this change unless the docs clearly support that as the intended method and you have a defensible employer source.
