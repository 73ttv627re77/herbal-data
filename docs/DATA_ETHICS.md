# Herbal Data Pipeline — Data Ethics & Compliance

## Data Collection Principles

### What We Collect
- Publicly posted comments from Reddit (via PRAW/API)
- Anonymized: usernames hashed with SHA256 before storage
- Raw JSON stored immutable — original text preserved for reprocessing

### What We Don't Collect
- Private messages or non-public posts
- Data from Facebook (intentionally excluded)
- Any data that requires login/authentication beyond Reddit API
- Personally identifiable information beyond hashed usernames

## Data Retention

### Raw Data (source_posts, source_comments)
- Stored indefinitely
- Immutable — never modified or deleted
- Rationale: enables re-derivation when NLP pipeline improves
- If takedown requested: raw record remains but is excluded from derived claims

### Derived Data (claims, aliases, preparations)
- Re-derived from raw on demand
- Can be recomputed if NLP logic changes
- Takedown: remove from source_posts → cascade removes derived claims

### Evidence Items
- Retained indefinitely
- Sourced from public PubMed/clinical databases

## Anonymization

```
Original username → SHA256 hash stored
Example: "user123" → "a665a459..." (stored in author_hash field)
```

- No raw usernames stored
- No cross-referencing with Reddit API possible (hash only)
- Post/comment content stored in full for provenance

## Takedown Process

If someone requests removal of their content:

1. Identify source via post/comment URL or author hash
2. Mark record as `withdrawn = true` in source_posts/source_comments
3. Cascade: withdrawn source_comments → claims via claim_sources → claims hidden from API
4. Raw data retained for audit (legal requirement) but excluded from all derived outputs
5. Confirmation sent to requester

**Note:** This is why raw data is immutable — we retain for legal compliance while removing from derived outputs.

## Reddit Terms of Service Compliance

- Only access public Reddit data via official API (PRAW)
- Respect rate limits (1 request/second)
- Attribute sources in claims (subreddit, post URL)
- Do not misrepresent Reddit data as proprietary
- Comply with Reddit's API terms of service (reddit.com/prefs/apps)

## Medical Misinformation Prevention

### Claim Modeling
- Every claim has `confidence` (0.0–1.0) — low-confidence claims surfaced with warnings
- `negation` flag prevents "X treats Y" when comment says "X didn't work for Y"
- `hedging` flag flags uncertain claims ("might help", "seems to")
- Never let community claims be represented as clinical evidence

### Evidence Grading
- `anecdotal` — community data only (no external evidence)
- `limited` — case reports, weak observational
- `mixed` — conflicting studies
- `moderate` — RCTs or strong observational
- `strong` — multiple RCTs + meta-analyses

### UI Contract (API-level)
Every API response includes `safety` struct:
```json
{
  "evidence_level": "anecdotal",
  "disclaimer": "This is not medical advice. Consult a healthcare professional before using herbal remedies.",
  "contraindications": [...],
  "interactions": [...]
}
```

### Content Rules
- Never suggest a remedy can cure, treat, or prevent a disease
- Always use "people reported", "community experience suggests", "some users found"
- Flag claims with high severity conditions (cardiovascular, pregnancy, etc.)
- Safety disclaimers are non-negotiable on any health-related endpoint

## Limitations Disclosure

- Community experiences ≠ clinical evidence
- NLP extraction has known failure modes:
  - Entity resolution errors ("magnesium" form not specified)
  - False positives (sarcasm, jokes not filtered)
  - Cultural context may be lost
- All claims include confidence score to signal uncertainty

## Data Governance

### Access Control
- API server requires no authentication for read (MVP)
- Write operations (testimonial submission) require authentication (future)
- Database access restricted to application layer

### Monitoring
- Track claim extraction volume per subreddit
- Flag sudden spikes in claim creation (possible spam/bot activity)
- Log NLP confidence distribution to detect model degradation

## Licensing

- Reddit data: subject to Reddit's API terms and CC BY-SA 4.0 license on content
- Derived data (structured claims): MIT license
- Evidence items: public domain (PubMed/open access publications)

## GDPR / CCPA Considerations

- Usernames anonymized (hash) — not PII
- Source content can be deleted on request (via takedown process)
- No cross-service tracking
- No profiling or automated decision-making on user data

## Review Cycle

This document reviewed:
- Before any new data source is added
- Before any NLP pipeline change that affects claim extraction
- Annually for compliance updates