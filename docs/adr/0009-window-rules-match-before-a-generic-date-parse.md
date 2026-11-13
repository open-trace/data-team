---
status: accepted
---

# Window rules match before a generic date parse

Phrases whose window we have defined are resolved by those rules. A date parser recognizes only what those rules do not claim, and it is given the request's reference date. Screen dates are never parsed as phrases.

The alternative was a hand-written pattern for every phrasing, which misses ordinary French, Portuguese, and variant wording, or letting the parser decide "this week," "this season," and a comparison. A generic parse returns a date, and those windows are not generic dates.
