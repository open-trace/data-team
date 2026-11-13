---
status: accepted
---

# The reference date is the request's UTC day

Relative phrases are computed from the UTC calendar date of that request. The resolver does not read the machine clock. The production API passes the timestamp of the request. Streamlit passes the timestamp of that run. Tests pass a fixed date so CI does not read the clock.

Screen dates are the exact dates the client sent. They are applied only when the question names no time, and they are not parsed as relative phrases.

The alternative was to call the clock inside the resolver and to run screen dates through the same phrase parser. "Last month" would then depend on whichever machine handled the request, and an explicit screen range could be snapped to a month or a season.
