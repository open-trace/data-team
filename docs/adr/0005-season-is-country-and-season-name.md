---
status: accepted
---

# A season is country and season name from dim_season

"This season" is the season for that country whose months contain the reference date. If none contain it, it is the season that ended most recently. The calendar is `dim_season` (country × season_name, start and end months). The last six months are used only when that country has no rows in `dim_season`, or when the question names no country.

The alternative was a hand-maintained seed CSV with a handful of seasons. That covered only a few countries and would drift from the Mart. Another alternative was one row per country; a country has several seasons, so a country-only row cannot say which season the question asked for.
