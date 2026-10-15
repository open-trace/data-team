# OpenTrace data-team

The language for questions about African agriculture: what was asked, where it is about, what was bought, and which evidence an answer may use.

## Language

### Question and place

**Question**:
The words of this turn.
_Avoid_: query, prompt

**Place**:
An African country or a named African region.
_Avoid_: country, geo, locale

**Question place**:
A Place named in the Question. A hard constraint on which evidence may be used.
_Avoid_: geo override, profile country

**Home place**:
The African country a person says they work in. A default only when the Question names no Place.
_Avoid_: profile country, Google locale

**Recency window**:
The calendar span a present-tense question asks for. Prices, news, and weather ask for the last 90 days. An annual subject asks for the current calendar year.
_Avoid_: open-ended time, the newest evidence that happens to exist

**Report year**:
The year of the newest report actually cited. The answer names it when the question asked for a report, and when it is earlier than the Recency window.
_Avoid_: treating the report year as the year that was asked for

**Forecast window**:
A calendar span that has not happened yet. Older observations may be cited, and the answer names them as older.
_Avoid_: treating those observations as the forecast

**Season window**:
The season for a country whose months contain the reference date. If none do, the season that ended most recently. A country with no seasons has no Season window.
_Avoid_: one season per country, the last six months as a season

**Compared windows**:
Two or more calendar spans a question sets side by side. Each span is its own window. "In 2024 and last month" is the whole of 2024 and last month, not one span covering both.
_Avoid_: merging them into a single range, dropping the relative span because a year was also named

**Named year**:
A calendar year written in the question. When the question is not comparing periods, the Named year is the only window.
_Avoid_: also applying a relative phrase in that question

**Screen dates**:
The exact dates the client sends with the question. They are the window only when the Question names no time. They are not parsed as a relative phrase, and they do not change a window the Question already resolved.
_Avoid_: time override

**Reference date**:
The UTC calendar date of the request. Relative phrases are computed from it.
_Avoid_: the machine clock, a date rewritten by phrase parsing

**Gap**:
No evidence for the Question. A Gap names the Question place. It names a filter only when one was actually applied.
_Avoid_: treating a dropped filter as the Question

### Who the answer is for

**Plan**:
What was bought: depth, exports, and answer length.
_Avoid_: using Plan to mean Persona, Place, Generation plan, or Bind plan

**Persona**:
The voice of the answer.
_Avoid_: stakeholder type as a second name for the same list

**Generation plan**:
The shape of this answer.
_Avoid_: Plan

**Bind plan**:
Which mart facts this Question may use.
_Avoid_: Plan, plan type

### Evidence

**Document evidence**:
Text from the corpora that bears on the Question.
_Avoid_: treating documents as the whole answer

**Fact evidence**:
Rows from the Mart that bear on the Question.
_Avoid_: bronze, raw, staging

**Mart**:
The dataset answers read.
_Avoid_: gold as a second name, bronze as the contract for answers

**Raw**:
Immutable landed source. Not a query target for answers.
_Avoid_: bronze as a synonym for the Mart

**Indicator class**:
The shared subject of a fact, such as production or climate. Production quantity and yield are different classes.
_Avoid_: mixing classes in one figure

**Source**:
Which dataset a fact came from. Facts from different Sources do not blend.
_Avoid_: treating every row in a class as one series

**ACF**:
The claim fields carried on a fact, and the score of evidence that was actually cited. Those are not the same thing.
_Avoid_: reading a Gap as a warehouse failure
