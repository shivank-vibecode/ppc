# Daily Paid UA Brief — [DATE]

## TL;DR
- [Most critical finding about DAU impact — with confidence level]
- [Key source/campaign insight — with numbers]
- [Recommended immediate action, or "need X data before recommending action"]

## What the Data Shows

Present findings from available data. Follow the signal — lead with whatever the data is screaming about today. Do not force every section below; use whichever are relevant.

Each finding must include: the observation (with numbers), what it means for DAU, and a confidence tag (HIGH / MEDIUM / LOW).

### [Headline Finding 1]
[Observation with numbers. Interpretation. Confidence level. If MEDIUM/LOW, note what could change the conclusion.]

### [Headline Finding 2]
[...]

Use tables, comparisons, or whatever format best communicates the insight. Examples of useful formats (use when appropriate, skip when not):

**Funnel snapshot** (when funnel health is the story):
| Metric | Today | 7d Avg | Delta | Confidence |
|--------|-------|--------|-------|------------|

**Source comparison** (when source quality divergence is the story):
| Source | Installs | FTT Rate | Funnel Depth | Verdict |
|--------|----------|----------|--------------|---------|

**Campaign outliers** (when specific campaigns are the story):
| Campaign | Source | Installs | FTT Rate | Issue |
|----------|--------|----------|----------|-------|

## What Would Sharpen This

For every finding where confidence is below HIGH, or where you hit "I know what but not why":

### [Finding reference] — currently [MEDIUM/LOW] confidence
- **Data needed**: [Specific data — columns, time range, format]
- **Where to get it**: [System/tool to export from]
- **What it unlocks**: [The specific question this data answers]
- **Impact on analysis**: [How confidence level would change, or what new analysis becomes possible]

## Recommended Actions

Only include actions backed by HIGH-confidence findings. For MEDIUM-confidence findings, frame as conditional: "If [data confirms X], then [action]."

For LOW-confidence findings, do not recommend action — instead, recommend gathering the data needed to raise confidence.

**Every action must include a Status and Previous Brief reference** to create continuity between briefs. This is the feedback loop — the user marks items done/blocked/skipped in the dashboard checklist, and those updates flow into the next brief.

| # | Status | Action | Basis | Confidence | vs Previous Brief |
|---|--------|--------|-------|------------|-------------------|
| 1 | NEW | [Action that was not in the prior brief] | [Evidence] | HIGH | First appearance |
| 2 | DONE | [Action completed by the user] | [Evidence it was done] | — | Was #N in [date] brief. User comment: "..." |
| 3 | ESCALATED | [Action from prior brief that was not acted on and is getting worse] | [Updated evidence] | HIGH | Was #N in [date] brief — Day N of recommending. [Cumulative impact since first recommendation] |
| 4 | CARRIED | [Action from prior brief, unchanged status] | [Same or updated evidence] | HIGH | Was #N in [date] brief |
| 5 | DROPPED | [Action from prior brief that is no longer relevant] | [Why it's no longer needed] | — | Was #N in [date] brief. Dropped because [reason] |

Status values:
- **NEW** — First time this action appears
- **DONE** — User marked it done in feedback.json or data confirms it was implemented
- **ESCALATED** — Carried forward from prior brief AND the underlying problem got worse
- **CARRIED** — Carried forward from prior brief, no material change
- **DROPPED** — Was in prior brief but no longer relevant (data changed, or user marked "skipped")
- **BLOCKED** — User marked it blocked in feedback.json — include their comment explaining why
