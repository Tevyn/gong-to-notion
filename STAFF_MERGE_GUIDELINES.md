# Agency Staff duplicate merge guidelines

Rules for resolving duplicate rows in the Notion **Agency Staff** database
(data source `664ccf5e-8cdf-43a4-863c-cfe8ccdef26b`). Written for agents doing
the merges one cluster at a time.

Current state of the work: 60 clusters, every one a pair, 120 rows total, out of
2,192 rows in the table. Counts quoted below are from that snapshot.

An unmerged cluster that you flag with a reason is a good outcome. A wrong merge
silently destroys a real person's call history, and the archived loser is the
only copy of it. When the evidence is thin, stop.

---

## 0. Prerequisites and hard technical constraints

**Read relation lists through the property endpoint, never from the page object.**
`GET /v1/pages/{id}` truncates relation values at 25 entries and sets
`has_more: true` on the property. One row in the current set (Randy Anderson)
reports 25 in the page object and actually has 30. Unioning truncated lists
deletes links. Always page through:

```
GET /v1/pages/{page_id}/properties/{property_id}?page_size=100
```

**`Agency` and `Customer Conversations` are `dual_property` relations.** Writing
the winner's `Customer Conversations` automatically updates each Customer
Interaction page's `Agency Staff`, and writing `Agency` updates the Agency
page's `Agency Staff`. Do not loop over call pages or agency pages. One PATCH on
the winner is the whole write.

**Every address the person is known by must end up on the winner.** The importer
matches Staff rows on the `Email` property *and* on the `Other Emails` property,
both indexed by `load_fill_caches`. An address that survives on neither will
recreate the duplicate on the next import, and for the 9 concurrent clusters
below that happens within weeks. `Other Emails` is free text, comma-separated;
write the loser's address there when it is not the one you keep as primary.

**Primary `Email` is the address on the most recent call**, per the `Last
Contacted` formula on each row or the Gong call dates. Do not rank by the page's
`Last edited`: 80 of the 120 rows in these clusters were last edited by the
importer rather than a human, so it records our writes, not which address is
current. It ranks `astanion@goriangle.org` (typo domain, 1 call) above
`astanion@gotriangle.org` (15 calls), and picks the retired `scmtd.com` over
`scmetro.org`.

Because the row matches both addresses either way, getting the primary wrong is
cosmetic and cheap to correct. Dropping an address is neither.

**Never cluster on the title.** 553 rows are titled `Unknown` after the April
seeding cleanup. Titles are not identity. Cluster on email, on Gong participant
names for the same address, and on the agency the calls belong to.

**Pace writes at roughly 3/sec.** The Notion client retries 429s, but pacing is
cheaper than backoff.

---

## 1. Winner selection, in order

1. **Page body wins.** If exactly one row has body content, that row is the
   winner regardless of link counts. Body blocks cannot be moved by the API, and
   inline databases cannot be copied at all. This applies to 5 of 60 clusters,
   and in all 5 the body holder has *fewer* call links than the other row.
2. **Most `Customer Conversations` links** (full list, not truncated).
3. Tie breakers, in order: has a non-empty `Email`; the address sits on a domain
   listed in the Agencies DB `Email Domains`; older `Created` date.

The typical cluster is one importer-created row carrying most of the call links
and one older human-created row carrying the qualitative content (`Role`,
`Notes`, sometimes a body). Whichever wins, the other side's content must
survive.

---

## 2. Field-by-field consolidation

| Property | Type | Rule | Frequency in the 60 clusters |
|---|---|---|---|
| `Name` | title | Keep a human-readable name if either row has one. Never leave `Unknown` on the winner when the loser has a real name. | 553 rows table-wide are `Unknown` |
| `Email` | email | One address present: copy up. Two present: primary is the one on the most recent call (§4); the other goes in `Other Emails`. | 38 one-sided, 20 two-address, 2 identical |
| `Other Emails` | rich_text | Union of both rows' existing values, plus whichever address did not become primary, comma-separated. Never drop an address. | empty today; the merge populates it |
| `Customer Conversations` | relation (dual) | **Union.** Read both full lists, write the union to the winner. Never replace. | 54 clusters differ, 3 one-sided, 3 identical |
| `Agency` | relation (dual) | **Union, do not choose.** A person can legitimately belong to two agencies (job change, contractor). | 17 one-sided, 33 identical, 1 conflicting |
| `Role` | rich_text | Prefer the value from the row with the most recent call activity; job titles go stale. If both are non-empty and materially different, keep the recent one and append `Previously: <old>` to `Notes`. | 28 one-sided, 11 identical, 9 conflicting |
| `Notes` | rich_text | Union. If both non-empty, concatenate with a source line per chunk. Merge breadcrumb also lands here. | 12 one-sided, 0 conflicting |
| `Department` | select | Copy up if one-sided. Conflict: keep the winner's, note the other. | empty on all 120 today |
| `Decision maker for Swiftly purchase?` | select | Copy up if one-sided. Conflict: keep the more affirmative value and note it. | empty on all 120 today |
| `Quotes about openess to future conversations` | phone_number | Mistyped in the schema, treat as free text. Copy up if one-sided, concatenate if both. | 4 one-sided |
| `Feedback`, `Changes`, `Research besties`, `Research Insights`, `Teams interacted with` | relation | Union generically. **Read these from the page object**: `GET /v1/data_sources/{id}` does not return them, so a schema-driven merge misses them. | empty on all 2,192 rows today |
| `Last Contacted`, `Swiftly Contacts` | formula | Derived. Never write. |  |
| `Created`, `Created by`, `Last edited`, `Last edited by` | system | Never write. Provenance lives in the breadcrumb. |  |

---

## 3. Page contents

- 55 of 60 clusters have no body on either side. Nothing to do.
- 5 clusters have a body on exactly one side. Make that row the winner (§1.1).
- 0 clusters have bodies on both sides today.
- 4 of the 5 bodies are the same research template: `heading_2`, two
  `paragraph`s, a `divider`, and **three `child_database` blocks**.

Rules:

- **Never try to move or copy a `child_database` block.** The API cannot, and
  attempts leave the inline database orphaned or empty.
- If both sides ever have bodies and only one has a `child_database`, that side
  wins and the other side's blocks get appended under a new
  `## Merged from duplicate` heading, with the loser's page URL beneath it.
- If both sides have bodies and both contain a `child_database`, stop and flag.
- Body blocks are only ever appended, never deleted from the winner.

---

## 4. Two live addresses: which one is primary

The default rule is the address on the **most recent call**, and the loser's
address always goes to `Other Emails`. The patterns below only change which
address to prefer when call recency is ambiguous or misleading.

| Pattern | Example from the current set | Primary |
|---|---|---|
| Typo domain | `dfranklin@cttansit.com` vs `dfranklin@cttransit.com`; `astanion@goriangle.org` vs `gotriangle.org` | The domain listed in the Agencies DB `Email Domains`, **even if the typo is more recent**. Gong will keep emitting the typo from the same recurring invite, which is exactly why it has to land in `Other Emails`. |
| Agency rebrand | `cmahood@myrts.com` → `rgrta.com`; `dtoups@scmtd.com` → `scmetro.org`; `asnyder@mtd.org` → `cumtd.com`; `myron@soltransride.com` → `solanocountytransit.gov`; `regan@watertransit.org` → `sfbayferry.com` | Most recent call. |
| Local-part variant, same org | `adrianmateos@` vs `amateos@basintransit.com`; `andrew.stclair@` vs `andrew.st.clair@capmetro.org`; `jespie@` vs `jasone@ridejaunt.org` | Most recent call. |
| Personal vs work | `zachary.agush@gmail.com` vs `zagush@ripta.com`; `keizhaasamson@gmail.com` vs `keizha@pareto.ai` | The work address, even when the free-mail one is more recent. |
| Employer change or contractor | `fbrown1@arlingtonva.us` vs `fiona.brown@transdev.com`; `anrivera@cttransit.com` vs `anthony.rivera2@ct.gov`; `keith.sanders@ratpdev.com` | Most recent call. **Union the `Agency` relation**, since both affiliations are real history. |
| Two orgs, concurrent | `pmattern@sunline.org` vs `pmattern@banningca.gov` | Do not merge on name alone. See §5. |

15 of the 20 two-address clusters span different domains, and **9 are
concurrent**: both addresses appear on calls since May 1, so neither is retired.
`dtoups@scmtd.com` and `dtoups@scmetro.org` both appear on June 29;
`asnyder@cumtd.com` on July 8 and `asnyder@mtd.org` on July 7. For these,
recency picks a coin flip that will flip again next month, and `Other Emails` is
the only thing keeping the merge from regressing.

Call activity as of 2026-07-31 (Gong, Jan 1 onward). Recompute rather than trust
this table if much time has passed:

| Cluster | Address A (calls, last) | Address B (calls, last) | Shape |
|---|---|---|---|
| `adrianmateos` | `adrianmateos@basintransit.com` (1, Jul 20) | `amateos@basintransit.com` (2, May 21) | concurrent |
| `amysnyder` | `asnyder@cumtd.com` (1, Jul 8) | `asnyder@mtd.org` (1, Jul 7) | concurrent |
| `andrewstclair` | `andrew.stclair@capmetro.org` (10, Jul 9) | `andrew.st.clair@capmetro.org` (7, Jun 25) | concurrent |
| `anthonyrivera` | `anrivera@cttransit.com` (7, Jul 30) | `anthony.rivera2@ct.gov` (2, May 28) | concurrent |
| `austinstanion` | `astanion@gotriangle.org` (15, Jul 24) | `astanion@goriangle.org` (1, May 7) | concurrent, typo |
| `colettevillarico` | `colettevillarico@gmail.com` (5, Jul 30) | `colette@pareto.ai` (20, Jun 25) | concurrent, personal vs work |
| `derektoups` | `dtoups@scmetro.org` (3, Jun 29) | `dtoups@scmtd.com` (1, Jun 29) | concurrent, same day |
| `keizhasamson` | `keizhaasamson@gmail.com` (4, Jul 30) | `keizha@pareto.ai` (11, Jun 25) | concurrent, personal vs work |
| `randyanderson` | `randerson@gocitybus.com` (28, Jul 15) | `r.anderson@gocitybus.com` (4, Jul 28) | concurrent |
| `chrismahood` | `cmahood@myrts.com` (1, Mar 25) | `cmahood@rgrta.com` (1, Mar 13) | migration |
| `dianafranklin` | `dfranklin@cttransit.com` (5, Apr 20) | `dfranklin@cttansit.com` (1, Apr 1) | migration, typo |
| `myronbanez` | `myron@solanocountytransit.gov` (1, Jul 15) | `myron@soltransride.com` (3, Apr 15) | migration |
| `zachagush` | `zagush@ripta.com` (7, Jul 29) | `zachary.agush@gmail.com` (1, Apr 30) | migration |
| `fionabrown` | `fiona.brown@transdev.com` (9, Jul 17) | `fbrown1@arlingtonva.us` (1, Mar 4) | old one quiet |
| `jeffburns` | `jburns@bcdcog.com` (17, Jul 14) | `jburns@ridecarta.com` (2, Mar 3) | old one quiet |
| `rafaelregan` | `rafael.regan@sfbayferry.com` (5, Jul 15) | `regan@watertransit.org` (1, Apr 15) | old one quiet |
| `amandasimmons` | `amanda_simmons@ncsu.edu` (2, Jun 25) | `asimmon4@ncsu.edu` (0, never) | one side absent |
| `paulmattern` | `pmattern@banningca.gov` (3, May 20) | `pmattern@sunline.org` (0, never) | one side absent, see §5 |
| `jasonespie` | `jespie@ridejaunt.org` (0, never) | `jasone@ridejaunt.org` (0, never) | no Gong activity |
| `koffikadjo` | `kkadjo@unh.edu` (0, never) | `kk1261@usnh.edu` (0, never) | no Gong activity |

The last four have no usable recency signal on at least one side. Fall back to
the pattern rules, and for `paulmattern` apply §5.

---

## 5. Do not merge: stop and flag

- Both addresses are active in the **same period** at **unrelated agencies**
  with no shared `Agency` link. Two people can share a name, and consultants
  working two agencies should stay separate if the calls are separate.
- The only thing matching the rows is a placeholder title (`Unknown`) or a bare
  first name (`Andy`, `Nick`, `Quinn`).
- Both sides have body content containing inline databases.
- You could not page a relation list to completion (`has_more` still true).
- The two rows' `Role` values describe clearly different jobs at different
  organizations *and* the call histories do not overlap in time.

Flag format: cluster key, both page URLs, the specific rule that stopped you,
and what evidence would settle it.

Start with the 3 clusters where one row has **zero** call links. Those are the
lowest-risk merges and a good pilot for review before running the rest.

---

## 6. Executing one merge

1. Read both rows fully: page object for properties, property endpoint for every
   relation, `GET /v1/blocks/{id}/children` for bodies.
2. Decide winner (§1). If a §5 rule fires, flag and stop.
3. PATCH the winner with the consolidated properties (§2).
4. Re-read the winner and verify: union sizes match, `Email` is the intended
   primary, and `Other Emails` contains every other address either row held.
5. PATCH the loser to clear its dual relations: `Customer Conversations: []`,
   `Agency: []`. This removes the stale chip from call and agency pages.
6. Archive the loser: `PATCH /v1/pages/{id}` with `{"in_trash": true}`. Never
   hard-delete; trash is recoverable.
7. Append to the winner's `Notes`:
   `Merged duplicate <loser email or "(no email)"> (<loser page url>) on <date>.`
   The address itself belongs in `Other Emails`, not here; `Notes` is the human
   breadcrumb, `Other Emails` is what the importer reads.
8. Log one TSV row: cluster key, winner URL, loser URL, primary email, other
   email, convo count before and after, body handling, flags.

Final check across the whole run: the table's row count should drop by exactly
the number of merges performed, and no Customer Interaction page should have
lost an `Agency Staff` chip.
