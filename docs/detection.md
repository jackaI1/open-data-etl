# Duplicate review and unusual ingredient strengths

I use these reports to inspect repeated product entries and odd-looking strength
values before counting or comparing catalogue records. Neither report removes
records or decides whether a medicine is safe. A rare strength can be legitimate.

## Run and inspect

```sh
make fixture SNAPSHOT_DATE=2026-11-01
make evaluate
```

The first command runs both stages on the public-data sample and loads six dated
tables into `data/fixture/warehouse/dpd.duckdb`. The unchanged sample has no
cross-key duplicate candidates, so an empty `product_links` table is expected.
`make pipeline` runs exactly the same stages on the downloaded catalogue.
The second command runs a separate, deliberately difficult labelled challenge
and writes `data/reports/detection_evaluation.json`.

```python
import duckdb

with duckdb.connect("data/fixture/warehouse/dpd.duckdb", read_only=True) as db:
    print(db.sql("""
        SELECT l.left_id, l.right_id, l.score, l.reason,
               a.brand_name, b.brand_name
        FROM product_links l
        JOIN drug_products a ON a.drug_code = l.left_id
                            AND a.snapshot_date = l.snapshot_date
        JOIN drug_products b ON b.drug_code = l.right_id
                            AND b.snapshot_date = l.snapshot_date
        WHERE l.is_match
        ORDER BY l.snapshot_date, l.left_id, l.right_id
    """).fetchall())
    print(db.sql("""
        SELECT drug_code, ingredient, strength, strength_unit,
               peer_count, peer_median, peer_mad, robust_z
        FROM strength_anomalies WHERE is_anomaly
        ORDER BY snapshot_date, abs(robust_z) DESC
    """).fetchall())
```

The `review_YYYY-MM-DD.json` report counts rows and flags for each new table.
It does not invent precision or recall for unlabelled live data. `linkage_blocks`
records block sizes and whether they were skipped, including singleton blocks.
Filter `skipped` to find gaps caused by the comparison budget.

## Downloaded-catalogue check

I also ran the full pipeline on 2026-11-04 using the marketed archive whose
checksum is recorded in [the public fixture notes](../tests/fixtures/README.md).
It retained all 13,379 products and the existing 46,045 status-history events.
The new stage compared 2,710 candidate pairs and accepted none: every pair had
conflicting valid DINs. None of the 25,353 blocks exceeded the comparison cap.
This does not establish that the catalogue contains no duplicates.

Of 19,403 ingredient-strength rows, 11,131 received scores and 637 were flagged
for review. Another 4,954 had too few peers, 3,316 had zero MAD and two contained
unsupported strength text. These are unlabelled review counts, not confirmed
errors or accuracy measurements. I can reproduce the run with `make pipeline`
and inspect the `review_YYYY-MM-DD.json` report and tables.

## How duplicate linkage works

I preserve the existing newest-row selection for each `drug_code`. The new stage
looks for duplicate products under *different* keys, which that selection cannot
find. It returns links between pairs, rather than collapsing connected groups:
a fuzzy A-to-B link and B-to-C link do not prove A and C are identical.

1. I build a complete product context from sorted ingredient/strength/unit sets,
   dosage form codes and route codes. Missing fields prevent fuzzy linkage.
   Class (human, veterinary and so on) is also part of the comparison key.
   Ingredient strength text must agree exactly, so `200` and `200.0` currently
   stay separate. This favours missed links over mixing different formulations.
2. **Blocking**, grouping likely candidates before comparing them, uses two keys:
   a valid eight-digit DIN (Drug Identification Number), or the complete context
   plus the first three normalized brand characters. Two products enter the
   candidate table if they share either key. Pairs are unique and ordered by ID.
3. I normalize brand and company names to uppercase ASCII letters and digits.
   **Fuzzy matching** measures spelling similarity with Levenshtein distance,
   the number of single-character edits needed to change one name into another.
   Similarity is `1 - distance / max(lengths)`; missing names score zero.
4. Equal valid DINs produce `score=1`, `is_match=true`, `reason=exact_din` even
   if names disagree. This score expresses the rule's strength, not a calibrated
   probability. Conflicting valid DINs always reject a fuzzy match.
5. With at least one unavailable DIN, a fuzzy link requires identical complete
   context and class, `0.8 * brand_similarity + 0.2 * company_similarity >= 0.90`,
   and company similarity at least `0.85`. Every candidate keeps its score,
   component similarities, decision, DINs and explanation, including rejections.

Blocks above 200 products are skipped and reported, limiting each permitted
block to at most 19,900 unordered comparisons. This bounds skew per block; the
number of blocks still grows with the catalogue. A three-letter typo can miss a
true link. An invalid or blank DIN is unavailable, never an exact identifier.
ASCII normalization can miss accented names. I keep all source products so a
reviewer can inspect these decisions instead of trusting them as corrections.

The implementation uses Spark joins, aggregations and SQL expressions, without
collecting the live catalogue into Python or using a Python scoring function.
Empty result partitions contain typed empty Parquet files so reruns can remove
old candidates while preserving other snapshots. All six warehouse tables load
in the existing transaction; Parquet still has the existing separate-store
recovery limitation.

## How unusual strengths are flagged

I retain the original strength text and parse only positive, finite, plain decimal
numbers. Ratios such as `1:1000`, negative values, zero, missing values and numeric
overflow remain in the output with `reason=unsupported_strength`.

For each ingredient and reported unit I require at least eight distinct product
keys. I calculate the exact median and **MAD**, the median absolute distance from
that median, using Spark aggregates. The robust (modified) z-score is
`0.6745 * (value - median) / MAD`. An absolute score above `3.5` flags a potential
outlier. Unlike the mean, the median resists a few extreme values. The formula
and threshold follow [NIST's outlier detection reference](https://itl.nist.gov/div898/handbook/eda/section3/eda35h.htm);
Spark supplies [the median and string-distance functions](https://spark.apache.org/docs/3.5.8/api/sql/).

Small groups get `too_few_peers`. A zero MAD gets `zero_mad`, with no invented
score or anomaly flag. `is_evaluated=false` and a null `robust_z` distinguish
these abstentions from scored normal values. MG and MCG stay separate; there is
no implicit unit conversion. Ingredient/unit peers can include different routes,
classes and combinations, so a flag is a catalogue-review prompt, never a
clinical error judgment. The current method can miss problems in uniform or
small groups and over-flag legitimate, unusually strong formulations.

## Labelled evaluation and limitations

The reproducible challenge currently reports:

| Stage | True positives | False positives | Missed positives | Precision | Recall |
| --- | ---: | ---: | ---: | ---: | ---: |
| Duplicate links | 5 | 1 | 2 | 83.3% | 71.4% |
| Strength anomalies | 2 | 0 | 1 | 100% | 66.7% |

Blocking compares 17 of 105 possible product pairs, an 83.8% reduction. It
retains five of seven labelled duplicate pairs. The false link is a product
without a DIN that resembles two products with different DINs; the available
fields cannot reliably distinguish them. This is why I treat fuzzy links as
review suggestions. Thirteen of thirty strength rows receive a z-score. The
remaining rows have unsupported values, too few peers or zero MAD.


`tests/fixtures/detection.json` contains invented product aliases and strength
values, with no personal data. The product `entity` labels describe identity;
every unordered pair is evaluated, including pairs never generated by blocking.
The anomaly labels are separate from the strength values passed to the detector.
Thresholds are fixed constants in `detection.py`; this small regression challenge
is not a held-out real-world accuracy estimate or a trained model.

Precision is the fraction of flagged cases whose labels are positive. Recall is
the fraction of labelled positives found. The report includes true positives,
false positives, false negatives and true negatives. Unscored labelled anomalies
count as missed positives. A zero prediction count gives null precision rather
than claiming perfect performance. Blocking recall separately exposes candidate
loss before fuzzy scoring. The challenge intentionally includes missed links
and a zero-MAD outlier, so it does not report a misleading perfect recall.

Use `make evaluate` to regenerate the measured numbers. CI runs the same
production functions in pytest and the command-line evaluation, then uploads
the JSON reports. The integration tests also check retained product counts,
nonempty duplicate links, empty replacement partitions, multiple snapshots,
quality-gate failures and transaction rollback.
