# Public-data fixture

The seven UTF-8 CSV files in `raw/` are a 60-product subset of Health Canada's
**marketed** Drug Product Database extract (`allfiles.zip`), published 2026-11-02
and downloaded 2026-11-03. Related company, ingredient, status, ATC, form, and route
rows are restricted to those product keys. The files contain public drug and
company information; they contain no patient or application records.

One older duplicate product row was added deliberately (61 raw product rows,
60 unique products) to exercise deduplication. Tests copy the fixture to a
pytest temporary directory before corrupting it. Source-provided duplicate
status events are retained to exercise status deduplication.

Source: https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles.zip

Schema reference: https://www.canada.ca/en/health-canada/services/drugs-health-products/drug-products/drug-product-database/read-file-drug-product-database-data-extract.html

Health Canada's data terms: https://healthycanadians.gc.ca/important-eng.php

Source archive SHA-256: `e89dc3e303c5372b101059b273cb96230d957a37ca760440b85db6a2e3575737`.

## Detection challenge

`detection.json` is synthetic, authored for repeatable regression checks. Its 15
product rows label seven true duplicate pairs across three entities; all 105
unordered pairs are scored in evaluation, including pairs excluded by blocking.
Other rows exercise conflicting DINs, different strengths, forms and routes, and
missing or conflicting names. One ambiguous missing-DIN product creates a false
positive against a lookalike with a different known identity. The Birch spelling change deliberately crosses
the prefix block and produces two false negatives. These labels describe the
invented records, not claims about real drug equivalence.

The 30 strength rows include two extremes in a varied MG group, one labelled
outlier in a zero-MAD group, unsupported values and a separate, small MCG group.
The zero-MAD outlier is deliberately missed. The evaluation counts that miss
against recall and reports how many rows were actually scored. These examples
are a development challenge, not independent estimates of production accuracy.
