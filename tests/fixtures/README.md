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
