# Graph model for duplicate pairs

I built a graph model for duplicate product entries to test whether the links between
products, their ingredients and their brand families help more than my rule matcher,
which compares names and companies with edit distance (the number of letter changes
between two names).

This guide explains how the model works, how I tested it and what the numbers do
and do not show. The short version is in the [README](../README.md#compare-a-graph-model-with-my-duplicate-rules).

## Run it

```sh
make install                                     # installs PyTorch with the rest of the project
make fixture SNAPSHOT_DATE=2026-11-01            # the 60-product sample the challenge comes from
make graph-test                                  # graph building, seeded training, challenge rebuild
make graph-eval                                  # writes data/reports/graph_match_evaluation.json
```

`make graph-challenge` rebuilds `tests/fixtures/graph_challenge.json` from the fixture
warehouse. The committed file is checked against that rebuild in `make graph-test`.
Java 17 is needed for the rule matcher, because it runs on the Spark pipeline.

## Graph

The graph has three kinds of node:

- **products**, with the brand name and company name as features;
- **ingredients**, one node per ingredient name;
- **brand families**, the first letter-and-digit word of a brand name, such as `APO`
  from `APO-ENALAPRIL`.

Each product links to its ingredients and its brand family, and each link has a reverse
edge, so a message can travel either way. Names become features by hashing their letter
trigrams (three-letter pieces) into 128 slots. Python's `hash()` changes between runs,
so the code uses `zlib.crc32`, which gives the same slots every time.

## Model

- Two rounds of message passing. In each round every node averages the messages from
  its neighbours for each relation, adds its own value and applies a ReLU function.
- Hidden size 32. Each node type has its own input layer.
- A pair head reads `a * b` and `|a - b|` for the two product embeddings. Both are
  symmetric, so the score does not depend on the order of the pair.

Training uses full-batch Adam with learning rate 0.01 and weight decay 0.0001, for 300
epochs. The loss is binary cross-entropy weighted by negatives over positives, because
duplicate pairs are rare. The random seed is `20261111`, and a test checks that two runs
with the same seed give the same loss history and scores.

Candidate pairs come from the existing blocking step in `dpd_etl.detection.link_products`.
Both matchers see the same candidates. A true pair that blocking never proposes counts
as a miss for both.

## Challenge

`scripts/build_graph_challenge.py` creates a labelled set from the public 60-product
sample, with seed `20261111`:

- a **copy** of each product with one change: a swapped brand letter, reordered brand
  words, a missing DIN, or a company suffix dropped or added. The original and the copy
  are a true duplicate pair;
- a **sibling** from the same company, with the same ingredients, form and route, a brand
  with ` 2` added and a different DIN. Blocking proposes siblings, so they are the hard
  negatives. Only the DIN separates them from the original.

The 60 original groups are split 40 for training and 20 held out. The held-out set has
60 products, 1,770 possible pairs and 20 true pairs. The graph is built from all records,
but the model only learns from labels on training pairs, and the score is only read on
held-out pairs.

This is a synthetic test. It shows how each matcher handles changes of these known
kinds. It does not estimate how often the live catalogue contains duplicates.

## Result

Numbers from `make graph-eval` with the default seed (see the report file for the misses
and false positives of each matcher):

| Matcher | True pairs found | Flagged pairs that are correct | Missed | False flags |
| --- | --- | --- | --- | --- |
| Rule matcher (edit distance and DIN) | 20 of 20 (recall 100%) | 20 of 30 (precision 67%) | 0 | 10 |
| Graph model | 16 of 20 (recall 80%) | 16 of 21 (precision 76%) | 4 | 5 |
| Graph model without links | 19 of 20 (recall 95%) | 19 of 24 (precision 79%) | 1 | 5 |

What these numbers show:

- The rule matcher finds every held-out pair, but all 10 of its false flags have a
  missing DIN on the original product. When a DIN is missing, its fallback accepts a
  close name match from the same company, so a sibling with a different DIN gets through.
- The graph model misses 4 pairs: three where the copy lost its DIN and one typo. It
  flags 5 siblings. It never reads DINs, so it cannot use the one field that separates
  a sibling from its original.
- Removing the links made the same network slightly better on this set. On this
  challenge, the relational structure did not help. Siblings share their original's
  ingredients and brand family, so the links carry the same signal that the names already
  give. I have not tested that explanation directly.

The first training loss is 0.91 and the last is 0.00004, so the model fits the 40
training positives closely. With 20 true pairs, each miss moves recall by 5 points,
so these differences are small. I ran one seed and did not measure how much the numbers
move with other seeds.

## Limits

- The challenge is synthetic and small. It cannot say how the model would do on the
  full catalogue.
- The model is not connected to the live pipeline. It scores the held-out challenge
  only.
- DIN is not an input to the model. Adding a DIN-agreement feature is the obvious next
  test, but I have not tried it, and I did not tune on the held-out pairs.
- Training runs on CPU; the CI job installs CPU-only PyTorch to keep the download small.
