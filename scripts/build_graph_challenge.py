"""Build the labelled duplicate challenge used to compare the graph matcher with the rule matcher.

Each product in the public 60-product sample gets two seeded companions:

- a copy with a realistic change (a swapped brand letter, reordered brand words, a
  missing DIN, or a company suffix dropped or added). The original and its copy are a
  true duplicate pair.
- a sibling from the same company with the same ingredients, form and route, a brand
  with " 2" added and a different DIN. It is a hard negative: blocking proposes it, and
  only the DIN tells it apart from the original.

Each original group is then split into training and held-out groups, so no held-out
product is ever seen as a training label.

The output is synthetic: it tests how each matcher handles known kinds of change,
and it says nothing about how often the live catalogue contains duplicates.
"""

import argparse
import csv
import json
import random
from pathlib import Path

import duckdb

SEED = 20261111
TRAIN_ENTITIES = 40
COPY_OFFSET = 1_000_000
SIBLING_OFFSET = 2_000_000
SUFFIXES = (" INC", " INC.", " LIMITED", " LTD", " CORP", " CORPORATION")
KINDS = ("typo", "word_order", "missing_din", "company_suffix")


def _first_form_and_route(raw_dir: Path) -> tuple[dict, dict]:
    forms, routes = {}, {}
    with (raw_dir / "form.txt").open(newline="", encoding="utf-8") as handle:
        for row in csv.reader(handle):
            forms.setdefault(int(row[0]), row[2])
    with (raw_dir / "route.txt").open(newline="", encoding="utf-8") as handle:
        for row in csv.reader(handle):
            routes.setdefault(int(row[0]), row[2])
    return forms, routes


def _typo(brand: str, rng: random.Random) -> str | None:
    if len(brand) < 6:
        return None
    i = rng.randrange(3, len(brand) - 1)
    chars = list(brand)
    chars[i], chars[i + 1] = chars[i + 1], chars[i]
    return "".join(chars)


def _word_order(brand: str) -> str | None:
    words = brand.split(" ")
    if len(words) < 2:
        return None
    return " ".join([words[-1], *words[:-1]])


def _company_suffix(company: str) -> str:
    for suffix in SUFFIXES:
        if company.endswith(suffix):
            return company[: -len(suffix)]
    return company + " INC"


def corrupt(product: dict, rng: random.Random) -> tuple[dict, str]:
    """Return a changed copy and the kind of change, falling back to a missing DIN."""
    copy = dict(product)
    kind = rng.choice(KINDS)
    if kind == "typo":
        changed = _typo(product["brand_name"], rng)
        if changed is None:
            kind = "missing_din"
        else:
            copy["brand_name"] = changed
    if kind == "word_order":
        changed = _word_order(product["brand_name"])
        if changed is None:
            kind = "missing_din"
        else:
            copy["brand_name"] = changed
    if kind == "missing_din":
        copy["din"] = None
    if kind == "company_suffix":
        copy["company_name"] = _company_suffix(product["company_name"])
    return copy, kind


def build(warehouse: Path, raw_dir: Path, seed: int = SEED) -> dict:
    rng = random.Random(seed)
    forms, routes = _first_form_and_route(raw_dir)
    with duckdb.connect(str(warehouse), read_only=True) as db:
        latest = db.execute("select max(snapshot_date) from drug_products").fetchone()[0]
        base = db.execute(
            """select drug_code, brand_name, din, company_name, drug_class
               from drug_products where snapshot_date = ? order by drug_code""",
            [latest],
        ).fetchall()
        ingredient_rows = db.execute(
            """select drug_code, ingredient, strength, strength_unit
               from drug_ingredients where snapshot_date = ? order by drug_code, ingredient""",
            [latest],
        ).fetchall()
    originals = []
    for code, brand, din, company, drug_class in base:
        originals.append(
            {
                "drug_code": code,
                "brand_name": brand,
                "din": din,
                "company_name": company,
                "drug_class": drug_class,
                "form": forms.get(code),
                "route": routes.get(code),
                "entity": f"p{code}",
                "corruption": None,
            }
        )
    copies = []
    for product in originals:
        copy, kind = corrupt(product, rng)
        copy.update(
            drug_code=product["drug_code"] + COPY_OFFSET,
            corruption=kind,
        )
        copies.append(copy)
    used_dins = {p["din"] for p in originals if p["din"]}
    siblings = []
    for product in originals:
        din = f"{rng.randrange(10**7, 10**8):08d}"
        while din in used_dins:
            din = f"{rng.randrange(10**7, 10**8):08d}"
        used_dins.add(din)
        siblings.append(
            {
                **product,
                "drug_code": product["drug_code"] + SIBLING_OFFSET,
                "brand_name": product["brand_name"] + " 2",
                "din": din,
                "entity": f"s{product['drug_code']}",
                "corruption": "sibling",
            }
        )
    products = originals + copies + siblings
    groups = sorted({p["drug_code"] % COPY_OFFSET for p in originals})
    rng.shuffle(groups)
    split = {code: ("train" if i < TRAIN_ENTITIES else "test") for i, code in enumerate(groups)}
    for product in products:
        product["split"] = split[product["drug_code"] % COPY_OFFSET]
    ingredients = []
    for code, ingredient, strength, unit in ingredient_rows:
        for drug_code in (code, code + COPY_OFFSET, code + SIBLING_OFFSET):
            ingredients.append(
                {
                    "drug_code": drug_code,
                    "ingredient": ingredient,
                    "strength": strength,
                    "strength_unit": unit,
                }
            )
    return {
        "description": "Synthetic duplicate challenge built from the public 60-product sample",
        "seed": seed,
        "source_snapshot": str(latest),
        "products": products,
        "ingredients": ingredients,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", type=Path, default=Path("data/fixture/warehouse/dpd.duckdb"))
    parser.add_argument("--raw-dir", type=Path, default=Path("tests/fixtures/raw"))
    parser.add_argument("--output", type=Path, default=Path("tests/fixtures/graph_challenge.json"))
    args = parser.parse_args()
    challenge = build(args.warehouse, args.raw_dir)
    args.output.write_text(json.dumps(challenge, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {len(challenge['products'])} products to {args.output}")


if __name__ == "__main__":
    main()
