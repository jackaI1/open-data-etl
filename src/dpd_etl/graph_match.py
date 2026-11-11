"""Graph neural network for duplicate-pair scoring, compared with the rule matcher.

The graph has three kinds of node: products, ingredients and brand families (the first
word of a brand name, such as "APO" or "PMS"). Product nodes link to their ingredients
and brand family, so products that share structure pass information to each other.
Two rounds of message passing turn each product into an embedding, and a small head
scores each candidate pair from the two embeddings.

Candidate pairs come from the existing blocking step in `detection.link_products`. The
model is trained only on pairs from training entities, and it is judged only on pairs
from held-out entities. Both matchers see the same candidates, and both miss any true
pair that blocking never proposed.
"""

import argparse
import json
import math
import re
import zlib
from dataclasses import dataclass, replace
from pathlib import Path

import torch
from torch import nn
from torch.nn import functional as F

from .detection import link_products, product_context
from .evaluate import metrics
from .transform import get_spark

FEATURE_DIM = 128
HIDDEN = 32
LAYERS = 2
EPOCHS = 300
LEARNING_RATE = 0.01
WEIGHT_DECAY = 1e-4
THRESHOLD = 0.5
SEED = 20261111
_EMPTY = torch.zeros(0, dtype=torch.long)

NODE_TYPES = ("product", "ingredient", "brand")
# (source type, relation name, destination type); every relation has a reverse edge.
RELATIONS = (
    ("product", "has_ingredient", "ingredient"),
    ("ingredient", "ingredient_of", "product"),
    ("product", "has_brand", "brand"),
    ("brand", "brand_of", "product"),
)


def trigram_vector(text: str | None, dim: int = FEATURE_DIM) -> list[float]:
    """Hash the letter trigrams of a name into a unit-length vector.

    `zlib.crc32` is used instead of `hash()`, so the same name gives the same vector in
    every process. Blank text gives a zero vector.
    """
    cleaned = " ".join(re.sub(r"[^A-Z0-9]+", " ", (text or "").upper()).split())
    vector = [0.0] * dim
    if not cleaned:
        return vector
    padded = f"  {cleaned} "
    for start in range(len(padded) - 2):
        vector[zlib.crc32(padded[start : start + 3].encode()) % dim] += 1.0
    norm = math.sqrt(sum(value * value for value in vector))
    return [value / norm for value in vector] if norm else vector


def brand_family(brand_name: str | None) -> str | None:
    """The first letter-and-digit word of a brand, such as "APO" from "APO-ENALAPRIL"."""
    for token in re.split(r"[^A-Z0-9]+", (brand_name or "").upper()):
        if token:
            return token
    return None


@dataclass(frozen=True)
class HeteroGraph:
    """Node features and directed edges for one catalogue snapshot."""

    drug_codes: tuple[int, ...]
    index: dict[int, int]
    features: dict[str, torch.Tensor]
    edges: dict[tuple[str, str, str], tuple[torch.Tensor, torch.Tensor]]

    def in_dims(self) -> dict[str, int]:
        return {node_type: x.shape[1] for node_type, x in self.features.items()}


def build_graph(products: list[dict], ingredients: list[dict]) -> HeteroGraph:
    """Build the product, ingredient and brand graph.

    `products` needs `drug_code`, `brand_name` and `company_name`. `ingredients` needs
    `drug_code` and `ingredient`. Each product appears once; a repeated drug code, or an
    ingredient for an unknown product, raises ValueError rather than guessing.
    """
    by_code: dict[int, dict] = {}
    for product in products:
        code = int(product["drug_code"])
        if code in by_code:
            raise ValueError(f"drug_code {code} appears more than once")
        by_code[code] = product
    codes = tuple(sorted(by_code))
    index = {code: position for position, code in enumerate(codes)}

    ingredient_names = sorted({str(row["ingredient"]).strip().upper() for row in ingredients})
    ingredient_index = {name: position for position, name in enumerate(ingredient_names)}
    families = sorted({brand_family(p["brand_name"]) for p in products} - {None})
    family_index = {name: position for position, name in enumerate(families)}

    product_features = [
        trigram_vector(by_code[code]["brand_name"]) + trigram_vector(by_code[code]["company_name"])
        for code in codes
    ]
    features = {
        "product": torch.tensor(product_features, dtype=torch.float32),
        "ingredient": torch.tensor(
            [trigram_vector(name) for name in ingredient_names], dtype=torch.float32
        ).reshape(len(ingredient_names), FEATURE_DIM),
        "brand": torch.tensor(
            [trigram_vector(name) for name in families], dtype=torch.float32
        ).reshape(len(families), FEATURE_DIM),
    }

    product_ingredient = set()
    for row in ingredients:
        code = int(row["drug_code"])
        if code not in index:
            raise ValueError(f"ingredient row refers to unknown drug_code {code}")
        product_ingredient.add(
            (index[code], ingredient_index[str(row["ingredient"]).strip().upper()])
        )
    product_brand = {
        (index[code], family_index[brand_family(by_code[code]["brand_name"])])
        for code in codes
        if brand_family(by_code[code]["brand_name"]) is not None
    }

    def pairs_tensor(pairs):
        ordered = sorted(pairs)
        src = torch.tensor([a for a, _ in ordered], dtype=torch.long)
        dst = torch.tensor([b for _, b in ordered], dtype=torch.long)
        return src, dst

    p_i = pairs_tensor(product_ingredient)
    p_b = pairs_tensor(product_brand)
    edges = {
        ("product", "has_ingredient", "ingredient"): p_i,
        ("ingredient", "ingredient_of", "product"): (p_i[1], p_i[0]),
        ("product", "has_brand", "brand"): p_b,
        ("brand", "brand_of", "product"): (p_b[1], p_b[0]),
    }
    return HeteroGraph(codes, index, features, edges)


class RelationalLayer(nn.Module):
    """One round of message passing: each node averages its neighbours' messages."""

    def __init__(self, dim: int):
        super().__init__()
        self.self_linear = nn.ModuleDict({t: nn.Linear(dim, dim) for t in NODE_TYPES})
        self.messages = nn.ModuleList([nn.Linear(dim, dim, bias=False) for _ in RELATIONS])

    def forward(self, h: dict[str, torch.Tensor], edges) -> dict[str, torch.Tensor]:
        out = {t: self.self_linear[t](x) for t, x in h.items()}
        for linear, relation in zip(self.messages, RELATIONS):
            src_type, _, dst_type = relation
            src, dst = edges[relation]
            if src.numel() == 0:
                continue
            sent = linear(h[src_type])[src]
            total = torch.zeros_like(out[dst_type]).index_add(0, dst, sent)
            counts = torch.zeros(out[dst_type].shape[0], 1).index_add(
                0, dst, torch.ones(dst.shape[0], 1)
            )
            out[dst_type] = out[dst_type] + total / counts.clamp(min=1.0)
        return {t: F.relu(x) for t, x in out.items()}


class GraphPairMatcher(nn.Module):
    """Two rounds of relational message passing, then a head that scores candidate pairs."""

    def __init__(self, in_dims: dict[str, int], hidden: int = HIDDEN, layers: int = LAYERS):
        super().__init__()
        self.inputs = nn.ModuleDict({t: nn.Linear(d, hidden) for t, d in in_dims.items()})
        self.layers = nn.ModuleList([RelationalLayer(hidden) for _ in range(layers)])
        self.head = nn.Sequential(nn.Linear(2 * hidden, hidden), nn.ReLU(), nn.Linear(hidden, 1))

    def product_embeddings(self, graph: HeteroGraph) -> torch.Tensor:
        h = {t: F.relu(self.inputs[t](x)) for t, x in graph.features.items()}
        for layer in self.layers:
            h = layer(h, graph.edges)
        return h["product"]

    def forward(self, graph: HeteroGraph, left: torch.Tensor, right: torch.Tensor):
        embeddings = self.product_embeddings(graph)
        a, b = embeddings[left], embeddings[right]
        # Symmetric pair features: the score does not depend on which product comes first.
        return self.head(torch.cat([a * b, (a - b).abs()], dim=1)).squeeze(1)


def _pair_index(graph: HeteroGraph, pairs):
    left = torch.tensor([graph.index[a] for a, _ in pairs], dtype=torch.long)
    right = torch.tensor([graph.index[b] for _, b in pairs], dtype=torch.long)
    return left, right


def train_matcher(
    graph: HeteroGraph,
    pairs: list[tuple[int, int]],
    labels: list[bool],
    *,
    seed: int = SEED,
    epochs: int = EPOCHS,
    learning_rate: float = LEARNING_RATE,
) -> tuple[GraphPairMatcher, list[float]]:
    """Train on labelled pairs with full-batch Adam; the same seed gives the same model."""
    if len(pairs) != len(labels) or not pairs:
        raise ValueError("pairs and labels must be non-empty and the same length")
    positives = sum(labels)
    if positives == 0 or positives == len(labels):
        raise ValueError("training needs both duplicate and non-duplicate pairs")
    torch.manual_seed(seed)
    model = GraphPairMatcher(graph.in_dims())
    left, right = _pair_index(graph, pairs)
    target = torch.tensor(labels, dtype=torch.float32)
    loss_fn = nn.BCEWithLogitsLoss(pos_weight=torch.tensor((len(labels) - positives) / positives))
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate, weight_decay=WEIGHT_DECAY)
    history = []
    model.train()
    for _ in range(epochs):
        optimizer.zero_grad()
        loss = loss_fn(model(graph, left, right), target)
        loss.backward()
        optimizer.step()
        history.append(float(loss))
    model.eval()
    return model, history


def score_pairs(model: GraphPairMatcher, graph: HeteroGraph, pairs) -> list[float]:
    """Probability that each candidate pair is a duplicate."""
    if not pairs:
        return []
    left, right = _pair_index(graph, pairs)
    with torch.no_grad():
        return torch.sigmoid(model(graph, left, right)).tolist()


# --- Comparison with the Spark rule matcher -------------------------------------------


def _spark_frames(spark, products: list[dict], ingredients: list[dict]):
    product_rows = [
        {k: p[k] for k in ["drug_code", "brand_name", "din", "company_name", "drug_class"]}
        for p in products
    ]
    products_df = spark.createDataFrame(
        product_rows,
        "drug_code int, brand_name string, din string, company_name string, drug_class string",
    )
    ingredients_df = spark.createDataFrame(
        [(r["drug_code"], r["ingredient"], r["strength"], r["strength_unit"]) for r in ingredients],
        "drug_code int, ingredient string, strength string, strength_unit string",
    )
    forms_df = spark.createDataFrame(
        [(p["drug_code"], p["form"]) for p in products],
        "drug_code int, pharm_form_code string",
    )
    routes_df = spark.createDataFrame(
        [(p["drug_code"], p["route"]) for p in products], "drug_code int, route_code string"
    )
    return products_df, ingredients_df, forms_df, routes_df


def rule_candidates(spark, products: list[dict], ingredients: list[dict]):
    """Run the production blocking and Levenshtein rule on a set of products."""
    frames = _spark_frames(spark, products, ingredients)
    context = product_context(*frames[1:])
    links, _ = link_products(frames[0], context)
    return {(r.left_id, r.right_id): r for r in links.collect()}


def _pair_key(a: int, b: int) -> tuple[int, int]:
    return (a, b) if a < b else (b, a)


def compare(spark, challenge: dict, *, seed: int = SEED, epochs: int = EPOCHS) -> dict:
    """Train on training entities, then score held-out candidates with both matchers."""
    products = challenge["products"]
    ingredients = challenge["ingredients"]
    by_code = {p["drug_code"]: p for p in products}
    train = [p for p in products if p["split"] == "train"]
    test = [p for p in products if p["split"] == "test"]
    held_codes = {p["drug_code"] for p in test}
    held_ingredients = [r for r in ingredients if r["drug_code"] in held_codes]
    train_ingredients = [r for r in ingredients if r["drug_code"] not in held_codes]

    graph = build_graph(products, ingredients)
    # Ablation: the same network with every link removed, so it sees only each product's names.
    no_links = replace(graph, edges={k: (_EMPTY, _EMPTY) for k in graph.edges})
    train_links = rule_candidates(spark, train, train_ingredients)
    train_pairs = [_pair_key(a, b) for a, b in train_links]
    train_labels = [by_code[a]["entity"] == by_code[b]["entity"] for a, b in train_pairs]
    model, history = train_matcher(graph, train_pairs, train_labels, seed=seed, epochs=epochs)
    isolated, _ = train_matcher(no_links, train_pairs, train_labels, seed=seed, epochs=epochs)

    test_links = rule_candidates(spark, test, held_ingredients)
    candidates = sorted(_pair_key(a, b) for a, b in test_links)
    universe = {
        _pair_key(a["drug_code"], b["drug_code"]) for i, a in enumerate(test) for b in test[i + 1 :]
    }
    truth = {pair for pair in universe if by_code[pair[0]]["entity"] == by_code[pair[1]]["entity"]}
    rule_predicted = {_pair_key(a, b) for (a, b), row in test_links.items() if row.is_match}
    scores = dict(zip(candidates, score_pairs(model, graph, candidates)))
    isolated_scores = dict(zip(candidates, score_pairs(isolated, no_links, candidates)))
    graph_predicted = {pair for pair, score in scores.items() if score >= THRESHOLD}
    isolated_predicted = {pair for pair, s in isolated_scores.items() if s >= THRESHOLD}
    candidate_set = set(candidates)

    def describe(pair, reason):
        left, right = pair
        copy = by_code[right] if by_code[right]["corruption"] else by_code[left]
        return {
            "pair": list(pair),
            "brands": [by_code[left]["brand_name"], by_code[right]["brand_name"]],
            "change": copy["corruption"],
            "reason": reason,
        }

    rule_scores = {_pair_key(a, b): row for (a, b), row in test_links.items()}

    def describe_rule(pair):
        row = rule_scores[pair]
        return f"rule {row.reason} (score {row.score:.3f})"

    def blocked_or(reason_of):
        return lambda pair: (
            "not proposed by blocking" if pair not in candidate_set else reason_of(pair)
        )

    def scored(predicted, miss_reason, false_positive_reason):
        result = metrics(truth, predicted, universe)
        result["misses"] = [describe(pair, miss_reason(pair)) for pair in sorted(truth - predicted)]
        result["false_positives"] = [
            describe(pair, false_positive_reason(pair)) for pair in sorted(predicted - truth)
        ]
        return result

    def graph_reason(score_map):
        return lambda pair: f"graph score {score_map[pair]:.3f}"

    rule = scored(
        rule_predicted,
        blocked_or(describe_rule),
        describe_rule,
    )
    graph_result = scored(
        graph_predicted,
        blocked_or(lambda p: f"graph score {scores[p]:.3f} below {THRESHOLD}"),
        graph_reason(scores),
    )
    isolated_result = scored(
        isolated_predicted,
        blocked_or(lambda p: f"graph score {isolated_scores[p]:.3f} below {THRESHOLD}"),
        graph_reason(isolated_scores),
    )
    return {
        "scope": (
            "Synthetic challenge built from the public 60-product sample, scored on held-out "
            "entities. It shows how each matcher handles known kinds of change; it is not an "
            "estimate of duplicates in the live catalogue."
        ),
        "seed": seed,
        "epochs": epochs,
        "threshold": THRESHOLD,
        "held_out": {
            "products": len(test),
            "all_pairs": len(universe),
            "true_pairs": len(truth),
            "candidate_pairs": len(candidates),
            "blocking_recall": len(truth & candidate_set) / len(truth) if truth else None,
        },
        "training": {
            "candidate_pairs": len(train_pairs),
            "positive_pairs": sum(train_labels),
            "first_loss": history[0],
            "final_loss": history[-1],
        },
        "rule": rule,
        "graph": graph_result,
        "graph_without_links": isolated_result,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--challenge", type=Path, default=Path("tests/fixtures/graph_challenge.json")
    )
    parser.add_argument(
        "--output", type=Path, default=Path("data/reports/graph_match_evaluation.json")
    )
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--epochs", type=int, default=EPOCHS)
    args = parser.parse_args()
    challenge = json.loads(args.challenge.read_text())
    spark = get_spark("dpd-graph-match")
    try:
        report = compare(spark, challenge, seed=args.seed, epochs=args.epochs)
    finally:
        spark.stop()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(
        json.dumps(
            {k: report[k] for k in ["held_out", "rule", "graph", "graph_without_links"]}, indent=2
        )
    )


if __name__ == "__main__":
    main()
