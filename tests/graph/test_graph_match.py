import dataclasses

import pytest
import torch

from dpd_etl.graph_match import (
    brand_family,
    build_graph,
    score_pairs,
    train_matcher,
    trigram_vector,
)

PRODUCTS = [
    {"drug_code": 1, "brand_name": "APO-ENALAPRIL", "company_name": "APOTEX INC"},
    {"drug_code": 2, "brand_name": "APO-ENALAPRIL 5", "company_name": "APOTEX INC"},
    {"drug_code": 3, "brand_name": "PMS-METHYLPHENIDATE", "company_name": "PHARMASCIENCE INC"},
    {"drug_code": 4, "brand_name": "APO-CAPTOPRIL", "company_name": "APOTEX INC"},
]
INGREDIENTS = [
    {"drug_code": 1, "ingredient": "ENALAPRIL"},
    {"drug_code": 1, "ingredient": "HYDROCHLOROTHIAZIDE"},
    {"drug_code": 2, "ingredient": "ENALAPRIL"},
    {"drug_code": 3, "ingredient": "METHYLPHENIDATE"},
    {"drug_code": 4, "ingredient": "CAPTOPRIL"},
]


def _edge_set(graph, relation):
    src, dst = graph.edges[relation]
    return set(zip(src.tolist(), dst.tolist()))


def test_trigram_vectors_are_stable_unit_length_and_blank_safe():
    first = trigram_vector("APO-ENALAPRIL")
    assert first == trigram_vector("apo enalapril")
    assert sum(v * v for v in first) == pytest.approx(1.0)
    assert set(trigram_vector(None)) == {0.0}
    assert brand_family("APO-ENALAPRIL 5") == "APO"
    assert brand_family("  -- ") is None


def test_graph_links_products_to_their_ingredients_and_brand_family():
    graph = build_graph(PRODUCTS, INGREDIENTS)
    assert graph.drug_codes == (1, 2, 3, 4)
    assert {k: v.shape[0] for k, v in graph.features.items()} == {
        "product": 4,
        "ingredient": 4,  # ENALAPRIL, HYDROCHLOROTHIAZIDE, METHYLPHENIDATE, CAPTOPRIL
        "brand": 2,  # APO and PMS
    }
    # Products 1, 2 and 4 share the APO brand node (index 0), so message passing links them.
    apo_members = {
        src for src, dst in _edge_set(graph, ("product", "has_brand", "brand")) if dst == 0
    }
    assert apo_members == {graph.index[1], graph.index[2], graph.index[4]}
    assert len(_edge_set(graph, ("product", "has_ingredient", "ingredient"))) == len(INGREDIENTS)


def test_reverse_edges_mirror_forward_edges():
    graph = build_graph(PRODUCTS, INGREDIENTS)
    forward = _edge_set(graph, ("product", "has_ingredient", "ingredient"))
    backward = _edge_set(graph, ("ingredient", "ingredient_of", "product"))
    assert backward == {(dst, src) for src, dst in forward}


def test_graph_rejects_ambiguous_or_unknown_records():
    with pytest.raises(ValueError, match="more than once"):
        build_graph([PRODUCTS[0], PRODUCTS[0]], [])
    with pytest.raises(ValueError, match="unknown drug_code"):
        build_graph(PRODUCTS, [{"drug_code": 99, "ingredient": "X"}])


def test_training_is_seeded_and_fits_the_labelled_pairs():
    graph = build_graph(PRODUCTS, INGREDIENTS)
    pairs = [(1, 2), (1, 3), (2, 3), (1, 4)]
    labels = [True, False, False, True]
    first, first_history = train_matcher(graph, pairs, labels, seed=7, epochs=150)
    second, second_history = train_matcher(graph, pairs, labels, seed=7, epochs=150)
    assert first_history == second_history
    assert score_pairs(first, graph, pairs) == score_pairs(second, graph, pairs)
    assert first_history[-1] < first_history[0]
    scores = score_pairs(first, graph, pairs)
    assert scores[0] > 0.5 and scores[3] > 0.5
    assert scores[1] < 0.5 and scores[2] < 0.5


def test_pair_scores_do_not_depend_on_order():
    graph = build_graph(PRODUCTS, INGREDIENTS)
    model, _ = train_matcher(graph, [(1, 2), (1, 3)], [True, False], seed=3, epochs=20)
    forward = score_pairs(model, graph, [(1, 2)])[0]
    backward = score_pairs(model, graph, [(2, 1)])[0]
    assert forward == pytest.approx(backward, abs=1e-6)


def test_training_refuses_one_class_and_empty_inputs():
    graph = build_graph(PRODUCTS, INGREDIENTS)
    with pytest.raises(ValueError, match="both duplicate and non-duplicate"):
        train_matcher(graph, [(1, 2)], [True], epochs=1)
    with pytest.raises(ValueError, match="non-empty"):
        train_matcher(graph, [], [], epochs=1)


def test_removing_links_keeps_nodes_and_features():
    graph = build_graph(PRODUCTS, INGREDIENTS)
    empty = {
        k: (torch.zeros(0, dtype=torch.long), torch.zeros(0, dtype=torch.long)) for k in graph.edges
    }
    isolated = dataclasses.replace(graph, edges=empty)
    assert isolated.in_dims() == graph.in_dims()
    assert isolated.drug_codes == graph.drug_codes
    _, history = train_matcher(isolated, [(1, 2), (1, 3)], [True, False], seed=1, epochs=5)
    assert len(history) == 5
