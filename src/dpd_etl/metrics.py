"""One definition for the dbt and Spark reporting metrics and their documentation."""
from importlib.resources import files
from pathlib import Path

import yaml


def definitions():
    return yaml.safe_load(files('dpd_etl').joinpath('metrics.yml').read_text())


def report_sql(relation):
    expressions = ',\n       '.join(f"{spec['expression']} as {name}"
                                    for name, spec in definitions()['metrics'].items())
    return f'SELECT snapshot_date, drug_class, {expressions}\nFROM {relation}\nGROUP BY snapshot_date, drug_class'


def generate(root=Path('.')):
    spec = definitions()
    model = "{{ config(materialized='view') }}\n" + report_sql("{{ ref('fct_product') }}") + '\n'
    (root / 'warehouse/models/marts/rpt_catalogue.sql').write_text(model)
    semantic = {'version': 2, 'models': [{
        'name': 'fct_product', 'semantic_model': {'enabled': True},
        'agg_time_dimension': 'snapshot_date',
        'columns': [
            {'name': 'product_key', 'entity': {'type': 'primary', 'name': 'product'}},
            {'name': 'snapshot_date', 'granularity': 'day', 'dimension': {'type': 'time'}},
            {'name': 'drug_class', 'dimension': {'type': 'categorical'}},
        ],
        'metrics': [
            {'name': name, 'label': name.replace('_', ' ').title(), 'type': 'simple',
             'description': metric['description'],
             'agg': metric['expression'].split('(')[0],
             'expr': '1' if metric['expression'] == 'count(*)' else metric['expression'].split('(')[1][:-1]}
            for name, metric in spec['metrics'].items()
        ],
    }]}
    # Merge semantic annotations into the existing model entry rather than defining it twice.
    schema = yaml.safe_load((root / 'warehouse/models/schema.yml').read_text())
    for model in schema['models']:
        if model['name'] == 'fct_product':
            annotations = semantic['models'][0]
            model['semantic_model'] = annotations['semantic_model']
            model['agg_time_dimension'] = annotations['agg_time_dimension']
            model['metrics'] = annotations['metrics']
            for annotated in annotations['columns']:
                column = next((c for c in model['columns'] if c['name'] == annotated['name']), None)
                if column is None:
                    model['columns'].append(annotated)
                else:
                    column.update(annotated)
    (root / 'warehouse/models/schema.yml').write_text(yaml.safe_dump(schema, sort_keys=False))
    lines = ['# Catalogue definitions', '', 'Generated with `python -m dpd_etl.metrics`.', '',
             f"Grain: {spec['grain']}.", '', '## Metrics', '',
             '| Metric | Meaning | SQL |', '| --- | --- | --- |']
    for name, metric in spec['metrics'].items():
        lines.append(f"| {name} | {metric['description']} | `{metric['expression']}` |")
    lines += ['', '## Lineage', '', '```mermaid', 'flowchart LR']
    lines += [f'  {source} --> {target}' for source, target in spec['lineage'].items()]
    lines += ['```', '', '## Table and column dictionary', '']
    for model in schema['models']:
        lines += [f"### {model['name']}", '', model['description'], '',
                  '| Column | Meaning | Checks |', '| --- | --- | --- |']
        for col in model.get('columns', []):
            checks = [t if isinstance(t, str) else next(iter(t)) for t in col.get('data_tests', [])]
            lines.append(f"| {col['name']} | {col.get('description', 'See table grain.')} | {', '.join(checks)} |")
        lines.append('')
    (root / 'docs/data-dictionary.md').write_text('\n'.join(lines) + '\n')


if __name__ == '__main__':
    generate()
