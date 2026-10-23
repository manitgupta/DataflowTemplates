#!/usr/bin/env python3
"""Populate the metrics spreadsheet (chart source for the deck) and create/update its charts via gwscli.

Usage: build_charts.py <spreadsheetId>   -> writes/updates charts.json {name: chartId}
All numbers are measured values from the experiment (see ../REPORT.md). Charts are sized (pixels) to match the slot
they occupy on the slide (~1.33 px per pt) so their fonts render at their nominal size.
"""
import json, os, subprocess, sys

SID = sys.argv[1]
HERE = os.path.dirname(os.path.abspath(__file__))


def gws(*args):
    r = subprocess.run(['gwscli', *args], capture_output=True, text=True)
    if r.returncode or '"isError": true' in r.stdout:
        raise SystemExit(f'gwscli failed: {r.stderr[-1500:]}\n{r.stdout[-1500:]}')
    return json.loads(r.stdout) if r.stdout.strip().startswith(('{', '[')) else r.stdout


BLUE = {'red': 0.102, 'green': 0.451, 'blue': 0.910}
GREEN = {'red': 0.094, 'green': 0.502, 'blue': 0.220}
ORANGE = {'red': 0.890, 'green': 0.455, 'blue': 0.0}
GREY = {'red': 0.600, 'green': 0.620, 'blue': 0.640}
PURPLE = {'red': 0.400, 'green': 0.180, 'blue': 0.600}
TEAL = {'red': 0.0, 'green': 0.482, 'blue': 0.514}

RESULTS = os.path.join(HERE, '..', '..', 'results')


def _samples(name):
    import datetime as dt
    out, t0 = [], None
    for line in open(os.path.join(RESULTS, name)):
        hh, n = line.split()[:2]
        t = dt.datetime.strptime(hh, '%H:%M:%S')
        if t0 is None:
            t0 = t
        m = ((t - t0).total_seconds() % 86400) / 60
        if n != 'NA':
            out.append((round(m), round(int(n) / 1e9, 3)))
    return out


def progress_rows():
    """Merged Flink / Dataflow samples: [minute, flink B rows, dataflow B rows] (blank where not sampled)."""
    f, d = dict(_samples('flink10t-samples.txt')), dict(_samples('df10t-samples.txt'))
    rows = [['Minutes since job start', 'Apache Flink', 'Dataflow']]
    for m in sorted(set(f) | set(d)):
        rows.append([m, f.get(m), d.get(m)])
    return rows


# name -> (sheetId, title, rows)
DATA = {
    'dataset': (101, 'Dataset', [
        ['Collection', 'Logical JSON (TB)', 'Documents (B)'],
        ['events', 4.223, 3.150], ['orders', 2.197, 0.844], ['follows (edges)', 1.359, 2.700],
        ['purchased (edges)', 1.278, 2.700], ['users', 1.249, 0.675], ['products', 0.974, 0.169],
        ['type_zoo', 0.014, 0.0135]]),
    'stages': (102, 'Stages', [
        ['Stage', 'Wall-clock (min)'],
        ['Export (ArangoDB to GCS)', 59],
        ['Import, Apache Flink', 139],
        ['Import, Dataflow', 164],
        ['Validation, every row', 32]]),
    'export_rotation': (103, 'ExportRotation', [
        ['Export configuration', 'Million docs/s'],
        ['Before: first coordinator only', 0.58],
        ['After: one coordinator per query', 3.35]]),
    'range_hash': (104, 'RangeVsHash', [
        ['Partitioning (16 partitions)', 'Docs read per doc exported'],
        ['RANGE on _key', 1],
        ['HASH(_key) % 16', 16]]),
    # 10 TB into Spanner Omni (12 servers): rows written vs minutes since job start (sampled every ~5 min)
    'import_progress': (105, 'ImportProgress', progress_rows()),
    'import_phases': (106, 'ImportPhases', [
        ['Import rate', 'Apache Flink', 'Dataflow'],
        ['Best ~5 min', 1683, 3698],
        ['Whole import', 1225, 1043]]),
    'counts': (107, 'Counts', [
        ['Table', 'ArangoDB (M docs)', 'Spanner Omni (M rows)'],
        ['events', 3149.55, 3149.55], ['follows', 2700, 2700], ['purchased', 2700, 2700],
        ['orders', 843.75, 843.75], ['users', 675, 675], ['products', 168.75, 168.75],
        ['type_zoo', 13.5, 13.5]]),
    'omni_time': (109, 'OmniTime', [
        ['Job (same export, jar and Omni cluster)', 'Apache Flink', 'Dataflow'],
        ['Import 10.25 B rows', 139.5, 164],
        ['DLQ replay ~5.0 M rows', 48.0, 45.5],
        ['Full-row validation 10.25 B', 31.6, 27.2]]),
    'omni_cpu': (110, 'OmniCpu', [
        ['Spanner Omni CPU during the import', 'Apache Flink', 'Dataflow'],
        ['Average', 73, 55], ['p95', 92, 82]]),
    'volume': (108, 'Volume', [
        ['Where', 'TB'],
        ['ArangoDB JSON', 11.29], ['ArangoDB disk', 5.79],
        ['GCS Avro', 4.27], ['Spanner Omni disk', 11.25]]),
}

# slide slot of each chart in points (w, h) -> pixel size
SLOTS = {'dataset': (380, 250), 'stages': (430, 250), 'export_rotation': (400, 260), 'range_hash': (310, 220),
         'import_progress': (440, 240), 'import_phases': (245, 240), 'counts': (440, 270), 'volume': (420, 270),
         'omni_time': (420, 250), 'omni_cpu': (250, 250)}
FONT = 'Google Sans'


def rng(sheet_id, r0, r1, c0, c1):
    return {'sheetId': sheet_id, 'startRowIndex': r0, 'endRowIndex': r1, 'startColumnIndex': c0, 'endColumnIndex': c1}


def basic(title, sheet_id, nrows, chart_type, series_cols, colors, horizontal=False, left_title='', bottom_title='',
          legend='NONE', labels=True):
    series = []
    for col, color in zip(series_cols, colors):
        s = {'series': {'sourceRange': {'sources': [rng(sheet_id, 0, nrows, col, col + 1)]}},
             'targetAxis': 'BOTTOM_AXIS' if horizontal else 'LEFT_AXIS',
             'colorStyle': {'rgbColor': color}}
        if labels:
            s['dataLabel'] = {'type': 'DATA', 'placement': 'OUTSIDE_END', 'textFormat': {'fontSize': 12, 'bold': True}}
        if chart_type == 'LINE':
            s['lineStyle'] = {'width': 4}
        series.append(s)
    axis_fmt = {'fontSize': 11, 'fontFamily': FONT}
    return {'title': title, 'titleTextFormat': {'fontSize': 14, 'bold': True, 'fontFamily': FONT}, 'fontName': FONT,
            'basicChart': {'chartType': chart_type, 'legendPosition': legend, 'headerCount': 1,
                           'axis': [{'position': 'BOTTOM_AXIS', 'title': bottom_title, 'format': axis_fmt},
                                    {'position': 'LEFT_AXIS', 'title': left_title, 'format': axis_fmt}],
                           'domains': [{'domain': {'sourceRange': {'sources': [rng(sheet_id, 0, nrows, 0, 1)]}}}],
                           'series': series}}


def with_ymax(spec, ymax):
    """Headroom above the tallest bar so its data label doesn't touch the title."""
    for ax in spec['basicChart']['axis']:
        if ax['position'] == 'LEFT_AXIS':
            ax['viewWindowOptions'] = {'viewWindowMin': 0, 'viewWindowMax': ymax, 'viewWindowMode': 'EXPLICIT'}
    return spec


def interpolate(spec):
    """Connect line points across blank cells (each target has its own sample times)."""
    spec['basicChart']['interpolateNulls'] = True
    return spec


def specs():
    return {
        'dataset': {'title': '11.3 TB of JSON by collection', 'fontName': FONT,
                    'titleTextFormat': {'fontSize': 14, 'bold': True, 'fontFamily': FONT},
                    'pieChart': {'legendPosition': 'LABELED_LEGEND', 'pieHole': 0.45,
                                 'domain': {'sourceRange': {'sources': [rng(101, 0, 8, 0, 1)]}},
                                 'series': {'sourceRange': {'sources': [rng(101, 0, 8, 1, 2)]}}}},
        'stages': basic('Wall-clock minutes per stage (10.25 B documents, Spanner Omni)', 102, 5, 'BAR', [1], [TEAL], True,
                        bottom_title='minutes'),
        'export_rotation': basic('Export throughput (million docs/s)', 103, 3, 'COLUMN', [1], [GREEN]),
        'range_hash': basic('Documents read per document exported', 104, 3, 'COLUMN', [1], [ORANGE]),
        'import_progress': interpolate(basic('Rows written into Spanner Omni (billions)', 105, len(progress_rows()), 'LINE', [1, 2],
                                             [ORANGE, BLUE], bottom_title='minutes since job start',
                                             legend='BOTTOM_LEGEND', labels=False)),
        'import_phases': with_ymax(basic('Import rate (thousand rows/s)', 106, 3, 'COLUMN', [1, 2], [ORANGE, BLUE],
                                         legend='BOTTOM_LEGEND'), 4500),
        'counts': basic('Rows per table: ArangoDB vs Spanner Omni (millions)', 107, 8, 'COLUMN', [1, 2], [GREY, GREEN],
                        legend='BOTTOM_LEGEND', labels=False),
        'volume': basic('Data volume at each step (TB)', 108, 5, 'COLUMN', [1], [BLUE]),
        'omni_time': basic('Wall-clock minutes per job', 109, 4, 'COLUMN', [1, 2], [ORANGE, BLUE],
                           legend='BOTTOM_LEGEND'),
        'omni_cpu': with_ymax(basic('Spanner Omni CPU % during the import', 110, 3, 'COLUMN', [1, 2], [ORANGE, BLUE],
                                    legend='BOTTOM_LEGEND'), 120),
    }


def position(name, sheet_id):
    w, h = SLOTS[name]
    return {'overlayPosition': {'anchorCell': {'sheetId': sheet_id, 'rowIndex': 1, 'columnIndex': 4},
                                'widthPixels': int(w * 1.333), 'heightPixels': int(h * 1.333)}}


def main():
    meta = gws('sheets', 'get_spreadsheet', '--spreadsheetId', SID)
    meta = meta.get('content', meta) if isinstance(meta, dict) else {}
    existing = {s['properties']['title'] for s in meta.get('sheets', [])}
    reqs = [{'addSheet': {'properties': {'sheetId': sid, 'title': title}}}
            for sid, title, _ in DATA.values() if title not in existing]
    if reqs:
        gws('sheets', 'update_spreadsheet', '--spreadsheetId', SID, *sum([['--requests', json.dumps(r)] for r in reqs], []))
    # plain numbers in the ImportProgress domain column (it kept a time format from earlier HH:MM data)
    gws('sheets', 'update_spreadsheet', '--spreadsheetId', SID, '--requests', json.dumps({'repeatCell': {
        'range': rng(105, 1, 150, 0, 1), 'cell': {'userEnteredFormat': {'numberFormat': {'type': 'NUMBER', 'pattern': '0'}}},
        'fields': 'userEnteredFormat.numberFormat'}}))
    for sid, title, rows in DATA.values():
        gws('sheets', 'update_values', '--spreadsheetId', SID, '--range', f"'{title}'!A1:H150",
            *sum([['--values', json.dumps(['' if v is None else v for v in row] + [''] * (8 - len(row)))]
                  for row in rows + [[]] * (150 - len(rows))], []))
    out_path = os.path.join(HERE, 'charts.json')
    charts = json.load(open(out_path)) if os.path.exists(out_path) else {}
    for name, spec in specs().items():
        sid = DATA[name][0]
        if name in charts:
            gws('sheets', 'update_spreadsheet', '--spreadsheetId', SID,
                '--requests', json.dumps({'updateChartSpec': {'chartId': charts[name], 'spec': spec}}),
                '--requests', json.dumps({'updateEmbeddedObjectPosition': {
                    'objectId': charts[name], 'newPosition': position(name, sid),
                    'fields': 'anchorCell,widthPixels,heightPixels'}}))
            print('updated', name, flush=True)
            continue
        res = gws('sheets', 'update_spreadsheet', '--spreadsheetId', SID, '--requests',
                  json.dumps({'addChart': {'chart': {'spec': spec, 'position': position(name, sid)}}}))
        res = res.get('content', res)
        charts[name] = res['replies'][0]['addChart']['chart']['chartId']
        json.dump(charts, open(out_path, 'w'), indent=1)
        print('chart', name, charts[name], flush=True)
    print('CHARTS_DONE', json.dumps(charts))


if __name__ == '__main__':
    main()
