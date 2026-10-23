#!/usr/bin/env python3
"""Build the ArangoDB -> Spanner feasibility deck with gwscli (Slides batchUpdate).

Usage: build_deck.py <presentationId> <spreadsheetId>   (charts.json from build_charts.py must exist)
Slides are 720 x 405 pt. Everything is drawn with native shapes/tables + linked Sheets charts (editable).
"""
import json, re, os, subprocess, sys

PID, SID = sys.argv[1], sys.argv[2]
HERE = os.path.dirname(os.path.abspath(__file__))
CHARTS = json.load(open(os.path.join(HERE, 'charts.json')))


def hexc(h):
    h = h.lstrip('#')
    return {'red': int(h[0:2], 16) / 255, 'green': int(h[2:4], 16) / 255, 'blue': int(h[4:6], 16) / 255}


DARK, GREY, LIGHTGREY, WHITE = '#202124', '#5F6368', '#F1F3F4', '#FFFFFF'
BLUE, LBLUE = '#1A73E8', '#E8F0FE'
GREEN, LGREEN = '#188038', '#E6F4EA'
ORANGE, LORANGE = '#E37400', '#FEF7E0'
RED, LRED = '#D93025', '#FCE8E6'
PURPLE, LPURPLE = '#9334E6', '#F3E8FD'
TEAL, LTEAL = '#007B83', '#E4F7FB'
NAVY = '#0B2A4A'
FONT, MONO = 'Google Sans', 'Roboto Mono'


class Deck:
    def __init__(self):
        self.reqs, self.n, self.slide_no = [], 0, 0

    def oid(self, p='e'):
        self.n += 1
        return f'{p}{self.n:04d}'

    def flush(self):
        import time
        for i in range(0, len(self.reqs), 400):
            chunk = self.reqs[i:i + 400]
            args = ['gwscli', 'slides', 'update_presentation', '--presentationId', PID]
            for r in chunk:
                args += ['--requests', json.dumps(r)]
            for attempt in range(10):
                res = subprocess.run(args, capture_output=True, text=True)
                out = res.stdout + res.stderr
                if res.returncode == 0 and not re.search(r'"isError":\s*true', out):
                    break
                if 'Quota exceeded' in out or 'RESOURCE_EXHAUSTED' in out or '429' in out:
                    time.sleep(65)
                    continue
                raise SystemExit(f'update failed: {out[-3000:]}\nfirst req: {json.dumps(chunk[0])[:500]}')
            else:
                raise SystemExit('quota retries exhausted')
            time.sleep(4)
        self.reqs = []

    # ---------- primitives ----------
    @staticmethod
    def props(page, x, y, w, h):
        return {'pageObjectId': page,
                'size': {'width': {'magnitude': max(w, 0.1), 'unit': 'PT'}, 'height': {'magnitude': max(h, 0.1), 'unit': 'PT'}},
                'transform': {'scaleX': 1, 'scaleY': 1, 'translateX': x, 'translateY': y, 'unit': 'PT'}}

    def slide(self, bg=WHITE, skipped=False):
        sid = self.oid('s')
        self.slide_no += 1
        self.reqs.append({'createSlide': {'objectId': sid, 'slideLayoutReference': {'predefinedLayout': 'BLANK'}}})
        if skipped:  # appendix slides: kept in the deck, skipped when presenting
            self.reqs.append({'updateSlideProperties': {'objectId': sid, 'slideProperties': {'isSkipped': True},
                                                        'fields': 'isSkipped'}})
        self.reqs.append({'updatePageProperties': {'objectId': sid, 'pageProperties': {
            'pageBackgroundFill': {'solidFill': {'color': {'rgbColor': hexc(bg)}}}}, 'fields': 'pageBackgroundFill'}})
        return sid

    def box(self, page, x, y, w, h, runs=None, fill=None, outline=None, shape='RECTANGLE', align='START',
            valign='TOP', outline_w=1.0, dash=None, pad=None):
        """runs: str or list of (text, size, bold, color[, font]) segments; '\n' separates paragraphs."""
        oid = self.oid('b')
        if shape == 'ROUND_RECTANGLE' and min(w, h) > 70:
            shape = 'RECTANGLE'  # corner radius scales with size: large rounded panels look like blobs
        self.reqs.append({'createShape': {'objectId': oid, 'shapeType': shape,
                                          'elementProperties': self.props(page, x, y, w, h)}})
        sp, fields = {}, []
        if fill:
            sp['shapeBackgroundFill'] = {'solidFill': {'color': {'rgbColor': hexc(fill)}}}
        else:
            sp['shapeBackgroundFill'] = {'propertyState': 'NOT_RENDERED'}
        fields.append('shapeBackgroundFill')
        if outline:
            sp['outline'] = {'outlineFill': {'solidFill': {'color': {'rgbColor': hexc(outline)}}},
                             'weight': {'magnitude': outline_w, 'unit': 'PT'}}
            if dash:
                sp['outline']['dashStyle'] = dash
        else:
            sp['outline'] = {'propertyState': 'NOT_RENDERED'}
        fields.append('outline')
        sp['contentAlignment'] = valign
        fields.append('contentAlignment')
        self.reqs.append({'updateShapeProperties': {'objectId': oid, 'shapeProperties': sp, 'fields': ','.join(fields)}})
        if runs:
            self.text(oid, runs, align)
        return oid

    def text(self, oid, runs, align='START', cell=None):
        if isinstance(runs, str):
            runs = [(runs, 12, False, DARK)]
        full = ''.join(r[0] for r in runs)
        if not full:
            return
        base = {'objectId': oid}
        if cell:
            base['cellLocation'] = {'rowIndex': cell[0], 'columnIndex': cell[1]}
        self.reqs.append({'insertText': {**base, 'text': full, 'insertionIndex': 0}})
        pos = 0
        for r in runs:
            t, size, bold, color = r[0], r[1], r[2], r[3]
            font = r[4] if len(r) > 4 else FONT
            if not t:
                continue
            style = {'fontFamily': font, 'fontSize': {'magnitude': size, 'unit': 'PT'}, 'bold': bold,
                     'foregroundColor': {'opaqueColor': {'rgbColor': hexc(color)}}}
            self.reqs.append({'updateTextStyle': {**base, 'style': style,
                                                  'textRange': {'type': 'FIXED_RANGE', 'startIndex': pos, 'endIndex': pos + len(t)},
                                                  'fields': 'fontFamily,fontSize,bold,foregroundColor'}})
            pos += len(t)
        self.reqs.append({'updateParagraphStyle': {**base, 'style': {'alignment': align, 'spaceAbove': {'magnitude': 0, 'unit': 'PT'},
                                                                      'spaceBelow': {'magnitude': 2, 'unit': 'PT'}},
                                                   'textRange': {'type': 'ALL'}, 'fields': 'alignment,spaceAbove,spaceBelow'}})

    def bullets(self, oid, start, end):
        self.reqs.append({'createParagraphBullets': {'objectId': oid, 'bulletPreset': 'BULLET_DISC_CIRCLE_SQUARE',
                                                     'textRange': {'type': 'FIXED_RANGE', 'startIndex': start, 'endIndex': end}}})

    def bullet_box(self, page, x, y, w, h, heading, items, color=DARK, size=11, fill=None, outline=None, hsize=13,
                   hcolor=None):
        runs = []
        if heading:
            runs.append((heading + '\n', hsize, True, hcolor or color))
        start = sum(len(r[0]) for r in runs)
        body = '\n'.join(items)
        runs.append((body, size, False, color))
        oid = self.box(page, x, y, w, h, runs, fill=fill, outline=outline)
        self.bullets(oid, start, start + len(body))
        if fill and (hcolor or color):
            self.box(page, x, y, w, 4, fill=hcolor or color)
        return oid

    def arrow(self, page, x1, y1, x2, y2, color=GREY, weight=2.0, dash=None, start_arrow=False):
        """Straight arrow; supports any direction (flips via negative scale)."""
        oid = self.oid('l')
        w, h = abs(x2 - x1), abs(y2 - y1)
        sx = -1 if x2 < x1 else 1
        sy = -1 if y2 < y1 else 1
        self.reqs.append({'createLine': {'objectId': oid, 'lineCategory': 'STRAIGHT', 'elementProperties': {
            'pageObjectId': page,
            'size': {'width': {'magnitude': max(w, 0.01), 'unit': 'PT'}, 'height': {'magnitude': max(h, 0.01), 'unit': 'PT'}},
            'transform': {'scaleX': sx, 'scaleY': sy, 'translateX': x1, 'translateY': y1, 'unit': 'PT'}}}})
        lp = {'endArrow': 'FILL_ARROW', 'weight': {'magnitude': weight, 'unit': 'PT'},
              'lineFill': {'solidFill': {'color': {'rgbColor': hexc(color)}}}}
        fields = 'endArrow,weight,lineFill'
        if start_arrow:
            lp['startArrow'] = 'FILL_ARROW'
            fields += ',startArrow'
        if dash:
            lp['dashStyle'] = dash
            fields += ',dashStyle'
        self.reqs.append({'updateLineProperties': {'objectId': oid, 'lineProperties': lp, 'fields': fields}})
        return oid

    def chart(self, page, name, x, y, w, h):
        oid = self.oid('c')
        self.reqs.append({'createSheetsChart': {'objectId': oid, 'spreadsheetId': SID, 'chartId': CHARTS[name],
                                                'linkingMode': 'LINKED', 'elementProperties': self.props(page, x, y, w, h)}})
        return oid

    def table(self, page, x, y, w, rows, col_w, size=9, header_fill=NAVY, row_h=18, zebra=LIGHTGREY, first_col_bold=False):
        oid = self.oid('t')
        nr, nc = len(rows), len(rows[0])
        self.reqs.append({'createTable': {'objectId': oid, 'rows': nr, 'columns': nc,
                                          'elementProperties': self.props(page, x, y, w, row_h * nr)}})
        for r, row in enumerate(rows):
            for c, val in enumerate(row):
                if val == '':
                    continue
                color = WHITE if r == 0 else DARK
                bold = r == 0 or (first_col_bold and c == 0)
                self.text(oid, [(str(val), size, bold, color)], cell=(r, c))
        self.reqs.append({'updateTableCellProperties': {'objectId': oid, 'tableRange': {
            'location': {'rowIndex': 0, 'columnIndex': 0}, 'rowSpan': 1, 'columnSpan': nc},
            'tableCellProperties': {'tableCellBackgroundFill': {'solidFill': {'color': {'rgbColor': hexc(header_fill)}}}},
            'fields': 'tableCellBackgroundFill'}})
        for r in range(2, nr, 2):
            self.reqs.append({'updateTableCellProperties': {'objectId': oid, 'tableRange': {
                'location': {'rowIndex': r, 'columnIndex': 0}, 'rowSpan': 1, 'columnSpan': nc},
                'tableCellProperties': {'tableCellBackgroundFill': {'solidFill': {'color': {'rgbColor': hexc(zebra)}}}},
                'fields': 'tableCellBackgroundFill'}})
        for c, cw in enumerate(col_w):
            self.reqs.append({'updateTableColumnProperties': {'objectId': oid, 'columnIndices': [c],
                                                              'tableColumnProperties': {'columnWidth': {'magnitude': cw, 'unit': 'PT'}},
                                                              'fields': 'columnWidth'}})
        self.reqs.append({'updateTableBorderProperties': {'objectId': oid, 'borderPosition': 'ALL', 'tableBorderProperties': {
            'tableBorderFill': {'solidFill': {'color': {'rgbColor': hexc('#DADCE0')}}},
            'weight': {'magnitude': 0.75, 'unit': 'PT'}}, 'fields': 'tableBorderFill,weight'}})
        return oid

    # ---------- composites ----------
    def header(self, page, title, kicker=None, accent=BLUE):
        if kicker:
            self.box(page, 30, 14, 600, 16, [(kicker.upper(), 9, True, accent)])
        # claim-style titles are full sentences: shrink long ones to stay on one line
        size = 21 if len(title) <= 52 else 19 if len(title) <= 62 else 17 if len(title) <= 72 else 15.5
        self.box(page, 30, 26 if kicker else 18, 660, 34, [(title, size, True, DARK)])
        self.box(page, 30, 62, 46, 3, fill=accent)
        self.box(page, 640, 383, 60, 14, [(f'{self.slide_no}', 8, False, GREY)], align='END')
        self.box(page, 30, 383, 400, 14, [('ArangoDB → Spanner feasibility study · Oct 2026', 8, False, GREY)])

    def tile(self, page, x, y, w, h, big, label, color, fill, big_size=26, label_size=10):
        self.box(page, x, y, w, h, fill=fill)
        self.box(page, x, y, 4, h, fill=color)
        self.box(page, x + 12, y + 6, w - 20, h * 0.48, [(big, big_size, True, color)], valign='MIDDLE')
        self.box(page, x + 12, y + h * 0.50, w - 20, h * 0.48, [(label, label_size, False, DARK)])

    def card(self, page, x, y, w, h, num, title, subtitle, items, color, fill):
        self.box(page, x, y, w, h, fill=fill)
        self.box(page, x, y, w, 4, fill=color)
        self.box(page, x + 10, y + 14, 28, 28, [(str(num), 13, True, WHITE)], fill=color, shape='ELLIPSE',
                 align='CENTER', valign='MIDDLE')
        self.box(page, x + 44, y + 10, w - 50, 36, [(title + '\n', 12.5, True, color), (subtitle, 8.5, False, GREY)],
                 valign='MIDDLE')
        body = '\n'.join(items)
        oid = self.box(page, x + 6, y + 54, w - 10, h - 58, [(body, 9.5, False, DARK)])
        self.bullets(oid, 0, len(body))


def clear(d):
    """Delete every existing slide (fresh rebuild; also removes the default title slide)."""
    res = subprocess.run(['gwscli', 'slides', 'read_presentation', '--presentationId', PID, '--fields', 'slides.objectId'],
                         capture_output=True, text=True)
    pres = json.loads(res.stdout).get('content', {})
    for sl in pres.get('slides', []):
        d.reqs.append({'deleteObject': {'objectId': sl['objectId']}})
    if d.reqs:
        d.flush()


def build():
    d = Deck()
    clear(d)

    # 1 ---------- Title ----------
    s = d.slide(NAVY)
    d.box(s, 40, 40, 640, 20, [('FEASIBILITY STUDY · SPANNER MIGRATION TOOLING', 10, True, '#8AB4F8')])
    d.box(s, 40, 64, 640, 90, [('Migrating ArangoDB to Spanner Omni', 34, True, WHITE), ('\nA 10 TB end-to-end migration with Google\'s migration tools', 16, False, '#E8EAED')])
    d.box(s, 40, 170, 300, 18, [('October 2026 · terraform-demo-host', 10, False, '#9AA0A6')])
    flow = [('ArangoDB 3.11', '10.25 B documents · 11.3 TB', '#77B300'), ('Cloud Storage', 'bulk-template Avro format', '#FBBC04'),
            ('Dataflow · Apache Flink', 'one optimized Beam jar', '#8AB4F8'), ('Spanner Omni', '12-server cluster', '#81C995')]
    x = 40
    for i, (t, sub, c) in enumerate(flow):
        d.box(s, x, 250, 140, 62, [(t + '\n', 13, True, NAVY), (sub, 9, False, NAVY)], fill=c, shape='ROUND_RECTANGLE',
              align='CENTER', valign='MIDDLE')
        if i < len(flow) - 1:
            d.arrow(s, x + 142, 281, x + 166, 281, color=WHITE, weight=2.5)
        x += 168
    d.box(s, 40, 330, 640, 40, [('Validated with gcs-spanner-dv (full-row hash comparison) and sampled document diffs', 10, False, '#BDC1C6')])
    d.flush()

    # 2 ---------- Answer first ----------
    s = d.slide()
    d.header(s, 'Yes: 10 TB of ArangoDB data loads into Spanner Omni in about 2.3 h on Flink', 'Answer first', GREEN)
    d.box(s, 30, 78, 220, 236, fill=NAVY)
    d.box(s, 44, 90, 194, 214, [('The question\n', 14, True, '#8AB4F8'),
                                ('An ArangoDB customer is moving to Spanner Omni, which runs anywhere.\n\n', 11, False, WHITE),
                                ('Can Google\'s migration tools move their data, including outside Google Cloud on Apache Flink?', 11, True, WHITE)])
    d.tile(s, 265, 78, 210, 112, '10.25 B', 'documents (11.3 TB of JSON) exported, loaded and checked row by row', BLUE, LBLUE, big_size=30)
    d.tile(s, 485, 78, 210, 112, '139 min', 'to load them into 12 Omni servers on Apache Flink (Dataflow: 164 min)', ORANGE, LORANGE, big_size=30)
    d.tile(s, 265, 202, 210, 112, '100 %', 'rows matched the export after both the Flink and the Dataflow run', GREEN, LGREEN, big_size=30)
    d.tile(s, 485, 202, 210, 112, 'Omni bug', 'Omni servers crash under Dataflow\'s write bursts; Dataflow had to slow its writes', RED, LRED, big_size=26)
    d.box(s, 30, 326, 665, 40, [('Bulk (offline) migration: pause writes during the ~1 h export. Same Beam pipeline on Flink and Dataflow; '
                                 'every run checked with exact counts and a full-row hash comparison.', 10, False, GREY)])
    d.flush()

    # 3 ---------- Starting point ----------
    s = d.slide()
    d.header(s, 'ArangoDB stores documents and graphs; Spanner Omni needs tables', 'Starting point')
    d.bullet_box(s, 30, 78, 290, 170, 'What an ArangoDB customer has',
                 ['JSON documents and edge collections (graphs)', 'Schemaless: attributes vary per document',
                  'System attributes _key, _id, _rev (+ _from, _to)', 'Sharded cluster: coordinators + DB servers (RocksDB)'],
                 fill=LGREEN, hcolor=GREEN)
    d.bullet_box(s, 400, 78, 290, 170, 'What Spanner Omni expects',
                 ['Tables with primary keys', 'Typed columns, plus a JSON type for semi-structured data',
                  'Relationships via keys, secondary indexes, interleaving', 'Horizontal scale, external consistency'],
                 fill=LBLUE, hcolor=BLUE)
    d.arrow(s, 326, 163, 394, 163, color=GREY, weight=3)
    d.box(s, 326, 130, 68, 30, [('migrate', 10, True, GREY)], align='CENTER', valign='BOTTOM')
    d.box(s, 30, 262, 660, 54, fill=LIGHTGREY, shape='ROUND_RECTANGLE')
    d.box(s, 42, 266, 640, 48, [('Already available: ', 10.5, True, DARK),
                                ('the sourcedb-to-spanner bulk template (MySQL, PostgreSQL, Oracle, SQL Server, Cassandra) and the '
                                 'gcs-spanner-dv validation template — but no ArangoDB source.', 10.5, False, DARK)], valign='MIDDLE')
    d.box(s, 30, 324, 660, 54, fill=LBLUE, shape='ROUND_RECTANGLE')
    d.box(s, 42, 328, 640, 48, [('Approach: ', 10.5, True, BLUE),
                                ('export ArangoDB with Spark into the exact GCS format the bulk template already writes and reads, '
                                 'then reuse the template (transform, Spanner write, DLQ) and the validation template unchanged.',
                                 10.5, False, DARK)], valign='MIDDLE')
    d.flush()

    # 4 ---------- How support was added ----------
    s = d.slide()
    d.header(s, 'Four building blocks let Google\'s migration templates read ArangoDB', 'What we built')
    cards = [
        ('Exporter', 'Spark + ArangoDB connector', ['Range-partitioned AQL reads on _key (primary index)', 'One coordinator per query',
                                                   'Count + revision consistency guard', 'Generates Spanner DDL'], '#4D7C0F', LGREEN),
        ('GCS format', 'same files the bulk sink writes', ['<root>/<table>/<md5>-N-of-M.avro', 'Records: tableName, shardId, primaryKeys, payload',
                                                               'Schema pinned byte-for-byte to the template'], ORANGE, LORANGE),
        ('GCS source', 'in sourcedb-to-spanner', ['sourceDbDialect=GCS', 'Tables from folders, schema from Avro headers',
                                                 'Same transform, write and DLQ path as MySQL', 'Re-imports any bulk GCS export'], BLUE, LBLUE),
        ('Validator', 'gcs-spanner-dv, shared reader', ['One BulkGcsAvroReader for both templates', 'Row-by-row hash: export vs. Spanner',
                                                       'Canonical JSON hashing', 'Results in BigQuery'], PURPLE, LPURPLE)]
    for i, (t, sub, items, c, f) in enumerate(cards):
        d.card(s, 30 + i * 168, 78, 156, 228, i + 1, t, sub, items, c, f)
    d.box(s, 30, 318, 660, 56, fill=LIGHTGREY, shape='ROUND_RECTANGLE')
    d.box(s, 42, 322, 640, 50, [('Also: ', 10, True, DARK),
                                ('Spanner Omni target (--spannerOmniEndpoint) · one optimized jar for Dataflow and Apache Flink (OmniSpannerWrite, compact coders) · '
                                 'JSON number canonicalization · full-value DLQ + replay tool · ~1,600 unit tests across the touched modules.',
                                 10, False, DARK)], valign='MIDDLE')
    d.flush()

    # 5 ---------- Architecture ----------
    s = d.slide()
    d.header(s, 'One export feeds Flink or Dataflow; a full-row check closes the loop', 'How the pieces fit')
    # five stage columns with 40 pt gutters for labelled arrows
    cols = [(15, 100, 'ArangoDB 3.11', '#4D7C0F', LGREEN, ['24 coordinators', '24 DB servers', '256 shards per collection']),
            (155, 120, 'Export · Dataproc', ORANGE, LORANGE, ['ArangoDB Spark connector', 'Key-range planner', 'Avro writer, bulk layout']),
            (315, 125, 'Cloud Storage', '#B06000', '#FEEFC3', ['<table>/<md5>-N-of-M .avro', 'SourceRowWithMetadata records', 'DDL + export summary']),
            (480, 115, 'Import · Dataflow / Flink', BLUE, LBLUE, ['sourcedb-to-spanner GCS source', 'one jar, two runners', 'OmniSpannerWrite']),
            (635, 72, 'Spanner Omni', GREEN, LGREEN, ['12 servers', 'TCP load balancer'])]
    for x, w, title, c, f, items in cols:
        d.box(s, x, 78, w, 190, fill=f, outline=c, outline_w=1.25)
        d.box(s, x, 78, w, 26, [(title, 10.5, True, WHITE)], fill=c, align='CENTER', valign='MIDDLE')
        yy = 114
        for it in items:
            d.box(s, x + 7, yy, w - 14, 42, [(it, 8.5, False, DARK)], fill=WHITE, outline='#DADCE0', align='CENTER',
                  valign='MIDDLE', outline_w=0.5)
            yy += 50
    gutters = [(115, 155, 'AQL ranges'), (275, 315, 'Avro'), (440, 480, 'read'), (595, 635, 'write')]
    for a, b, lab in gutters:
        d.box(s, a - 4, 150, (b - a) + 8, 16, [(lab, 7.5, True, GREY)], align='CENTER', valign='BOTTOM')
        d.arrow(s, a + 2, 172, b - 2, 172, color=DARK, weight=2.25)
    # lower lane: validation (reads GCS + Spanner) and DLQ replay
    d.box(s, 155, 300, 440, 74, fill=LPURPLE, outline=PURPLE, outline_w=1.25)
    d.box(s, 165, 304, 420, 68, [('Validation · gcs-spanner-dv (Dataflow)\n', 10.5, True, PURPLE),
                                 ('Same BulkGcsAvroReader over the export + reads Spanner Omni → hashes every row → BigQuery '
                                  '(summary, per-table stats, mismatched rows). Plus validate.py: counts and sampled '
                                  'document diff against live ArangoDB.', 8.5, False, DARK)])
    d.arrow(s, 377, 269, 377, 299, color=PURPLE, weight=2)
    d.arrow(s, 671, 269, 671, 337, color=PURPLE, weight=2, dash='DASH')
    d.arrow(s, 670, 337, 597, 337, color=PURPLE, weight=2, dash='DASH')
    d.box(s, 15, 300, 120, 74, fill=LRED, outline=RED, outline_w=1)
    d.box(s, 21, 304, 110, 68, [('Dead-letter queue\n', 9.5, True, RED),
                                ('Rows Spanner rejects keep their full values; replay_dlq.py re-inserts them.', 8, False, DARK)])
    d.flush()

    # 6 ---------- Mapping ----------
    s = d.slide()
    d.header(s, 'Each document becomes one row: keys as columns, the document as JSON', 'Data model')
    src = ('// edge collection "follows"\n{\n  "_key": "f001162682012",\n  "_id": "follows/f001162682012",\n'
           '  "_from": "users/u000162682012",\n  "_to": "users/u000278853041",\n  "_rev": "_mSqQsDW-_x",\n'
           '  "since": "2014-10-08T17:31:42.132Z",\n  "weight": 0.46245,\n  "muted": false,\n  "note": "…"\n}')
    d.box(s, 30, 74, 250, 22, [('ArangoDB document', 11, True, '#4D7C0F')])
    d.box(s, 30, 98, 250, 200, [(src, 8.5, False, '#E8EAED', MONO)], fill='#263238', shape='ROUND_RECTANGLE')
    ddl = ('CREATE TABLE follows (\n  key      STRING(254) NOT NULL,\n  id       STRING(MAX),\n  rev      STRING(64),\n'
           '  from_id  STRING(MAX) NOT NULL,\n  to_id    STRING(MAX) NOT NULL,\n  doc      JSON NOT NULL, -- full document\n'
           '  from_key STRING(254) AS (REGEXP_EXTRACT(\n           from_id, r\'^[^/]+/(.*)$\')) STORED,\n'
           '  to_key   STRING(254) AS (…) STORED\n) PRIMARY KEY (key);\nCREATE INDEX follows_by_from ON follows(from_id);')
    d.box(s, 330, 74, 360, 22, [('Spanner table (generated DDL)', 11, True, BLUE)])
    d.box(s, 330, 98, 360, 200, [(ddl, 8.5, False, '#E8EAED', MONO)], fill='#0B2A4A', shape='ROUND_RECTANGLE')
    d.arrow(s, 284, 198, 326, 198, color=DARK, weight=3)
    d.bullet_box(s, 30, 306, 660, 72, None,
                 ['Lossless: the whole document (system attributes included) is stored in a JSON column; nothing is dropped.',
                  'Document collections → key, id, rev, doc. Edge collections add from_id/to_id + helper columns and indexes.',
                  'Typed access: add generated columns over the JSON, e.g. email STRING(MAX) AS (JSON_VALUE(doc, \'$.email\')) STORED.'],
                 size=9.5)
    d.flush()

    # 10 ---------- Timeline ----------
    s = d.slide()
    d.header(s, 'End to end: a 59-minute export plus a 139-minute Flink import', 'Performance', TEAL)
    d.chart(s, 'stages', 20, 72, 430, 250)
    facts = [('Export: 59 min', '2.9 M docs/s · 7,477 Avro files · counts + revisions equal before & after', TEAL, LTEAL),
             ('Flink: 139 min', '1.23 M rows/s avg · Omni CPU 73 % avg · no Omni crashes', ORANGE, LORANGE),
             ('Dataflow: 164 min', '1.04 M rows/s avg · writer concurrency 2 (Omni crash, see appendix)', BLUE, LBLUE),
             ('Validation: 32 / 27.2 min', 'every row hashed, 0 mismatches, both runs', GREEN, LGREEN)]
    for i, (t, sub, c, f) in enumerate(facts):
        y = 76 + i * 62
        d.box(s, 466, y, 224, 54, fill=f, shape='ROUND_RECTANGLE')
        d.box(s, 466, y, 4, 54, fill=c)
        d.box(s, 478, y + 4, 208, 48, [(t + '\n', 11, True, c), (sub, 8.5, False, DARK)], valign='MIDDLE')
    d.box(s, 20, 330, 670, 44, [('One export, two imports into the same 12-server Omni cluster, one after the other (separate databases). '
                                 'Plus a replay of ~5.0 M DLQ rows (48 min Flink run, 45.5 min Dataflow run). Data generation (~90 min) is test setup only.', 9, False, GREY)])
    d.flush()

    # 12 ---------- Import performance ----------
    s = d.slide()
    d.header(s, 'Flink loaded Spanner Omni in 139 min, Dataflow in 164 min, with the same jar', 'Performance', TEAL)
    d.chart(s, 'import_progress', 15, 70, 440, 240)
    d.chart(s, 'import_phases', 462, 70, 245, 240)
    steps = [('Flink', 'pipelined shuffles: reads and writes overlap; ~1.7 M rows/s peak', ORANGE),
             ('Flink', '139 min, no Omni server crashes', ORANGE),
             ('Dataflow', 'batch shuffle barrier: read first, then a write burst', BLUE),
             ('Dataflow', '164 min at writer concurrency 2; 7 Omni server crashes', BLUE)]
    for i, (t, sub, c) in enumerate(steps):
        x = 15 + i * 174
        d.box(s, x, 318, 166, 58, fill=LIGHTGREY, shape='ROUND_RECTANGLE')
        d.box(s, x, 318, 166, 4, fill=c)
        d.box(s, x + 6, 324, 156, 50, [(t + '\n', 9.5, True, c), (sub, 8.5, False, DARK)])
    d.flush()

    # 13b ---------- Dataflow vs Flink on Spanner Omni: table ----------
    s = d.slide()
    d.header(s, 'Same jar, same Omni cluster: Flink was faster and crash-free', 'Side by side', ORANGE)
    rows = [['', 'Apache Flink 1.20 (GKE)', 'Dataflow'],
            ['Workers', '32 TaskManagers × 16 slots (n2-standard-16)', '32 × n2-standard-16'],
            ['Target', 'Spanner Omni, 12 × z3-highmem-44, empty database, 640 splits', 'same cluster, separate empty database'],
            ['Import 10.25 B docs', '139 min · 1.23 M rows/s avg', '164 min · 1.04 M rows/s avg'],
            ['Peak import rate', '~1.7 M rows/s', '~3.7 M rows/s'],
            ['Writer settings', '8 in-flight commits per writer', '2 in-flight commits per writer (8 crashed Omni)'],
            ['Omni server crashes', 'none', '7 (2026.r4 large-value bug)'],
            ['Rows to DLQ → replayed', '4,989,187 → 48 min (1-row txns)', '4,988,262 → 45.5 min'],
            ['gcs-spanner-dv, every row', 'MATCH 10,250,550,033 / 0 (32 min)', 'MATCH 10,250,550,033 / 0 (27 min)']]
    tid = d.table(s, 30, 74, 660, rows, [170, 245, 245], size=9, row_h=30, header_fill=NAVY, first_col_bold=True)
    d.reqs.append({'updateTableCellProperties': {'objectId': tid, 'tableRange': {
        'location': {'rowIndex': 0, 'columnIndex': 1}, 'rowSpan': 1, 'columnSpan': 1},
        'tableCellProperties': {'tableCellBackgroundFill': {'solidFill': {'color': {'rgbColor': hexc(ORANGE)}}}},
        'fields': 'tableCellBackgroundFill'}})
    d.reqs.append({'updateTableCellProperties': {'objectId': tid, 'tableRange': {
        'location': {'rowIndex': 0, 'columnIndex': 2}, 'rowSpan': 1, 'columnSpan': 1},
        'tableCellProperties': {'tableCellBackgroundFill': {'solidFill': {'color': {'rgbColor': hexc(BLUE)}}}},
        'fields': 'tableCellBackgroundFill'}})
    d.box(s, 30, 380 - 14, 660, 14, [('Both runs read the same GCS export (bulk format) in us-central1; the Omni cluster was idle before each run.', 7.5, False, GREY)])
    d.flush()

    # 13c ---------- Dataflow vs Flink on Spanner Omni: charts ----------
    s = d.slide()
    d.header(s, 'Dataflow had to slow its writes to avoid an Omni crash; Flink did not', 'Side by side', ORANGE)
    d.chart(s, 'omni_time', 15, 70, 420, 250)
    d.chart(s, 'omni_cpu', 445, 70, 250, 250)
    tiles = [('100 % = 100 %', 'of 10.25 B rows matched by gcs-spanner-dv after both runs', GREEN, LGREEN),
             ('~5.0 M', 'DLQ rows per run: JSON doubles Spanner Omni can\'t store exactly; replayed', ORANGE, LORANGE),
             ('0 vs 7', 'Omni server crashes: Flink run vs. Dataflow run (2026.r4 bug)', BLUE, LBLUE)]
    for i, (big, label, c, f) in enumerate(tiles):
        d.tile(s, 15 + i * 228, 326, 220, 52, big, label, c, f, big_size=13, label_size=8)
    d.flush()

    # 13 ---------- Correctness ----------
    s = d.slide()
    d.header(s, 'All 10.25 B rows arrived intact, checked row by row after both runs', 'Validation', GREEN)
    d.chart(s, 'counts', 15, 70, 440, 270)
    tiles = [('10.25 B / 10.25 B', 'rows match in gcs-spanner-dv after the Flink run, every row hashed'),
             ('10.25 B / 10.25 B', 'rows match in gcs-spanner-dv after the Dataflow run'),
             ('205,012 / 205,012', 'rows match on the hostile-data run (0 mismatches)')]
    for i, (big, label) in enumerate(tiles):
        d.tile(s, 466, 74 + i * 90, 226, 82, big, label, GREEN, LGREEN, big_size=20, label_size=9.5)
    d.box(s, 15, 346, 677, 30, [('Exact counts on all 8 tables at 10 TB (10,250,550,033 rows) after both runs. Only number formatting differs: Spanner Omni normalizes JSON '
                                 '(e.g. -9223372036854775808.0 → -9.223372036854776e+18), the value is identical.', 8.5, False, GREY)])
    d.flush()

    # R ---------- Recommendation ----------
    s = d.slide()
    d.header(s, 'Use Flink for Omni migrations today; fix the Omni crash for full-speed Dataflow', 'Recommendation', GREEN)
    steps = [('1  Export', 'Spark exporter on any Spark cluster: ~1 h for 10 TB, consistency-checked, bulk-template Avro on GCS / S3 / ADLS', ORANGE, LORANGE),
             ('2  Import', 'The optimized Beam jar on Apache Flink (GKE or any Kubernetes): 139 min for 10.25 B docs on 12 Omni servers', BLUE, LBLUE),
             ('3  Validate', 'gcs-spanner-dv hashes every row of the export against Spanner Omni; replay the ~0.05 % DLQ rows first', GREEN, LGREEN)]
    for i, (t, sub, c, f) in enumerate(steps):
        d.box(s, 30 + i * 223, 78, 212, 128, fill=f)
        d.box(s, 30 + i * 223, 78, 212, 4, fill=c)
        d.box(s, 42 + i * 223, 88, 192, 112, [(t + '\n', 14, True, c), (sub, 10, False, DARK)])
    d.bullet_box(s, 30, 216, 325, 160, 'Before a customer run',
                 ['Fix or upgrade Spanner Omni 2026.r4-lts: large-value crash under write bursts',
                  'Dataflow works today with writer concurrency 2 (164 min); Flink needs no change',
                  'Bulk only: pause writes during the export (no change capture yet)'],
                 fill=LRED, hcolor=RED, size=9.5)
    d.bullet_box(s, 365, 216, 325, 160, 'Sizing (measured)',
                 ['~2.3 k rows/s per Omni vCPU (1.23 M rows/s on 528 vCPU)',
                  'Omni disk ≈ 1.0× the logical JSON per copy loaded (3 replicas, compressed)',
                  'Pre-split every table (~50 points per server); 32 × 16-vCPU Flink TaskManagers'],
                 fill=LTEAL, hcolor=TEAL, size=9.5)
    d.flush()

    # A ---------- Appendix divider ----------
    s = d.slide(NAVY, skipped=True)
    d.box(s, 40, 150, 640, 60, [('Appendix', 36, True, WHITE)])
    d.box(s, 40, 210, 640, 40, [('Setup, data, exporter details, sizing, issues log, Flink settings and Omni crash evidence', 13, False, '#BDC1C6')])
    d.flush()

    # 9 ---------- Dataset ----------
    s = d.slide(skipped=True)
    d.header(s, 'Appendix: the test dataset, an e-commerce graph at 10 TB', 'Appendix')
    d.chart(s, 'dataset', 20, 72, 380, 240)
    rows = [['Collection', 'Docs', 'Avg JSON'], ['events', '3.15 B', '1.3 KB'], ['follows (edge)', '2.70 B', '0.5 KB'],
            ['purchased (edge)', '2.70 B', '0.5 KB'], ['orders', '844 M', '2.6 KB'], ['users', '675 M', '1.9 KB'],
            ['products', '169 M', '5.8 KB'], ['type_zoo', '13.5 M', '1.0 KB'], ['hostile_keys', '33', '0.1 KB']]
    d.table(s, 420, 74, 270, rows, [130, 70, 70], size=9, row_h=26)
    d.box(s, 20, 322, 670, 56, fill=LORANGE)
    d.box(s, 20, 322, 4, 56, fill=ORANGE)
    d.box(s, 32, 325, 650, 50, [('Hard cases on purpose: ', 9.5, True, ORANGE),
                                ('nested objects and arrays, mixed types for one attribute, null vs. missing, 2^63 integers, 1e300, -0.0, '
                                 'unicode/emoji, odd attribute names, a 2 MB document, dangling edges, keys with every legal _key character. '
                                 'Generated with Spark through the ArangoDB connector at ~2 M docs/s.', 9.5, False, DARK)], valign='MIDDLE')
    d.flush()

    # 8 ---------- Environment ----------
    s = d.slide(skipped=True)
    d.header(s, 'Appendix: test environment', 'Appendix')
    rows = [['Role', 'Component', 'Configuration'],
            ['Source', 'ArangoDB 3.11.14 cluster', '24 × z3-highmem-16-highlssd (384 vCPU, 144 TB local NVMe); 3 agents, 24 coordinators, 24 DB servers; RF 1; 256 shards/collection'],
            ['Export', 'Dataproc 2.2 (Spark 3.5)', '32 × n2-standard-16 (64 executors × 8 cores); ArangoDB Spark connector 1.10.0'],
            ['Staging', 'Cloud Storage', 'Avro in the bulk-template layout; 7,477 files, 4.27 TB'],
            ['Import A', 'Apache Flink 1.20 on GKE', '32 TaskManagers (n2-standard-16) × 16 slots; Flink Kubernetes Operator 1.16.1'],
            ['Import B', 'Dataflow', '32 × n2-standard-16; same jar and writer settings'],
            ['Target', 'Spanner Omni 2026.r4-lts', '12 × z3-highmem-44-highlssd (528 vCPU, 216 TB NVMe), one zone, internal TCP LB; 640 pre-split points'],
            ['Validation', 'gcs-spanner-dv (Dataflow)', 'Full-row hash comparison of the export vs. Spanner Omni → BigQuery; partitioned reads']]
    d.table(s, 30, 78, 660, rows, [80, 160, 420], size=9, row_h=34, first_col_bold=True)
    d.flush()

    # 7 ---------- Exporter ----------
    s = d.slide(skipped=True)
    d.header(s, 'Appendix: how the exporter reads ArangoDB in parallel, exactly once', 'Appendix', ORANGE)
    d.box(s, 30, 74, 330, 20, [('Key space of one collection, split into ranges', 10.5, True, DARK)])
    colors = ['#F28B82', '#FBBC04', '#FFF475', '#CCFF90', '#A7FFEB', '#AECBFA']
    for i, c in enumerate(colors):
        d.box(s, 30 + i * 55, 98, 55, 30, [(f'range {i + 1}', 8.5, True, DARK)], fill=c, outline=WHITE, align='CENTER', valign='MIDDLE')
        d.box(s, 32 + i * 55, 158, 51, 26, [(f'coord {i % 24 + 1}', 8, False, WHITE)], fill=ORANGE, shape='ROUND_RECTANGLE',
              align='CENTER', valign='MIDDLE')
        d.arrow(s, 57 + i * 55, 156, 57 + i * 55, 130, color=ORANGE, weight=1.5)
    d.box(s, 30, 190, 330, 104, [('• Each range = one AQL query  FILTER d._key >= lo AND d._key < hi  → a primary-index range scan on every shard (each document read once).\n'
                                  '• Each query is pinned to its own coordinator → load spread over all 24.\n'
                                  '• Range boundaries must increase in both byte order and AQL collation order (otherwise ranges can overlap or leave gaps).', 9, False, DARK)])
    d.chart(s, 'range_hash', 380, 74, 310, 220)
    checks = [('Per-range index-only COUNTs sum to the collection count', GREEN),
              ('Collection count + revision equal before and after the export', GREEN),
              ('Rows in the written Avro files equal the source count', GREEN)]
    for i, (t, c) in enumerate(checks):
        d.box(s, 30 + i * 223, 306, 212, 66, [('✓ ', 13, True, c), (t, 9.5, False, DARK)], fill=LGREEN, shape='ROUND_RECTANGLE',
              valign='MIDDLE')
    d.flush()

    # 11 ---------- Export performance ----------
    s = d.slide(skipped=True)
    d.header(s, 'Appendix: export — one coordinator fix gave a 5.8× speed-up', 'Appendix', TEAL)
    d.chart(s, 'export_rotation', 20, 72, 400, 260)
    d.bullet_box(s, 440, 76, 250, 150, 'Root cause',
                 ['A connector query-mode read is one Spark partition', 'Its driver sends every query to the first configured endpoint',
                  'Result: one coordinator at load 22, the other 23 idle'], fill=LRED, hcolor=RED, size=9.5)
    d.bullet_box(s, 440, 236, 250, 96, 'Fix (in the exporter)',
                 ['Pin each range query to its own coordinator, round-robin', '0.58 → 3.35 M docs/s; 10 TB in 51 min'],
                 fill=LGREEN, hcolor=GREEN, size=9.5)
    d.box(s, 20, 342, 670, 32, [('Tip: list every coordinator in --endpoints; throughput scales with coordinators and Spark cores.', 9.5, True, TEAL)])
    d.flush()

    # 14 ---------- Volume ----------
    s = d.slide(skipped=True)
    d.header(s, 'Appendix: storage footprint along the pipeline', 'Appendix', TEAL)
    d.chart(s, 'volume', 20, 72, 420, 270)
    d.bullet_box(s, 458, 80, 232, 262, 'Planning numbers',
                 ['GCS staging ≈ 0.38× logical JSON (Avro + snappy)', 'ArangoDB on disk ≈ 0.5× logical JSON (RocksDB, LZ4)',
                  'Spanner Omni ≈ 1.0× logical JSON on disk (3 replicas, compressed, incl. logs)',
                  'Plan local NVMe / Hyperdisk for ~2× the logical data per copy loaded',
                  'Omni needs no scale-down step: size the server fleet for the load'],
                 fill=LTEAL, hcolor=TEAL, size=9.5)
    d.flush()

    # 15 ---------- Challenges ----------
    s = d.slide(skipped=True)
    d.header(s, 'Appendix: issues log — what we hit and how it is solved', 'Appendix', RED)
    rows = [['Challenge', 'Impact', 'Solution'],
            ['ArangoDB 3.12+ Community license caps data at 100 GB', 'A 10 TB test needs a key', 'Used 3.11.14 (Apache-2.0); 3.12 needs an Enterprise/eval key'],
            ['Connector query mode: one partition, first coordinator only', '0.58 M docs/s, one hot coordinator', 'Many range queries, one coordinator each → 3.35 M docs/s'],
            ['AQL collation vs. primary-index byte order for _key ranges', 'Ranges could overlap or leave gaps', 'Boundaries increasing in both orders + per-range count check'],
            ['Spanner exact JSON mode rejects some number texts', '~0.05 % of rows rejected', 'Shortest-form canonicalization; replay rest with PARSE_JSON(round) — values unchanged'],
            ['One bad row ⇒ the whole batch is retried row by row', 'Slow follows tail', 'Writer bisects failing batches (k bad rows ≈ 2k·log n commits)'],
            ['gcs-spanner-dv hashed raw JSON text', 'Every JSON row a false mismatch', 'Canonical JSON hashing → 100 % match'],
            ['Multi-server Omni aborts multi-row DML over scattered keys', 'DLQ replay stuck retrying', 'Single-row transactions'],
            ['Omni tablets stuck in unload (RET_CHECK PREUNLOAD)', 'Reads of a few key ranges hang', 'Rolling restart of affected servers'],
            ['Flink TaskManagers ran out of heap on large JSON rows', 'Job restarts', 'Managed memory 0.4 → 0.2, network 12 → 4 GB'],
            ['Omni 2026.r4 crash on cancelled large-value reads', 'Dataflow write bursts crashed servers', 'Dataflow writer concurrency 8 → 2'],
            ['Crashed clusters stop serving (tablets stuck loading)', 'Imports and reads time out', 'Rolling restart of all servers']]
    d.table(s, 30, 74, 660, rows, [240, 160, 260], size=7.5, row_h=25, header_fill=RED, zebra=LRED)
    d.flush()

    # 16 ---------- Sizing guidance ----------
    s = d.slide(skipped=True)
    d.header(s, 'Appendix: sizing and settings for a customer migration', 'Appendix', BLUE)
    d.box(s, 30, 74, 330, 18, [('Measured rules of thumb', 12, True, BLUE)])
    metrics = [('120–140 k docs/s', 'export per 16-vCPU ArangoDB node (2.9–3.35 M docs/s on 24)'),
               ('~2.3 k rows/s', 'import per Spanner Omni vCPU (1.23 M rows/s on 528 vCPU, 0.5–6 KB JSON rows)'),
               ('~3.5 h', 'for 10 B docs / 10 TB: 1 h export + 2.3 h import on Flink (+ replay)'),
               ('1.0×', 'Spanner Omni disk vs. logical JSON (staging: 0.38×)')]
    for i, (big, label) in enumerate(metrics):
        d.tile(s, 30 + (i % 2) * 168, 98 + (i // 2) * 112, 160, 104, big, label, BLUE, LBLUE, big_size=20, label_size=9.5)
    d.box(s, 30, 330, 328, 44, [('Measured on: 24-node ArangoDB, 512 Spark cores, Flink 32 × 16 slots (or 32 Dataflow workers), '
                                 'Spanner Omni 12 × z3-highmem-44 (528 vCPU).', 8.5, False, GREY)])
    d.box(s, 380, 74, 310, 18, [('Recommended settings', 12, True, GREEN)])
    cfg = ('# exporter (spark-submit)\n--endpoints=<all coordinators>\n--partitioning=RANGE\n--targetDocsPerPartition=2000000\n'
           '--rangesPerWrite=16 --parallelWrites=40\n\n# sourcedb-to-spanner (Dataflow)\n--sourceDbDialect=GCS\n'
           '--sourceConfigURL=gs://<bucket>/<export>/\n# documents with many floating-point values\n'
           '--writerMode=COMMIT --writerBatchRows=8000\n# Flink: 16 slots/TM, task off-heap 8g, managed 0.2')
    d.box(s, 380, 98, 310, 200, [(cfg, 8.5, False, '#E8EAED', MONO)], fill='#263238')
    d.bullet_box(s, 380, 306, 310, 68, None,
                 ['Pre-split every table in Omni (~50 points per server)', 'Quiesce writes during the export; validate with gcs-spanner-dv'],
                 size=9.5)
    d.flush()

    # F ---------- Flink settings ----------
    s = d.slide(skipped=True)
    d.header(s, 'Appendix: Flink deployment settings (the only runner-specific part)', 'Appendix', ORANGE)
    rows = [['Setting', 'Value', 'Why'],
            ['Deployment', 'Flink 1.20 on GKE, Flink Kubernetes Operator 1.16.1, application mode', 'Same shape on any Kubernetes'],
            ['TaskManagers', '32 × n2-standard-16, 16 slots each → parallelism 512', 'Matches Dataflow\'s 512 threads'],
            ['Memory', 'process 46 g; task off-heap 8 g; managed 0.2; network 0.1 / max 4 g', 'Off-heap avoids System.gc() storms; 0.4 managed OOMed on JSON rows'],
            ['Execution', 'batch-shuffle-mode ALL_EXCHANGES_PIPELINED; sorted inputs; no checkpoints', 'Reads and writes overlap'],
            ['Beam', '--useDataStreamForBatch --objectReuse --maxBundleSize=100000', 'Fewer copies between operators'],
            ['Writer', '--writerMode=COMMIT, 8 in-flight commits, 8,000 rows / 16 MB per commit', 'OmniSpannerWrite (same on Dataflow)'],
            ['Network', 'Firewall rule from the GKE pod range to the Omni ports', 'Pods use alias IPs outside the node subnet']]
    d.table(s, 30, 78, 660, rows, [110, 330, 220], size=8.5, row_h=34, header_fill=ORANGE, zebra=LORANGE)
    d.flush()

    # C ---------- Omni crash evidence ----------
    s = d.slide(skipped=True)
    d.header(s, 'Appendix: evidence — the Spanner Omni large-value crash, by run', 'Appendix', RED)
    log = ('F1021 22:18:32 an Omni server CHECK] Check failed: preq->task()->status() is OK\n'
           '  (CANCELLED: Fiber cancelled before read started; LargeValue{MF{991874, 32752}x4; ...})\n'
           '  ... Omni server code · Omni server code · Omni server code  Pending Paxos request failed.')
    d.box(s, 30, 76, 660, 52, [(log, 8, False, '#E8EAED', MONO)], fill='#263238')
    rows = [['Run (10.25 B rows, 12 Omni servers)', 'Writer in-flight commits', 'Omni server crashes', 'Outcome'],
            ['Flink import (139 min)', '8', '0', 'completed'],
            ['Flink DLQ replay + full validation', '—', '0', 'MATCH 10.25 B / 0'],
            ['Dataflow attempt 1', '8', '8', 'crawled, cancelled'],
            ['Dataflow attempt 2 (idle cluster)', '8', '4', 'wedged, cancelled'],
            ['Dataflow attempt 3 (+ type_zoo splits)', '8', '9', 'wedged, cancelled'],
            ['Dataflow final (164 min)', '2', '7', 'completed']]
    d.table(s, 30, 138, 660, rows, [230, 140, 130, 160], size=9, row_h=26, header_fill=RED, zebra=LRED)
    d.box(s, 30, 326, 660, 50, [('Why Dataflow and not Flink: ', 9.5, True, RED),
                                ('Dataflow batch puts a shuffle barrier before the writer, so it reads first and then bursts commits; '
                                 'commits exceeded the 120 s client deadline and were cancelled mid large-value read. Flink\'s pipelined '
                                 'shuffles pace writes behind reads. Each crash restarts the server and stalls its splits for minutes; '
                                 'wedged clusters needed a rolling restart.', 9, False, DARK)])
    d.flush()

    # 17 ---------- Limits & next steps ----------
    s = d.slide(skipped=True)
    d.header(s, 'Appendix: limitations and next steps', 'Appendix', PURPLE)
    d.bullet_box(s, 30, 78, 320, 290, 'Current limitations',
                 ['Bulk (offline) only: no change data capture; writes must pause during the export',
                  'No cross-collection snapshot (per-collection consistency guard detects changes)',
                  'Re-runs upsert: deletes at the source are not propagated',
                  'Spanner Omni ran insecure (plaintext, no auth); 12 servers because of the Z3 regional quota',
                  'Omni 2026.r4-lts crashes on cancelled large-value reads under write bursts (reported to the Omni team)',
                  'On multi-server Omni, DLQ replay must use single-row transactions; gcs-spanner-dv needs --spannerOmniPartitionedReads'],
                 fill=LRED, hcolor=RED, size=10)
    d.bullet_box(s, 370, 78, 320, 290, 'Next steps',
                 ['Upstream the changes: GCS source, shared reader, DV JSON fix, DLQ fix',
                  'Live migration: ArangoDB change capture → Spanner (minimal downtime)',
                  'Schema assist: suggest typed / generated columns from document attributes',
                  'Spanner Omni with TLS/auth; retest Dataflow at full writer concurrency on a fixed Omni build',
                  'Wire the in-pipeline split points to the GCS source (pre-split automatically)',
                  'Publish exporter jars and a step-by-step guide'],
                 fill=LPURPLE, hcolor=PURPLE, size=10)
    d.flush()
    print('DECK_DONE slides', d.slide_no)


if __name__ == '__main__':
    build()
