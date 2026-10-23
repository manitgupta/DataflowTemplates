#!/usr/bin/env python3
"""Download slide thumbnails (PNG) for visual review. Usage: thumbs.py <presentationId> <outdir> [slideIds...]"""
import json, os, subprocess, sys, time, urllib.request

pid, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
ids = sys.argv[3:]
if not ids:
    res = subprocess.run(['gwscli', 'slides', 'read_presentation', '--presentationId', pid, '--fields', 'slides.objectId'],
                         capture_output=True, text=True)
    ids = [s['objectId'] for s in json.loads(res.stdout)['content']['slides']]
for n, sid in enumerate(ids, 1):
    for attempt in range(5):
        r = subprocess.run(['gwscli', 'slides', 'read_slide_page_thumbnail', '--presentationId', pid, '--pageId', sid,
                            '--thumbnailProperties', json.dumps({'thumbnailSize': 'LARGE', 'mimeType': 'PNG'})],
                           capture_output=True, text=True)
        try:
            body = json.loads(r.stdout)
            body = body.get('content', body)
            url = body['contentUrl']
            break
        except Exception:  # noqa: BLE001
            time.sleep(20)
    else:
        print('failed', sid, r.stdout[-300:], r.stderr[-300:])
        continue
    path = os.path.join(out, f'{n:02d}-{sid}.png')
    urllib.request.urlretrieve(url, path)
    print(path, flush=True)
