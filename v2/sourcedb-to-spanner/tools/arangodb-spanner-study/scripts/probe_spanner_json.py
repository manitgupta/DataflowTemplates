#!/usr/bin/env python3
"""Which JSON number texts does Spanner's PARSE_JSON (exact mode) accept? One query per value."""
import os, subprocess, sys

vals = sys.argv[1:] or ['0.1', '0.025826', '0.46245', '14.65451999999999', '14.65452', '5e-324', '4.9E-324',
                        '0.5', '0.25826', '0.0258260', '2.5826e-2', '0.02582600000000000', '0.12345678901234567',
                        '0.123456789012345', '0.1234567890123456', '1.5', '123.456', '0.061657', '0.21313']
for v in vals:
    r = subprocess.run(['gcloud', 'spanner', 'databases', 'execute-sql', 'e2e', '--instance=arango-spanner',
                        f"--sql=SELECT TO_JSON_STRING(PARSE_JSON('[{v}]')) j"], capture_output=True, text=True,
                       env=dict(os.environ, CLOUDSDK_ACTIVE_CONFIG_NAME='omni'))
    out = r.stdout.strip().splitlines()[-1] if r.returncode == 0 else 'REJECT'
    print(f'{v:>24} -> {out}', flush=True)
