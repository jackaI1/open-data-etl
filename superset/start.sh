#!/bin/sh
set -eu
superset db upgrade
# Creation is idempotent without masking other CLI failures.
python - <<'PY'
import os
from superset.app import create_app
from superset import security_manager
app = create_app()
with app.app_context():
    if not security_manager.find_user(username='admin'):
        user = security_manager.add_user('admin', 'Local', 'Reviewer',
            'admin@example.invalid', security_manager.find_role('Admin'),
            password=os.environ['SUPERSET_ADMIN_PASSWORD'])
        if not user:
            raise RuntimeError('admin creation failed')
PY
superset init
python /app/dashboard/import_dashboard.py
exec gunicorn --bind 0.0.0.0:8088 --workers 1 --threads 4 --timeout 120 'superset.app:create_app()'
