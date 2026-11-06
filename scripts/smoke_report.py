"""Exercise the container's real HTTP listener; always reap the owned server."""

import subprocess
import time
import urllib.request

started_at = time.time()
server = subprocess.Popen([
    "gunicorn", "--bind", "127.0.0.1:8080", "dpd_etl.report_service:create_app()"
])
print(f"Owned report smoke: PID={server.pid} started={started_at} port=8080; "
      "cleanup=Popen.terminate()/wait(), confined to this container")
try:
    for attempt in range(100):
        if server.poll() is not None:
            raise RuntimeError("report server exited early")
        try:
            with urllib.request.urlopen("http://127.0.0.1:8080/healthz", timeout=1) as response:
                assert response.status == 200
                assert b'"ok"' in response.read()
            print("Report HTTP smoke passed")
            break
        except OSError:
            time.sleep(0.1)
    else:
        raise RuntimeError("report server did not become ready")
finally:
    server.terminate()
    try:
        server.wait(timeout=10)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait()
    print(f"Report server reaped: PID={server.pid} exit={server.returncode}")
