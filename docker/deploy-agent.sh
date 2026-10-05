#!/bin/bash
# Compatible deploy entry point; recovery/status are handled by the same journal.
set -euo pipefail
exec python3 -B "$(cd -- "$(dirname -- "$0")" && pwd)/deploy_agent.py" "$@"
