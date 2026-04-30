# CatchX — Tilt dev workflow
# Run:  tilt up
# Stop: tilt down
#
# Both containers start via docker-compose. Source directories are mounted as
# volumes (docker-compose.dev.yaml) so edits are visible immediately:
#   - backend/  → uvicorn --reload picks up Python changes
#   - frontend/ → NiceGUI reload=true picks up Python changes
#
# Full image rebuilds only happen when requirements.txt changes.

docker_compose(
    ["docker-compose.yaml", "docker-compose.dev.yaml"],
    project_name="catchx",
)

# ── Backend ──────────────────────────────────────────────────────────────────

dc_resource(
    "backend",
    labels=["services"],
    # Trigger a full rebuild only when dependencies change
    trigger_mode=TRIGGER_MODE_AUTO,
)

# Reinstall deps inside the running container when requirements.txt changes,
# then restart so uvicorn picks up any newly installed packages.
local_resource(
    "backend-deps",
    cmd="docker exec catchx-backend pip install -q -r /app/backend/requirements.txt",
    deps=["backend/requirements.txt"],
    resource_deps=["backend"],
    labels=["deps"],
)

# ── Frontend ─────────────────────────────────────────────────────────────────

dc_resource(
    "frontend",
    labels=["services"],
    resource_deps=["backend"],
    trigger_mode=TRIGGER_MODE_AUTO,
)

local_resource(
    "frontend-deps",
    cmd="docker exec catchx-frontend pip install -q -r /app/frontend/requirements.txt",
    deps=["frontend/requirements.txt"],
    resource_deps=["frontend"],
    labels=["deps"],
)

# ── Links shown in the Tilt UI ───────────────────────────────────────────────

dc_resource("frontend", links=[
    link("http://localhost:4000", "Frontend (NiceGUI)"),
    link("http://localhost:4000/old", "Frontend /old"),
])

dc_resource("backend", links=[
    link("http://localhost:8000/docs", "Backend API docs"),
    link("http://localhost:8000/health", "Health check"),
])
