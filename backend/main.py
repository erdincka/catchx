from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import configure_logging
from routes import cluster, data, monitoring, code, mcp, settings as settings_routes

configure_logging()

app = FastAPI(title="CatchX Data Fabric API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(cluster.router, prefix="/api/cluster", tags=["cluster"])
app.include_router(data.router, prefix="/api/data", tags=["data"])
app.include_router(monitoring.router, prefix="/api/monitoring", tags=["monitoring"])
app.include_router(code.router, prefix="/api/code", tags=["code"])
app.include_router(settings_routes.router, prefix="/api/settings", tags=["settings"])
app.include_router(mcp.router, prefix="/api/mcp", tags=["mcp"])


@app.get("/health")
def health():
    return {"status": "ok"}
