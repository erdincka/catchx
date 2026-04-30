import asyncio
import json
import logging

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from config import MON_REFRESH_INTERVAL
from store import ClusterConfig
from services.monitoring import collect_all_metrics

logger = logging.getLogger("routes.monitoring")

router = APIRouter()


@router.get("/metrics")
async def get_metrics(
    mapr_host: str = Query(...),
    mapr_user: str = Query(...),
    mapr_pass: str = Query(...),
):
    config = ClusterConfig(host=mapr_host, user=mapr_user, password=mapr_pass)
    metrics = await collect_all_metrics(config)
    return metrics


async def _sse_generator(config: ClusterConfig):
    while True:
        try:
            metrics = await collect_all_metrics(config)
            yield f"event: metrics\ndata: {json.dumps(metrics)}\n\n"
        except Exception as error:
            yield f"event: error\ndata: {json.dumps({'message': str(error)})}\n\n"
        yield "event: heartbeat\ndata: {}\n\n"
        await asyncio.sleep(MON_REFRESH_INTERVAL)


@router.get("/stream")
async def monitoring_stream(
    mapr_host: str = Query(...),
    mapr_user: str = Query(...),
    mapr_pass: str = Query(...),
):
    config = ClusterConfig(host=mapr_host, user=mapr_user, password=mapr_pass)
    return StreamingResponse(
        _sse_generator(config),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
