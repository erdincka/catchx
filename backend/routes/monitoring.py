import asyncio
import json
import logging

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from config import MON_REFRESH_INTERVAL
from store import ClusterConfig, get_cluster_config
from services.monitoring import collect_all_metrics

logger = logging.getLogger("routes.monitoring")

router = APIRouter()


@router.get("/metrics")
async def get_metrics(config: ClusterConfig = Depends(get_cluster_config)):
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
async def monitoring_stream(config: ClusterConfig = Depends(get_cluster_config)):
    return StreamingResponse(
        _sse_generator(config),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
