"""FastAPI app entrypoint for standalone migration backend."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.analysis import router as analysis_router
from .api.connections import router as connections_router
from .api.health import router as health_router
from .api.integration import router as integration_router
from .api.migrations import router as migrations_router
from .config import settings
from .db import init_db


def create_app() -> FastAPI:
    app = FastAPI(title=settings.app_name, version=settings.app_version)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health_router, prefix=settings.api_prefix)
    app.include_router(connections_router, prefix=settings.api_prefix)
    app.include_router(integration_router, prefix=settings.api_prefix)
    app.include_router(analysis_router, prefix=settings.api_prefix)
    app.include_router(migrations_router, prefix=settings.api_prefix)

    @app.on_event("startup")
    def on_startup() -> None:
        init_db()

    return app


app = create_app()
