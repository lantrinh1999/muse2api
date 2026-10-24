from fastapi import APIRouter

from . import admin, anthropic, chat, dashboard, health, images, media, models, responses, videos


def build_router() -> APIRouter:
    router = APIRouter()
    for module in (health, models, chat, anthropic, responses, images, videos, media, admin, dashboard):
        router.include_router(module.router)
    return router
