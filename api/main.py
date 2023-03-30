from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

from api.db import read_client, check_weather, insert_weather
from base.db_physical import MongoRouter
from demo.D00_init_server_setup import ROUTERS

app = FastAPI()


def get_router() -> MongoRouter:
    for router in ROUTERS:
        if router.healthy():
            return router
    raise HTTPException(status_code=500, detail="No routers online")


@app.post("/weather/get")
async def get_weather(loc: dict):
    router = get_router()
    return check_weather(router, loc["lon"], loc["lat"])


@app.post("/weather/post")
async def post_weather(data: dict):
    router = get_router()
    result = insert_weather(router, data)
    response = {
        "router": router.container_name,
        "_id": str(result.inserted_id)
    }

    return JSONResponse(content=jsonable_encoder(response))

