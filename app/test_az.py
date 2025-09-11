import os
import logging
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from botbuilder.core import (
    BotFrameworkAdapterSettings,
    BotFrameworkAdapter,
    TurnContext,
)
from botbuilder.schema import Activity, ActivityTypes

load_dotenv()

# FastAPI app
app = FastAPI()

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Bot credentials (from Azure App Registration)
APP_ID = os.getenv("MICROSOFT_APP_ID")
APP_PASSWORD = os.getenv("MICROSOFT_APP_PASSWORD")

# Adapter (handles auth, tokens, replies)
adapter_settings = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
adapter = BotFrameworkAdapter(adapter_settings)

# Handle incoming requests from Bot Framework
@app.post("/api/messages")
async def messages(req: Request):
    body = await req.json()
    logger.info("Received: %s", body)

    # Deserialize into an Activity
    activity = Activity().deserialize(body)
    auth_header = req.headers.get("Authorization", "")

    async def turn_handler(turn_context: TurnContext):
        if turn_context.activity.type == ActivityTypes.message and turn_context.activity.text:
            user_text = turn_context.activity.text.strip()
            await turn_context.send_activity(f"Echo: {user_text}")
        else:
            await turn_context.send_activity(f"Received activity of type {turn_context.activity.type}")

    try:
        await adapter.process_activity(activity, auth_header, turn_handler)
        return JSONResponse(status_code=200, content={})
    except Exception as e:
        logger.exception("Error in processing activity")
        return JSONResponse(status_code=500, content={"error": str(e)})

# Health check
@app.get("/")
def health():
    return {"status": "ok"}
