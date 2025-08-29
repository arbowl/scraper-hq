""" Aggregator for local LLM-powered news aggregation."""

import asyncio
import importlib
import json
import os
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Optional

import httpx
import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse
from pydantic import BaseModel, Field


class Column(BaseModel):
    """A column in a table."""
    key: str
    label: str


class SortSpec(BaseModel):
    """A sort specification for a table."""
    key: str
    dir: str = Field("desc", pattern="^(asc|desc)$")


class TemplateDescriptor(BaseModel):
    """A template descriptor for a table."""
    template: str
    columns: list[Column]
    sort: Optional[SortSpec] = None
    max_rows: int = 200


class DataChunk(BaseModel):
    """A data chunk for a table."""
    template: str
    stream: str
    rows: list[dict[str, Any]]
    mode: str = Field("append", pattern="^(append|replace)$")
    ts: str


# ---------- Config ----------
class SourceConfig(BaseModel):
    """A source configuration."""
    module: str
    template: str
    stream: str
    interval_seconds: int = 60
    config: dict[str, Any] = {}


class AppConfig(BaseModel):
    """An application configuration."""
    templates: list[TemplateDescriptor]
    sources: list[SourceConfig]


# ---------- Simple bus for SSE fanout ----------
class Bus:
    """A simple bus for SSE fanout."""

    def __init__(self) -> None:
        self._subscribers: list[asyncio.Queue] = []
        self._lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue:
        """Subscribe to the bus."""
        q: asyncio.Queue = asyncio.Queue()
        async with self._lock:
            self._subscribers.append(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue) -> None:
        """Unsubscribe from the bus."""
        async with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    async def publish(self, message: dict[str, Any]) -> None:
        """Publish a message to the bus."""
        async with self._lock:
            for q in list(self._subscribers):
                try:
                    q.put_nowait(message)
                except asyncio.QueueFull:
                    pass


# ---------- App state ----------
class State:
    """The application state."""

    def __init__(self) -> None:
        self.cfg: Optional[AppConfig] = None
        self.templates: dict[str, TemplateDescriptor] = {}
        self.buffers: dict[
            tuple[str, str], Deque[dict[str, Any]]
        ] = defaultdict(deque)
        self.bus = Bus()
        self.tasks: list[asyncio.Task] = []
        self.queries_file = Path("queries.json")
        self.past_queries: list[str] = self._load_queries()

    def _load_queries(self) -> list[str]:
        """Load past queries from disk"""
        try:
            if self.queries_file.exists():
                with open(self.queries_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception:
            pass
        return []

    def _save_queries(self) -> None:
        """Save past queries to disk"""
        try:
            with open(self.queries_file, 'w', encoding='utf-8') as f:
                json.dump(self.past_queries, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def add_query(self, query: str) -> None:
        """Add a new query to history and save to disk"""
        if query.strip() and query not in self.past_queries:
            self.past_queries.insert(0, query)
            self.past_queries = self.past_queries[:100]
            self._save_queries()

    def get_queries(self) -> list[str]:
        """Get all past queries"""
        return self.past_queries.copy()

    def apply_chunk(self, chunk: DataChunk) -> None:
        """Apply a data chunk to the state."""
        key = (chunk.template, chunk.stream)
        td = self.templates.get(chunk.template)
        if not td:
            return
        buf = self.buffers[key]
        if chunk.mode == "replace":
            buf.clear()
        for row in chunk.rows:
            buf.append(row)
        while len(buf) > td.max_rows:
            buf.popleft()


state = State()


# ---------- Utilities ----------
ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
SOURCES_DIR = ROOT / "sources"
CONFIG_PATH = ROOT / "config.yaml"
OLLAMA_URL = os.environ.get(
    "OLLAMA_URL", "http://192.168.0.170:11434/api/generate"
)


def now_iso() -> str:
    """Get the current time in ISO format."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_config() -> AppConfig:
    """Load the application configuration."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    cfg = AppConfig(**raw)
    state.cfg = cfg
    state.templates = {t.template: t for t in cfg.templates}
    return cfg


async def run_source_loop(src: SourceConfig) -> None:
    """Periodically import and run the source's run(config) async generator."""
    module_path = f"sources.{src.module}"
    mod = importlib.import_module(module_path)
    if not hasattr(mod, "run"):
        print(f"[source:{src.module}] No run() found; skipping")
        return
    base_cfg = dict(src.config)
    base_cfg["template"] = src.template
    base_cfg["stream"] = src.stream
    print(f"[source:{src.module}] starting @ every {src.interval_seconds}s")
    while True:
        try:
            agen = mod.run(base_cfg)
            async for raw_chunk in agen:
                try:
                    chunk = DataChunk(**raw_chunk)
                except Exception as e:
                    print(f"[source:{src.module}] invalid chunk: {e}")
                    continue
                if chunk.template not in state.templates:
                    print(
                        f"[source:{src.module}] unknown template "
                        f"{chunk.template}"
                    )
                    continue
                state.apply_chunk(chunk)
                await state.bus.publish(chunk.model_dump())
        except Exception as e:
            print(f"[source:{src.module}] error: {e}")
        await asyncio.sleep(src.interval_seconds)


# ---------- FastAPI setup ----------
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup():
    """Start the application."""
    cfg = load_config()
    for src in cfg.sources:
        task = asyncio.create_task(run_source_loop(src))
        state.tasks.append(task)


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the static/index.html."""
    with open(STATIC_DIR / "index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/templates")
async def get_templates():
    """Get all templates."""
    return JSONResponse([t.model_dump() for t in state.templates.values()])


@app.get("/snapshot")
async def get_snapshot():
    """Return all current buffers keyed by template->stream."""
    out: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(dict)
    for (template, stream), buf in state.buffers.items():
        out[template][stream] = list(buf)
    return JSONResponse(out)


@app.get("/events")
async def sse(request: Request):
    """Server-Sent Events: pushes DataChunk JSON per message."""
    q = await state.bus.subscribe()

    async def event_gen():
        try:
            yield f"data: {json.dumps({'type':'hello','ts': now_iso()})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                payload = json.dumps(msg)
                yield f"data: {payload}\n\n"
        finally:
            await state.bus.unsubscribe(q)

    return StreamingResponse(event_gen(), media_type="text/event-stream")


class SummarizeBody(BaseModel):
    """A summarize body."""
    template: str
    stream: str
    limit: int = 50
    prompt: Optional[str] = None


def get_data(template: str, stream: str, limit: int) -> str:
    """Get formatted data for the specified template and stream."""
    key = (template, stream)
    rows = list(state.buffers.get(key, []))[-limit:]
    
    lines = [
        "You are a concise analyst. Summarize the key trends and "
        "notable items.",
        "Return bullet points (max 8).",
        "\nRows:\n",
    ]
    
    for i, r in enumerate(rows, 1):
        title = str(r.get("title", ""))[:200]
        score = r.get("score")
        ts = r.get("ts")
        url = r.get("url")
        lines.append(f"{i}. {title} | score={score} | ts={ts} | url={url}")
    
    return "\n".join(lines)


@app.post("/summarize")
async def summarize(request: SummarizeBody):
    """Summarize data using Ollama LLM with streaming response."""
    try:
        # Get data for the specified template and stream
        data = get_data(request.template, request.stream, request.limit)
        if not data:
            return {"text": "No data available for summarization."}
        
        # Prepare the prompt
        if request.prompt:
            prompt = f"{request.prompt}\n\nHere is the data to analyze:\n{data}"
        else:
            prompt = f"Please provide a comprehensive summary and analysis of the following data:\n{data}"
        
        # Stream the LLM response
        async def generate_stream():
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
                    response = await client.post(
                        OLLAMA_URL,
                        json={
                            "model": "nous-hermes2",
                            "prompt": prompt,
                            "stream": True
                        },
                        headers={"Content-Type": "application/json"}
                    )
                    
                    async for line in response.aiter_lines():
                        if line.strip():
                            try:
                                chunk = json.loads(line)
                                if "response" in chunk:
                                    yield f"data: {json.dumps({'token': chunk['response']})}\n\n"
                                if chunk.get("done", False):
                                    break
                            except json.JSONDecodeError:
                                continue
                                
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
        
        return StreamingResponse(
            generate_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache", 
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*"
            }
        )
        
    except Exception as e:
        return {"text": f"Error during summarization: {str(e)}"}


class QueryBody(BaseModel):
    """A query body."""
    query: str


@app.get("/queries")
async def get_queries():
    """Get all past queries."""
    return JSONResponse(state.get_queries())


@app.post("/queries")
async def add_query(body: QueryBody):
    """Add a new query to history"""
    state.add_query(body.query)
    return JSONResponse({"status": "ok"})


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=True)
