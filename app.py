""" Aggregator for local LLM-powered news aggregation."""

import asyncio
import importlib
import json
import os
import time
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

from db import db


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
        try:
            db.migrate_from_json("queries.json")
        except Exception as e:
            print(f"Migration warning: {e}")

    def add_query(self, query: str, source: Optional[str] = None,
                  template: Optional[str] = None, stream: Optional[str] = None,
                  tags: Optional[list[str]] = None) -> int:
        """Add a new query to database"""
        if query.strip():
            return db.add_query(query, source, template, stream, tags)
        return 0

    def get_queries(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get all past queries from database"""
        return db.get_queries(limit=limit)

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
    
    try:
        db.insert_sample_tags()
    except Exception as e:
        print(f"Warning: Could not insert sample tags: {e}")


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the static/index.html."""
    with open(STATIC_DIR / "index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/templates")
async def get_templates():
    """Get all templates."""
    return JSONResponse([t.model_dump() for t in state.templates.values()])


@app.get("/tags")
async def get_tags():
    """Get all available tags."""
    try:
        tags = db.get_available_tags()
        return JSONResponse(tags)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


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
    template: Optional[str] = None
    stream: Optional[str] = None
    tags: Optional[list[str]] = None
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


def get_data_by_tags(tags: list[str], limit: int) -> str:
    """Get formatted data for items with specified tags from database."""
    try:
        queries = db.get_queries_by_tags(tags, limit // 2)
        outputs = db.get_outputs_by_tags(tags, limit // 2)
        
        lines = [
            "You are a concise analyst. Summarize the key trends and "
            "notable items from the tagged content.",
            "Return bullet points (max 8).",
            f"\nAnalyzing content tagged with: {', '.join(tags)}",
            "\nQueries:\n",
        ]
        
        for i, q in enumerate(queries[:limit // 2], 1):
            query_text = str(q.get("query", ""))[:200]
            template = q.get("template", "N/A")
            stream = q.get("stream", "N/A")
            ts = q.get("created_at", "N/A")
            lines.append(f"{i}. Query: {query_text} | template={template} | stream={stream} | ts={ts}")
        
        lines.append("\nOutputs:\n")
        
        for i, o in enumerate(outputs[:limit // 2], 1):
            output_text = str(o.get("output_text", ""))[:200]
            template = o.get("template", "N/A")
            stream = o.get("stream", "N/A")
            ts = o.get("created_at", "N/A")
            lines.append(f"{i}. Output: {output_text} | template={template} | stream={stream} | ts={ts}")
        
        return "\n".join(lines)
        
    except Exception as e:
        return f"Error retrieving tagged data: {str(e)}"


@app.post("/summarize")
async def summarize(request: SummarizeBody):
    """Summarize data using Ollama LLM with streaming response."""
    try:
        if request.tags:
            data = get_data_by_tags(request.tags, request.limit)
            source_info = f"tags: {', '.join(request.tags)}"
        elif request.template and request.stream:
            data = get_data(request.template, request.stream, request.limit)
            source_info = f"template: {request.template}, stream: {request.stream}"
        else:
            return {"text": "Either tags or template+stream must be provided for summarization."}
        
        if not data or data.startswith("Error"):
            return {"text": "No data available for summarization."}
        
        if request.prompt:
            prompt = f"{request.prompt}\n\nHere is the data to analyze:\n{data}"
        else:
            prompt = f"Please provide a comprehensive summary and analysis of the following data:\n{data}"
        
        query_id = state.add_query(
            request.prompt or "Summarize data", 
            source="summarize_endpoint",
            template=request.template,
            stream=request.stream,
            tags=request.tags or ["summarization", "data_analysis"]
        )
        
        async def generate_stream():
            start_time = time.time()
            full_response = []
            
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
                                    token = chunk['response']
                                    full_response.append(token)
                                    yield f"data: {json.dumps({'token': token})}\n\n"
                                if chunk.get("done", False):
                                    break
                            except json.JSONDecodeError:
                                continue
                                
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
            finally:
                if full_response and query_id:
                    processing_time = int((time.time() - start_time) * 1000)
                    output_text = ''.join(full_response)
                    
                    try:
                        db.add_output(
                            query_id=query_id,
                            output_text=output_text,
                            model_name="nous-hermes2",
                            prompt_used=prompt,
                            source="summarize_endpoint",
                            template=request.template,
                            stream=request.stream,
                            tags=request.tags or ["summarization", "data_analysis"],
                            processing_time_ms=processing_time,
                            metadata={
                                "template": request.template,
                                "stream": request.stream,
                                "tags": request.tags,
                                "limit": request.limit,
                                "source_info": source_info,
                                "data_rows": len(data.split('\n')) - 4
                            }
                        )
                    except Exception as db_error:
                        print(f"Failed to store output in database: {db_error}")
        
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


class BatchLLMRequest(BaseModel):
    """A batch LLM request."""
    query: str
    scope: str = Field("current", pattern="^(current|all|tags)$")
    template: Optional[str] = None
    stream: Optional[str] = None
    tags: Optional[list[str]] = None
    limit: int = 50


@app.get("/queries")
async def get_queries():
    """Get all past queries."""
    return JSONResponse(state.get_queries())


@app.post("/queries")
async def add_query(body: QueryBody):
    """Add a new query to history"""
    query_id = state.add_query(body.query)
    return JSONResponse({"status": "ok", "query_id": query_id})


@app.get("/db/stats")
async def get_database_stats():
    """Get database statistics."""
    try:
        stats = db.get_stats()
        return JSONResponse(stats)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/db/queries/{query_id}")
async def get_query_details(query_id: int):
    """Get a specific query with all its outputs."""
    try:
        query_data = db.get_query_with_outputs(query_id)
        if query_data:
            return JSONResponse(query_data)
        else:
            return JSONResponse({"error": "Query not found"}, status_code=404)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/db/search")
async def search_queries(q: str, limit: int = 50):
    """Search queries by text content."""
    try:
        results = db.search_queries(q, limit=limit)
        return JSONResponse(results)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/batch-results/{batch_id}")
async def get_batch_results(batch_id: int):
    """Get detailed results for a batch LLM analysis."""
    try:
        # Get the batch query with all its outputs
        query_with_outputs = db.get_query_with_outputs(batch_id)
        if not query_with_outputs:
            return JSONResponse({"error": "Batch query not found"}, status_code=404)
        
        # Extract outputs from the query data
        outputs = query_with_outputs.get('outputs', [])
        if not outputs:
            return JSONResponse({"error": "No outputs found for this batch"}, status_code=404)
        
        # Format the results
        results = []
        for output in outputs:
            metadata = output.get('metadata', {})
            # Parse metadata if it's stored as JSON string
            if isinstance(metadata, str):
                try:
                    import json
                    metadata = json.loads(metadata)
                except:
                    metadata = {}
            
            # Check if this output has an error
            if output.get('error'):
                results.append({
                    "item_index": metadata.get('item_index', 0),
                    "template": output.get('template', 'Unknown'),
                    "stream": output.get('stream', 'Unknown'),
                    "error": output.get('error'),
                    "data": metadata.get('original_data', {})
                })
            else:
                results.append({
                    "item_index": metadata.get('item_index', 0),
                    "template": output.get('template', 'Unknown'),
                    "stream": output.get('stream', 'Unknown'),
                    "processing_time_ms": output.get('processing_time_ms', 0),
                    "analysis": output.get('output_text', ''),
                    "data": metadata.get('original_data', {})
                })
        
        # Sort by item index
        results.sort(key=lambda x: x.get('item_index', 0))
        
        # Generate serialized output
        serialized_output = generate_serialized_output(query_with_outputs.get('query', ''), results)
        
        return JSONResponse({
            "batch_id": batch_id,
            "query": query_with_outputs.get('query', ''),
            "total_items": len(results),
            "results": results,
            "serialized_output": serialized_output
        })
        
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/test-endpoint")
async def test_endpoint():
    """Test endpoint to verify server is working."""
    return {"message": "Test endpoint working!"}

@app.post("/batch-llm")
async def batch_llm_analysis(request: BatchLLMRequest):
    """Run LLM analysis on multiple items based on scope."""
    try:
        items_to_process = []
        
        if request.scope == "current":
            if request.template:
                template_items = []
                for (template, stream), buf in state.buffers.items():
                    if template == request.template:
                        template_items.append((stream, buf))
                
                if not template_items:
                    return JSONResponse(
                        {"error": "No data available for the current template"}, 
                        status_code=400
                    )
                
                all_items = []
                for stream_name, stream_data in template_items:
                    stream_items = list(stream_data)[-request.limit:]
                    for row in stream_items:
                        all_items.append({
                            "template": request.template,
                            "stream": stream_name,
                            "data": row
                        })
                
                items_to_process = sorted(all_items, key=lambda x: x["data"].get("ts", ""), reverse=True)[:request.limit]
            else:
                return JSONResponse(
                    {"error": "Template required for current scope"}, 
                    status_code=400
                )
        elif request.scope == "tags":
            if request.tags:
                for (template, stream), buf in state.buffers.items():
                    for row in buf:
                        if row.get("tags") and set(row["tags"]).intersection(request.tags):
                            items_to_process.append({
                                "template": template,
                                "stream": stream,
                                "data": row
                            })
                
                if not items_to_process:
                    return JSONResponse(
                        {"error": f"No data available for tags: {', '.join(request.tags)}"},
                        status_code=400
                    )
                
                # Sort by timestamp (newest first) and apply limit
                items_to_process = sorted(items_to_process, key=lambda x: x["data"].get("ts", ""), reverse=True)[:request.limit]
            else:
                return JSONResponse(
                    {"error": "Tags required for current scope"},
                    status_code=400
                )
        else:
            for (template, stream), buf in state.buffers.items():
                rows = list(buf)[-request.limit:]
                for row in rows:
                    items_to_process.append({
                        "template": template,
                        "stream": stream,
                        "data": row
                    })
        
        if not items_to_process:
            return JSONResponse({"error": "No items to process"}, status_code=400)
        
        batch_query_id = state.add_query(
            request.query,
            source="batch_llm_analysis",
            template=request.template if request.scope == "current" else "all_feeds",
            stream=request.stream if request.scope == "current" else "all_feeds",
            tags=request.tags or ["batch_analysis", "llm_processing"]
        )
        
        results = []
        total_items = len(items_to_process)
        
        for i, item in enumerate(items_to_process):
            try:
                data_text = format_item_for_llm(item["data"], item["template"])
                
                prompt = f"{request.query}\n\nAnalyze this item:\n{data_text}"
                
                start_time = time.time()
                async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
                    response = await client.post(
                        OLLAMA_URL,
                        json={
                            "model": "nous-hermes2",
                            "prompt": prompt,
                            "stream": False
                        },
                        headers={"Content-Type": "application/json"}
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        output_text = result.get("response", "")
                        processing_time = int((time.time() - start_time) * 1000)
                        
                        db.add_output(
                            query_id=batch_query_id,
                            output_text=output_text,
                            model_name="nous-hermes2",
                            prompt_used=prompt,
                            source="batch_llm_analysis",
                            template=item["template"],
                            stream=item["stream"],
                            tags=request.tags or ["batch_analysis", "llm_processing"],
                            processing_time_ms=processing_time,
                            metadata={
                                "item_index": i,
                                "total_items": total_items,
                                "template": item["template"],
                                "stream": item["stream"],
                                "original_data": item["data"]
                            }
                        )
                        
                        results.append({
                            "item_index": i,
                            "template": item["template"],
                            "stream": item["stream"],
                            "data": item["data"],
                            "analysis": output_text,
                            "processing_time_ms": processing_time
                        })
                    else:
                        results.append({
                            "item_index": i,
                            "template": item["template"],
                            "stream": item["stream"],
                            "data": item["data"],
                            "error": f"LLM request failed: {response.status_code}"
                        })
                        
            except Exception as e:
                results.append({
                    "item_index": i,
                    "template": item["template"],
                    "stream": item["stream"],
                    "data": item["data"],
                    "error": str(e)
                })
        
        serialized_output = generate_serialized_output(request.query, results)
        
        return JSONResponse({
            "batch_query_id": batch_query_id,
            "total_items": total_items,
            "processed_items": len(results),
            "results": results,
            "serialized_output": serialized_output
        })
        
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/batch-llm-stream")
async def batch_llm_analysis_stream(request: BatchLLMRequest):
    """Run LLM analysis with real-time progress streaming."""
    async def generate():
        try:
            items_to_process = []
            
            if request.scope == "current":
                if request.template:
                    template_items = []
                    for (template, stream), buf in state.buffers.items():
                        if template == request.template:
                            template_items.append((stream, buf))
                    
                    if not template_items:
                        error_msg = f'No data available for template "{request.template}". Available templates: {list(set(key[0] for key in state.buffers.keys()))}'
                        yield f"data: {json.dumps({'type': 'error', 'error': error_msg})}\n\n"
                        return
                    
                    all_items = []
                    for stream_name, stream_data in template_items:
                        stream_items = list(stream_data)[-request.limit:]
                        for row in stream_items:
                            all_items.append({
                                "template": request.template,
                                "stream": stream_name,
                                "data": row
                            })
                    
                    items_to_process = sorted(all_items, key=lambda x: x["data"].get("ts", ""), reverse=True)[:request.limit]
                else:
                    yield f"data: {json.dumps({'type': 'error', 'error': 'Template required for current scope'})}\n\n"
                    return
            elif request.scope == "tags":
                if request.tags:
                    try:
                        with open("config.yaml", 'r', encoding='utf-8') as f:
                            import yaml
                            config = yaml.safe_load(f)
                        
                        source_tags = {}
                        if 'sources' in config:
                            for source in config['sources']:
                                if 'template' in source and 'stream' in source and 'tags' in source:
                                    key = (source['template'], source['stream'])
                                    source_tags[key] = source['tags']
                        
                        for (template, stream), buf in state.buffers.items():
                            source_key = (template, stream)
                            if source_key in source_tags:
                                source_tag_list = source_tags[source_key]
                                if set(request.tags).intersection(source_tag_list):
                                    for row in buf:
                                        items_to_process.append({
                                            "template": template,
                                            "stream": stream,
                                            "data": row
                                        })
                        
                        print(f"DEBUG: Tags scope - collected {len(items_to_process)} items before limit, limit={request.limit}")
                        
                        if not items_to_process:
                            error_msg = f'No data available for tags: {", ".join(request.tags)}'
                            yield f"data: {json.dumps({'type': 'error', 'error': error_msg})}\n\n"
                            return
                        
                        # Sort by timestamp (newest first) and apply limit
                        items_to_process = sorted(items_to_process, key=lambda x: x["data"].get("ts", ""), reverse=True)[:request.limit]
                        print(f"DEBUG: Tags scope - after limit: {len(items_to_process)} items")
                    except Exception as e:
                        error_msg = f'Failed to load config for tag filtering: {str(e)}'
                        yield f"data: {json.dumps({'type': 'error', 'error': error_msg})}\n\n"
                        return
                else:
                    yield f"data: {json.dumps({'type': 'error', 'error': 'Tags required for current scope'})}\n\n"
                    return
            else:
                for (template, stream), buf in state.buffers.items():
                    rows = list(buf)[-request.limit:]
                    for row in rows:
                        items_to_process.append({
                            "template": template,
                            "stream": stream,
                            "data": row
                        })
            
            if not items_to_process:
                yield f"data: {json.dumps({'type': 'error', 'error': 'No items to process'})}\n\n"
                return
            
            batch_query_id = state.add_query(
                request.query,
                source="batch_llm_analysis_stream",
                template=request.template if request.scope == "current" else "all_feeds",
                stream=request.stream if request.scope == "current" else "all_feeds",
                tags=request.tags or ["batch_analysis", "llm_processing", "streaming"]
            )
            
            yield f"data: {json.dumps({'type': 'progress', 'current': 0, 'total': len(items_to_process), 'message': 'Starting analysis...'})}\n\n"
            
            results = []
            total_items = len(items_to_process)
            
            for i, item in enumerate(items_to_process):
                try:
                    progress = int(((i + 1) / total_items) * 100)
                    message = f"Processing item {i + 1}/{total_items} ({item['template']}/{item['stream']})"
                    yield f"data: {json.dumps({'type': 'progress', 'current': i + 1, 'total': total_items, 'progress': progress, 'message': message})}\n\n"
                    
                    data_text = format_item_for_llm(item["data"], item["template"])
                    
                    prompt = f"{request.query}\n\nAnalyze this item:\n{data_text}"
                    
                    start_time = time.time()
                    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
                        response = await client.post(
                            OLLAMA_URL,
                            json={
                                "model": "nous-hermes2",
                                "prompt": prompt,
                                "stream": False
                            },
                            headers={"Content-Type": "application/json"}
                        )
                        
                        if response.status_code == 200:
                            result = response.json()
                            output_text = result.get("response", "")
                            processing_time = int((time.time() - start_time) * 1000)
                            
                            db.add_output(
                                query_id=batch_query_id,
                                output_text=output_text,
                                model_name="nous-hermes2",
                                prompt_used=prompt,
                                source="batch_llm_analysis_stream",
                                template=item["template"],
                                stream=item["stream"],
                                tags=request.tags or ["batch_analysis", "llm_processing", "streaming"],
                                processing_time_ms=processing_time,
                                metadata={
                                    "item_index": i,
                                    "total_items": total_items,
                                    "template": item["template"],
                                    "stream": item["stream"],
                                    "original_data": item["data"]
                                }
                            )
                            
                            results.append({
                                "item_index": i,
                                "template": item["template"],
                                "stream": item["stream"],
                                "data": item["data"],
                                "analysis": output_text,
                                "processing_time_ms": processing_time
                            })
                            
                            yield f"data: {json.dumps({'type': 'item_complete', 'item_index': i, 'template': item['template'], 'stream': item['stream'], 'processing_time': processing_time})}\n\n"
                        else:
                            results.append({
                                "item_index": i,
                                "template": item["template"],
                                "stream": item["stream"],
                                "data": item["data"],
                                "error": f"LLM request failed: {response.status_code}"
                            })
                            
                            yield f"data: {json.dumps({'type': 'item_error', 'item_index': i, 'error': f'LLM request failed: {response.status_code}'})}\n\n"
                            
                except Exception as e:
                    results.append({
                        "item_index": i,
                        "template": item["template"],
                        "stream": item["stream"],
                        "data": item["data"],
                        "error": str(e)
                    })
                    
                    yield f"data: {json.dumps({'type': 'item_error', 'item_index': i, 'error': str(e)})}\n\n"
            
            serialized_output = generate_serialized_output(request.query, results)
            
            completion_data = {
                'type': 'complete', 
                'query': request.query, 
                'batch_query_id': batch_query_id, 
                'total_items': total_items, 
                'processed_items': len(results)
            }
            yield f"data: {json.dumps(completion_data)}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\n\n"
    
    return StreamingResponse(generate(), media_type="text/event-stream")


def format_item_for_llm(item_data: dict, template: str) -> str:
    """Format item data for LLM analysis."""
    if template == "subreddit":
        return f"Title: {item_data.get('title', 'N/A')}\nScore: {item_data.get('score', 'N/A')}\nTime: {item_data.get('ts', 'N/A')}\nURL: {item_data.get('url', 'N/A')}"
    elif template == "rss":
        return f"Title: {item_data.get('title', 'N/A')}\nSource: {item_data.get('source', 'N/A')}\nTime: {item_data.get('ts', 'N/A')}\nURL: {item_data.get('url', 'N/A')}"
    elif template == "legistar_meetings":
        return f"Title: {item_data.get('title', 'N/A')}\nBody: {item_data.get('body_name', 'N/A')}\nLocation: {item_data.get('location', 'N/A')}\nTime: {item_data.get('meeting_time', 'N/A')}\nURL: {item_data.get('url', 'N/A')}"
    else:
        return str(item_data)


def generate_serialized_output(query: str, results: list) -> str:
    """Generate a serialized output string for further LLM analysis."""
    lines = [
        f"# LLM Analysis Results for: {query}",
        f"Generated at: {datetime.now(timezone.utc).isoformat()}",
        f"Total items analyzed: {len(results)}",
        "",
        "## Analysis Results:",
        ""
    ]
    
    for result in results:
        if "error" in result:
            lines.append(f"### Item {result['item_index'] + 1} - ERROR")
            lines.append(f"**Template:** {result['template']}")
            lines.append(f"**Stream:** {result['stream']}")
            lines.append(f"**Error:** {result['error']}")
            lines.append("")
        else:
            lines.append(f"### Item {result['item_index'] + 1}")
            lines.append(f"**Template:** {result['template']}")
            lines.append(f"**Stream:** {result['stream']}")
            lines.append(f"**Processing Time:** {result['processing_time_ms']}ms")
            lines.append("")
            lines.append("**Original Data:**")
            lines.append(f"- Title: {result['data'].get('title', 'N/A')}")
            lines.append(f"- URL: {result['data'].get('url', 'N/A')}")
            lines.append(f"- Time: {result['data'].get('ts', 'N/A')}")
            lines.append("")
            lines.append("**LLM Analysis:**")
            lines.append(result['analysis'])
            lines.append("")
            lines.append("---")
            lines.append("")
    
    return "\n".join(lines)


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8082, reload=True)
