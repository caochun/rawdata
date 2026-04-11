"""A2A server — wraps Hermes AIAgent (with loom tools) as an A2A HTTP service.

Environment variables:
  LOOM_ROOT                    Path to the data repo (defaults to cwd).
  HERMES_HOME                  Path to .hermes dir (defaults to ~/.hermes).
  HERMES_ENABLE_PROJECT_PLUGINS  Set to "1" to load project plugins (loom tools).
  A2A_AGENT_NAME               Display name in the agent card.
  A2A_DESCRIPTION              One-line description in the agent card.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="a2a-agent")


def _ensure_hermes_on_path() -> None:
    """Add vendor/hermes-agent to sys.path so we can import from it."""
    adapter_dir = Path(__file__).resolve().parent          # .hermes/a2a_adapter/
    project_root = adapter_dir.parent.parent               # rawdata/
    hermes_dir = project_root / "vendor" / "hermes-agent"
    if hermes_dir.is_dir() and str(hermes_dir) not in sys.path:
        sys.path.insert(0, str(hermes_dir))


def _make_agent():
    """Create a Hermes AIAgent configured with loom plugin tools."""
    _ensure_hermes_on_path()

    from run_agent import AIAgent
    from hermes_cli.config import load_config
    from hermes_cli.runtime_provider import resolve_runtime_provider

    config = load_config()
    model_cfg = config.get("model")
    default_model = ""
    config_provider = None
    if isinstance(model_cfg, dict):
        default_model = str(model_cfg.get("default") or "")
        config_provider = model_cfg.get("provider")
    elif isinstance(model_cfg, str):
        default_model = model_cfg.strip()

    kwargs: dict = {
        "platform": "a2a",
        "enabled_toolsets": ["loom"],
        "quiet_mode": True,
        "model": default_model,
    }

    try:
        runtime = resolve_runtime_provider(requested=config_provider)
        kwargs.update({
            "provider": runtime.get("provider"),
            "api_mode": runtime.get("api_mode"),
            "base_url": runtime.get("base_url"),
            "api_key": runtime.get("api_key"),
        })
    except Exception:
        logger.debug("A2A agent falling back to default provider resolution", exc_info=True)

    agent = AIAgent(**kwargs)
    agent._print_fn = lambda *a, **kw: None  # suppress stdout noise
    return agent


def _extract_text(context) -> str:
    """Pull plain text from an A2A RequestContext."""
    try:
        from a2a.types import TextPart
        msg = context.message
        if msg is None:
            return ""
        texts = []
        for part in msg.parts or []:
            root = getattr(part, "root", part)
            if isinstance(root, TextPart):
                texts.append(root.text)
            elif hasattr(root, "text"):
                texts.append(str(root.text))
        return "\n".join(texts).strip()
    except Exception:
        logger.debug("Failed to extract message text", exc_info=True)
        return ""


class LoomA2AExecutor:
    """A2A AgentExecutor: runs queries through Hermes + loom tools."""

    def __init__(self):
        self._agent = _make_agent()
        self._history: list[dict] = []
        self._lock = asyncio.Lock()

    async def execute(self, context, event_queue) -> None:
        from a2a.server.tasks.task_updater import TaskUpdater
        from a2a.types import TaskState, TextPart

        updater = TaskUpdater(
            event_queue=event_queue,
            task_id=context.task_id,
            context_id=context.context_id,
        )
        await updater.update_status(TaskState.working)

        text = _extract_text(context)
        if not text:
            await updater.add_artifact(parts=[TextPart(text="(empty query)")])
            await updater.update_status(TaskState.completed, final=True)
            return

        history = self._history
        loop = asyncio.get_running_loop()

        def _run():
            return self._agent.run_conversation(
                user_message=text,
                conversation_history=history,
                task_id=context.task_id,
            )

        try:
            result = await loop.run_in_executor(_executor, _run)
        except Exception as e:
            logger.exception("Agent error for task %s", context.task_id)
            await updater.add_artifact(parts=[TextPart(text=f"Error: {e}")])
            await updater.update_status(TaskState.failed, final=True)
            return

        if result.get("messages"):
            async with self._lock:
                self._history = result["messages"]

        response = result.get("final_response", "")
        await updater.add_artifact(parts=[TextPart(text=response)])
        await updater.update_status(TaskState.completed, final=True)

    async def cancel(self, context, event_queue) -> None:
        from a2a.server.tasks.task_updater import TaskUpdater
        from a2a.types import TaskState
        updater = TaskUpdater(
            event_queue=event_queue,
            task_id=context.task_id,
            context_id=context.context_id,
        )
        await updater.update_status(TaskState.canceled, final=True)


def build_app(host: str = "0.0.0.0", port: int = 8100,
              name: str | None = None, description: str | None = None):
    """Build and return the A2A FastAPI app."""
    from a2a.server.agent_execution import AgentExecutor
    from a2a.server.apps import A2AFastAPIApplication
    from a2a.server.request_handlers.default_request_handler import DefaultRequestHandler
    from a2a.server.tasks.inmemory_task_store import InMemoryTaskStore
    from a2a.types import AgentCapabilities, AgentCard, AgentSkill

    agent_name = name or os.environ.get("A2A_AGENT_NAME", "loom-agent")
    agent_desc = description or os.environ.get(
        "A2A_DESCRIPTION",
        "A loom data agent. Query CSV-backed tables in natural language.",
    )

    card = AgentCard(
        name=agent_name,
        description=agent_desc,
        url=f"http://{host}:{port}",
        version="1.0.0",
        defaultInputModes=["text"],
        defaultOutputModes=["text"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[
            AgentSkill(
                id="loom_data",
                name="Loom Data Access",
                description=(
                    "Query, add, update and delete rows in loom CSV tables. "
                    "Supports filters, search, sort, pagination, and aggregations."
                ),
                tags=["data", "csv", "loom"],
                inputModes=["text"],
                outputModes=["text"],
            )
        ],
    )

    # Wrap our executor to satisfy the abstract base class
    class _Executor(AgentExecutor):
        def __init__(self):
            self._inner = LoomA2AExecutor()

        async def execute(self, context, event_queue):
            await self._inner.execute(context, event_queue)

        async def cancel(self, context, event_queue):
            await self._inner.cancel(context, event_queue)

    executor = _Executor()
    task_store = InMemoryTaskStore()
    handler = DefaultRequestHandler(agent_executor=executor, task_store=task_store)
    a2a_app = A2AFastAPIApplication(agent_card=card, http_handler=handler)
    return a2a_app.build()  # returns the FastAPI app (ASGI-compatible)


def run(host: str = "0.0.0.0", port: int = 8100,
        name: str | None = None, description: str | None = None,
        log_level: str = "info") -> None:
    """Start the A2A server (blocking)."""
    import uvicorn
    app = build_app(host=host, port=port, name=name, description=description)
    logger.info("loom A2A server starting on http://%s:%s", host, port)
    uvicorn.run(app, host=host, port=port, log_level=log_level)
