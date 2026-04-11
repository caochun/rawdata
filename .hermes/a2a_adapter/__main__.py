"""Entry point: python -m .hermes.a2a_adapter  (or called by hermes.sh --a2a)."""

import argparse
import logging
import sys
from pathlib import Path


def _setup_logging(level: str = "info") -> None:
    logging.basicConfig(
        stream=sys.stderr,
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for noisy in ("httpx", "httpcore", "openai", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def _load_env() -> None:
    try:
        from hermes_cli.env_loader import load_hermes_dotenv
        from hermes_constants import get_hermes_home
        load_hermes_dotenv(hermes_home=get_hermes_home())
    except Exception:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Start loom A2A server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument("--name", default=None, help="Agent name in card")
    parser.add_argument("--description", default=None, help="Agent description")
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args()

    _setup_logging(args.log_level)
    _load_env()

    # Ensure hermes-agent is importable
    hermes_dir = Path(__file__).resolve().parent.parent.parent / "vendor" / "hermes-agent"
    if hermes_dir.is_dir() and str(hermes_dir) not in sys.path:
        sys.path.insert(0, str(hermes_dir))

    # Add adapter dir to path so we can import server without relative import
    adapter_dir = str(Path(__file__).resolve().parent)
    if adapter_dir not in sys.path:
        sys.path.insert(0, adapter_dir)

    from server import run
    run(
        host=args.host,
        port=args.port,
        name=args.name,
        description=args.description,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
