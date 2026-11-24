"""Start a Temporal dev server through the Python SDK (downloads the dev-server binary on
first run) and keep it up until interrupted. For machines without Docker.

    python scripts/local_temporal.py --port 7233
"""

import argparse
import asyncio

from temporalio.testing import WorkflowEnvironment


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=7233)
    ap.add_argument("--db", default=".local-data/temporal.db", help='"" for in-memory')
    a = ap.parse_args()
    env = await WorkflowEnvironment.start_local(
        port=a.port,
        dev_server_database_filename=a.db or None,
        ui=False,
        download_dest_dir=".local-tools",
    )
    print(f"temporal dev server on localhost:{a.port}", flush=True)
    try:
        await asyncio.Event().wait()
    finally:
        await env.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
