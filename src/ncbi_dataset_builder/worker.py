from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .workflow import BuilderConfig, DatasetBuilder


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Execute one durable dataset task (normally from Slurm)"
    )
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--task-index", type=int, required=True)
    parser.add_argument("--processor", required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--email", default=os.environ.get("NCBI_EMAIL"))
    parser.add_argument("--retry-failed", action="store_true")
    arguments = parser.parse_args(argv)
    builder = DatasetBuilder(
        BuilderConfig(
            workspace=arguments.workspace,
            email=arguments.email,
            ncbi_api_key=os.environ.get("NCBI_API_KEY"),
            max_workers=1,
        )
    )
    plan = builder.load_plan(arguments.plan)
    outcome = builder.run_task(
        plan,
        arguments.task_index,
        arguments.processor,
        retry_failed=arguments.retry_failed,
    )
    print(
        json.dumps(
            {
                "task_id": outcome.task_id,
                "status": outcome.status,
                "error": outcome.error,
            }
        )
    )
    return 1 if outcome.status == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
