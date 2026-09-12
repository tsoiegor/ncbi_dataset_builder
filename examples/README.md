# Examples

- [`custom_processor.py`](custom_processor.py): minimal IDE-documented processor
  callable for a simple server.
- [`local_execution.py`](local_execution.py): 100-CPU local queue with a free
  storage reserve.
- [`slurm_single_node.py`](slurm_single_node.py): one Slurm allocation.
- [`slurm_distributed.py`](slurm_distributed.py): coordinator plus sample jobs.
- [`catalog_transform.py`](catalog_transform.py): audited Polars transformation.

The paths, email, and API key are illustrative. The API key is deliberately
fake. Copy an example and change its values before running it.

Install the checkout first:

```bash
python -m pip install -e ".[progress]"
```

Then run a local example from the project root with `python
examples/local_execution.py`. The Slurm examples call `sbatch`; add
`submit=False` to their `builder.submit_slurm(...)` call when you only want to
generate and inspect the coordinator script. `custom_processor.py` can be used
on Slurm as `custom_processor:process_sample` when the `examples` directory is
on compute-node `PYTHONPATH`; for a real deployment, place it in an installed
Python package.

Both catalog sources are demonstrated: `local_execution.py` loads RunInfo CSV,
while `slurm_distributed.py` fetches directly from NCBI. Either
`DatasetBuilder.load_runs()` or `fetch_runs()` can be substituted in every
workflow.
