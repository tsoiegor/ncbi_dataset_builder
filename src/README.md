# Source tree

This directory uses the standard Python `src` layout. The installable package is
[`ncbi_dataset_builder`](ncbi_dataset_builder/README.md). Keeping importable
source below `src/` prevents tests from accidentally importing the checkout
instead of the installed package.

Do not add `src` to production `PYTHONPATH` as a substitute for installation;
use `python -m pip install .` or the editable `python -m pip install -e
".[dev,progress]"` form documented in the project [README](../README.md).
