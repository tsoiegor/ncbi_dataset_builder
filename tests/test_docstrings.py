import ast
from pathlib import Path

import pytest

PACKAGE = Path(__file__).parents[1] / "src" / "ncbi_dataset_builder"
DEFINITION_TYPES = (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


def definitions():
    for path in PACKAGE.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, DEFINITION_TYPES):
                yield path, node


@pytest.mark.parametrize(
    ("path", "node"),
    list(definitions()),
    ids=lambda value: value.name,
)
def test_every_class_and_function_has_a_docstring(path, node):
    assert ast.get_docstring(node), f"Missing docstring at {path}:{node.lineno}"


def test_function_docstrings_name_their_arguments():
    missing = []
    for path, node in definitions():
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        arguments = [
            argument.arg
            for argument in (
                *node.args.posonlyargs,
                *node.args.args,
                *node.args.kwonlyargs,
            )
            if argument.arg not in {"self", "cls"}
        ]
        docstring = ast.get_docstring(node) or ""
        absent = [argument for argument in arguments if argument not in docstring]
        if absent:
            missing.append(f"{path}:{node.lineno} {node.name}: {absent}")
    assert not missing, "Docstrings do not name arguments:\n" + "\n".join(missing)


def test_dataclass_docstrings_name_constructor_fields():
    missing = []
    for path, node in definitions():
        if not isinstance(node, ast.ClassDef):
            continue
        is_dataclass = any(
            isinstance(decorator, ast.Name)
            and decorator.id == "dataclass"
            or isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Name)
            and decorator.func.id == "dataclass"
            for decorator in node.decorator_list
        )
        if not is_dataclass:
            continue
        docstring = ast.get_docstring(node) or ""
        fields = [
            item.target.id
            for item in node.body
            if isinstance(item, ast.AnnAssign)
            and isinstance(item.target, ast.Name)
            and "ClassVar" not in ast.unparse(item.annotation)
        ]
        absent = [field for field in fields if field not in docstring]
        if absent:
            missing.append(f"{path}:{node.lineno} {node.name}: {absent}")
    assert not missing, "Dataclass docstrings do not name fields:\n" + "\n".join(missing)
