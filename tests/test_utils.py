from typing import Union
from unittest import mock

import pytest
from flowpipe import Graph, Node, INode, InputPlug, OutputPlug
from flowpipe.node import FunctionNode
from flowpipe_celery_adapter import flowpipe_to_celery
from flowpipe_celery_adapter.utils import register_flowpipe_nodes_as_tasks


@Node(outputs=["result"])
def Add(a: Union[int, float], b: Union[int, float]) -> dict:
    """Add a and b"""
    return {"result": a + b}


class Multiply(INode):
    """Multipls a and b"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        InputPlug("a", self)
        InputPlug("b", self)
        OutputPlug("result", self)

    def compute(self, a, b):
        return {"result": a * b}


@pytest.mark.usefixtures("celery_session_app")
@pytest.mark.usefixtures("celery_session_worker")
@pytest.fixture(scope="session", autouse=True)
def celery_register_tasks(celery_session_app):
    def correct_module_name_for_node(node):
        if isinstance(node, FunctionNode):
            return f"tests.test_utils.{node.class_name}"
        return f"tests.test_utils.{node.__class__.__name__}"

    with mock.patch(
        "flowpipe_celery_adapter.utils.get_celery_task_name",
        side_effect=correct_module_name_for_node,
    ):
        register_flowpipe_nodes_as_tasks(celery_session_app, module=__name__)

    yield celery_session_app


@pytest.mark.usefixtures("celery_session_app")
@pytest.mark.usefixtures("celery_session_worker")
def test_flowpipe_to_celery_converts_graph_to_chord(celery_session_app):
    """
    This is the Flowpipe Graph:

        +-------------+          +---------------+
        |    Add1     |          |   Multiply    |
        |-------------|          |---------------|
        o a<1>        |     +--->o a<>           |
        o b<1>        |     |--->o b<>           |
        |    result<> o-----+    |      result<> o
        +-------------+     |    +---------------+
        +-------------+     |
        |    Add2     |     |
        |-------------|     |
        o a<1>        |     |
        o b<1>        |     |
        |    result<> o-----+
        +-------------+

    The resulting Celery workflow looks like this:

        +--------------+          +------------+
        |  Add1, Add2  o--------->o  Multiply  |
        +--------------+          +------------+

    The expected result is:

        (1 + 1) * (1 + 1) = 4
    """
    graph = Graph()
    add1 = Add(name="Add1", graph=graph, a=1, b=1)
    add2 = Add(name="Add2", graph=graph, a=1, b=1)
    multiply = Multiply(name="Multiply", graph=graph)
    add1.outputs["result"] >> multiply.inputs["a"]
    add2.outputs["result"] >> multiply.inputs["b"]

    workflow = flowpipe_to_celery(celery_session_app, graph)

    task = workflow.apply_async()
    result = task.get(timeout=5)

    assert result[0]["name"] == "Multiply"

    # Verify that the calculation that the Graph performs is correct
    assert result[0]["outputs"]["result"]["value"] == 4  # (1 + 1) * (1 + 1)
