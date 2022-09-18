import inspect
import sys

import celery
import celery.canvas
from flowpipe import Graph, INode
from flowpipe.node import FunctionNode

UNREGISTERED_FLOWPIPE_NODE = "__UnregisteredFlowpipeNode__"
"""Fallback for flowpipe nodes that are not registered (for whatever reason)."""


def flowpipe_to_celery(celery_app: celery.Celery, graph: Graph) -> celery.canvas._chord:
    """Every row in the graph's evaluation grid converts to a group.
    The groups get chained.
    Please note that while this translation does maintain the correct order of
    execution, it is not a 1:1 translation of the flowpipe graph to celery.
    The reason is that Celery does not provide the same concepts that flowpipe
    does.
    Here is an example:

    Flowpipe Graph:

        +-----------+          +----------+
        |    n1     |          |    n3    |
        |-----------|          |----------|
        o in1<1>    |     +--->o in1<>    |
        o in2<2>    |     +--->o in2<>    |
        |     out<> o-----|    |    out<> o
        +-----------+     |    +----------+
        +-----------+     |
        |    n2     |     |
        |-----------|     |
        o in1<1>    |     |
        o in2<2>    |     |
        |     out<> o-----+
        +-----------+

    Resulting Celery Groups:

        +-----------+          +----------+
        |  n1, n2   o--------->o    n3    |
        +-----------+          +----------+
    """
    registered_task_names = celery.current_app.tasks.keys()
    groups = []
    for row in graph.evaluation_matrix:
        signatures = []
        for node in row:
            task_name = get_celery_task_name(node)
            # Fallback if the node is not registered as a celery task yet.
            if task_name not in registered_task_names:
                task_name = UNREGISTERED_FLOWPIPE_NODE
            signatures.append(celery_app.signature(task_name, kwargs=node.serialize()))
        groups.append(celery.group(signatures))
    workflow = groups[0]
    for group in groups[1:]:
        workflow = workflow | group
    return workflow


def get_celery_task_name(node: INode) -> str:
    """The task names of a flowpipe node is the "full module path".

    This how celery does it with the addition of FunctionNodes taking the
    module from the wrapped function, otherwise they would all be using
    `flowpipe.node.FunctionNode` as their task name.
    """
    if isinstance(node, FunctionNode):
        return f"{node.func.__module__}.{node.class_name}"
    return f"{node.__module__}.{node.class_name}"


def register_flowpipe_nodes_as_tasks(app: celery.Celery, module: str):
    """Register all Flowpipe Nodes from the given module as Celery task."""
    from flowpipe_celery_adapter import FlowpipeTask

    app.register_task(FlowpipeTask(UNREGISTERED_FLOWPIPE_NODE))
    for _, obj in inspect.getmembers(sys.modules[module]):
        if isinstance(obj, INode):
            app.register_task(FlowpipeTask(get_celery_task_name(obj)))
