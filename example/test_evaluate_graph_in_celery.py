import json
import os
import sys

import celery
from flowpipe import Graph

from nodes.nodes import Add, Multiply

# Lazy way to get the flowpipe_celery_adapter imported
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from flowpipe_celery_adapter import flowpipe_to_celery


# Get an instance of your Celery app
celery_app = celery.Celery("app", broker="redis://localhost:6379")
celery_app.conf.update(
    CELERY_RESULT_BACKEND="redis://localhost:6379",
    CELERY_TASK_SERIALIZER="json",
    CELERY_RESULT_SERIALIZER="json",
)

# IMPORTANT: This import has be here after the definition of the celery app.
# Otherwise the adapter can not get the registered tasks.
# I do not understand why this is necessary. Maybe someone knows.
import app.app


def main():
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
    # Create a Flowpipe Graph
    graph = Graph()
    add1 = Add(name="Add1", graph=graph, a=1, b=1)
    add2 = Add(name="Add2", graph=graph, a=1, b=1)
    multiply = Multiply(name="Multiply", graph=graph)
    add1.outputs["result"] >> multiply.inputs["a"]
    add2.outputs["result"] >> multiply.inputs["b"]

    # Convert it to a Celery Workflow
    workflow = flowpipe_to_celery(celery_app, graph)

    # Run it in Celery
    task = workflow.apply_async()

    # Inspect the result
    result = task.get()
    # The result is the nodes of the last group.
    # In this case it is only the `Multiply` node.
    print(json.dumps(result, indent=2))
    assert result[0]["name"] == "Multiply"

    # Verify that the calculation that the Graph performs is correct
    assert result[0]["outputs"]["result"]["value"] == 4  # (1 + 1) * (1 + 1)


if __name__ == "__main__":
    main()
