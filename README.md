# Flowpipe Celery Adapter

Easily evaluate Flowpipe Graphs in Celery.

## Quickstart

1. Register your Flowpipe Nodes as Tasks in your Celery app:

```python
from celery import Celery

app = Celery('flowpipe-celery', broker='amqp://guest@localhost//')

# OPTIONAL: Import your flowpipe nodes here so they can be registered as tasks.
from my_flowpipe_nodes import *

# MANDATORY: Register a task for each of your flowpipe nodes.
# This also registers a fallback task for any unregistered node
from flowpipe_celery_adapter.utils import register_flowpipe_nodes_as_tasks
register_flowpipe_nodes_as_tasks(app, module=__name__)

# Now start your app
if __name__ == "__main__":
    app.start()
```

2. Now you can evaluate any Flowpipe Graph via Celery:

```python
import celery
import flowpipe

from flowpipe_celery_adapter import flowpipe_to_celery

# Create a Flowpipe Graph
graph = flowpipe.Graph()
Add = Add(graph=graph, a=1, b=1)

# Get an instance of your Celery app
app = Celery('flowpipe-celery', broker='amqp://guest@localhost//')

# Convert the Flowpipe Graph to a Celery Workflow
workflow = flowpipe_to_celery(celery_app, graph)

# Run it in Celery
workflow.apply_async()
```

## Explore the example app

The example app launches Celery with a Redis backend as well as Flower. The `./tests/test_evaluate_graph_in_celery.py` sends a Flowpipe Graph for evaluation to Celery.

1. Start the app.

```sh
cd example
docker-compose up
```

2. Once the app is up and running, run the test code. Look at the comments in the python file for details.

```sh
poetry install
poetry run python example/test_evaluate_graph_in_celery.py
```

3. Verify the results in the browser via Flower http://localhost:5555/tasks.
   Look for the result of the `nodes.nodes.Multiply`, and find the value of the `result` output plug, it should be 4.
