"""The Celery app."""
from celery import Celery
from flowpipe_celery_adapter.utils import register_flowpipe_nodes_as_tasks

# Import all the Flowpipe Nodes so we can register a Task for each Node.
# This step is optional, `register_flowpipe_nodes_as_tasks` will always add a
# fallback task for all unregistered Flowpipe Nodes.
from nodes.nodes import *

# TODO: This does not seem right, improve it:
app = Celery("flowpipe", broker="redis://redis:6379")
app.conf.update(
    CELERY_RESULT_BACKEND="redis://@redis:6379",
    CELERY_TASK_SERIALIZER="json",
    CELERY_RESULT_SERIALIZER="json",
)


# Register all Nodes that can be found in this module.
register_flowpipe_nodes_as_tasks(app, module=__name__)


if __name__ == "__main__":
    app.start()
