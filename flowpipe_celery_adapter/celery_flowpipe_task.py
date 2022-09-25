from celery import Task
from flowpipe import evaluator


class FlowpipeTask(Task):
    """Evaluate a Flowpipe Node in Celery."""

    def __init__(self, name: str, *args, **kwargs):
        """Initialize the Task with the path to a Flowpipe Node.

        Args:
            name: The path to a Flowpipe Node, e. g. `nodes.math.Add`.
        """
        super().__init__(*args, **kwargs)
        self.name = name

    def run(self, *args, **kwargs) -> dict:
        """Deserialize Node, assign upstream data, evaluate it, return result.

        Args:
            args: If there are upstream nodes, they are in the first element in args
            kwargs: The serialized node dump
        """
        node_dump = kwargs
        upstream_nodes = []
        if args:
            upstream_nodes = args[0]

        nodes_data = {node_dump["identifier"]: node_dump}
        for upstream_node in upstream_nodes:
            nodes_data[upstream_node["identifier"]] = upstream_node

        evaluator._evaluate_node_in_process(node_dump["identifier"], nodes_data)

        return nodes_data[node_dump["identifier"]]
