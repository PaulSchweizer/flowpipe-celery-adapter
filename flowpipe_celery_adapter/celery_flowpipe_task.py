from celery import Task
from flowpipe.graph import Graph
from flowpipe.node import INode


class FlowpipeTask(Task):
    def __init__(self, name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

    def run(self, *args, **kwargs) -> dict:
        """
        Args:
            args: If there are upstream nodes, they are in the first element in args
            kwargs: The serialized node dump
        """
        node_dump = kwargs
        if isinstance(kwargs, list):
            node_dump = kwargs[-1]

        upstream_nodes = []
        if args:
            upstream_nodes = args[0]

        graph = Graph()
        node = INode.from_json(node_dump)
        node.graph = graph

        # 1. Get input values from connected upstream nodes from upstream nodes
        for name, data in node_dump["inputs"].items():
            node.inputs[name].value = data["value"]
            for connection_node, plug in data["connections"].items():
                connected_data = [
                    n for n in upstream_nodes if n["identifier"] == connection_node
                ][0]
                node.inputs[name].value = connected_data["outputs"][plug]["value"]

        node.evaluate()

        # TODO: Only update the serialized data
        data = node.serialize()
        return data
