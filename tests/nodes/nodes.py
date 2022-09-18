from typing import Union
from flowpipe.node import Node


@Node(outputs=["result"])
def Add(a: Union[int, float], b: Union[int, float]) -> dict:
    """Add a and b"""
    return {"result": a + b}


@Node(outputs=["result"])
def Multiply(a: Union[int, float], b: Union[int, float]) -> dict:
    """Multipls a and b"""
    return {"result": a * b}
