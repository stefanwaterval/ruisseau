"""Example DAG file for manual validation tests and documentation."""

from ruisseau.dag import DAG
from ruisseau.task import Task

dag = DAG(
    name="example",
    tasks=[Task(id="A"), Task(id="B", deps=("A",)), Task(id="C", deps=("B",))],
)
