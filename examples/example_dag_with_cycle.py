"""Example DAG file with loop for manual validation tests and documentation."""

from ruisseau.dag import DAG
from ruisseau.task import Task

dag = DAG(
    name="example",
    tasks=[
        Task(id="A", deps=("B",)),
        Task(id="B", deps=("A",)),
    ],
)
