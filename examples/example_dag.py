"""Example DAG file for manual validation tests and documentation."""

from ruisseau.dag import DAG
from ruisseau.task import Task

dag = DAG(
    name="example",
    tasks=[Task(id="A"), Task(id="B", deps=("A",)), Task(id="C", deps=("B",))],
)


def run_A():
    print("Running A")


def run_B():
    print("Running B")


def run_C():
    print("Running C")


runnables = {
    "A": run_A,
    "B": run_B,
    "C": run_C,
}
