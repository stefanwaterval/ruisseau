from ruisseau.dag import DAG
from ruisseau.task import Task


def assert_precedes(order, a, b):
    pos = {id: i for i, id in enumerate(order)}
    assert pos[a] < pos[b]


def test_topo_simple_chain():
    task1 = Task(id="A")
    task2 = Task(id="B", deps=("A",))
    task3 = Task(id="C", deps=("B",))

    dag = DAG(name="simple_chain_dag", tasks=[task1, task2, task3])
    order = dag.topological_order()

    assert set(order) == {"A", "B", "C"}
    assert_precedes(order, "A", "B")
    assert_precedes(order, "B", "C")
    assert len(order) == len({task.id for task in [task1, task2, task3]})


def test_topo_diamond():
    task1 = Task(id="A")
    task2 = Task(id="B", deps=("A",))
    task3 = Task(id="C", deps=("A",))
    task4 = Task(id="D", deps=("B", "C"))

    dag = DAG(name="diamond_dag", tasks=[task1, task2, task3, task4])
    order = dag.topological_order()

    assert set(order) == {"A", "B", "C", "D"}
    assert_precedes(order, "A", "B")
    assert_precedes(order, "A", "C")
    assert_precedes(order, "B", "D")
    assert_precedes(order, "C", "D")
    assert len(order) == len({task.id for task in [task1, task2, task3, task4]})


def test_topo_multiple_roots():
    task1 = Task(id="A")
    task2 = Task(id="B")
    task3 = Task(id="C", deps=("A", "B"))

    dag = DAG("multi_roots", tasks=[task1, task2, task3])
    order = dag.topological_order()

    assert set(order) == {"A", "B", "C"}
    assert_precedes(order, "A", "C")
    assert_precedes(order, "B", "C")
    assert len(order) == len({task.id for task in [task1, task2, task3]})
