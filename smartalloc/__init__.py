"""
SMarTalloc
==========

A collection of utility functions for using Microsoft's Z3 theorem prover to
find the maximal allocation of limited resources given a prioritized list of
discrete tasks to be executed.

Resources and Tasks
-------------------
``smartalloc`` is designed to be as thin of a wrapper around the Z3 library as
possible, so it does not define classes called "Resource" or "Task."

Instead, Z3 primitives, such as Int or Real, are considered the resources, and
tasks that they are allocated for are represented by (collections of) Z3
constraints.

Allocation
----------
The main workhorse of ``smartalloc`` is the ``allocate()`` function.  It takes
two arguments: the intrinsic constraints on the resources, and a list of the
constraints required for each task.

Intrinsic resource constraints are applied immediately, and if the system
already has no solution then an exception will be raised.  After this, the
constraints for each task are added to the system.  If the system is still
satisfiable then the task is considered "worked" and the next task will be
added.  If the system is not satisfiable, that task will be removed from the
system before moving to the next.  All tasks provided will be attempted to be
added to the system; the ordering of the tasks determines which tasks get to
claim their necessary resources first.

The ``allocate()`` function returns a tuple of two items.  The first is the
allocation description object (in the form of a Z3 model).  The value of a
resource variable can be retrieved from it by using it like a dictionary with
the resource variable as the key.  The second is a list of the indices of the
tasks from the input are "worked" in the allocation result.
"""
from uuid import uuid4 as _uuid4
from z3 import (If, And as _Z3And, Or as _Z3Or, Solver as _Z3Solver,
                Bool as _Z3Bool, Int as _Z3Int, Real as _Z3Real, Sum as _Z3Sum)


__version__ = '0.0.0'


__all__ = [
    'InvalidResourceConstraints',
    'Int',
    'Real',
    'If',
    'all',
    'any',
    'allocate',
    'constrain_within_range',
    'sum',
]


class InvalidResourceConstraints(Exception):
    """
    Exception thrown when no resource allocation can satisfy basic resource
    constraints (before considering tasking constraints).
    """
    pass


def Bool(name=None):
    """
    Creates a resource variable on the domain of Boolean values (true or false).

    Parameters
    ----------
    name : str, optional
        Name of the variable.  All variable objects with the same name and
        domain reference the same modeled variable.  Default's to a randomly
        generated UUID.

    Returns
    -------
    boolean-valued resource variable for use in allocation constraints
    """
    name = str(_uuid4()) if name is None else name
    return _Z3Bool(name)


def Int(name=None):
    """
    Creates a resource variable on the domain of integers.

    Parameters
    ----------
    name : str, optional
        Name of the variable.  All variable objects with the same name and
        domain reference the same modeled variable.  Default's to a randomly
        generated UUID.

    Returns
    -------
    integer-valued resource variable for use in allocation constraints
    """
    name = str(_uuid4()) if name is None else name
    return _Z3Int(name)


def Real(name=None):
    """
    Creates a resource variable on the domain of real numbers.

    Parameters
    ----------
    name : str, optional
        Name of the variable.  All variable objects with the same name and
        domain reference the same modeled variable.  Default's to a randomly
        generated UUID.

    Returns
    -------
    real-valued resource variable for use in allocation constraints
    """
    name = str(_uuid4()) if name is None else name
    return _Z3Real(name)


def all(*constraints):
    """
    Create a constraint requiring all specified constraints be simultaneously
    satisfied.

    Parameters
    ----------
    constraints : constraints, iterables of constraints, etc.
        One or more constraints that must all be satisfied.

    Returns
    -------
    constraint
        Single constraint object representing the satisfaction of all provided
        constraints.

    Notes
    -----
    Arguments to this function may be individual constraints or iterables of
    constraints (or iterables of iterables of constraints, etc.).
    """
    return _Z3And(*_flatten_constraints(*constraints))


def any(*constraints):
    """
    Create a constraint requiring at least one of the specified constraints be
    satisfied.

    Parameters
    ----------
    constraints : constraints, iterables of constraints, etc.
        One or more constraints where at least one of the provided constraints
        must be satisfied.

    Returns
    -------
    constraint
        Single constraint object representing the satisfaction of at least one
        provided constraint.

    Notes
    -----
    Arguments to this function may be individual constraints or iterables of
    constraints (or iterables of iterables of constraints, etc.).
    """
    return _Z3Or(*_flatten_constraints(*constraints))


def _flatten_constraints(*constraints):
    all_constraints = []
    for c in constraints:
        try:
            all_constraints.extend(_flatten_constraints(*c))
        except TypeError:
            all_constraints.append(c)
    return all_constraints


def allocate(resource_constraints, task_constraints):
    """
    Allocate limited resources to a prioritized list of tasks.

    Parameters
    ----------
    resource_constraints : iterable of constraints
        Constraints that must all be satisfied even before any allocation
        occurs.  If these constraints cannot all be satisfied, then an
        InvalidResourceConstraints exception will be raised.

    task_constraints : iterable of iterables of constraints
        List of collections of constraints, where each collection defines a
        "task."

    Returns
    -------
    allocation : Mapping
        Resource description for the tasks for which resources were
        completely and successfully allocated.
    tasks_worked : list of int
        Indices of items from task_constraints that were successfully
        incorporated into the allocation.

    Notes
    -----
    Values of resource variables can be retrieved from the returned allocation
    object by using the variable as a key.  For example, if x is a resource,
    allocation[x] provides the value.
    """
    s = _Z3Solver()
    _add_to_solver(s, resource_constraints)
    if s.check().r != 1:
        raise InvalidResourceConstraints

    tasks_worked = []

    for i, task in enumerate(task_constraints):
        s.push()
        _add_to_solver(s, task)
        tasks_worked.append(i)
        if s.check().r != 1:
            s.pop()
            tasks_worked.pop()

    s.check()
    return s.model(), tasks_worked


def _add_to_solver(solver, constraints):
    solver.add(_flatten_constraints(constraints))


def constrain_within_range(target, center, half_width):
    """
    Create constraint that a variable (or expression) must be in the range
    [center - half_width, center + half_width].

    Parameters
    ----------
    target : variable or expression
        The variable or expressions that must lie within the range.
    center : constant, variable, or expression
        The center of the range.
    half_width : constant, variable, or expression
        Half of the width of the range.

    Returns
    -------
    constraint
        A constraint object that the target must lie in the range
        [center - half_width, center + half_width].
    """
    return all(target >= center - half_width, target <= center + half_width)


def sum(*expressions):
    """
    Create an expression consisting of the sum of all of the provided variables
    and/or expressions.

    Parameters
    ----------
    expressions : variables or expressions
        Variables or expressions to be summed.

    Returns
    -------
    expression
        Expression object representing the sum of the inputs.
    """
    return _Z3Sum(*expressions)
