# SMarTalloc

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
