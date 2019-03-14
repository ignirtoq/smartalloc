import smartalloc as sa


def define_blade(num_tasks):
    """
    Create a computing resource pool class that can support up to a given number
    of parallel tasks spread across all instances.

    Parameters
    ----------
    num_tasks : int
        Maximum number of parallel tasks that can run across all blades.

    Returns
    -------
    Blade
        Blade class supporting the specified number of parallel tasks spread
        across all instances.
    """
    _num_tasks = num_tasks

    class Blade:
        """
        Single computing resource out of the pool.

        Attributes
        ----------
        running : array_like of Bool
            Boolean variables indicating if a task is running on this instance.
        num_cpu : float
            Maximum number of CPU cores available for tasking.
        total_mem : int
            Total memory available for tasking.
        """

        # Total number of tasks supported across all Blade instances.
        num_tasks = _num_tasks

        # Resource cost variables for each task.
        cpu = [sa.Real() for _ in range(num_tasks)]
        mem = [sa.Int() for _ in range(num_tasks)]

        def __init__(self, num_cpu, total_mem):
            self.num_cpu = num_cpu
            self.total_mem = total_mem
            self.running = [sa.Bool() for _ in range(self.num_tasks)]

        def _cond_cpu(self, i):
            return self._cond(i, self.cpu)

        def _cond_mem(self, i):
            return self._cond(i, self.mem)

        def _cond(self, i, rsrc):
            return sa.If(self.running[i], rsrc[i], 0)

        def get_constraints(self):
            """
            Generate the aggregate constraints intrinsic to this blade instance.

            Returns
            -------
            constraint
            """
            return sa.all(
                sa.sum(*[self._cond_cpu(i) for i in range(self.num_tasks)])
                <= self.num_cpu,
                sa.sum([self._cond_mem(i) for i in range(self.num_tasks)])
                <= self.total_mem,
            )

        @classmethod
        def get_global_constraints(cls):
            """
            Generate the aggregate constraints intrinsic to the Blade class.

            Returns
            -------
            constraint
            """
            return sa.all(
                *[cls.cpu[i] >= 0 for i in range(cls.num_tasks)],
                *[cls.mem[i] >= 0 for i in range(cls.num_tasks)],
            )

    return Blade


class Task:
    """
    A single computing job to be executed on a blade instance.

    Attributes
    ----------
    id : int
        Unique identifier for the task, indexed from zero.
    cpu : float
        Processing cost to execute the task in units of CPU cores.
    mem : int
        Memory cost to execute the task.
    """
    def __init__(self, identifier, cpu_cost, mem_cost):
        self.id = identifier
        self.cpu = cpu_cost
        self.mem = mem_cost

    def _assign_blade(self, blade):
        """
        Create the condition that the task is executing on the provided blade.

        Parameters
        ----------
        blade : Blade
            Blade instance on which to execute the task.

        Returns
        -------
        condition
            Condition requiring the task executes on the specified blade.
        """
        return sa.all(blade.running[self.id] == True)

    def _assign_blades(self, blades):
        """
        Create the condition that the task can execute on any of the specified
        blades.

        Parameters
        ----------
        blades : array_like of Blade
            Collection of blades that can run the task.

        Returns
        -------
        condition
            Condition requiring the task to execute on any of the provided
            blades.
        """
        return sa.any(
            *[self._assign_blade(blade) for blade in blades]
        )

    def get_constraints(self, blades):
        """
        Create the constraints necessary for the task to be executed.

        Parameters
        ----------
        blades : array_like of Blade
            Collection of blades that can run the task.

        Returns
        -------
        condition
            Aggregate of constraints required to execute the task.
        """
        Blade = type(blades[0])

        return sa.all(
            Blade.cpu[self.id] == self.cpu,
            Blade.mem[self.id] == self.mem,
            self._assign_blades(blades),
        )


def main():
    num_blades = 4
    num_cpus_per_blade = 8
    total_mem_per_blade = 128

    task_cpu = [.4, 2, 3, 5, 7] * 4
    task_mem = [1, 12, 48, 64, 96] * 4
    num_tasks = len(task_cpu)

    assert len(task_cpu) == len(task_mem)

    tasks = [Task(i, task_cpu[i], task_mem[i]) for i in range(num_tasks)]

    Blade = define_blade(num_tasks)
    blades = [Blade(num_cpus_per_blade, total_mem_per_blade)
              for _ in range(num_blades)]

    resource_constraints = [
        Blade.get_global_constraints(),
        *[b.get_constraints() for b in blades]
    ]

    task_constraints = [
        task.get_constraints(blades) for task in tasks
    ]

    alloc, worked = sa.allocate(resource_constraints, task_constraints)

    worked_per_blade = [
        [i for i in range(num_tasks) if alloc[blade.running[i]]]
        for blade in blades
    ]

    print(f'Tasks Worked: {worked}')
    for i, b_worked in enumerate(worked_per_blade):
        print(f'Blade {i} worked: {b_worked}')


if __name__ == '__main__':
    main()
