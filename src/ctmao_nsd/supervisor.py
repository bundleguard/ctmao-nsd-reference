"""Thread-owned supervisor agent and deterministic result aggregation."""

from __future__ import annotations

from .config import RuntimeConfig
from .delegation import DelegationContext, DelegationError
from .memory import ThreadLocalMemory
from .child_agent import ChildAgent
from .types import ResultStatus, TaskResult, TaskSpec


class SupervisorAgent:
    """Routes a root task to bounded child agents inside one worker thread."""

    def __init__(
        self, worker_id: str, memory: ThreadLocalMemory, config: RuntimeConfig
    ) -> None:
        """Bind the supervisor to its worker-local dependencies."""
        self._worker_id = worker_id
        self._memory = memory
        self._config = config

    async def execute(
        self, task: TaskSpec, deadline: float | None = None
    ) -> TaskResult:
        """Delegate root children and aggregate outcomes in declaration order."""
        context = DelegationContext.root(
            task.task_id, self._worker_id, self._config, deadline
        )
        self._memory.set("root_task", task.name)
        try:
            context.validate_child_count(len(task.children))
            child_results: list[TaskResult] = []
            for index, child_task in enumerate(task.children, start=1):
                child_context = context.for_child(
                    child_task.task_id, f"child:{child_task.name}:{index}"
                )
                # Supervisors route declared children once; nested siblings may run
                # concurrently inside each ChildAgent.
                child_results.append(
                    await ChildAgent(self._worker_id, self._memory).execute(
                        child_task, child_context
                    )
                )
            children = tuple(child_results)
            failed = [child for child in children if not child.succeeded]
            status = ResultStatus.FAILED if failed else ResultStatus.SUCCEEDED
            return TaskResult(
                task_id=task.task_id,
                name=task.name,
                status=status,
                worker_id=self._worker_id,
                agent_path=context.agent_path,
                output=f"aggregated {len(children)} child result(s)" if not failed else None,
                error=f"{len(failed)} child branch(es) failed" if failed else None,
                children=children,
            )
        except DelegationError as exc:
            return TaskResult(
                task_id=task.task_id,
                name=task.name,
                status=ResultStatus.REJECTED,
                worker_id=self._worker_id,
                agent_path=context.agent_path,
                error=str(exc),
            )
