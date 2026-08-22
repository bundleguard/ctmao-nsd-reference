"""Thread-owned supervisor agent and deterministic result aggregation."""

from __future__ import annotations

from collections.abc import Callable

from .config import RuntimeConfig
from .agent_runtime import AgentRuntime, DeterministicRuntime
from .delegation import DelegationContext, DelegationError
from .memory import ThreadLocalMemory
from .child_agent import ChildAgent
from .types import ResultStatus, TaskResult, TaskSpec, WorkerObservation


class SupervisorAgent:
    """Routes a root task to bounded child agents inside one worker thread."""

    def __init__(
        self,
        worker_id: str,
        memory: ThreadLocalMemory,
        config: RuntimeConfig,
        event_sink: Callable[[WorkerObservation], None] | None = None,
        runtime: AgentRuntime | None = None,
    ) -> None:
        """Bind the supervisor to its worker-local dependencies."""
        self._worker_id = worker_id
        self._memory = memory
        self._config = config
        self._event_sink = event_sink
        self._runtime = runtime or DeterministicRuntime()

    async def execute(
        self, task: TaskSpec, deadline: float | None = None
    ) -> TaskResult:
        """Delegate root children and aggregate outcomes in declaration order."""
        context = DelegationContext.root(
            task.task_id, self._worker_id, self._config, deadline
        )
        self._emit(
            WorkerObservation(
                "task.started",
                f"Supervisor {self._worker_id} started {task.name}",
                task_id=task.task_id,
                agent_path=context.agent_path,
            )
        )
        self._memory.set("root_task", task.name)
        try:
            context.validate_child_count(len(task.children))
            child_results: list[TaskResult] = []
            for index, child_task in enumerate(task.children, start=1):
                child_context = context.for_child(
                    child_task.task_id, f"child:{child_task.name}:{index}"
                )
                self._emit(
                    WorkerObservation(
                        "delegation.created",
                        f"Supervisor {self._worker_id} delegated {child_task.name}",
                        task_id=child_task.task_id,
                        parent_task_id=task.task_id,
                        agent_path=context.agent_path,
                    )
                )
                # Supervisors route declared children once; nested siblings may run
                # concurrently inside each ChildAgent.
                child_results.append(
                    await ChildAgent(
                        self._worker_id,
                        self._memory,
                        self._event_sink,
                        self._runtime,
                        self._config.publish_result_summaries,
                        self._config.publish_agent_messages,
                    ).execute(
                        child_task, child_context
                    )
                )
                child_result = child_results[-1]
                event_type = (
                    "delegation.completed"
                    if child_result.succeeded
                    else "delegation.failed"
                )
                detail = (
                    f": {child_result.output}"
                    if self._config.publish_result_summaries
                    and child_result.succeeded
                    and child_result.output
                    else ""
                )
                self._emit(
                    WorkerObservation(
                        event_type,
                        f"Supervisor {self._worker_id} received {child_result.name} "
                        f"{child_result.status.value}{detail}",
                        task_id=child_result.task_id,
                        parent_task_id=task.task_id,
                        agent_path=context.agent_path,
                        status=child_result.status.value,
                        importance="high" if not child_result.succeeded else "normal",
                    )
                )
            children = tuple(child_results)
            failed = [child for child in children if not child.succeeded]
            status = ResultStatus.FAILED if failed else ResultStatus.SUCCEEDED
            result = TaskResult(
                task_id=task.task_id,
                name=task.name,
                status=status,
                worker_id=self._worker_id,
                agent_path=context.agent_path,
                output=f"aggregated {len(children)} child result(s)" if not failed else None,
                error=f"{len(failed)} child branch(es) failed" if failed else None,
                children=children,
            )
            self._emit_terminal(result, context.agent_path)
            return result
        except DelegationError as exc:
            result = TaskResult(
                task_id=task.task_id,
                name=task.name,
                status=ResultStatus.REJECTED,
                worker_id=self._worker_id,
                agent_path=context.agent_path,
                error=str(exc),
            )
            self._emit_terminal(result, context.agent_path)
            return result

    def _emit_terminal(
        self, result: TaskResult, agent_path: tuple[str, ...]
    ) -> None:
        event_type = "task.completed" if result.succeeded else f"task.{result.status.value}"
        self._emit(
            WorkerObservation(
                event_type,
                f"Supervisor {self._worker_id} {result.status.value} {result.name}",
                task_id=result.task_id,
                agent_path=agent_path,
                status=result.status.value,
                importance="high" if not result.succeeded else "normal",
            )
        )

    def _emit(self, observation: WorkerObservation) -> None:
        if self._event_sink is not None:
            self._event_sink(observation)
