"""Recursive child-agent execution with structured timeout handling."""

from __future__ import annotations

import asyncio
import logging

from .agent import Agent
from .context import current_agent_path, current_worker_id
from .delegation import DelegationContext, DelegationError
from .memory import ThreadLocalMemory
from .types import ResultStatus, TaskResult, TaskSpec

LOGGER = logging.getLogger(__name__)


class ChildAgent(Agent):
    """Executes one node and recursively delegates its declared child nodes."""

    def __init__(self, worker_id: str, memory: ThreadLocalMemory) -> None:
        """Bind the agent to one worker and its private memory."""
        self._worker_id = worker_id
        self._memory = memory

    async def execute(self, task: TaskSpec, context: DelegationContext) -> TaskResult:
        """Run a node within the execution tree's single absolute deadline."""
        worker_token = current_worker_id.set(self._worker_id)
        path_token = current_agent_path.set(context.agent_path)
        try:
            remaining = context.remaining_time()
            if remaining <= 0:
                raise TimeoutError("execution-tree deadline expired")
            return await asyncio.wait_for(
                self._execute_bounded(task, context), timeout=remaining
            )
        except asyncio.TimeoutError:
            LOGGER.warning("Task %s timed out on %s", task.name, self._worker_id)
            return self._result(
                task,
                context,
                ResultStatus.TIMED_OUT,
                error="execution-tree deadline expired",
            )
        except DelegationError as exc:
            return self._result(task, context, ResultStatus.REJECTED, error=str(exc))
        except Exception as exc:  # exception is converted at the thread boundary
            LOGGER.exception("Task %s failed on %s", task.name, self._worker_id)
            return self._result(
                task, context, ResultStatus.FAILED, error=f"{type(exc).__name__}: {exc}"
            )
        finally:
            current_agent_path.reset(path_token)
            current_worker_id.reset(worker_token)

    async def _execute_bounded(
        self, task: TaskSpec, context: DelegationContext
    ) -> TaskResult:
        context.validate_child_count(len(task.children))
        self._memory.set("last_task", task.name)
        self._memory.set("last_agent_depth", context.depth)
        if task.duration:
            await asyncio.sleep(task.duration)
        if task.should_fail:
            raise RuntimeError(f"task {task.name!r} requested failure")

        child_calls = []
        for index, child_task in enumerate(task.children, start=1):
            agent_name = f"child:{child_task.name}:{index}"
            child_context = context.for_child(child_task.task_id, agent_name)
            child_calls.append(ChildAgent(self._worker_id, self._memory).execute(
                child_task, child_context
            ))
        children = tuple(await asyncio.gather(*child_calls)) if child_calls else ()
        child_failures = [child for child in children if not child.succeeded]
        if child_failures:
            return self._result(
                task,
                context,
                ResultStatus.FAILED,
                error=f"{len(child_failures)} child task(s) did not succeed",
                children=children,
            )
        return self._result(
            task,
            context,
            ResultStatus.SUCCEEDED,
            output=f"processed {task.name}: {task.value!r}",
            children=children,
        )

    def _result(
        self,
        task: TaskSpec,
        context: DelegationContext,
        status: ResultStatus,
        *,
        output: str | None = None,
        error: str | None = None,
        children: tuple[TaskResult, ...] = (),
    ) -> TaskResult:
        return TaskResult(
            task_id=task.task_id,
            name=task.name,
            status=status,
            worker_id=self._worker_id,
            agent_path=context.agent_path,
            output=output,
            error=error,
            children=children,
        )
