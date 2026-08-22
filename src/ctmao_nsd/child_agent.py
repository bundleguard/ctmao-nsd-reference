"""Recursive child-agent execution with structured timeout handling."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from .agent import Agent
from .agent_runtime import AgentRuntime, DeterministicRuntime, RuntimeRequest
from .context import current_agent_path, current_worker_id
from .delegation import DelegationContext, DelegationError
from .memory import ThreadLocalMemory
from .normalized_events import redact_text
from .types import ResultStatus, TaskResult, TaskSpec, WorkerObservation

LOGGER = logging.getLogger(__name__)


class ChildAgent(Agent):
    """Executes one node and recursively delegates its declared child nodes."""

    def __init__(
        self,
        worker_id: str,
        memory: ThreadLocalMemory,
        event_sink: Callable[[WorkerObservation], None] | None = None,
        runtime: AgentRuntime | None = None,
        publish_result_summaries: bool = False,
        publish_agent_messages: bool = False,
    ) -> None:
        """Bind the agent to one worker and its private memory."""
        self._worker_id = worker_id
        self._memory = memory
        self._event_sink = event_sink
        self._runtime = runtime or DeterministicRuntime()
        self._publish_result_summaries = publish_result_summaries
        self._publish_agent_messages = publish_agent_messages

    async def execute(self, task: TaskSpec, context: DelegationContext) -> TaskResult:
        """Run a node within the execution tree's single absolute deadline."""
        worker_token = current_worker_id.set(self._worker_id)
        path_token = current_agent_path.set(context.agent_path)
        self._emit(
            WorkerObservation(
                "task.started",
                f"{context.agent_path[-1]} started {task.name}",
                task_id=task.task_id,
                parent_task_id=context.lineage[-2] if len(context.lineage) > 1 else None,
                agent_path=context.agent_path,
            )
        )
        try:
            remaining = context.remaining_time()
            if remaining <= 0:
                raise TimeoutError("execution-tree deadline expired")
            return await asyncio.wait_for(
                self._execute_bounded(task, context), timeout=remaining
            )
        except asyncio.TimeoutError:
            safe_name, _ = redact_text(task.name)
            LOGGER.warning("Task %s timed out on %s", safe_name, self._worker_id)
            return self._result(
                task,
                context,
                ResultStatus.TIMED_OUT,
                error="execution-tree deadline expired",
            )
        except DelegationError as exc:
            return self._result(task, context, ResultStatus.REJECTED, error=str(exc))
        except Exception as exc:  # exception is converted at the thread boundary
            safe_name, _ = redact_text(task.name)
            safe_error, _ = redact_text(str(exc))
            LOGGER.error(
                "Task %s failed on %s: %s: %s",
                safe_name,
                self._worker_id,
                type(exc).__name__,
                safe_error,
            )
            return self._result(
                task,
                context,
                ResultStatus.FAILED,
                error=f"{type(exc).__name__}: {safe_error}",
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
        output = await self._runtime.execute(
            RuntimeRequest(
                task.task_id,
                task.name,
                task.value,
                task.duration,
                task.should_fail,
                self._worker_id,
                context.agent_path,
            )
        )

        child_calls = []
        for index, child_task in enumerate(task.children, start=1):
            agent_name = f"child:{child_task.name}:{index}"
            child_context = context.for_child(child_task.task_id, agent_name)
            self._emit(
                WorkerObservation(
                    "delegation.created",
                    f"{context.agent_path[-1]} delegated {child_task.name}",
                    task_id=child_task.task_id,
                    parent_task_id=task.task_id,
                    agent_path=context.agent_path,
                )
            )
            child_calls.append(
                ChildAgent(
                    self._worker_id,
                    self._memory,
                    self._event_sink,
                    self._runtime,
                    self._publish_result_summaries,
                    self._publish_agent_messages,
                ).execute(
                    child_task, child_context
                )
            )
        children = tuple(await asyncio.gather(*child_calls)) if child_calls else ()
        for child_result in children:
            self._emit_delegation_result(task, context, child_result)
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
            output=output,
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
        result = TaskResult(
            task_id=task.task_id,
            name=task.name,
            status=status,
            worker_id=self._worker_id,
            agent_path=context.agent_path,
            output=output,
            error=error,
            children=children,
        )
        if self._publish_result_summaries and result.succeeded and result.output:
            self._emit(
                WorkerObservation(
                    "finding.reported",
                    f"{context.agent_path[-1]} reported: {result.output}",
                    task_id=task.task_id,
                    parent_task_id=(
                        context.lineage[-2] if len(context.lineage) > 1 else None
                    ),
                    agent_path=context.agent_path,
                    status="reported",
                    importance="high",
                )
            )
        if (
            self._publish_agent_messages
            and task.publish_output
            and result.succeeded
            and result.output
        ):
            self._emit(
                WorkerObservation(
                    "message.sent",
                    result.output,
                    task_id=task.task_id,
                    parent_task_id=(
                        context.lineage[-2] if len(context.lineage) > 1 else None
                    ),
                    agent_path=context.agent_path,
                    status="delivered",
                    importance="high",
                    attributes=(
                        ("speaker_id", self._worker_id),
                        ("recipient_id", task.recipient_id),
                        ("message_kind", task.message_kind),
                    ),
                )
            )
        event_type = "task.completed" if result.succeeded else f"task.{status.value}"
        self._emit(
            WorkerObservation(
                event_type,
                f"{context.agent_path[-1]} {status.value} {task.name}",
                task_id=task.task_id,
                parent_task_id=context.lineage[-2] if len(context.lineage) > 1 else None,
                agent_path=context.agent_path,
                status=status.value,
                importance="high" if not result.succeeded else "normal",
            )
        )
        return result

    def _emit_delegation_result(
        self,
        task: TaskSpec,
        context: DelegationContext,
        child: TaskResult,
    ) -> None:
        event_type = "delegation.completed" if child.succeeded else "delegation.failed"
        detail = (
            f": {child.output}"
            if self._publish_result_summaries and child.succeeded and child.output
            else ""
        )
        self._emit(
            WorkerObservation(
                event_type,
                f"{context.agent_path[-1]} received {child.name} {child.status.value}{detail}",
                task_id=child.task_id,
                parent_task_id=task.task_id,
                agent_path=context.agent_path,
                status=child.status.value,
                importance="high" if not child.succeeded else "normal",
            )
        )

    def _emit(self, observation: WorkerObservation) -> None:
        if self._event_sink is not None:
            self._event_sink(observation)
