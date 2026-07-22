"""Cross-Thread Multi-Agent Orchestration with Nested Sub-Agent Delegation."""

from .config import RuntimeConfig
from .orchestrator import (
    OrchestrationReport,
    OrchestrationCancelledError,
    OrchestrationDeadlineExceeded,
    Orchestrator,
    OrchestratorBusyError,
    OrchestratorClosedError,
)
from .thread_manager import WorkerUnavailableError
from .types import ResultStatus, TaskResult, TaskSpec

__all__ = [
    "OrchestrationReport",
    "OrchestrationCancelledError",
    "OrchestrationDeadlineExceeded",
    "Orchestrator",
    "OrchestratorBusyError",
    "OrchestratorClosedError",
    "ResultStatus",
    "RuntimeConfig",
    "TaskResult",
    "TaskSpec",
    "WorkerUnavailableError",
]

__version__ = "0.1.1"
