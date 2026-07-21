"""Cross-Thread Multi-Agent Orchestration with Nested Sub-Agent Delegation."""

from .config import RuntimeConfig
from .orchestrator import OrchestrationReport, Orchestrator, OrchestratorClosedError
from .types import ResultStatus, TaskResult, TaskSpec

__all__ = [
    "OrchestrationReport",
    "Orchestrator",
    "OrchestratorClosedError",
    "ResultStatus",
    "RuntimeConfig",
    "TaskResult",
    "TaskSpec",
]

__version__ = "0.1.0"
