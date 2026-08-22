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
from .normalized_events import NormalizedEvent
from .observability import EventBus, EventProjection
from .conference import (
    ConferenceMode,
    ConferenceTurn,
    Verbosity,
    VoiceAssignment,
    assign_voices,
    project_briefing,
    project_conference_turns,
)
from .speech import AsyncSpeakerQueue, NullSpeechProvider
from .agent_runtime import (
    AgentRuntime,
    DeterministicRuntime,
    ModelBackedRuntime,
    ModelProvider,
    ModelRequest,
    ModelResponse,
    RuntimeRequest,
)
from .external_adapter import (
    AdapterResult,
    CodexExecJsonlAdapter,
    CodexThreadSnapshotAdapter,
    ExternalRuntimeAdapter,
)

__all__ = [
    "OrchestrationReport",
    "OrchestrationCancelledError",
    "OrchestrationDeadlineExceeded",
    "Orchestrator",
    "OrchestratorBusyError",
    "OrchestratorClosedError",
    "AsyncSpeakerQueue",
    "AgentRuntime",
    "AdapterResult",
    "ConferenceMode",
    "ConferenceTurn",
    "CodexThreadSnapshotAdapter",
    "CodexExecJsonlAdapter",
    "EventBus",
    "EventProjection",
    "ExternalRuntimeAdapter",
    "DeterministicRuntime",
    "ModelBackedRuntime",
    "ModelProvider",
    "ModelRequest",
    "ModelResponse",
    "NormalizedEvent",
    "NullSpeechProvider",
    "ResultStatus",
    "RuntimeRequest",
    "RuntimeConfig",
    "TaskResult",
    "TaskSpec",
    "WorkerUnavailableError",
    "Verbosity",
    "VoiceAssignment",
    "assign_voices",
    "project_briefing",
    "project_conference_turns",
]

__version__ = "0.2.0"
