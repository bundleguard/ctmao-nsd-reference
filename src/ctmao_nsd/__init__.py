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
    project_message_turns,
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
from .interactive_conference import (
    ConferenceBusyError,
    DemoConferenceProvider,
    InteractiveConference,
    Participant,
)
from .openai_provider import OpenAIProviderError, OpenAIResponsesProvider

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
    "ConferenceBusyError",
    "ConferenceMode",
    "ConferenceTurn",
    "CodexThreadSnapshotAdapter",
    "CodexExecJsonlAdapter",
    "EventBus",
    "EventProjection",
    "ExternalRuntimeAdapter",
    "DeterministicRuntime",
    "DemoConferenceProvider",
    "InteractiveConference",
    "ModelBackedRuntime",
    "ModelProvider",
    "ModelRequest",
    "ModelResponse",
    "NormalizedEvent",
    "NullSpeechProvider",
    "OpenAIProviderError",
    "OpenAIResponsesProvider",
    "Participant",
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
    "project_message_turns",
]

__version__ = "0.3.0"
