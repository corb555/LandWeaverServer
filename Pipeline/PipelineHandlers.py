class OrchestratorHandlers:
    """Envelope handlers for pipeline events."""

    def __init__(self, orchestrator: "PipelineOrchestrator") -> None:
        self.orch = orchestrator
