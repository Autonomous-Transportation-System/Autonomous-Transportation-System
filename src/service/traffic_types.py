"""Shared, dependency-free contracts for traffic workflow compilation and scheduling."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class Candidate:
    instance_id: str
    capability_id: str
    implementation_id: str
    version: str
    cpu: float = 1.0
    memory_mb: float = 64.0
    expected_seconds: float = 0.01
    cold_start_seconds: float = 0.0
    adapter_seconds: float = 0.0
    max_concurrency: int = 1
    healthy: bool = True


@dataclass(frozen=True)
class PlanNode:
    node_id: str
    capability_id: str
    depends_on: Tuple[str, ...] = ()
    parameters: Dict[str, Any] = field(default_factory=dict)
    candidates: Tuple[Candidate, ...] = ()
    priority: int = 0
    max_attempts: int = 2
    timeout_seconds: float = 30.0
    reuse_key: Optional[str] = None


@dataclass(frozen=True)
class WorkflowPlan:
    workflow_id: str
    app_id: str
    template: str
    nodes: Tuple[PlanNode, ...]
    target_frames: Tuple[int, ...]
    requested_outputs: Tuple[str, ...]
    contract: Dict[str, Any] = field(default_factory=dict)
    created_at: float = 0.0
    deadline: Optional[float] = None
    priority: int = 0
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Assignment:
    task_id: str
    workflow_id: str
    node_id: str
    instance_id: str
    reservation_id: str
    attempt: int
    candidate: Candidate
    parameters: Dict[str, Any] = field(default_factory=dict)
