"""Unified query IR: single export after decompose for control-plane consumers."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from ml.rag.chatbot.class_supervisor import SupervisorPlan
from ml.rag.chatbot.reasoner_plan import ReasonerPlan
from ml.rag.chatbot.routing_plan import RoutingPlan
from ml.rag.chatbot.turn_contract import TurnContract

def _str_list(raw: Any) -> tuple[str, ...]:
    if not isinstance(raw, list):
        return ()
    return tuple(str(x).strip() for x in raw if str(x).strip())


def _entity_roles_from_raw(raw: Any) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Parse entity_roles from dict or list-of-pairs into immutable role → labels."""
    if isinstance(raw, dict):
        items = list(raw.items())
    elif isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        return ()
    out: list[tuple[str, tuple[str, ...]]] = []
    seen: set[str] = set()
    for item in items:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            role, labels = item[0], item[1]
        else:
            continue
        role_s = str(role or "").strip().lower()
        if not role_s or role_s in seen:
            continue
        if isinstance(labels, (list, tuple)):
            labs = tuple(str(x).strip() for x in labels if str(x).strip())
        elif str(labels or "").strip():
            labs = (str(labels).strip(),)
        else:
            labs = ()
        if not labs:
            continue
        seen.add(role_s)
        out.append((role_s, labs))
    return tuple(out)


@dataclass(frozen=True)
class QueryFacets:
    """Sanitized semantic facets (decomposition without control-plane spillover)."""

    intent: str = ""
    entities: tuple[str, ...] = ()
    geography: tuple[str, ...] = ()
    domains: tuple[str, ...] = ()
    time_start: str = ""
    time_end: str = ""
    africa_default: bool = False
    africa_panel: bool = False
    expanded_regions: tuple[str, ...] = ()
    corpus_domain_tags: tuple[str, ...] = ()
    reasoner_job: str = ""
    reasoner_shape: str = ""
    job: str = ""
    geo_scope: str = ""
    primary_measure_hints: tuple[str, ...] = ()
    # Role → warehouse labels (land_use, hazard, item, pest, season, livestock).
    entity_roles: tuple[tuple[str, tuple[str, ...]], ...] = ()

    @classmethod
    def from_decomposition(cls, decomposition: dict[str, Any] | None) -> QueryFacets:
        dec = decomposition if isinstance(decomposition, dict) else {}
        return cls(
            intent=str(dec.get("intent") or "").strip(),
            entities=_str_list(dec.get("entities")),
            geography=_str_list(dec.get("geography")),
            domains=_str_list(dec.get("domains")),
            time_start=str(dec.get("time_start") or "").strip()[:10],
            time_end=str(dec.get("time_end") or "").strip()[:10],
            africa_default=bool(dec.get("africa_default")),
            africa_panel=bool(dec.get("africa_panel")),
            expanded_regions=_str_list(dec.get("expanded_regions")),
            corpus_domain_tags=_str_list(dec.get("corpus_domain_tags")),
            reasoner_job=str(dec.get("reasoner_job") or "").strip(),
            reasoner_shape=str(dec.get("reasoner_shape") or "").strip(),
            job=str(dec.get("job") or "").strip(),
            geo_scope=str(dec.get("geo_scope") or "").strip(),
            primary_measure_hints=_str_list(dec.get("primary_measure_hints")),
            entity_roles=_entity_roles_from_raw(dec.get("entity_roles")),
        )

    def to_decomposition(self) -> dict[str, Any]:
        """Rebuild decomposition dict for engine/retriever facets kwargs."""
        out: dict[str, Any] = {
            "intent": self.intent,
            "entities": list(self.entities),
            "geography": list(self.geography),
            "domains": list(self.domains),
            "time_start": self.time_start,
            "time_end": self.time_end,
        }
        if self.africa_default:
            out["africa_default"] = True
        if self.africa_panel:
            out["africa_panel"] = True
        if self.expanded_regions:
            out["expanded_regions"] = list(self.expanded_regions)
        if self.corpus_domain_tags:
            out["corpus_domain_tags"] = list(self.corpus_domain_tags)
        if self.reasoner_job:
            out["reasoner_job"] = self.reasoner_job
        if self.reasoner_shape:
            out["reasoner_shape"] = self.reasoner_shape
        if self.job:
            out["job"] = self.job
        if self.geo_scope:
            out["geo_scope"] = self.geo_scope
        if self.primary_measure_hints:
            out["primary_measure_hints"] = list(self.primary_measure_hints)
        if self.entity_roles:
            out["entity_roles"] = {role: list(labels) for role, labels in self.entity_roles}
        return out

    def to_dict(self) -> dict[str, Any]:
        raw = asdict(self)
        for key in (
            "entities",
            "geography",
            "domains",
            "expanded_regions",
            "corpus_domain_tags",
            "primary_measure_hints",
        ):
            raw[key] = list(raw[key])
        raw["entity_roles"] = {role: list(labels) for role, labels in self.entity_roles}
        return raw

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> QueryFacets:
        if not isinstance(raw, dict):
            return cls()
        return cls(
            intent=str(raw.get("intent") or "").strip(),
            entities=tuple(str(x).strip() for x in (raw.get("entities") or []) if str(x).strip()),
            geography=tuple(str(x).strip() for x in (raw.get("geography") or []) if str(x).strip()),
            domains=tuple(str(x).strip() for x in (raw.get("domains") or []) if str(x).strip()),
            time_start=str(raw.get("time_start") or "").strip()[:10],
            time_end=str(raw.get("time_end") or "").strip()[:10],
            africa_default=bool(raw.get("africa_default")),
            africa_panel=bool(raw.get("africa_panel")),
            expanded_regions=tuple(
                str(x).strip() for x in (raw.get("expanded_regions") or []) if str(x).strip()
            ),
            corpus_domain_tags=tuple(
                str(x).strip() for x in (raw.get("corpus_domain_tags") or []) if str(x).strip()
            ),
            reasoner_job=str(raw.get("reasoner_job") or "").strip(),
            reasoner_shape=str(raw.get("reasoner_shape") or "").strip(),
            job=str(raw.get("job") or "").strip(),
            geo_scope=str(raw.get("geo_scope") or "").strip(),
            primary_measure_hints=tuple(
                str(x).strip() for x in (raw.get("primary_measure_hints") or []) if str(x).strip()
            ),
            entity_roles=_entity_roles_from_raw(raw.get("entity_roles")),
        )


@dataclass(frozen=True)
class QueryIR:
    """Single query understanding export: facets + turn + routing spine."""

    query: str
    facets: QueryFacets
    turn: TurnContract
    routing: RoutingPlan
    supervisor: SupervisorPlan
    task_mode: str = "chat"
    primary_measures: tuple[str, ...] = ()
    companion_measures: tuple[str, ...] = ()
    matched_bundles: tuple[str, ...] = ()
    measure_id: str = ""
    recency_tier: str = ""
    route_candidate: str = "full_rag"
    analytical_mode: bool = False
    reasoner: ReasonerPlan | None = None

    def decomposition(self) -> dict[str, Any]:
        """Full decomposition dict including measure/bundle routing fields."""
        dec = self.facets.to_decomposition()
        if self.primary_measures:
            dec["primary_measures"] = list(self.primary_measures)
        if self.companion_measures:
            dec["companion_measures"] = list(self.companion_measures)
        if self.matched_bundles:
            dec["matched_bundles"] = list(self.matched_bundles)
        return dec

    def to_dict(self) -> dict[str, Any]:
        return {
            "query": self.query,
            "facets": self.facets.to_dict(),
            "turn": self.turn.to_dict(),
            "routing": self.routing.to_dict(),
            "supervisor": self.supervisor.to_dict(),
            "task_mode": self.task_mode,
            "primary_measures": list(self.primary_measures),
            "companion_measures": list(self.companion_measures),
            "matched_bundles": list(self.matched_bundles),
            "measure_id": self.measure_id,
            "recency_tier": self.recency_tier,
            "route_candidate": self.route_candidate,
            "analytical_mode": self.analytical_mode,
            "reasoner": self.reasoner.to_dict() if self.reasoner is not None else None,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> QueryIR | None:
        if not isinstance(raw, dict):
            return None
        reasoner_raw = raw.get("reasoner")
        return cls(
            query=str(raw.get("query") or "").strip(),
            facets=QueryFacets.from_dict(raw.get("facets") if isinstance(raw.get("facets"), dict) else None),
            turn=TurnContract.from_dict(raw.get("turn") if isinstance(raw.get("turn"), dict) else None),
            routing=RoutingPlan.from_dict(raw.get("routing") if isinstance(raw.get("routing"), dict) else None)
            or RoutingPlan(),
            supervisor=SupervisorPlan.from_dict(
                raw.get("supervisor") if isinstance(raw.get("supervisor"), dict) else None
            )
            or SupervisorPlan(classes=(), secondary=(), out_of_scope=()),
            task_mode=str(raw.get("task_mode") or "chat").strip(),
            primary_measures=tuple(
                str(m).strip().lower() for m in (raw.get("primary_measures") or []) if str(m).strip()
            ),
            companion_measures=tuple(
                str(m).strip().lower() for m in (raw.get("companion_measures") or []) if str(m).strip()
            ),
            matched_bundles=tuple(
                str(b).strip() for b in (raw.get("matched_bundles") or []) if str(b).strip()
            ),
            measure_id=str(raw.get("measure_id") or "").strip(),
            recency_tier=str(raw.get("recency_tier") or "").strip(),
            route_candidate=str(raw.get("route_candidate") or "full_rag").strip(),
            analytical_mode=bool(raw.get("analytical_mode")),
            reasoner=ReasonerPlan.from_dict(reasoner_raw if isinstance(reasoner_raw, dict) else None),
        )


def compile_query_ir(
    *,
    query: str,
    decomposition: dict[str, Any],
    turn: TurnContract,
    routing: RoutingPlan,
    supervisor: SupervisorPlan,
    reasoner: ReasonerPlan | None = None,
    task_mode: str = "chat",
    recency_tier: str = "",
    route_candidate: str = "full_rag",
    analytical_mode: bool = False,
) -> QueryIR:
    """Compose the unified IR after all decompose mutations (incl. slot reasoner)."""
    dec = decomposition if isinstance(decomposition, dict) else {}
    pm = _str_list(dec.get("primary_measures"))
    if not pm and turn.measure_id:
        pm = (turn.measure_id,)
    cm = _str_list(dec.get("companion_measures"))
    bundles = _str_list(dec.get("matched_bundles"))
    measure_id = str(turn.measure_id or routing.primary_measure_id or (pm[0] if pm else "")).strip()
    return QueryIR(
        query=str(query or "").strip(),
        facets=QueryFacets.from_decomposition(dec),
        turn=turn,
        routing=routing,
        supervisor=supervisor,
        reasoner=reasoner,
        task_mode=str(task_mode or "chat").strip(),
        primary_measures=tuple(m.lower() for m in pm),
        companion_measures=tuple(m.lower() for m in cm),
        matched_bundles=bundles,
        measure_id=measure_id,
        recency_tier=str(recency_tier or "").strip(),
        route_candidate=str(route_candidate or "full_rag").strip(),
        analytical_mode=bool(analytical_mode),
    )


def compile_early_query_ir(query: str, route_candidate: str) -> QueryIR:
    """Minimal IR for meta/greeting/product short-circuit paths."""
    return QueryIR(
        query=str(query or "").strip(),
        facets=QueryFacets(),
        turn=TurnContract(),
        routing=RoutingPlan(),
        supervisor=SupervisorPlan(classes=(), secondary=(), out_of_scope=()),
        route_candidate=str(route_candidate or "full_rag").strip(),
    )


def query_ir_from_state(state: dict[str, Any] | None) -> QueryIR | None:
    """Load typed IR from graph state (query_ir key or legacy fields)."""
    if not isinstance(state, dict):
        return None
    raw = state.get("query_ir")
    if isinstance(raw, dict):
        return QueryIR.from_dict(raw)
    dec = state.get("decomposition")
    if not isinstance(dec, dict):
        return None
    turn = TurnContract.from_dict(state.get("turn_contract") if isinstance(state.get("turn_contract"), dict) else None)
    routing = RoutingPlan.from_dict(state.get("routing_plan") if isinstance(state.get("routing_plan"), dict) else None)
    supervisor = SupervisorPlan.from_dict(
        state.get("supervisor_plan") if isinstance(state.get("supervisor_plan"), dict) else None
    )
    if routing is None or supervisor is None:
        return None
    reasoner = ReasonerPlan.from_dict(
        state.get("reasoner_plan") if isinstance(state.get("reasoner_plan"), dict) else None
    )
    return compile_query_ir(
        query=str(state.get("query") or state.get("enriched_query") or "").strip(),
        decomposition=dec,
        turn=turn,
        routing=routing,
        supervisor=supervisor,
        reasoner=reasoner,
        task_mode=str(state.get("task_mode") or "chat"),
        recency_tier=str(state.get("recency_tier") or ""),
        route_candidate=str(state.get("route_candidate") or "full_rag"),
        analytical_mode=bool(state.get("analytical_mode")),
    )


__all__ = [
    "QueryFacets",
    "QueryIR",
    "compile_early_query_ir",
    "compile_query_ir",
    "query_ir_from_state",
]
