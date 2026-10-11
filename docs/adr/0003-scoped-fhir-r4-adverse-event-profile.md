---
status: accepted
date: 2026-08-29
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0003. Scoped FHIR R4 AdverseEvent profile as the payload contract

## Context and Problem Statement

The full FHIR `AdverseEvent` resource has dozens of complex optional nested fields. Central Ingestion needs a concrete, documented contract to validate strictly and to route malformed events to the dead-letter queue.

## Decision Drivers

- Strict structural and domain validation
- Realistic DLQ routing for malformed events
- Avoid validation logic for clinical fields that do not exercise the distributed-systems problem

## Considered Options

- A documented subset of FHIR R4 AdverseEvent (chosen)
- Unstructured JSON
- Full FHIR R4 parser

## Decision Outcome

The payload contract is a scoped FHIR R4 `AdverseEvent`: `resourceType`, `identifier` (idempotency key), `actuality`, `category`, `subject`, `event`, `date` (event time), `recordedDate`, `seriousness`, `severity`, `outcome`, `recorder`, `study`, `location`, `suspectEntity`. `schemaVersion` was added later by ADR 0014.

### Consequences

- `internal/model.AdverseEvent.Validate` enforces the profile; failures are dead-lettered.
- `category`, `outcome` and `recorder` are optional in code.

### Confirmation

`internal/model` validation tests and `docs/api/edge-openapi.yaml` (schema `AdverseEvent`).

## Pros and Cons of the Options

### A documented subset of FHIR R4 AdverseEvent (chosen)

- Good, because domain-authentic and strictly validatable
- Bad, because does not cover the full specification

### Unstructured JSON

- Good, because trivial to accept
- Bad, because does not reflect pharmaceutical domain authenticity and gives validation nothing to enforce

### Full FHIR R4 parser

- Good, because complete
- Bad, because bloats validation with clinical fields irrelevant to the system's engineering goals

## More Information

Evidence: [proposal "Define Scoped FHIR R4 AdverseEvent Schema Profile"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md §2.3 (../../PLAN.md).
