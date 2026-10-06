from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum

from .experiments import StageOutcome


class BoundaryKind(StrEnum):
    INTERVAL = "interval"
    LOWER_CENSORED = "lower_censored"
    UPPER_CENSORED = "upper_censored"
    UNBOUNDED = "unbounded"
    UNKNOWN_CENSORED = "unknown_censored"


@dataclass(frozen=True)
class BoundaryEstimate:
    kind: BoundaryKind
    sustainable_rate: float | None
    unsustainable_rate: float | None
    relative_width: float | None
    observations: tuple[tuple[float, StageOutcome], ...]


class AdaptiveBracketSearch:
    def __init__(
        self,
        initial_rate: float,
        *,
        growth_factor: float = 2.0,
        relative_tolerance: float = 0.10,
        minimum_rate: float = 1.0,
        maximum_rate: float | None = None,
        maximum_stages: int = 10,
    ) -> None:
        numeric_values = (initial_rate, growth_factor, relative_tolerance, minimum_rate)
        if not all(math.isfinite(value) for value in numeric_values):
            raise ValueError("search parameters must be finite")
        if maximum_rate is not None and not math.isfinite(maximum_rate):
            raise ValueError("maximum_rate must be finite")
        if initial_rate <= 0 or growth_factor <= 1 or relative_tolerance <= 0:
            raise ValueError("invalid search parameters")
        if minimum_rate <= 0 or maximum_rate is not None and maximum_rate < minimum_rate:
            raise ValueError("invalid search bounds")
        if maximum_stages <= 0:
            raise ValueError("maximum_stages must be positive")
        self.initial_rate = initial_rate
        self.growth_factor = growth_factor
        self.relative_tolerance = relative_tolerance
        self.minimum_rate = minimum_rate
        self.maximum_rate = maximum_rate
        self.maximum_stages = maximum_stages
        self._observations: list[tuple[float, StageOutcome]] = []

    def observe(self, rate: float, outcome: StageOutcome) -> None:
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError("rate must be finite and positive")
        if rate < self.minimum_rate:
            raise ValueError("rate is below minimum_rate")
        if self.maximum_rate is not None and rate > self.maximum_rate:
            raise ValueError("rate exceeds maximum_rate")
        sustainable_rates = [
            existing_rate
            for existing_rate, existing_outcome in self._observations
            if existing_outcome == StageOutcome.SUSTAINABLE
        ]
        unsustainable_rates = [
            existing_rate
            for existing_rate, existing_outcome in self._observations
            if existing_outcome == StageOutcome.UNSUSTAINABLE
        ]
        if outcome == StageOutcome.SUSTAINABLE and any(
            failed_rate < rate for failed_rate in unsustainable_rates
        ):
            raise ValueError("non-monotonic evidence: sustainable above unsustainable rate")
        if outcome == StageOutcome.UNSUSTAINABLE and any(
            passed_rate > rate for passed_rate in sustainable_rates
        ):
            raise ValueError("non-monotonic evidence: unsustainable below sustainable rate")
        valid_at_rate = {
            existing_outcome
            for existing_rate, existing_outcome in self._observations
            if existing_rate == rate
            and existing_outcome in (StageOutcome.SUSTAINABLE, StageOutcome.UNSUSTAINABLE)
        }
        if valid_at_rate and outcome in (StageOutcome.SUSTAINABLE, StageOutcome.UNSUSTAINABLE):
            if outcome not in valid_at_rate:
                raise ValueError(f"conflicting valid outcome for rate {rate}")
        self._observations.append((rate, outcome))

    @property
    def observations(self) -> tuple[tuple[float, StageOutcome], ...]:
        return tuple(sorted(self._observations, key=lambda item: item[0]))

    def _bounds(self) -> tuple[float | None, float | None]:
        sustainable = [
            rate for rate, outcome in self._observations
            if outcome == StageOutcome.SUSTAINABLE
        ]
        unsustainable = [
            rate for rate, outcome in self._observations
            if outcome == StageOutcome.UNSUSTAINABLE
        ]
        lower = max(sustainable, default=None)
        upper_candidates = [rate for rate in unsustainable if lower is None or rate > lower]
        upper = min(upper_candidates, default=None)
        return lower, upper

    def next_rate(self) -> float | None:
        if len(self._observations) >= self.maximum_stages:
            return None
        lower, upper = self._bounds()
        if lower is not None and upper is not None:
            if (upper - lower) / lower <= self.relative_tolerance:
                return None
            return (lower + upper) / 2

        if not self._observations:
            return min(self.initial_rate, self.maximum_rate or self.initial_rate)

        valid_rates = [
            rate for rate, outcome in self._observations
            if outcome in (StageOutcome.SUSTAINABLE, StageOutcome.UNSUSTAINABLE)
        ]
        if not valid_rates:
            return min(self.initial_rate, self.maximum_rate or self.initial_rate)

        if lower is None:
            candidate = min(valid_rates) / self.growth_factor
            return candidate if candidate >= self.minimum_rate else None

        candidate = lower * self.growth_factor
        if self.maximum_rate is not None:
            if lower >= self.maximum_rate:
                return None
            candidate = min(candidate, self.maximum_rate)
        observed_valid_rates = set(valid_rates)
        return None if candidate in observed_valid_rates else candidate

    def estimate(self) -> BoundaryEstimate:
        lower, upper = self._bounds()
        relative_width = (
            (upper - lower) / lower if lower is not None and upper is not None else None
        )
        if lower is not None and upper is not None:
            kind = BoundaryKind.INTERVAL
        elif lower is not None:
            kind = BoundaryKind.LOWER_CENSORED
        elif upper is not None:
            kind = BoundaryKind.UPPER_CENSORED
        elif self._observations:
            kind = BoundaryKind.UNKNOWN_CENSORED
        else:
            kind = BoundaryKind.UNBOUNDED
        return BoundaryEstimate(kind, lower, upper, relative_width, self.observations)
