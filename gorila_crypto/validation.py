    early_econ = economic_positive_fraction(early)
    late_econ = economic_positive_fraction(late)

    passed = bool(
        late_base >= config.min_fold_pass_fraction
        and late_econ >= config.min_fold_pass_fraction
        and ll_rel <= config.temporal_max_logloss_rel_increase
        and brier_change <= config.temporal_max_brier_increase
        and net_drop <= config.temporal_max_net_drop_bps
    )
    return TemporalStabilityMetrics(
        early_fold_count=len(early),
        late_fold_count=len(late),
        early_log_loss=float(early_ll),
        late_log_loss=float(late_ll),
        early_brier=float(early_brier),
        late_brier=float(late_brier),
        early_net_mean_bps=float(early_net),
        late_net_mean_bps=float(late_net),
        early_baseline_pass_fraction=float(early_base),
        late_baseline_pass_fraction=float(late_base),
        early_economic_positive_fraction=float(early_econ),
        late_economic_positive_fraction=float(late_econ),
        log_loss_relative_change=float(ll_rel),
        brier_change=float(brier_change),
        net_drop_bps=float(net_drop),
        passed=passed,
    )


class QualityGateBlocked(RuntimeError):
    """Raised when OOS validation is attempted before data quality passes."""


def require_quality_gate(
    quality_report: Mapping[str, object],
    *,
    minimum_rows: int,
    expected_replay_fingerprint: str | None = None,
) -> None:
    """Hard-stop OOS until the exact replay slice has passed data quality."""
    if minimum_rows < 1:
        raise ValueError("minimum_rows must be positive")
    status = str(quality_report.get("status") or "")
    rows = int(quality_report.get("rows") or 0)
    if status != "PASS":
        reasons = quality_report.get("reasons") or ()
        raise QualityGateBlocked(
            f"quality_gate_status={status or 'UNKNOWN'} reasons={tuple(reasons)}"
        )
    if rows < minimum_rows:
        raise QualityGateBlocked(
            f"quality_gate_rows={rows} below required minimum={minimum_rows}"
        )
    if expected_replay_fingerprint is not None:
        actual_fingerprint = str(quality_report.get("replay_fingerprint") or "")
        if actual_fingerprint != expected_replay_fingerprint:
            raise QualityGateBlocked(
                "quality_gate_replay_fingerprint_mismatch:"
                f"expected={expected_replay_fingerprint} actual={actual_fingerprint}"
            )


def run_quality_gated_walk_forward(
    dataset: Sequence[ForecastDatasetRow],
    feature_names: Sequence[str],
    config: WalkForwardConfig,
    policy: EconomicPolicySpec,
    *,
    quality_report: Mapping[str, object],
    minimum_quality_rows: int,
    replay_fingerprint: str,
    model_id: str = "crypto-ridge-logit-wf",
    model_version: str = "1",
    placebo_block_size: int = 20,
    placebo_iterations: int = 500,
    stress_scenarios: Sequence[StressScenario] = (),
    candidate_strategy_returns: Sequence[Sequence[float]] = (),
) -> ValidationReport:
    """Run PIT/OOS evaluation only after a passed quality gate."""
    require_quality_gate(
        quality_report,
        minimum_rows=minimum_quality_rows,
        expected_replay_fingerprint=replay_fingerprint,
    )
    return run_walk_forward_validation(
        dataset,
        feature_names,
        config,
        policy,
        model_id=model_id,
        model_version=model_version,
        placebo_block_size=placebo_block_size,
        placebo_iterations=placebo_iterations,
        stress_scenarios=stress_scenarios,
        candidate_strategy_returns=candidate_strategy_returns,
    )


def run_walk_forward_validation(
    dataset: Sequence[ForecastDatasetRow],
    feature_names: Sequence[str],
    config: WalkForwardConfig,
    policy: EconomicPolicySpec,
    *,
    model_id: str = "crypto-ridge-logit-wf",
    model_version: str = "1",
    placebo_block_size: int = 20,
    placebo_iterations: int = 500,
    stress_scenarios: Sequence[StressScenario] = (),
    candidate_strategy_returns: Sequence[Sequence[float]] = (),
) -> ValidationReport:
    config.validate()
    if not dataset:
        raise ValueError("validation dataset cannot be empty")
    folds = make_walk_forward_folds(dataset, config)
    if not folds:
        return ValidationReport(
            status="INSUFFICIENT_OOS_DATA",
            dataset_rows=len(dataset),
            folds=(),
            fold_baseline_pass_fraction=0.0,
            fold_economic_positive_fraction=0.0,
            temporal_stability=TemporalStabilityMetrics(
                early_fold_count=0,
                late_fold_count=0,
                early_log_loss=float("nan"),
                late_log_loss=float("nan"),
                early_brier=float("nan"),
                late_brier=float("nan"),
                early_net_mean_bps=float("nan"),
                late_net_mean_bps=float("nan"),
                early_baseline_pass_fraction=0.0,
                late_baseline_pass_fraction=0.0,
                early_economic_positive_fraction=0.0,
                late_economic_positive_fraction=0.0,
                log_loss_relative_change=float("nan"),
                brier_change=float("nan"),
                net_drop_bps=float("nan"),
                passed=False,
            ),
            oos_probabilities=(),
            oos_labels=(),
            oos_returns_bps=(),
            placebo_p_value=None,
            placebo_iterations=0,
            stability_by_symbol={},
            stability_by_horizon_ms={},
            stress_results={},
            multiple_testing_p_value=None,
            dsr_p_value=None,
            pbo=None,
            research_robustness_status="INSUFFICIENT_OOS_DATA",
            research_robustness_reasons=("NO_OOS_FOLDS",),
            promotion_eligible=False,
        )

    fold_evaluations: list[FoldEvaluation] = []
    oos_probabilities: list[float] = []
    oos_labels: list[int] = []
    oos_returns: list[float] = []

    for fold in folds:
        model = fit_ridge_logistic(
            dataset,
            fold.train_indices,
            feature_names,
            config,
            model_id=model_id,
            version=f"{model_version}.fold{fold.fold_id}",
        )
        test_rows = [dataset[i] for i in fold.test_indices]
        probabilities = [predict_probability(model, row.snapshot) for row in test_rows]
        labels = [row.label.realized_target for row in test_rows]
        fold_metric = probabilistic_metrics(labels, probabilities)
        baseline_fifty = baseline_constant(labels, 0.5)
        baseline_prevalence = baseline_constant(labels, model.training_positive_rate)
        economic = economic_metrics(test_rows, probabilities, policy)

        fold_evaluations.append(
            FoldEvaluation(
                fold=fold,
                model=model,
                probabilistic=fold_metric,
                baseline_fifty=baseline_fifty,
                baseline_prevalence=baseline_prevalence,
                economic=economic,
            )
        )
        oos_probabilities.extend(probabilities)
        oos_labels.extend(labels)
        oos_returns.extend(row.label.realized_signed_return_bps for row in test_rows)

    stability_by_symbol, stability_by_horizon = stability_metrics(
        [dataset[i] for fold in folds for i in fold.test_indices],
        oos_probabilities,
    )
    placebo_p, placebo_n = placebo_logloss_edge(
        oos_labels,
        oos_probabilities,
        block_size=placebo_block_size,
        iterations=placebo_iterations,
        seed=config.seed,
    )

    if not stress_scenarios:
        stress_scenarios = (
            StressScenario("base"),
            StressScenario("stress_1", cost_multiplier=2.0, slippage_multiplier=2.0),
            StressScenario("stress_2", cost_multiplier=4.0, slippage_multiplier=4.0),
        )
    stress_results = {}
    oos_rows = [dataset[i] for fold in folds for i in fold.test_indices]
    for scenario in stress_scenarios:
        scenario.validate()
        stress_results[scenario.name] = economic_metrics(
            oos_rows,
            oos_probabilities,
            policy,
            return_haircut=scenario.return_haircut,
            cost_multiplier=scenario.cost_multiplier,
            slippage_multiplier=scenario.slippage_multiplier,
        )

    temporal = temporal_stability_metrics(fold_evaluations, config)
    aggregate = probabilistic_metrics(oos_labels, oos_probabilities)
    aggregate_baseline = baseline_constant(oos_labels, 0.5)
    prevalence_values = [
        fold.model.training_positive_rate
        for fold in fold_evaluations
        for _ in fold.fold.test_indices
    ]
    aggregate_prevalence = probabilistic_metrics(oos_labels, prevalence_values)
    aggregate_economic = economic_metrics(oos_rows, oos_probabilities, policy)
    minimum_fold_count = len(folds) >= 3
    baseline_beat = all(
        (
            aggregate.log_loss < baseline.log_loss
            and aggregate.brier < baseline.brier
        )
        for baseline in (aggregate_baseline, aggregate_prevalence)
    )
    fold_baseline_passes = [
        (
            fold_eval.probabilistic.log_loss < fold_eval.baseline_fifty.log_loss
            and fold_eval.probabilistic.brier < fold_eval.baseline_fifty.brier
            and fold_eval.probabilistic.log_loss < fold_eval.baseline_prevalence.log_loss
            and fold_eval.probabilistic.brier < fold_eval.baseline_prevalence.brier
        )
        for fold_eval in fold_evaluations
    ]
    fold_baseline_pass_fraction = (
        sum(fold_baseline_passes) / len(fold_baseline_passes)
        if fold_baseline_passes
        else 0.0
    )
    fold_economic_positive_fraction = (
        sum(fold_eval.economic.net_mean_bps > 0.0 for fold_eval in fold_evaluations)
        / len(fold_evaluations)
        if fold_evaluations
        else 0.0
    )
    economic_positive = aggregate_economic.net_mean_bps > 0.0
    explicit_friction_pass = (
        policy.round_trip_cost_bps + policy.round_trip_slippage_bps
    ) > 0.0
    adjusted_placebo_values = holm_bonferroni(
        [placebo_p] * PREREGISTERED_CRYPTO_PROTOCOL.declared_hypothesis_family_size
    )
    adjusted_placebo = adjusted_placebo_values[0] if adjusted_placebo_values else None
    multiple_testing_pass = (
        adjusted_placebo is not None
        and adjusted_placebo <= PREREGISTERED_CRYPTO_PROTOCOL.multiple_testing_alpha
    )

    strategy_net_series: list[float] = []
    for row, probability in zip(oos_rows, oos_probabilities):
        if probability >= policy.long_threshold:
            action = 1
        elif probability <= policy.short_threshold:
            action = -1
        else:
            action = 0