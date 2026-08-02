# Proptest regressions

Generated Proptest failure files in this directory are committed. They replay
minimized counterexamples before new randomized cases, so a discovered failure
is reproducible in local runs and CI. Convert confirmed failures to focused
deterministic regressions as well when that improves diagnosis.
