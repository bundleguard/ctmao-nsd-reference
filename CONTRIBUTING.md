# Contributing

Contributions should preserve the defining CTMAO-NSD invariants: worker-owned
mutable state, orchestrator-mediated cross-thread exchange, bounded delegation,
one absolute execution deadline, and deterministic cleanup.

Before submitting a change:

```bash
python -m pip install --editable .
python -m unittest discover -s tests -v
python main.py
python examples/two_threads.py
```

Keep changes focused, add deterministic regression coverage for concurrency
behavior, and update the README and changelog when public contracts change. Do
not introduce third-party orchestration frameworks into the reference runtime.
