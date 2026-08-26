"""Structured concurrency: nothing outlives the block that started it.

Three rules, one per module, and each of them closes a hole that `asyncio`
leaves open by default:

**A task has an owner.** `scope.py` — `asyncio.create_task` gives you a task the
loop holds only weakly, whose exception nobody retrieves and whose lifetime
nobody bounds. `TaskScope` makes the lifetime lexical, and it has two exit
rules because a fan-out and a daemon need opposite ones.

**A wait has a ceiling that composes.** `deadline.py` — per-call timeouts do
not add up to a request budget, and three five-second calls under a five-second
budget take fifteen seconds. `deadline()` nests, clamps rather than extends,
and names the scope that actually expired.

**Cleanup finishes.** `cancel.py` — a cancelled handler's `finally:` runs on a
cancelled task, so the release that has to happen is exactly the await that
gets cut. `asyncio.shield` protects the work and not the wait, which turns the
problem into an unowned task rather than solving it; `protect` and `finalize`
keep waiting instead.

The package is `structured` rather than `concurrency` because `src/concurrency`
is already taken by the *other* meaning of the word — `If-Match`, ETags and
`version_id_col`, which are about two writers racing at one row rather than two
coroutines racing on one loop.

Related, and deliberately not merged into this:

- `src/parallel` fans work *out* — bounded gathers and a process pool. It
  answers "how many at once", where this package answers "for how long, and
  who cleans up". `gather_bounded` already cancels and drains its siblings;
  what it has never had is a budget that spans the whole handler.
- `src/decorators/retry.py` bounds *attempts*. A retry loop inside a deadline
  is the composition that works: attempts stop when the budget is gone, and
  `DeadlineExceeded` is not a `TimeoutError`, so the retrier does not treat a
  spent budget as one more transient failure worth sleeping on.

See `docs/structured-concurrency.md`.
"""

from src.structured.cancel import finalize, protect
from src.structured.deadline import (
    Deadline,
    clamp_to_deadline,
    current_deadline,
    deadline,
)
from src.structured.errors import DeadlineExceeded, TaskScopeClosedError
from src.structured.scope import TaskScope, WhenScopeExits

__all__ = [
    "Deadline",
    "DeadlineExceeded",
    "TaskScope",
    "TaskScopeClosedError",
    "WhenScopeExits",
    "clamp_to_deadline",
    "current_deadline",
    "deadline",
    "finalize",
    "protect",
]
