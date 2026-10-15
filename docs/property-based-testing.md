# Property-based testing

An example-based test asserts a fact about one input. A property-based test
asserts a fact about *every* input, and leaves finding the awkward ones to a
library. [Hypothesis](https://hypothesis.readthedocs.io) generates inputs from
a strategy, runs the test on each, and — when one fails — *shrinks* it to the
smallest input that still fails before reporting it. The failure arrives as
`""` or `"a" * 129`, not as the forty-character string that happened to be
generated.

```
uv run pytest tests/test_pagination_properties.py tests/test_serializer_properties.py
HYPOTHESIS_PROFILE=ci uv run pytest       # the search CI runs: 500 examples each
```

Two modules, 32 properties:

| file | what it is about |
| --- | --- |
| `tests/test_pagination_properties.py` | `src/pagination.py` — the cursor codec, `build_page`, and a full traversal |
| `tests/test_serializer_properties.py` | every request and response schema — round-trips, and each declared bound |

The example-based suites they sit beside (`tests/test_pagination.py`,
`tests/test_users_profile.py`, `tests/test_s3_upload.py`, `tests/test_auth.py`)
are unchanged and are still the ones to read first. An example says what the
code does; a property says what it must never stop doing. A suite wants both,
and the property is not a replacement for the worked example that documents the
happy path.

## Where a property earns its place, and where it does not

Property-based testing is not uniformly better. It costs a strategy per input
domain, it spends runtime on examples that are nearly all uninteresting, and
for code whose requirement is "return this exact body" a property degenerates
into a reimplementation of the code in the test — which is the failure mode to
watch for. It pays where **a requirement is stated over a range rather than at
a point**. Three such ranges exist here.

### 1. The requirement spans more than one call

Every assertion in `tests/test_pagination.py` is about a single `build_page`
call, and the bugs cursor pagination actually has are not visible from inside
one. Take the cursor from the overflow sentinel rather than from the last
included row:

```python
# the bug
next_cursor = encode_cursor(get_cursor(items[limit]))
# the fix
next_cursor = encode_cursor(get_cursor(page_items[-1]))
```

Both versions return a page of exactly `limit` well-formed rows with
`has_more=True` and a cursor that decodes to a plausible value. Every
single-page assertion passes. The next request then asks for rows *after* the
sentinel, and the sentinel itself — a real row — is never delivered to anybody.

`test_a_full_traversal_visits_every_row_exactly_once` drives the loop a client
drives: fetch `limit + 1` rows with `WHERE id > :cursor`, follow `next_cursor`,
repeat until `has_more` is false, then assert the concatenation equals the whole
dataset. It is the only assertion in this repository that fails on the version
above, and it fails on every dataset with more than one page. Verified by
injecting exactly that change: three properties fail, no example-based test
does.

The page-count budget inside that loop is an assertion rather than a `while`
condition, which matters more than it looks: a cursor that fails to advance
loops forever rather than failing, and an infinite loop in a property test is a
hung CI job with no counterexample in it.

### 2. The requirement is a boundary, and a boundary has two sides

`RegisterRequest.password` is `min_length=8, max_length=128`. The example suite
asserts that a good password is accepted and that `"short"` is refused. Neither
says the line is at 8 — so `min_length=7` passes the whole suite, and the API
starts accepting a credential that is one character weaker than documented.

The schema properties are therefore stated as equivalences — *accepted exactly
when the documented predicate holds* — which makes a bound wrong in either
direction a failure:

```python
accepted = PASSWORD_MIN <= len(password) <= PASSWORD_MAX
assert _accepts(RegisterRequest, email=email, password=password) is accepted
```

That form has a trap, and it is the reason `sized_text` exists.
`st.text(max_size=140)` concentrates on short strings, so the one length out of
141 that distinguishes `min_length=7` from `min_length=8` is generated almost
never: the property passes most runs and fails occasionally, which is worse than
not having it. `sized_text` draws the *length* first — uniformly, and half the
time from within one character of a declared bound — so the boundary is in the
sample on most examples. Injecting `min_length=7` is how that was checked: the
uniform-length version let it through (one length in 141 at 50 examples is a
70% chance of missing it, and it did), and the boundary-weighted version has
failed on it every run since.

The bounds are written as literals in the test rather than imported from the
schema. A property that imports the constant it is checking cannot notice the
constant changing; `WHERE the number lives` is the schema's business, and one
small test reconciles the literal with it so that a deliberate change is a
failing test rather than a silently weaker suite.

### 3. The requirement is "this value survives the wire"

A response model's whole job is that what the handler built is what the client
parses. `test_every_schema_round_trips_through_json` asserts
`model_validate_json(model_dump_json(x)) == x` over an arbitrary instance of
every schema the application serialises, and the `model_dump()` companion
asserts the same for the Python path the streaming export and the event
payloads take. A `datetime` that comes back naive, a `FrozenDict` that
validates into something else, a serialiser that loses a field — each is a
client reading a value the server did not send, and none of them is visible to
a test that checks a status code and one key.

The third property in that class,
`test_the_serialised_body_has_exactly_the_declared_fields`, is about the
published contract rather than the value: no field dropped by an `exclude`, none
added by an alias nobody meant to publish.

## The profiles

`tests/conftest.py` registers two and `HYPOTHESIS_PROFILE` selects one, default
`dev`:

| profile | examples | example database | where |
| --- | --- | --- | --- |
| `dev` | 50 | on | a local run; fast loop, cumulative search |
| `ci` | 500 | off | `.github/workflows/ci.yml`, the `test` job |

The **example database** is the asymmetry worth understanding. Locally,
Hypothesis writes the inputs that failed into `.hypothesis/` and replays them
first on the next run, so a bug found once is re-checked for free forever —
which is why the directory is gitignored but not disabled. On CI the runner is
discarded, so a database there would be written, never read, and would suggest a
cumulative search that is not happening. `print_blob` is on in both: a CI
failure prints a blob to paste into `@reproduce_failure`, which is otherwise a
seed nobody has.

`deadline=None` in both, and it is the one setting here that could be mistaken
for carelessness. Hypothesis fails an example that takes longer than 200ms, and
the first example of any property in this suite is not timing the property: it
is paying for a Pydantic core schema to be built and a validator to be
compiled. That cost lands on whichever example runs first and on no other,
which is a flake whose frequency depends on test order. These properties are
microseconds of pure validation with no I/O in them at all, so a per-example
limit protects against nothing that `max_examples` does not already bound.

The `ci` profile costs about 20 seconds on top of a ~2 minute suite.

## Why these are not in the mutation gate

`src/pagination.py` is one of the eight modules under `only_mutate` in
`pyproject.toml` (see [docs/mutation-testing.md](./mutation-testing.md)), and
the obvious next move — adding these properties to
`pytest_add_cli_args_test_selection` so mutants face them too — is deliberately
not made. Two reasons, in order of weight:

1. **The gate is a floor, and a floor has to be reproducible.** mutmut's score
   would become a function of Hypothesis's seed: a mutant killed by example
   #211 on one run survives on the next, and a threshold that moves on its own
   is a threshold people learn to re-run rather than read. Derandomising the
   profile would fix the arithmetic and cost the search, which is the whole
   value of the properties.
2. **There is nothing there to find.** The current run leaves fifteen
   survivors, and exactly one is in `src/pagination.py`:
   `items[:limit] if has_more else items` becoming an unconditional slice. That
   mutant is *equivalent* — when `len(items) <= limit` the slice returns the
   same rows — so no test, generated or written, can distinguish it. The other
   pagination mutants are already killed.

The properties and the mutation gate are answering the same question from
opposite ends — "is there an input that distinguishes these two programs?" —
and they are run separately because one of them has to produce the same number
twice.

## Adding a property

1. Write the strategy for the input domain, not for the input. If it needs an
   `assume()` that rejects most of what it generates, it is the wrong strategy:
   draw the dependent values together in an `@st.composite` instead, the way
   `overflowing_result_sets` draws rows and a limit they overflow. Hypothesis
   reports heavy filtering as a health check and is right to — a property that
   reaches its own precondition one example in ten has a tenth of the examples
   it claims.
2. State the property as an equivalence where the requirement is a boundary,
   and as an invariant where it is a shape. Avoid the third option, which is
   recomputing the expected output in the test.
3. Check that it can fail. Break the implementation the way a careless change
   would — a comparison, a bound, an index — and watch the property catch it.
   Every property in these two modules was validated that way; the three
   pagination mutations and three schema mutations are in the PR that added
   them.
4. Keep it free of I/O. These run inside the ordinary `test` job with no
   fixtures, which is what makes 500 examples cost 20 seconds rather than
   twenty minutes.
