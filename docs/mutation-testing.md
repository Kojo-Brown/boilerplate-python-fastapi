# Mutation testing

Coverage says a line ran. It does not say anything ran because of it. A suite can
execute every branch of `src/pagination.py` and still pass if `len(items) > limit`
becomes `>=` — which is a page that silently drops or duplicates a row, found by
nobody, with 100% coverage on the file.

Mutation testing asks the other question. [mutmut](https://mutmut.readthedocs.io)
makes one small change to the source — `>` to `>=`, `+` to `-`, a literal to its
successor — runs the tests that touch that function, and records whether any of
them noticed. A mutant that nobody noticed is a *survivor*, and a survivor is
usually a missing assertion. The score is the share of mutants the suite caught.

```
uv run mutmut run                     # mutate and test; writes mutants/
uv run mutmut results                 # which mutants survived
uv run mutmut show <mutant>           # the diff for one of them
uv run mutmut browse                  # the interactive version of both
uv run mutmut export-cicd-stats       # mutants/mutmut-cicd-stats.json
uv run python scripts/check_mutation_score.py   # the gate
```

A full run is about 390 mutants and takes roughly 45 seconds on four cores. It is
cheap because it is scoped — see below.

## The gate

`mutmut run` **exits 0 whether or not mutants survived.** It reports; it does not
block. So the threshold lives in `scripts/check_mutation_score.py`, which reads
mutmut's exported JSON and exits non-zero below the floor. The `mutation` job in
`.github/workflows/ci.yml` runs the three steps in order, and the last one is the
one that can fail the build.

The script separates two failures that look the same from a distance:

- **exit 1** — the score is below the threshold. A real finding; go read
  `mutmut results`.
- **exit 2** — the score could not be computed: no stats file, malformed JSON, a
  bucket mutmut renamed, an interrupted run, or a run with no mutants at all.

That last case is the one worth stating plainly. An `only_mutate` pattern that
matches nothing produces a run of zero mutants, and the obvious arithmetic either
divides by zero or reports 100%. A gate that returns green for measuring nothing
is worse than no gate, so zero mutants is an error.

Two more deliberate choices in the scoring:

- **Timeouts count as detected.** A mutant that makes the suite hang *has* been
  caught — `while pos < length` became `while pos <= length` and the scanner
  stopped terminating. Whether a given infinite-loop mutant is filed under
  `timeout` or `killed` also depends on how loaded the runner is (this repo sees
  two or three of them depending on the machine), so counting only `killed` would
  make the score drift with CI's mood rather than with the test suite.
- **`suspicious` and `segfault` do not.** Both mean mutmut could not determine an
  outcome. A gate that reads "could not tell" as "caught it" passes on a broken
  runner, which is the worst thing it could do.

### The threshold

`DEFAULT_THRESHOLD` is **95.0%**, against a measured 96.15% (375 of 390). That is
about four mutants of headroom: enough that adding a function to one of the
mutated modules does not fail the build on arithmetic alone, and tight enough that
deleting a meaningful assertion does. Raising it is a deliberate edit with its own
PR, the same as lowering it; what it must never be is lowered to make a red build
green, which is the one move that converts this gate into decoration.

## What is mutated, and why so little

`only_mutate` in `pyproject.toml` names **eight modules**:

| module | what a survivor there would mean |
| --- | --- |
| `src/pagination.py` | an off-by-one in a page boundary |
| `src/concurrency/tags.py` | the entity-tag grammar accepting or rejecting wrongly |
| `src/concurrency/etag.py` | `If-Match` comparing with the wrong function |
| `src/concurrency/conditional.py` | `If-None-Match` answering 304 where it owes 412 |
| `src/redaction/keys.py` | a field name matcher that over- or under-matches |
| `src/redaction/values.py` | a validator that passes a non-secret or drops a secret |
| `src/redaction/walk.py` | a depth or cycle bound in the wrong place |
| `src/redaction/processor.py` | a structural log key being rewritten |

`src/` is 214 modules and the suite is around 5,000 tests. Mutating all of it
would be hours per run, and nearly all of those hours would be spent on code where
a survivor says nothing: a changed log level, a swapped keyword in a `Depends()`
wiring line, a literal in a Prometheus bucket list. Mutation score is only
informative where a survivor is a *missing assertion*, which means pure decision
logic with no I/O — boundary comparisons, parser state machines, matching rules.
The eight above are exactly that, and the seven test files that cover them run in
about a second, which is what keeps a mutant cheap enough to be worth generating.

The list is an **allow** list rather than a deny list on purpose. A new module is
simply not mutated until somebody names it here — the failure direction that does
not break the build for the person who just wrote it. Adding one is a deliberate
edit, with the threshold to re-measure afterwards.

### Three configuration lines that are load-bearing

**`pytest_add_cli_args = ["--no-cov", ...]`.** `[tool.pytest.ini_options].addopts`
carries `--cov=src --cov-fail-under=80`, and the seven selected test files cover
nowhere near 80% of `src/`. Left alone, pytest exits non-zero on *every* mutant
run including the unmutated baseline — and mutmut reads a non-zero exit as
"killed". The gate would report a perfect score while testing nothing. This is the
single easiest way to get a mutation gate that cannot fail, so the flag is not a
convenience.

**`mutate_only_covered_lines = true`.** Without it, mutants land on lines the
selected tests never execute and are reported as survivors that no test could have
killed. That drags the score down for reasons a contributor cannot act on, and the
only available fix is to stop believing the number.

**`do_not_mutate_patterns`** excludes a line that is nothing but a long string
literal, which in these eight modules is always the human-readable `detail` of a
raised exception. mutmut mutates such a string four ways — to `None`, dropped,
case-flipped, `XX`-wrapped — and all four survive, because nothing asserts the
prose of a 400 and nothing should: the wording is for a person reading a response
body, and pinning it in a test turns every copy-edit into a failing suite. They
were 22 of 57 survivors before the pattern, enough noise to bury the real findings
underneath.

What *is* contractual about those messages is asserted instead. The header name a
precondition error blames is covered in `tests/test_conditional_get.py`, because
`src/concurrency/tags.py` says a 400 naming the wrong header "sends a client to
read the wrong half of its own code".

## The `src`-package problem

mutmut 3 assumes the src-*layout*: a directory named `src/` that is an import root
holding packages imported *without* a `src.` prefix. It bakes that in twice —
`get_mutant_name` calls `strip_prefix(module_name, prefix="src.")` when deriving a
mutant key from a file path, and `record_trampoline_hit` asserts outright that a
module name never starts with `src.`.

Here `src` is not a layout. It *is* the distribution's importable package: every
module says `from src.foo import bar`, and `[tool.hatch.build.targets.wheel]`
ships it under that name. So the two halves of mutmut disagree about the same
function — from `src/pagination.py` it derives the key
`pagination.x_build_page__mutmut_1`, while the suite imports `src.pagination` and
the trampoline reports a hit for `src.pagination.build_page`. Nothing matches, and
mutmut fails loudly: first that assertion, then a guard reading "tests recorded
trampoline hits but none match any mutant key".

`mutmut_src_layout.py` closes the gap. mutmut puts both `mutants/` and
`mutants/src/` on `sys.path`, so the mutated copy of each file is reachable under
two names; left alone Python executes it twice and builds two unrelated module
objects, each with its own trampoline state, so the tests exercise one while mutmut
activates mutants in the other. The module installs an import finder that collapses
the two names onto one object. It is loaded only by mutmut, through
`pytest_add_cli_args = ["-p", "mutmut_src_layout"]`; a normal `pytest` run never
imports it, and if mutmut stops stripping the prefix, deleting the file and that
one argument is the whole revert. Its docstring carries the detail, including the
two ways Python will double-execute a module if the finder is naive about it.

Renaming the package was the alternative, and it is a 400-file change to suit one
dev tool. Putting the aliasing in `src/__init__.py` was the other, and it ships
import machinery in the wheel to serve something that never runs in production.

## What this found

The item that added the gate also closed the gaps it exposed. These are the ones
worth repeating, because each test had looked fine:

- **`_luhn_ok` was passing with a third of its body unexecuted.** Every valid card
  number in the suite was `4111111111111111` — all ones and a four — so no doubled
  digit ever exceeded nine and the "subtract nine" step never ran. Three mutants of
  that line survived. Fixed by adding the scheme test numbers that contain a digit
  of five or more in a doubled position.
- **A test that asserted nothing it claimed to.** A new case for `_jwt_header_ok`'s
  base64 padding read `scrub(f"token {blob} refused")`. `_AUTH_SCHEME` matches
  `Token <something>` as a credential-bearing header, so the blob was redacted
  before `_JWT` was ever consulted. The test passed, its name said "header
  whose encoding needs padding", and the padding mutants stayed alive. mutmut was
  the only thing that noticed — a caught instance of the exact failure the gate is
  for, in a test written *for* the gate.
- **`match="If-None-Match"` is weaker than it looks.** `pytest.raises(match=...)`
  is a search, so `XXIf-None-MatchXX` satisfies it and mutmut's `XX`-wrapping
  mutant survived. Anchoring to `\AIf-None-Match ` is the assertion the test
  always meant.
- **The depth limit was never pinned, only observed.** `test_depth_is_bounded`
  nested `MAX_DEPTH + 2` deep and asked whether truncation happened *somewhere*,
  which counting from one, comparing with `>` instead of `>=`, and stepping by two
  all satisfy — they truncate at the wrong place, and truncating early silently
  drops the part of a log event somebody is reading. Both sides of the boundary are
  pinned now, for mappings and sequences separately, because the descent is written
  twice.
- **Nested key redaction was untested in isolation.** The nesting test put an
  `email` key over an email address, so the key policy and the value policy would
  each redact it alone, and disabling the key check inside containers changed
  nothing. A harmless value under a sensitive name is the case that separates them.
- **`exc_info` passed through by accident.** The structural-keys test set
  `exc_info=True`, and a bool is a type the walk returns untouched anyway, so it
  passed whether or not `STRUCTURAL_KEYS` was consulted at all. With structlog's
  real three-tuple the walk has plenty to say: it rebuilds the tuple and renders
  each member through `str()`, so an exception whose message holds an address comes
  back as a redacted string where `format_exc_info` expects an exception — and the
  traceback is gone from the log.
- **`widened_with` splits on commas, and nothing said so.** Splitting on
  whitespace instead turns `employee number` into two one-word phrases, and a
  one-word `employee` matches `employee_id`, `employee_count`, `employees` — the
  over-matching `src/redaction/keys.py` exists to avoid. The old tests only checked
  the name the operator wrote.
- **`KeyPolicy.__init__`'s `default=0` was unexecuted code.** `max(..., default=0)`
  consults the default only for an empty phrase set, which nothing in `src/` builds.
  Removing the keyword raises `ValueError`; making it `None` raises `TypeError` from
  the `min` in `_scan`. Both looked fine in every other test.

## The survivors that are still there

Fifteen mutants survive, and none of them is a missing test. Each is *equivalent*:
it changes the source without changing the behaviour, so no input can distinguish
it. Chasing one of these produces a test that documents the mutation tool rather
than the requirement, which is how a mutation score stops being worth reading.

- `pagination.build_page`: `items[:limit] if has_more else items` → always slice.
  When `len(items) <= limit`, `items[:limit] is items`-equal, so the branch is
  decoration either way.
- `_luhn_ok`: `value > 9` → `>= 9`. `value` is a doubled digit, so it is even and
  never exactly 9.
- `_luhn_ok`: `total += value` → `-=`. The check is `total % 10 == 0`, and
  `-t % 10 == 0` exactly when `t % 10 == 0`.
- `_jwt_header_ok`: `-len % 4` → `+len % 4` and `% 5`. Stripped base64 lengths are
  never ≡ 1 (mod 4), so on the reachable residues both mutants produce *more*
  padding than needed, never less — and `binascii.a2b_base64` stops at the first
  `=` and ignores the rest.
- `_sub_jwt`: `text.split(".", 1)[0]` → `split(".")` and `split(".", 2)`. The
  maxsplit cannot change element `[0]`.
- `KeyPolicy.__init__`: `default=0` → `default=1`. `_longest` is only the scan
  window, and a window of one over an empty phrase set still matches nothing.
- `KeyPolicy.is_sensitive` ×3: the three mutants of the memo (never read it, read
  the wrong key, store `None`). The cache is a performance structure; defeating it
  makes `_scan` run again and return the same verdict.
- `KeyPolicy._scan` ×3: the three mutants of the `range` bounds. Each only adds
  lengths that slice past the end of `parts`, which yields a shorter tuple already
  tried at a smaller length.
- `parse_entity_tag_list`: `while pos < length` → `<=`. The extra iteration hits
  the `if pos >= length: break` guard immediately.

Three further mutants of `parse_entity_tag_list` are **timeouts** — they turn the
scanner into an infinite loop — and the gate counts them as detected, which is what
they are.

## When a survivor appears

A survivor is a claim about your tests, not an order. Read the diff with
`mutmut show <mutant>` and decide which it is:

1. **A missing assertion.** Write the test. This is the usual case and the reason
   the gate exists.
2. **An equivalent mutant.** Add it to the list above with the argument for why no
   input can distinguish it. If the score drops below the threshold because of
   genuinely equivalent mutants, the honest fix is to narrow `only_mutate` or to
   lower the threshold *with that reasoning written down* — not to add a test that
   asserts an implementation detail.

What is never the fix: weakening an assertion, or lowering the threshold to make a
red build green. Both leave the number intact and the suite worse, which is the
one outcome this gate was added to prevent.
