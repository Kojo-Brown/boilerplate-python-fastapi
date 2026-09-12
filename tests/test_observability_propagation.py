"""W3C context propagation across a carrier that is not an HTTP header map.

The cases here are the ones a `dict(headers)` implementation gets wrong and
nobody notices: a duplicated `traceparent` (where the carrier must surface both
and let the propagator decide, rather than quietly resolving it the opposite way
from the HTTP side), a republished record accumulating a context per hop, and a
header whose bytes are not text.
"""

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import (
    format_span_id,
    format_trace_id,
    get_current_span,
    use_span,
)

from src.observability.propagation import (
    PROPAGATION_FIELDS,
    MessageHeaderGetter,
    MessageHeaders,
    configure_propagation,
    extract_trace_context,
    inject_trace_context,
)

VALID_TRACEPARENT = b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
TRACE_ID_HEX = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID_HEX = "00f067aa0ba902b7"


def traceparent_of(headers: MessageHeaders) -> str:
    return next(value.decode() for name, value in headers if name == "traceparent")


class TestTheGetter:
    def test_it_finds_a_header_whatever_its_case(self) -> None:
        getter = MessageHeaderGetter()
        headers: MessageHeaders = (("TraceParent", VALID_TRACEPARENT),)
        assert getter.get(headers, "traceparent") == [VALID_TRACEPARENT.decode()]

    def test_an_absent_header_is_none_rather_than_empty(self) -> None:
        # The propagator distinguishes the two: None is "no context here",
        # while an empty list is "a key that is present and unusable".
        assert MessageHeaderGetter().get((("other", b"1"),), "traceparent") is None

    def test_every_occurrence_is_returned(self) -> None:
        getter = MessageHeaderGetter()
        headers: MessageHeaders = (
            ("traceparent", VALID_TRACEPARENT),
            ("traceparent", b"00-" + b"a" * 32 + b"-" + b"b" * 16 + b"-01"),
        )
        assert len(getter.get(headers, "traceparent") or []) == 2

    def test_bytes_that_are_not_text_yield_no_usable_value(self) -> None:
        headers: MessageHeaders = (("traceparent", b"\xff\xfe"),)
        assert MessageHeaderGetter().get(headers, "traceparent") == []

    def test_keys_lists_every_header_name(self) -> None:
        headers: MessageHeaders = (("a", b"1"), ("b", b"2"), ("a", b"3"))
        assert MessageHeaderGetter().keys(headers) == ["a", "b", "a"]


class TestExtract:
    def setup_method(self) -> None:
        configure_propagation()

    def test_a_traceparent_becomes_the_parent_of_what_runs_under_it(self) -> None:
        context = extract_trace_context((("traceparent", VALID_TRACEPARENT),))
        span_context = get_current_span(context).get_span_context()
        assert format_trace_id(span_context.trace_id) == TRACE_ID_HEX
        assert format_span_id(span_context.span_id) == SPAN_ID_HEX
        assert span_context.is_remote

    def test_the_sampled_flag_survives_the_hop(self) -> None:
        sampled = extract_trace_context((("traceparent", VALID_TRACEPARENT),))
        assert get_current_span(sampled).get_span_context().trace_flags.sampled
        not_sampled = extract_trace_context(
            (("traceparent", VALID_TRACEPARENT[:-2] + b"00"),)
        )
        assert not get_current_span(not_sampled).get_span_context().trace_flags.sampled

    def test_no_headers_is_no_context_rather_than_an_error(self) -> None:
        context = extract_trace_context(())
        assert not get_current_span(context).get_span_context().is_valid

    def test_a_malformed_traceparent_starts_a_fresh_trace(self) -> None:
        context = extract_trace_context((("traceparent", b"not-a-traceparent"),))
        assert not get_current_span(context).get_span_context().is_valid

    def test_a_duplicated_traceparent_resolves_to_the_first(self) -> None:
        # The carrier surfaces both (see TestTheGetter) and the propagator
        # decides. SDK 1.44 takes the first; a carrier collapsed into a mapping
        # would have taken the last, and this test is what would notice if the
        # two ever stopped agreeing.
        context = extract_trace_context(
            (
                ("traceparent", VALID_TRACEPARENT),
                ("traceparent", b"00-" + b"a" * 32 + b"-" + b"b" * 16 + b"-01"),
            )
        )
        assert (
            format_trace_id(get_current_span(context).get_span_context().trace_id)
            == TRACE_ID_HEX
        )

    def test_undecodable_bytes_start_a_fresh_trace(self) -> None:
        context = extract_trace_context((("traceparent", b"\xff\xfe"),))
        assert not get_current_span(context).get_span_context().is_valid

    def test_tracestate_travels_with_the_traceparent(self) -> None:
        context = extract_trace_context(
            (
                ("traceparent", VALID_TRACEPARENT),
                ("tracestate", b"vendor=opaque-value"),
            )
        )
        state = get_current_span(context).get_span_context().trace_state
        assert state.get("vendor") == "opaque-value"


class TestInject:
    def setup_method(self) -> None:
        configure_propagation()
        self.exporter = InMemorySpanExporter()
        self.provider = TracerProvider()
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.tracer = self.provider.get_tracer(__name__)

    def teardown_method(self) -> None:
        self.provider.shutdown()

    def test_a_recording_span_is_written_into_the_headers(self) -> None:
        span = self.tracer.start_span("publish")
        with use_span(span, end_on_exit=True):
            headers = inject_trace_context()
        span_context = span.get_span_context()
        version, trace_id, span_id, flags = traceparent_of(headers).split("-")
        assert version == "00"
        assert trace_id == format_trace_id(span_context.trace_id)
        assert span_id == format_span_id(span_context.span_id)
        # The sampled bit, without pinning the others: the W3C flags byte has
        # grown a second defined bit (random trace id) and may grow more.
        assert int(flags, 16) & 0x01

    def test_existing_headers_are_kept(self) -> None:
        with use_span(self.tracer.start_span("publish"), end_on_exit=True):
            headers = inject_trace_context((("schema", b"7"), ("origin", b"api")))
        assert ("schema", b"7") in headers
        assert ("origin", b"api") in headers

    def test_the_input_is_not_mutated(self) -> None:
        original: MessageHeaders = (("schema", b"7"),)
        with use_span(self.tracer.start_span("publish"), end_on_exit=True):
            inject_trace_context(original)
        assert original == (("schema", b"7"),)

    def test_republishing_replaces_rather_than_appends(self) -> None:
        # A record that goes round the retry ladder must not collect a
        # traceparent per hop: the consumer would then join whichever one its
        # propagator happens to pick, which is the ambiguity TestExtract pins.
        with use_span(self.tracer.start_span("first"), end_on_exit=True):
            once = inject_trace_context()
        with use_span(self.tracer.start_span("second"), end_on_exit=True):
            twice = inject_trace_context(once)
        assert [name for name, _ in twice].count("traceparent") == 1
        assert traceparent_of(twice) != traceparent_of(once)

    def test_a_stale_context_is_removed_when_nothing_is_recording(self) -> None:
        stale: MessageHeaders = (("traceparent", VALID_TRACEPARENT), ("schema", b"7"))
        headers = inject_trace_context(stale)
        assert headers == (("schema", b"7"),)

    def test_nothing_is_added_outside_a_span(self) -> None:
        assert inject_trace_context((("schema", b"7"),)) == (("schema", b"7"),)

    def test_an_explicit_context_can_be_injected(self) -> None:
        # The consumer-side shape: extract from one record, inject into the
        # next, without either being the process's ambient context.
        context = extract_trace_context((("traceparent", VALID_TRACEPARENT),))
        headers = inject_trace_context(context=context)
        assert traceparent_of(headers).startswith(f"00-{TRACE_ID_HEX}-")


class TestRoundTrip:
    def setup_method(self) -> None:
        configure_propagation()
        self.exporter = InMemorySpanExporter()
        self.provider = TracerProvider()
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.tracer = self.provider.get_tracer(__name__)

    def teardown_method(self) -> None:
        self.provider.shutdown()

    def test_a_consumer_span_joins_the_producer_trace(self) -> None:
        with use_span(self.tracer.start_span("produce"), end_on_exit=True):
            headers = inject_trace_context((("schema", b"7"),))

        context = extract_trace_context(headers)
        with use_span(
            self.tracer.start_span("consume", context=context), end_on_exit=True
        ):
            pass

        produced, consumed = (
            {span.name: span for span in self.exporter.get_finished_spans()}[name]
            for name in ("produce", "consume")
        )
        assert consumed.context.trace_id == produced.context.trace_id
        assert consumed.parent is not None
        assert consumed.parent.span_id == produced.context.span_id

    def test_baggage_travels_too(self) -> None:
        from opentelemetry.baggage import get_baggage, set_baggage

        context = set_baggage("tenant", "acme")
        headers = inject_trace_context(context=context)
        assert any(name == "baggage" for name, _ in headers)
        assert get_baggage("tenant", extract_trace_context(headers)) == "acme"


class TestPropagationFields:
    def test_it_covers_the_w3c_headers(self) -> None:
        assert PROPAGATION_FIELDS == {"traceparent", "tracestate", "baggage"}
