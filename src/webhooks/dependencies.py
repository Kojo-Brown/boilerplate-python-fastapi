"""The FastAPI seam: a route parameter that is an authenticated delivery.

    @router.post("/webhooks/orders")
    async def receive(delivery: VerifiedWebhookDep) -> dict[str, str]:
        event = json.loads(delivery.body)
        ...

The handler never reads the request and never sees an unverified byte. That is
the point of returning the body from the dependency rather than leaving the
handler to call `await request.body()` again: the bytes that were signed are the
bytes that should be parsed, and a second read is an invitation for the two to
differ.

There is deliberately **no pydantic model parameter** in that sketch. A route
declaring one has FastAPI parse and validate the body before any dependency
runs, so the 422 for a malformed payload is answered ahead of the 401 for a
forged one — an unauthenticated caller gets to find out how this endpoint's
schema works. Take `VerifiedWebhookDep` and parse `delivery.body` inside the
handler, where the body has been authenticated first.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Request

from src.config import settings
from src.webhooks.errors import WebhookPayloadTooLargeError
from src.webhooks.factory import get_webhook_verifier
from src.webhooks.verifier import VerifiedDelivery, WebhookVerifier

VerifierDep = Annotated[WebhookVerifier, Depends(get_webhook_verifier)]


async def read_signed_body(request: Request, max_bytes: int) -> bytes:
    """Read the raw body, refusing one larger than `max_bytes`.

    `Content-Length` is checked first so an oversized delivery is refused before
    it is read, and the length of what actually arrived is checked afterwards
    because the header is a claim: it can be absent under
    `Transfer-Encoding: chunked` and it can lie. The second check is the one
    that is true, and the first is the one that is cheap.

    What this cannot do is bound a chunked body's memory, since by the time the
    length is known the bytes are in this process. Starlette buffers the whole
    body to serve `await request.body()`, so the real defence there is the ASGI
    server's own limit or the proxy in front of it; this cap bounds the HMAC and
    keeps an accidental 900 MiB POST from reaching a handler.
    """
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > max_bytes:
        raise WebhookPayloadTooLargeError(
            "Webhook payload is larger than this endpoint accepts.",
            details={"max_bytes": max_bytes},
        )

    body = await request.body()
    if len(body) > max_bytes:
        raise WebhookPayloadTooLargeError(
            "Webhook payload is larger than this endpoint accepts.",
            details={"max_bytes": max_bytes},
        )
    return body


async def verify_webhook_request(
    request: Request, verifier: VerifierDep
) -> VerifiedDelivery:
    """Authenticate the request as a webhook delivery, or raise.

    Only the signature header is read. No timestamp header is consulted even
    when one is present — see `src/webhooks/verifier.py` on why reading it would
    undo the scheme.
    """
    body = await read_signed_body(request, settings.WEBHOOK_MAX_BODY_BYTES)
    return await verifier.verify(
        signature=request.headers.get(verifier.signature_header), body=body
    )


#: An authenticated delivery, as a route parameter.
VerifiedWebhookDep = Annotated[VerifiedDelivery, Depends(verify_webhook_request)]
