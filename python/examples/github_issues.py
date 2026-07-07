from __future__ import annotations

from collections.abc import Iterator, Mapping

from cdf_sdk import Context, Row, resource


@resource(
    name="github.issues",
    primary_key=("id",),
    cursor="updated_at",
    parallel=True,
)
def issues(ctx: Context) -> Iterator[Row]:
    token = ctx.secrets.get("secret://env/GITHUB_TOKEN")
    since = ctx.cursor.get("updated_at")
    params = {"since": since} if isinstance(since, str) else None
    response = ctx.http.get(
        "https://api.github.com/issues",
        headers={"authorization": f"Bearer {token}"},
        params=params,
    )
    payload = response.json()
    if not isinstance(payload, list):
        ctx.logger.warning("GitHub issues response was not a list")
        return

    for raw in payload:
        if not isinstance(raw, Mapping):
            continue
        issue_id = raw.get("id")
        title = raw.get("title")
        updated_at = raw.get("updated_at")
        if not isinstance(issue_id, int) or not isinstance(updated_at, str):
            continue
        yield {
            "id": issue_id,
            "title": title if isinstance(title, str) else "",
            "updated_at": updated_at,
        }
