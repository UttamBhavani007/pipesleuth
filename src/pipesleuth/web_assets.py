"""Closed manifest for local browser assets; no filesystem paths from requests."""

UI_ASSETS = {
    "/": ("index.html", "text/html"),
    "/ui/style.css": ("style.css", "text/css"),
    "/ui/app.mjs": ("app.mjs", "text/javascript"),
    "/ui/core.mjs": ("core.mjs", "text/javascript"),
    "/ui/investigations.mjs": ("investigations.mjs", "text/javascript"),
    "/ui/repairs.mjs": ("repairs.mjs", "text/javascript"),
}

CSP = ("default-src 'none'; script-src 'self'; style-src 'self'; "
       "connect-src 'self'; img-src 'self'; base-uri 'none'; "
       "form-action 'none'; frame-ancestors 'none'; object-src 'none'")


def is_ui_asset(scope):
    """Only literal manifest URLs are public; encoded aliases grant no access."""
    path = scope.get("path", "")
    return (path in UI_ASSETS
            and scope.get("raw_path", path.encode("ascii")) == path.encode("ascii"))
