"""Interactive tool that reads a connector_spec.yaml and prompts the user
to provide each connection parameter, then writes the result as JSON.

Supports two modes:
  - cli    : Terminal-based prompts (default)
  - browser: Opens a local web form in the browser

When the connector_spec.yaml includes an ``oauth`` section, the tool can
run the OAuth 2.0 authorization code flow to obtain a refresh token
automatically — the user only needs to provide client_id and client_secret.
"""

import argparse
import getpass
import html
import json
import secrets
import sys
import threading
import urllib.parse
import urllib.request
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

import yaml

SOURCES_DIR = Path("src/databricks/labs/community_connector/sources")
TESTS_DIR = Path("tests/unit/sources")
DEFAULT_PORT = 9876


# ---------------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------------

def find_project_root() -> Path:
    """Walk up from cwd to find the project root (contains pyproject.toml)."""
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return current


def resolve_spec_path(source_name: str, project_root: Path) -> Path:
    """Locate and validate the connector_spec.yaml for a source."""
    spec_path = project_root / SOURCES_DIR / source_name / "connector_spec.yaml"
    if not spec_path.exists():
        print(f"Error: spec file not found: {spec_path}", file=sys.stderr)
        sys.exit(1)
    return spec_path


def load_spec(spec_path: Path) -> dict:
    """Parse and return a YAML spec file."""
    with open(spec_path) as f:
        return yaml.safe_load(f)


def extract_parameters(spec: dict) -> list[dict]:
    """Extract connection parameters from the spec, or exit on error."""
    try:
        return spec["connection"]["parameters"]
    except (KeyError, TypeError):
        print("Error: YAML does not contain connection.parameters", file=sys.stderr)
        sys.exit(1)


def extract_oauth_config(spec: dict) -> dict | None:
    """Return the oauth section from the spec, or None."""
    return spec.get("connection", {}).get("oauth")


# ---------------------------------------------------------------------------
# OAuth helpers
# ---------------------------------------------------------------------------

def _build_auth_url(
    oauth_cfg: dict,
    client_id: str,
    redirect_uri: str,
    state: str,
) -> str:
    base = oauth_cfg["authorization_url"]
    qs: dict[str, str] = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state,
    }
    scopes = oauth_cfg.get("scopes", "")
    if scopes:
        qs["scope"] = scopes
    for k, v in oauth_cfg.get("extra_auth_params", {}).items():
        qs[k] = str(v)
    return base + "?" + urllib.parse.urlencode(qs)


def _exchange_code(
    oauth_cfg: dict,
    code: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
) -> dict:
    token_url = oauth_cfg["token_url"]
    body = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
    }).encode()
    req = urllib.request.Request(token_url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


# ---------------------------------------------------------------------------
# CLI mode
# ---------------------------------------------------------------------------

def prompt_for_parameter(param: dict) -> str | None:
    """Prompt the user for a single parameter value in the terminal."""
    name = param["name"]
    description = param.get("description", "").strip()
    required = param.get("required", False)
    is_secret = param.get("secret", False)

    tag = "REQUIRED" if required else "optional"
    print(f"\n── {name} ({tag}) ──")
    if description:
        print(f"   {description}")

    prompt_text = f"  {name}: "
    if is_secret:
        value = getpass.getpass(prompt_text)
    else:
        value = input(prompt_text)

    value = value.strip()

    if not value:
        if required:
            print(f"  ⚠  '{name}' is required. Please provide a value.")
            return prompt_for_parameter(param)
        return None

    return value


def _prompt_oauth_setting(name: str, default: str) -> str:
    """Prompt for an OAuth setting, showing the default. Enter keeps the default."""
    print(f"\n── {name} ──")
    value = input(f"  [{default}]: ").strip()
    return value or default


def _run_oauth_flow_cli(  # pylint: disable=too-many-statements
    oauth_cfg: dict,
    client_id: str,
    client_secret: str,
    port: int,
) -> str | None:
    """Start a temporary local server, open the browser for OAuth, return the refresh token."""
    state = secrets.token_urlsafe(32)
    result: dict[str, str] = {}
    done = threading.Event()
    redirect_uri = f"http://localhost:{port}/oauth/callback"

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # pylint: disable=invalid-name
            """Handle the OAuth callback redirect."""
            parsed = urllib.parse.urlparse(self.path)
            if not parsed.path.rstrip("/").endswith("/oauth/callback"):
                self.send_response(404)
                self.end_headers()
                return

            qs = urllib.parse.parse_qs(parsed.query)

            if qs.get("state", [""])[0] != state:
                self._page("Error", "Invalid state parameter. Please try again.")
                result["error"] = "state_mismatch"
                done.set()
                return

            if "error" in qs:
                err = qs["error"][0]
                self._page("Authorization Denied", f"Provider error: {err}")
                result["error"] = err
                done.set()
                return

            code = qs.get("code", [""])[0]
            if not code:
                self._page("Error", "No authorization code received.")
                result["error"] = "no_code"
                done.set()
                return

            try:
                token_data = _exchange_code(
                    oauth_cfg, code, client_id, client_secret, redirect_uri,
                )
            except Exception as exc:
                self._page("Token Exchange Failed", str(exc))
                result["error"] = str(exc)
                done.set()
                return

            if "error" in token_data:
                self._page("Token Error", token_data.get("error", "unknown"))
                result["error"] = token_data["error"]
                done.set()
                return

            rt = token_data.get("refresh_token")
            if rt:
                result["refresh_token"] = rt
                self._page("Success!", "Authorization complete. You can close this tab.")
            else:
                result["error"] = "No refresh_token in response"
                self._page("Error", "The provider did not return a refresh token.")
            done.set()

        def _page(self, title: str, message: str):
            body = (
                "<!DOCTYPE html><html><head><style>"
                "body{font-family:sans-serif;display:flex;justify-content:center;"
                "align-items:center;min-height:100vh;background:#f5f7fa}"
                ".b{background:#fff;border-radius:12px;padding:48px;text-align:center;"
                "box-shadow:0 4px 24px rgba(0,0,0,.08);max-width:420px}"
                f"</style></head><body><div class='b'><h2>{_esc(title)}</h2>"
                f"<p>{_esc(message)}</p></div></body></html>"
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body.encode())

        def log_message(self, fmt, *args):
            pass

    try:
        server = HTTPServer(("127.0.0.1", port), CallbackHandler)
    except OSError:
        print(f"  Port {port} is in use. Try a different port with --port.")
        return None

    auth_url = _build_auth_url(oauth_cfg, client_id, redirect_uri, state)

    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.5},
        daemon=True,
    )
    thread.start()

    print("\n  Redirect URI (register in your OAuth app if not done):")
    print(f"    {redirect_uri}")
    print("\n  Opening browser for authorization…")
    print("  If the browser doesn't open, visit:")
    print(f"    {auth_url}")
    webbrowser.open(auth_url)

    try:
        done.wait(timeout=300)
    except KeyboardInterrupt:
        print("\n  OAuth flow cancelled.")
        server.shutdown()
        thread.join(timeout=2)
        return None

    server.shutdown()
    thread.join(timeout=2)

    if "error" in result:
        print(f"  Error: {result['error']}")
        return None
    return result.get("refresh_token")


def run_cli(  # pylint: disable=too-many-locals,too-many-statements,too-many-branches
    source_name: str, output_file: Path, port: int,
) -> None:
    """Run the CLI-based interactive authentication flow."""
    project_root = find_project_root()
    spec_path = resolve_spec_path(source_name, project_root)
    spec = load_spec(spec_path)
    display_name = spec.get("display_name", source_name)
    parameters = extract_parameters(spec)
    oauth_cfg = extract_oauth_config(spec)

    if oauth_cfg:
        manual_params = [
            p for p in parameters if p["name"] != "refresh_token"
        ]
    else:
        manual_params = parameters
    required = [p for p in manual_params if p.get("required", False)]
    optional = [p for p in manual_params if not p.get("required", False)]

    print(f"\n{'=' * 60}")
    print(f"  Configure connection for: {display_name}")
    if oauth_cfg:
        print("  OAuth 2.0 flow will obtain the refresh token automatically")
    total_req = len(required) + (1 if oauth_cfg else 0)
    print(f"  {total_req} required, {len(optional)} optional parameter(s)")
    print(f"{'=' * 60}")

    collected: dict[str, str] = {}

    if required:
        print("\n▸ Required parameters:")
        for param in required:
            value = prompt_for_parameter(param)
            if value is not None:
                collected[param["name"]] = value

    if optional:
        print("\n▸ Optional parameters (press Enter to skip):")
        for param in optional:
            value = prompt_for_parameter(param)
            if value is not None:
                collected[param["name"]] = value

    if oauth_cfg:
        cid = collected.get("client_id", "")
        csec = collected.get("client_secret", "")
        if cid and csec:
            # Let user review / override OAuth settings
            print("\n▸ OAuth 2.0 Settings (press Enter to accept defaults):")
            effective_cfg = dict(oauth_cfg)
            effective_cfg["authorization_url"] = _prompt_oauth_setting(
                "authorization_url", oauth_cfg["authorization_url"],
            )
            effective_cfg["token_url"] = _prompt_oauth_setting(
                "token_url", oauth_cfg["token_url"],
            )
            effective_cfg["scopes"] = _prompt_oauth_setting(
                "scopes", oauth_cfg.get("scopes", ""),
            )

            print("\n▸ OAuth 2.0 Authorization")
            refresh_token = _run_oauth_flow_cli(effective_cfg, cid, csec, port)
            if refresh_token:
                collected["refresh_token"] = refresh_token
                print("\n  ✓ Refresh token obtained")
            else:
                print("\n  ✗ OAuth flow did not succeed. Enter the token manually:")
                token_param = {
                    "name": "refresh_token",
                    "required": True,
                    "secret": True,
                }
                value = prompt_for_parameter(token_param)
                if value:
                    collected["refresh_token"] = value

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(collected, f, indent=2)
        f.write("\n")
    print(f"\n✓ Configuration saved to {output_file}")


# ---------------------------------------------------------------------------
# Browser mode
# ---------------------------------------------------------------------------

_CSS = """\
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
background:#f5f7fa;color:#1e293b;min-height:100vh;display:flex;justify-content:center;padding:40px 16px}
.card{background:#fff;border-radius:12px;box-shadow:0 4px 24px rgba(0,0,0,.08);max-width:640px;
width:100%;padding:40px;margin:auto}
h1{font-size:1.5rem;font-weight:700;margin-bottom:4px}
.subtitle{font-size:.875rem;color:#64748b;margin-bottom:32px;line-height:1.5}
.section-title{font-size:.8rem;font-weight:600;text-transform:uppercase;letter-spacing:.05em;
color:#94a3b8;margin:28px 0 12px;padding-bottom:8px;border-bottom:1px solid #e2e8f0}
.section-title:first-of-type{margin-top:0}
.field{margin-bottom:20px}
.field label{display:block;font-weight:600;font-size:.875rem;margin-bottom:4px}
.field .desc{font-size:.8rem;color:#64748b;margin-bottom:6px;line-height:1.4}
.field input[type=text],.field input[type=password]{width:100%;padding:10px 12px;border:1px solid #cbd5e1;
border-radius:8px;font-size:.875rem;transition:border .15s}
.field input:focus{outline:none;border-color:#3b82f6;box-shadow:0 0 0 3px rgba(59,130,246,.15)}
.tag{display:inline-block;font-size:.65rem;font-weight:600;padding:2px 6px;border-radius:4px;
margin-left:6px;vertical-align:middle}
.tag.req{background:#fee2e2;color:#b91c1c}
.tag.opt{background:#e0f2fe;color:#0369a1}
.auth-select{width:100%;padding:10px 12px;border:1px solid #cbd5e1;border-radius:8px;
font-size:.875rem;background:#fff;color:#1e293b;cursor:pointer;margin-bottom:20px;
transition:border .15s;appearance:auto}
.auth-select:focus{outline:none;border-color:#3b82f6;box-shadow:0 0 0 3px rgba(59,130,246,.15)}
.auth-panel{display:none}
.auth-panel.active{display:block}
.oauth-btn{display:block;width:100%;padding:12px;background:#059669;color:#fff;border:none;
border-radius:8px;font-size:.875rem;font-weight:600;cursor:pointer;transition:background .15s;
margin-top:8px}
.oauth-btn:hover{background:#047857}
.oauth-status{font-size:.8rem;margin-top:8px;min-height:1.2em}
.oauth-status.pending{color:#d97706}
.oauth-status.success{color:#059669}
.oauth-status.error{color:#dc2626}
button[type=submit]{width:100%;padding:12px;background:#2563eb;color:#fff;border:none;
border-radius:8px;font-size:.95rem;font-weight:600;cursor:pointer;margin-top:28px;transition:background .15s}
button[type=submit]:hover{background:#1d4ed8}
.success-card{text-align:center;padding:60px 40px}
.success-card .check{font-size:3rem;margin-bottom:16px}
.success-card h1{margin-bottom:12px}
.success-card p{color:#64748b;font-size:.9rem;line-height:1.5}
.success-card code{background:#f1f5f9;padding:2px 8px;border-radius:4px;font-size:.8rem}
.redirect-uri{background:#f1f5f9;padding:8px 12px;border-radius:6px;font-size:.75rem;
color:#64748b;margin-bottom:16px;word-break:break-all}
.redirect-uri strong{color:#334155}
"""

_AUTH_METHODS_JS = """\
function switchAuth(){
  var sel=document.getElementById('auth_method_choice');
  var idx=sel.selectedIndex;
  document.querySelectorAll('.auth-panel').forEach(function(p,i){
    p.classList.toggle('active',i===idx);
    p.querySelectorAll('input').forEach(function(inp){inp.disabled=i!==idx;});
  });
}
document.addEventListener('DOMContentLoaded',function(){
  var sel=document.getElementById('auth_method_choice');
  if(sel) switchAuth();
});
"""

_OAUTH_JS = """\
async function startOAuth(){
  var cidEl=document.getElementById('client_id');
  var csecEl=document.getElementById('client_secret');
  if(!cidEl||!cidEl.value.trim()){alert('Please fill in client_id first.');return}
  if(!csecEl||!csecEl.value.trim()){alert('Please fill in client_secret first.');return}
  var body={};
  document.querySelectorAll('input[name]:not([disabled])').forEach(function(el){
    if(el.value.trim()) body[el.name]=el.value.trim();
  });
  var au=document.getElementById('__oauth_authorization_url');
  var tu=document.getElementById('__oauth_token_url');
  var sc=document.getElementById('__oauth_scopes');
  if(au) body.__oauth_authorization_url=au.value.trim();
  if(tu) body.__oauth_token_url=tu.value.trim();
  if(sc) body.__oauth_scopes=sc.value.trim();
  var st=document.getElementById('oauth-status');
  if(st){st.textContent='Starting OAuth flow\\u2026';st.className='oauth-status pending'}
  try{
    var resp=await fetch('/oauth/start',{
      method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)
    });
    if(!resp.ok) throw new Error('Server error '+resp.status);
    var data=await resp.json();
    if(st){st.textContent='Waiting for authorization in popup\\u2026'}
    var popup=window.open(data.auth_url,'oauth_popup','width=600,height=700,scrollbars=yes');
    if(!popup&&st){st.textContent='Popup blocked \\u2014 please allow popups and try again.';
      st.className='oauth-status error'}
  }catch(e){if(st){st.textContent='Failed: '+e.message;st.className='oauth-status error'}}
}
window.addEventListener('message',function(e){
  if(!e.data||!e.data.type) return;
  var st=document.getElementById('oauth-status');
  if(e.data.type==='oauth_complete'){
    var f=document.getElementById('refresh_token');
    if(f) f.value=e.data.refresh_token;
    if(st){st.textContent='\\u2713 Refresh token obtained';st.className='oauth-status success'}
  }else if(e.data.type==='oauth_error'){
    if(st){st.textContent='\\u2717 '+e.data.error;st.className='oauth-status error'}
  }
});
"""


def _esc(text: str) -> str:
    return html.escape(text, quote=True)


def _render_field(param: dict, disabled: bool = False) -> str:
    name = param["name"]
    desc = param.get("description", "").strip()
    required = param.get("required", False)
    is_secret = param.get("secret", False)

    tag_cls = "req" if required else "opt"
    tag_label = "required" if required else "optional"
    input_type = "password" if is_secret else "text"
    req_attr = "required" if required else ""
    dis_attr = "disabled" if disabled else ""

    lines = ['<div class="field">']
    lines.append(f'<label for="{_esc(name)}">{_esc(name)}'
                 f'<span class="tag {tag_cls}">{tag_label}</span></label>')
    if desc:
        lines.append(f'<div class="desc">{_esc(desc)}</div>')
    lines.append(f'<input type="{input_type}" id="{_esc(name)}" name="{_esc(name)}" '
                 f'autocomplete="off" {req_attr} {dis_attr}/>')
    lines.append('</div>')
    return "\n".join(lines)


def _render_oauth_section(oauth_cfg: dict, port: int) -> str:
    """Render the OAuth 2.0 section with editable settings and Authorize button."""
    redirect_uri = f"http://localhost:{port}/oauth/callback"
    auth_url = _esc(oauth_cfg.get("authorization_url", ""))
    token_url = _esc(oauth_cfg.get("token_url", ""))
    scopes = _esc(oauth_cfg.get("scopes", ""))

    lines = [
        '<div class="section-title">OAuth 2.0 Authorization</div>',
        f'<div class="redirect-uri"><strong>Redirect URI</strong> '
        f'(register in your OAuth app): <code>{_esc(redirect_uri)}</code></div>',
        # authorization_url — no name attr so it doesn't submit with the form
        '<div class="field">',
        '<label for="__oauth_authorization_url">authorization_url'
        '<span class="tag opt">editable</span></label>',
        f'<input type="text" id="__oauth_authorization_url"'
        f' value="{auth_url}" autocomplete="off"/>',
        '</div>',
        # token_url
        '<div class="field">',
        '<label for="__oauth_token_url">token_url'
        '<span class="tag opt">editable</span></label>',
        f'<input type="text" id="__oauth_token_url" value="{token_url}" autocomplete="off"/>',
        '</div>',
        # scopes
        '<div class="field">',
        '<label for="__oauth_scopes">scopes'
        '<span class="tag opt">editable</span></label>',
        f'<input type="text" id="__oauth_scopes" value="{scopes}" autocomplete="off"/>',
        '</div>',
        # hidden refresh_token (filled by OAuth flow, submitted with form)
        '<input type="hidden" id="refresh_token" name="refresh_token" />',
        # Authorize button + status
        '<button type="button" class="oauth-btn" onclick="startOAuth()">Authorize</button>',
        '<div class="oauth-status" id="oauth-status"></div>',
    ]
    return "\n".join(lines)


def _build_form_html(  # pylint: disable=too-many-locals,too-many-branches
    spec: dict, source_name: str, port: int,
) -> str:
    display_name = spec.get("display_name", source_name)
    conn = spec.get("connection", {})
    auth_methods = conn.get("auth_methods", [])
    shared_params = conn.get("parameters", [])
    oauth_cfg = extract_oauth_config(spec)

    parts: list[str] = []
    parts.append(f'<h1>{_esc(display_name)}</h1>')
    parts.append('<div class="subtitle">'
                 'Configure credentials for community connector development. '
                 'These will be used to connect to a real source system so the connector '
                 'can be tested, validated, and iterated on. '
                 'We recommend using a testing, dev, or sandbox account.'
                 '</div>')
    parts.append('<form method="POST" action="/save">')

    if auth_methods:
        parts.append('<div class="section-title">Authentication Method</div>')
        parts.append('<select id="auth_method_choice" name="__auth_method__" '
                     'class="auth-select" onchange="switchAuth()">')
        for method in auth_methods:
            m_name = method["name"]
            m_desc = method.get("description", m_name).strip().split(".")[0]
            parts.append(f'<option value="{_esc(m_name)}">{_esc(m_desc)}</option>')
        parts.append('</select>')

        for i, method in enumerate(auth_methods):
            active = "active" if i == 0 else ""
            parts.append(f'<div class="auth-panel {active}">')
            for param in method.get("parameters", []):
                parts.append(_render_field(param, disabled=i != 0))
            parts.append('</div>')

    if shared_params:
        if oauth_cfg:
            params_to_show = [
                p for p in shared_params
                if p["name"] != "refresh_token"
            ]
        else:
            params_to_show = shared_params
        required = [p for p in params_to_show if p.get("required", False)]
        optional = [p for p in params_to_show if not p.get("required", False)]
        if required:
            label = "Required Parameters" if not auth_methods else "Common Required Parameters"
            parts.append(f'<div class="section-title">{label}</div>')
            for param in required:
                parts.append(_render_field(param))
        if optional:
            label = "Optional Parameters" if not auth_methods else "Common Optional Parameters"
            parts.append(f'<div class="section-title">{label}</div>')
            for param in optional:
                parts.append(_render_field(param))

    # OAuth section at the bottom
    if oauth_cfg:
        parts.append(_render_oauth_section(oauth_cfg, port))

    parts.append('<button type="submit">Save Configuration</button>')
    parts.append('</form>')

    # Build <script> block
    js_parts = []
    if auth_methods:
        js_parts.append(_AUTH_METHODS_JS)
    if oauth_cfg:
        js_parts.append(_OAUTH_JS)
    js_block = "\n".join(js_parts)

    body = "\n".join(parts)
    return (f'<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<title>Configure {_esc(display_name)}</title>'
            f'<style>{_CSS}</style><script>{js_block}</script></head>'
            f'<body><div class="card">{body}</div></body></html>')


def _build_success_html(display_name: str, output_file: Path) -> str:
    body = (f'<div class="success-card">'
            f'<div class="check">&#10003;</div>'
            f'<h1>Configuration Saved</h1>'
            f'<p>Your <strong>{_esc(display_name)}</strong> connection parameters '
            f'have been saved to:<br/><code>{_esc(str(output_file))}</code></p>'
            f'<p style="margin-top:24px;color:#94a3b8;font-size:.8rem">'
            f'You can close this tab. The server will shut down automatically.</p>'
            f'</div>')
    return (f'<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<title>Saved</title><style>{_CSS}</style></head>'
            f'<body><div class="card">{body}</div></body></html>')


def _build_oauth_popup_html(
    refresh_token: str | None = None, error: str | None = None,
) -> str:
    """HTML for the OAuth popup that sends the result back to the opener via postMessage."""
    if refresh_token:
        title = "Authorization Successful"
        message = "Token obtained! This window will close automatically."
        payload = json.dumps({"type": "oauth_complete", "refresh_token": refresh_token})
        auto_close = "setTimeout(function(){window.close()},1500);"
    else:
        title = "Authorization Failed"
        message = f"Error: {error or 'unknown'}"
        payload = json.dumps({"type": "oauth_error", "error": error or "unknown"})
        auto_close = ""

    return (
        f'<!DOCTYPE html><html><head><meta charset="utf-8">'
        f'<title>{_esc(title)}</title>'
        f'<style>body{{font-family:sans-serif;display:flex;justify-content:center;'
        f'align-items:center;min-height:100vh;background:#f5f7fa}}'
        f'.b{{background:#fff;border-radius:12px;padding:48px;text-align:center;'
        f'box-shadow:0 4px 24px rgba(0,0,0,.08);max-width:420px}}</style></head>'
        f'<body><div class="b"><h2>{_esc(title)}</h2><p>{_esc(message)}</p></div>'
        f'<script>if(window.opener){{window.opener.postMessage({payload},"*");}}'
        f'{auto_close}</script></body></html>'
    )


def run_browser(  # pylint: disable=too-many-statements,too-many-locals
    source_name: str, output_file: Path, port: int,
) -> None:
    """Run the browser-based interactive authentication flow."""
    project_root = find_project_root()
    spec_path = resolve_spec_path(source_name, project_root)
    spec = load_spec(spec_path)
    display_name = spec.get("display_name", source_name)
    oauth_cfg = extract_oauth_config(spec)
    form_html = _build_form_html(spec, source_name, port)

    # Shared mutable state for the OAuth flow (state_token -> credentials)
    oauth_pending: dict[str, dict] = {}
    shutdown_event = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # pylint: disable=invalid-name
            """Serve the form or handle OAuth callback."""
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path.rstrip("/")

            if path == "/oauth/callback" and oauth_cfg:
                self._handle_oauth_callback(parsed)
                return

            self._send_html(form_html)

        def do_POST(self):  # pylint: disable=invalid-name
            """Handle form submission or OAuth start."""
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path.rstrip("/")
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)

            if path == "/oauth/start" and oauth_cfg:
                self._handle_oauth_start(raw)
                return

            if path == "/save":
                self._handle_save(raw)
                return

            self.send_response(404)
            self.end_headers()

        def _handle_oauth_start(self, raw_body: bytes):
            body = json.loads(raw_body.decode())
            client_id = body.get("client_id", "")
            client_secret = body.get("client_secret", "")

            # Build effective config from spec defaults + user overrides
            effective_cfg = dict(oauth_cfg)
            if body.get("__oauth_authorization_url"):
                effective_cfg["authorization_url"] = body["__oauth_authorization_url"]
            if body.get("__oauth_token_url"):
                effective_cfg["token_url"] = body["__oauth_token_url"]
            if "__oauth_scopes" in body:
                effective_cfg["scopes"] = body["__oauth_scopes"]

            state = secrets.token_urlsafe(32)
            redirect_uri = f"http://localhost:{port}/oauth/callback"
            oauth_pending[state] = {
                "client_id": client_id,
                "client_secret": client_secret,
                "effective_cfg": effective_cfg,
            }
            auth_url = _build_auth_url(effective_cfg, client_id, redirect_uri, state)
            self._send_json({"auth_url": auth_url})

        def _handle_oauth_callback(self, parsed):
            qs = urllib.parse.parse_qs(parsed.query)
            state = qs.get("state", [""])[0]

            if state not in oauth_pending:
                self._send_html(_build_oauth_popup_html(error="Invalid state. Try again."))
                return

            pending = oauth_pending.pop(state)
            effective_cfg = pending.get("effective_cfg", oauth_cfg)

            if "error" in qs:
                self._send_html(_build_oauth_popup_html(error=qs["error"][0]))
                return

            code = qs.get("code", [""])[0]
            if not code:
                self._send_html(_build_oauth_popup_html(error="No authorization code received."))
                return

            redirect_uri = f"http://localhost:{port}/oauth/callback"
            try:
                token_data = _exchange_code(
                    effective_cfg, code,
                    pending["client_id"], pending["client_secret"],
                    redirect_uri,
                )
            except Exception as exc:
                self._send_html(_build_oauth_popup_html(error=str(exc)))
                return

            if "error" in token_data:
                self._send_html(_build_oauth_popup_html(error=token_data.get("error", "unknown")))
                return

            rt = token_data.get("refresh_token")
            if rt:
                self._send_html(_build_oauth_popup_html(refresh_token=rt))
            else:
                err = "No refresh token in provider response."
                self._send_html(_build_oauth_popup_html(error=err))

        def _handle_save(self, raw_body: bytes):
            fields = urllib.parse.parse_qs(raw_body.decode(), keep_blank_values=True)
            collected: dict[str, str] = {}
            for key, values in fields.items():
                if key.startswith("__"):
                    continue
                val = values[0].strip() if values else ""
                if val:
                    collected[key] = val

            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w") as f:
                json.dump(collected, f, indent=2)
                f.write("\n")

            self._send_html(_build_success_html(display_name, output_file))
            print(f"\n✓ Configuration saved to {output_file}")
            shutdown_event.set()

        def _send_html(self, content: str):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(content.encode())

        def _send_json(self, obj: dict):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(obj).encode())

        def log_message(self, fmt, *args):
            pass

    try:
        server = HTTPServer(("127.0.0.1", port), Handler)
    except OSError:
        print(f"Error: port {port} is already in use. Try --port <number>.", file=sys.stderr)
        sys.exit(1)

    actual_port = server.server_address[1]
    url = f"http://localhost:{actual_port}"

    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.5},
        daemon=True,
    )
    thread.start()

    print(f"\n  Authenticate '{display_name}' in your browser:")
    print(f"  → {url}")
    if oauth_cfg:
        print("\n  OAuth redirect URI (register in your app settings):")
        print(f"    {url}/oauth/callback")
    print("\n  Waiting for form submission … (Ctrl+C to cancel)")
    webbrowser.open(url)

    try:
        shutdown_event.wait()
    except KeyboardInterrupt:
        print("\nCancelled.")

    server.shutdown()
    thread.join(timeout=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point for the authenticate tool."""
    parser = argparse.ArgumentParser(
        description=(
            "Interactively collect connection parameters defined"
            " in a connector_spec.yaml and save them as JSON."
        ),
    )
    parser.add_argument(
        "-s", "--source", required=True,
        help="Source connector name (e.g. zendesk, stripe, github)",
    )
    parser.add_argument(
        "-o", "--output", default=None,
        help="Output file path (default: tests/unit/sources/<source>/configs/dev_config.json)",
    )
    parser.add_argument(
        "-m", "--mode", choices=["cli", "browser"], default="cli",
        help="Interaction mode: 'cli' for terminal prompts "
             "(default), 'browser' for a local web form",
    )
    parser.add_argument(
        "-p", "--port", type=int, default=DEFAULT_PORT,
        help=f"Port for the local server / OAuth callback (default: {DEFAULT_PORT})",
    )
    args = parser.parse_args()

    if args.output:
        output_file = Path(args.output)
    else:
        project_root = find_project_root()
        output_file = project_root / TESTS_DIR / args.source / "configs" / "dev_config.json"

    if args.mode == "browser":
        run_browser(args.source, output_file, args.port)
    else:
        run_cli(args.source, output_file, args.port)


if __name__ == "__main__":
    main()
