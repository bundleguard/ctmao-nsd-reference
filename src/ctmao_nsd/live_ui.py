"""Loopback-only live dashboard with SSE and browser-local speech synthesis."""

from __future__ import annotations

import json
import secrets
import threading
from collections import deque
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from uuid import UUID
from typing import Protocol

from .conference import (
    ConferenceMode,
    project_briefing,
    project_conference_turns,
    project_message_turns,
)
from .interactive_conference import ConferenceBusyError
from .normalized_events import NormalizedEvent
from .observability import EventBus, EventProjection
from .dashboard_v3 import DASHBOARD_HTML as _DASHBOARD_HTML


class ConferenceControl(Protocol):
    """Synchronous boundary exposed to the loopback HTTP control plane."""

    def start(self, objective: str) -> object: ...
    def cancel(self) -> object: ...
    def snapshot(self) -> object: ...


class LiveDashboard:
    """Serve a bounded live execution view without entering the runtime core."""

    def __init__(
        self,
        event_bus: EventBus,
        host: str = "127.0.0.1",
        control: ConferenceControl | None = None,
    ) -> None:
        if host not in ("127.0.0.1", "localhost"):
            raise ValueError("the V1 dashboard is loopback-only")
        self.event_bus = event_bus
        self.control = control
        self.projection = EventProjection()
        self.token = secrets.token_urlsafe(24)
        self._condition = threading.Condition()
        self._messages: deque[tuple[int, str]] = deque(maxlen=512)
        self._delivery_sequence = 0
        self._run_events: dict[UUID, list[NormalizedEvent]] = {}
        self._closed = False
        self._started = False
        self._listener_connected = threading.Event()
        handler = self._handler_type()
        self._server = ThreadingHTTPServer((host, 0), handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._subscription = event_bus.subscribe(self._accept_event, capacity=256)

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/?token={self.token}"

    def start(self) -> str:
        if self._closed:
            raise RuntimeError("dashboard is closed")
        if self._started:
            return self.url
        self._thread.start()
        self._started = True
        return self.url

    def wait_for_listener(self, timeout: float | None = None) -> bool:
        """Wait until the first authorized SSE listener connects."""
        return self._listener_connected.wait(timeout)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.event_bus.unsubscribe(self._subscription)
        with self._condition:
            self._condition.notify_all()
        # HTTPServer.shutdown() waits for serve_forever() and deadlocks if the
        # dashboard is closed before start().
        if self._started:
            self._server.shutdown()
        self._server.server_close()
        if self._started:
            self._thread.join(timeout=1.0)

    def _accept_event(self, event: NormalizedEvent) -> None:
        self.projection.apply(event)
        if event.run_id is not None:
            if (
                event.event_type == "orchestration.run.started"
                and event.run_id not in self._run_events
                and len(self._run_events) >= 8
            ):
                self._run_events.pop(next(iter(self._run_events)))
            self._run_events.setdefault(event.run_id, []).append(event)
        turns = project_conference_turns((event,), mode=ConferenceMode.TEXT)
        messages = project_message_turns((event,))
        briefings = (
            project_briefing(self._run_events.get(event.run_id, ()))
            if event.event_type.startswith("orchestration.run.")
            else ()
        )
        payload = {
            "event": event.to_dict(),
            "turns": [_turn_dict(turn) for turn in turns],
            "messages": [_message_dict(turn, event) for turn in messages],
            "briefings": [_turn_dict(turn) for turn in briefings],
        }
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        with self._condition:
            self._delivery_sequence += 1
            self._messages.append((self._delivery_sequence, encoded))
            self._condition.notify_all()

    def _messages_after(self, sequence: int) -> tuple[tuple[int, str], ...]:
        with self._condition:
            return tuple(item for item in self._messages if item[0] > sequence)

    def _handler_type(self) -> type[BaseHTTPRequestHandler]:
        dashboard = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "CTMAOConference/0.3"

            def do_GET(self) -> None:  # noqa: N802 - stdlib callback spelling
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                if query.get("token", [None])[0] != dashboard.token:
                    self.send_error(HTTPStatus.FORBIDDEN)
                    return
                if not self._host_allowed():
                    self.send_error(HTTPStatus.FORBIDDEN)
                    return
                if not self._origin_allowed():
                    self.send_error(HTTPStatus.FORBIDDEN)
                    return
                if parsed.path == "/":
                    page = (
                        _LEGACY_DASHBOARD_HTML
                        if query.get("view", [None])[0] == "legacy"
                        else _DASHBOARD_HTML
                    )
                    self._send_bytes("text/html; charset=utf-8", page)
                elif parsed.path == "/api/state":
                    state = _snapshot_dict(dashboard.projection.snapshot())
                    state["conference"] = (
                        dict(dashboard.control.snapshot())
                        if dashboard.control is not None
                        else None
                    )
                    self._send_json(state)
                elif parsed.path == "/events":
                    self._stream_events()
                else:
                    self.send_error(HTTPStatus.NOT_FOUND)

            def do_POST(self) -> None:  # noqa: N802 - stdlib callback spelling
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                if query.get("token", [None])[0] != dashboard.token:
                    self._discard_request_body()
                    self._send_json({"error": "forbidden"}, HTTPStatus.FORBIDDEN)
                    return
                if not self._host_allowed() or not self._origin_allowed():
                    self._discard_request_body()
                    self._send_json({"error": "forbidden"}, HTTPStatus.FORBIDDEN)
                    return
                if dashboard.control is None:
                    self._send_json(
                        {"error": "interactive control is unavailable"},
                        HTTPStatus.NOT_IMPLEMENTED,
                    )
                    return
                if parsed.path == "/api/runs":
                    try:
                        body = self._read_json()
                        objective = body.get("objective")
                        if not isinstance(objective, str):
                            raise ValueError("objective must be a string")
                        state = dashboard.control.start(objective)
                    except ConferenceBusyError as exc:
                        self._send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
                        return
                    except ValueError as exc:
                        self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
                        return
                    self._send_json(dict(state), HTTPStatus.ACCEPTED)
                    return
                if parsed.path == "/api/runs/cancel":
                    self._send_json(dict(dashboard.control.cancel()), HTTPStatus.ACCEPTED)
                    return
                self._send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)

            def _discard_request_body(self) -> None:
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    return
                if 0 < length <= 8192:
                    self.rfile.read(length)

            def _read_json(self) -> dict[str, object]:
                if not self.headers.get("Content-Type", "").lower().startswith(
                    "application/json"
                ):
                    raise ValueError("Content-Type must be application/json")
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    raise ValueError("invalid Content-Length") from None
                if not 1 <= length <= 8192:
                    raise ValueError("request body must contain 1 to 8192 bytes")
                try:
                    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
                        value: dict[str, object] = {}
                        for key, item in items:
                            if key in value:
                                raise ValueError("duplicate JSON key")
                            value[key] = item
                        return value

                    value = json.loads(
                        self.rfile.read(length).decode("utf-8", "strict"),
                        object_pairs_hook=pairs,
                        parse_constant=lambda _: (_ for _ in ()).throw(
                            ValueError("non-finite JSON number")
                        ),
                    )
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                    raise ValueError("request body must be valid UTF-8 JSON") from None
                if not isinstance(value, dict):
                    raise ValueError("request body must be a JSON object")
                return value

            def _host_allowed(self) -> bool:
                request_host = self.headers.get("Host", "")
                host, port = dashboard._server.server_address[:2]
                return request_host in {f"{host}:{port}", f"localhost:{port}"}

            def _origin_allowed(self) -> bool:
                origin = self.headers.get("Origin")
                if origin is None:
                    return True
                host, port = dashboard._server.server_address[:2]
                return origin == f"http://{host}:{port}"

            def _headers(
                self,
                content_type: str,
                length: int | None = None,
                status: HTTPStatus = HTTPStatus.OK,
            ) -> None:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("X-Frame-Options", "DENY")
                self.send_header("Referrer-Policy", "no-referrer")
                self.send_header("Cross-Origin-Resource-Policy", "same-origin")
                self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
                if length is not None:
                    self.send_header("Content-Length", str(length))
                self.end_headers()

            def _send_bytes(self, content_type: str, body: str) -> None:
                encoded = body.encode("utf-8")
                self._headers(content_type, len(encoded))
                self.wfile.write(encoded)

            def _send_json(
                self, value: object, status: HTTPStatus = HTTPStatus.OK
            ) -> None:
                encoded = json.dumps(
                    value, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8")
                self._headers("application/json; charset=utf-8", len(encoded), status)
                self.wfile.write(encoded)

            def _stream_events(self) -> None:
                self._headers("text/event-stream; charset=utf-8")
                dashboard._listener_connected.set()
                try:
                    sequence = int(self.headers.get("Last-Event-ID", "0"))
                except ValueError:
                    sequence = 0
                try:
                    while not dashboard._closed:
                        messages = dashboard._messages_after(sequence)
                        for sequence, payload in messages:
                            frame = f"id: {sequence}\ndata: {payload}\n\n".encode("utf-8")
                            self.wfile.write(frame)
                        if messages:
                            self.wfile.flush()
                        else:
                            self.wfile.write(b": heartbeat\n\n")
                            self.wfile.flush()
                        with dashboard._condition:
                            dashboard._condition.wait(timeout=5.0)
                except (BrokenPipeError, ConnectionResetError):
                    return

            def log_message(self, format: str, *args: object) -> None:
                return

        return Handler


def _snapshot_dict(snapshot: object) -> dict[str, object]:
    return {
        "session_id": str(snapshot.session_id) if snapshot.session_id else None,
        "run_id": str(snapshot.run_id) if snapshot.run_id else None,
        "run_status": snapshot.run_status,
        "latest_sequence": snapshot.latest_sequence,
        "workers": dict(snapshot.workers),
        "tasks": {key: dict(value) for key, value in snapshot.tasks.items()},
        "timeline": [dict(item) for item in snapshot.timeline],
    }


def _turn_dict(turn: object) -> dict[str, object]:
    """Serialize all provenance needed to audit a projected conference turn."""
    return {
        "turn_id": turn.turn_id,
        "session_id": str(turn.session_id),
        "run_id": str(turn.run_id) if turn.run_id else None,
        "speaker_id": turn.speaker_id,
        "text": turn.text,
        "importance": turn.importance.value,
        "evidence": turn.evidence.value,
        "source_evidence": turn.source_evidence.value,
        "source_event_ids": [str(item) for item in turn.source_event_ids],
        "transformation_name": turn.transformation_name,
        "transformation_version": turn.transformation_version,
        "lifecycle_valid": turn.lifecycle_valid,
        "redactions": dict(turn.redactions),
    }


def _message_dict(turn: object, event: NormalizedEvent) -> dict[str, object]:
    value = _turn_dict(turn)
    attributes = dict(event.attributes)
    value["recipient_id"] = attributes.get("recipient_id", "conference")
    value["message_kind"] = attributes.get("message_kind", "statement")
    return value


_LEGACY_DASHBOARD_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CTMAO-NSD Live Agent Conference</title><style>
:root{color-scheme:dark;--bg:#08111d;--panel:#111f30;--ink:#eef6ff;--muted:#9db0c5;--cyan:#51d5d9;--gold:#f4bf60}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top,#16314a,var(--bg) 48%);color:var(--ink);font:15px system-ui}
header{padding:22px 5vw;display:flex;justify-content:space-between;align-items:center}h1{margin:0;font-size:clamp(22px,4vw,38px)}button{background:#17344d;color:var(--ink);border:1px solid #315675;border-radius:8px;padding:9px 13px}
main{display:grid;grid-template-columns:minmax(240px,1fr) minmax(320px,2fr);gap:16px;padding:0 5vw 40px}.panel{background:color-mix(in srgb,var(--panel) 92%,transparent);border:1px solid #263c52;border-radius:14px;padding:18px;min-height:190px}.wide{grid-column:1/-1}
.live{color:var(--cyan)}.row{padding:9px 0;border-bottom:1px solid #263748}.speaker{color:var(--gold);font-weight:700}.meta{color:var(--muted);font-size:12px}.pill{padding:5px 9px;border-radius:999px;background:#17344d}.controls,.agent-controls{display:flex;gap:8px;flex-wrap:wrap;align-items:center}.agent-row{display:flex;justify-content:space-between;gap:12px}.agent-controls select{max-width:170px;background:#0c1b2a;color:var(--ink);border:1px solid #315675;border-radius:6px;padding:5px}.tree-row{padding:5px 0}.failed{color:#ff9b9b}details{margin-top:5px}.status{align-self:center;color:var(--muted);font-size:12px}@media(max-width:760px){main{grid-template-columns:1fr}.wide{grid-column:auto}.agent-row{display:block}}
</style></head><body><header><div><div class="live">● LIVE</div><h1>Agent Conference</h1></div><div class="controls"><button id="mute">Mute</button><button id="pause">Pause</button><button id="mode">Conference</button></div></header>
<main><section class="panel"><h2>Agents</h2><div id="agents" class="meta">Waiting for agents…</div></section><section class="panel"><h2>Current activity</h2><div id="activity">Waiting for execution…</div><p id="audio-status" class="status">Browser speech is optional; the transcript remains authoritative.</p></section><section class="panel wide"><h2>Delegation tree</h2><div id="tree" class="meta">Waiting for delegated tasks…</div></section><section class="panel wide"><h2>Transcript</h2><div class="controls"><button id="brief" disabled>Play latest briefing</button></div><div id="transcript"></div></section></main>
<script>
const token=new URLSearchParams(location.search).get('token');let muted=false,paused=false,briefing=false,speaking=false,latestBriefing=[];const voices=new Map(),speakerMute=new Set(),usedVoices=new Set(),spokenSources=new Set(),taskNodes=new Map();let speechQueue=[];
function hash(s){let h=2166136261;for(const c of s){h^=c.charCodeAt(0);h=Math.imul(h,16777619)}return h>>>0}
function installedVoices(){return 'speechSynthesis'in window?[...speechSynthesis.getVoices()].sort((a,b)=>a.name.localeCompare(b.name)):[]}
function voiceFor(speaker){const all=installedVoices();if(voices.has(speaker)){const chosen=all.find(v=>v.name===voices.get(speaker));if(chosen)return chosen;voices.delete(speaker)}if(!all.length)return null;const pool=speaker==='orchestrator'?all:(all.length>1?all.slice(1):all);let i=speaker==='orchestrator'?0:hash(speaker)%pool.length,selected=pool[i];for(let n=0;n<pool.length;n++){const candidate=pool[(i+n)%pool.length];if(!usedVoices.has(candidate.name)){selected=candidate;break}}voices.set(speaker,selected.name);usedVoices.add(selected.name);return selected}
function pumpSpeech(){if(speaking||muted||paused||!speechQueue.length||!('speechSynthesis'in window))return;const turn=speechQueue.shift(),u=new SpeechSynthesisUtterance(turn.text),voice=voiceFor(turn.speaker_id);if(voice)u.voice=voice;u.rate=.9+(hash(turn.speaker_id)%21)/100;u.pitch=.9+(hash(turn.speaker_id+'pitch')%21)/100;speaking=true;u.onend=()=>{speaking=false;pumpSpeech()};u.onerror=()=>{document.querySelector('#audio-status').textContent='Speech was unavailable; follow the complete transcript below.';speaking=false;pumpSpeech()};speechSynthesis.speak(u)}
function speak(turn){if(muted||paused||speakerMute.has(turn.speaker_id)||!('speechSynthesis'in window))return;const key=[...turn.source_event_ids].sort().join(',');if(spokenSources.has(key))return;if(speechQueue.length>=32){if(turn.importance==='critical')speechQueue.shift();else return}spokenSources.add(key);speechQueue.push(turn);pumpSpeech()}
function resetSpeech(){speechQueue=[];speaking=false;spokenSources.clear();if('speechSynthesis'in window)speechSynthesis.cancel()}
function addTurn(t,e,spoken=true){const row=document.createElement('div');row.className='row';row.innerHTML='<span class="speaker"></span> <span class="text"></span><div class="meta"></div><details><summary>Provenance</summary><div class="meta provenance"></div></details>';row.querySelector('.speaker').textContent=t.speaker_id+':';row.querySelector('.text').textContent=t.text;row.querySelector('.meta').textContent=`#${e.sequence} · ${e.occurred_at} · ${t.evidence}`;row.querySelector('.provenance').textContent=`run ${t.run_id||'none'} · ${t.transformation_name}@${t.transformation_version} · sources ${t.source_event_ids.join(', ')} · source evidence ${t.source_evidence} · lifecycle valid ${t.lifecycle_valid} · redactions ${JSON.stringify(t.redactions)}`;document.querySelector('#transcript').prepend(row);if(spoken)speak(t)}
function fillVoiceSelect(select,id){const current=voices.get(id)||'';select.replaceChildren(new Option('Auto',''));for(const v of installedVoices())select.add(new Option(v.name,v.name));select.value=current}
function renderTree(){const box=document.querySelector('#tree');box.textContent='';const children=new Map();for(const n of taskNodes.values()){const p=n.parent||'';if(!children.has(p))children.set(p,[]);children.get(p).push(n)}function walk(parent,depth){for(const n of (children.get(parent)||[]).sort((a,b)=>a.label.localeCompare(b.label))){const row=document.createElement('div');row.className='tree-row'+(n.status==='failed'?' failed':'');row.style.paddingLeft=`${depth*18}px`;row.textContent=`${depth?'↳ ':'● '}${n.label} — ${n.status||'active'}`;box.append(row);walk(n.id,depth+1)}}walk('',0)}
function eventUpdate(e){document.querySelector('#activity').textContent=e.summary;if(e.task_id){const path=e.agent_path||[],previous=taskNodes.get(e.task_id),depth=path.length,label=path.length?path[path.length-1]:(e.worker_id||e.task_id);taskNodes.set(e.task_id,{id:e.task_id,parent:e.parent_task_id||previous?.parent||'',label:!previous||depth>=previous.depth?label:previous.label,depth:Math.max(depth,previous?.depth||0),status:e.status||previous?.status||e.event_type.split('.').pop()});renderTree()}const id=e.origin==='orchestrator'?'orchestrator':((e.agent_path||[]).length?e.agent_path.join('/'):(e.worker_id||''));if(id){const a=document.querySelector('#agents');if(a.textContent.startsWith('Waiting'))a.textContent='';let el=document.getElementById('a-'+hash(id));if(!el){el=document.createElement('div');el.id='a-'+hash(id);el.className='row agent-row';const info=document.createElement('span'),controls=document.createElement('span'),select=document.createElement('select'),mute=document.createElement('button');info.className='agent-info';controls.className='agent-controls';select.setAttribute('aria-label',`Voice for ${id}`);fillVoiceSelect(select,id);select.onchange=()=>{if(select.value)voices.set(id,select.value);else{voices.delete(id);voiceFor(id)};renderAgentsVoice(id)};mute.textContent='Mute agent';mute.onclick=()=>{speakerMute.has(id)?speakerMute.delete(id):speakerMute.add(id);mute.textContent=speakerMute.has(id)?'Unmute agent':'Mute agent';el.style.opacity=speakerMute.has(id)?.55:1};controls.append(select,mute);el.append(info,controls);a.append(el)}el.querySelector('.agent-info').textContent=`${id} — ${e.status||e.event_type.split('.').pop()}${voices.has(id)?' — '+voices.get(id):''}`}}
function renderAgentsVoice(id){const el=document.getElementById('a-'+hash(id));if(el){const state=el.querySelector('.agent-info').textContent.split(' — ')[1]||'active';el.querySelector('.agent-info').textContent=`${id} — ${state.split(' — ')[0]}${voices.has(id)?' — '+voices.get(id):''}`}}
if('speechSynthesis'in window)speechSynthesis.onvoiceschanged=()=>{document.querySelectorAll('.agent-row').forEach(el=>{const select=el.querySelector('select'),info=el.querySelector('.agent-info').textContent,id=info.split(' — ')[0];fillVoiceSelect(select,id)})};
const es=new EventSource(`/events?token=${encodeURIComponent(token)}`);es.onmessage=x=>{const p=JSON.parse(x.data);eventUpdate(p.event);p.turns.forEach(t=>addTurn(t,p.event,!briefing));if(p.briefings.length){latestBriefing=p.briefings;document.querySelector('#brief').disabled=false}if(briefing)p.briefings.forEach(t=>addTurn(t,p.event,true))};
document.querySelector('#mute').onclick=e=>{muted=!muted;resetSpeech();e.target.textContent=muted?'Unmute':'Mute'};
document.querySelector('#pause').onclick=e=>{paused=!paused;paused?speechSynthesis.pause():speechSynthesis.resume();e.target.textContent=paused?'Resume':'Pause'};
document.querySelector('#mode').onclick=e=>{briefing=!briefing;resetSpeech();e.target.textContent=briefing?'Briefing':'Conference'};
document.querySelector('#brief').onclick=()=>{resetSpeech();for(const turn of latestBriefing)speak(turn)};
</script></body></html>"""
