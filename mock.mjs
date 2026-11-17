// Deterministic LLM endpoint + request counter for harness-bench.
// APIs: Anthropic /v1/messages, OpenAI /v1/responses and /v1/chat/completions. Route prefix /t/<tag>/ tags every request.
// Script: scenario keyword in the conversation picks a list of shell steps; next step = first whose marker is absent from tool output.
// Fault injection per tag: POST /ctl {tag, kind: 429|401|500|reset, after, count, window_s}: after `after` model requests pass, the next `count`
// requests (or all requests during `window_s` seconds from the first injected one) fail: HTTP status, or socket destroyed (reset).
import http from "node:http";
import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { randomBytes } from "node:crypto";

const port = Number(process.env.HB_PORT || 18899);
const outDir = process.env.HB_OUT || "/tmp/hb-mock";
const ttftBase = Number(process.env.HB_TTFT_MS ?? 1700);
const ttftPerKtok = Number(process.env.HB_TTFT_PER_KTOK_MS ?? 0);
const tps = Number(process.env.HB_TPS ?? 120);
const keepBodies = process.env.HB_BODIES !== "0";
mkdirSync(`${outDir}/bodies`, { recursive: true });
const log = `${outDir}/requests.jsonl`;
const faults = new Map();
const quotas = new Map();
let seq = 0;

const SCRIPTS = {
	HBTASK: [
		["cat calc.py", "def add"],
		["sed -i 's/return a - b/return a + b/' calc.py && printf 'PATCH%s\\n' ED", "PATCHED"],
		["python3 test_calc.py", "TESTS OK"],
	],
	HBSLOW: [
		["printf 'ONE-%s\\n' DONE", "ONE-DONE"],
		["python3 slow_step.py", "TWO-DONE"],
		["printf 'THREE-%s\\n' DONE", "THREE-DONE"],
	],
	HBIDLE: [["printf 'IDLE-%s\\n' READY", "IDLE-READY"]],
	// graded-fault task: each step reads 16 KB and appends one line to sidefx.log, so re-executed steps show up as duplicate side effects
	HBFX: [1, 2, 3, 4].map((k) => [`printf 'FX-%s\\n' ${k} | tee -a sidefx.log; cat big_0${k}.txt`, `FX-${k}`]),
	HBSTREAM: [["printf 'ONE-%s\\n' DONE", "ONE-DONE"]],
	HBLONG: Array.from({ length: 20 }, (_, i) => { const k = String(i + 1).padStart(2, "0"); return [`printf 'BIG-%s-END\\n' ${k}; cat big_${k}.txt`, `BIG-${k}-END`]; }),
};
// Count-driven scripts: step i is issued when the conversation holds i tool outputs (no marker needed); HBLOOP never ends its turn.
const SEQ = {
	HBHANG: ["python3 -c 'input()'", "python3 -c 'import time; time.sleep(100000)'"],
	HBFLOOD: ["head -c 500000000 /dev/zero | tr '\\0' a"],
	HBHUGE: ["head -c 50000000 /dev/zero | tr '\\0' a", "head -c 7500000 /dev/urandom | base64 -w0", "head -c 2000000 /dev/urandom"],
	HBLOOP: [],
	HBBURST: ["cat big_01.txt", "cat big_02.txt", "cat big_03.txt"],
	// heavy work turn for the burst phase: 12 reads of 16 KB, a 20 KB report written through the shell, re-read, word count, then a streamed markdown answer
	HBWORK: [...Array.from({ length: 12 }, (_, i) => `cat big_${String(i + 1).padStart(2, "0")}.txt`),
		"cat big_01.txt big_02.txt | head -c 20000 > report.md && wc -c report.md", "cat report.md", "wc -w big_*.txt report.md"],
	HBGROW: [],
	HBMARATHON: Array.from({ length: 300 }, (_, i) => `cat big_${String((i % 20) + 1).padStart(2, "0")}.txt; echo STEP-${i}`),
};
const REPORT_MD = ["# File review", "", "Read all twenty files. Summary:", "",
	...Array.from({ length: 20 }, (_, i) => `- **big_${String(i + 1).padStart(2, "0")}.txt**: maritime word list, ~16 KB, ends with marker \`BIG-${String(i + 1).padStart(2, "0")}-END\`.`),
	"", "## Counts", "", "| file | lines | bytes |", "|---|---|---|", ...Array.from({ length: 20 }, (_, i) => `| big_${String(i + 1).padStart(2, "0")}.txt | 190 | 16000 |`),
	"", "## Sample", "", "```python", "from pathlib import Path", "for f in sorted(Path('.').glob('big_*.txt')):", "    print(f.name, len(f.read_text().split()))", "```", ""].join("\n");
const FINAL = { HBFX: "All four steps done.", HBTASK: "Fixed: add() now returns a + b and the tests pass.", HBSLOW: "All three steps done.", HBIDLE: "Ready.", HBLONG: REPORT_MD, HBHANG: "Hang steps done.", HBBURST: REPORT_MD, HBWORK: REPORT_MD, HBGROW: REPORT_MD, HBMARATHON: REPORT_MD, HBHUGE: "Huge outputs done.", HBFLOOD: "Flood done.", HBSTREAM: "STREAM-BEGIN " + "the harness keeps streaming this long benchmark answer while the fault happens ".repeat(150) + "STREAM-END" };

const PROMPT_HINT = { HBFX: "run the four benchmark steps in order", HBTASK: "fix calc.py so the tests pass" };
const MIDSTREAM = new Set(["truncate", "stall", "trickle", "sse_error", "malformed"]);
// Model-level faults: applied only to a tool-call reply (the count is consumed only then). n/a cells are flagged in the request log.
const MODEL_FAULTS = new Set(["truncate_tool", "max_tokens_tool", "bad_tool_call", "unknown_tool", "dup_events", "reorder"]);
const isDelta = (api, e) => api === "anthropic" ? e[0] === "content_block_delta" : api === "responses" ? /\.delta$/.test(e.type) : api === "chat" ? !!(e.choices?.[0]?.delta?.tool_calls?.[0]?.function?.arguments || e.choices?.[0]?.delta?.content) : true;
function planFault(p, kind, api) {
	if (kind === "unknown_tool") return { ...p, name: "hb_unknown_tool", args: { query: "status" }, input: p.input };
	if (kind !== "bad_tool_call") return p;
	if (p.kind === "custom") return { ...p, input: "const r = await tools.exec_command({cmd: ;" };
	if (api === "gemini") return { ...p, args: {} };
	return { ...p, rawArgs: JSON.stringify(p.args).slice(0, -1) + ',"x":}' };
}
// returns [events, cutAt|null, na]
function mutate(ev, api, kind) {
	const d = ev.map((e, i) => (isDelta(api, e) ? i : -1)).filter((i) => i >= 0 && !(api === "gemini" && false));
	// responses: the item added/done events are doubled too (same ids), so a harness can dedupe by item id
	if (kind === "dup_events") return [ev.flatMap((e, i) => (d.includes(i) || (api === "responses" && /^response\.output_item\.(added|done)$/.test(e.type)) ? [e, e] : [e])), null, false];
	if (kind === "reorder") {
		if (api !== "anthropic" && api !== "responses") return [ev, null, true];
		const out = [...ev], [x] = out.splice(d[0], 1);
		out.splice(1, 0, x);
		return [out, null, false];
	}
	if (kind === "truncate_tool") {
		if (api === "gemini") return [ev, 0, true];
		return [ev, d[Math.min(d.length - 1, Math.max(1, Math.floor(d.length * 0.75)))], false];  // past the side effect (tee), so executing the partial call shows up as a duplicate
	}
	if (kind === "max_tokens_tool") {
		const half = d[Math.max(0, Math.floor(d.length * 0.75))];
		if (api === "anthropic") return [[...ev.slice(0, half), ["content_block_stop", { type: "content_block_stop", index: 0 }], ["message_delta", { type: "message_delta", delta: { stop_reason: "max_tokens", stop_sequence: null }, usage: { output_tokens: 8 } }], ["message_stop", { type: "message_stop" }]], null, false];
		if (api === "responses") return [[...ev.slice(0, half), { type: "response.incomplete", response: { id: ev[0].response.id, object: "response", status: "incomplete", incomplete_details: { reason: "max_output_tokens" }, model: ev[0].response.model, output: [] } }], null, false];
		if (api === "chat") return [[...ev.slice(0, half), { ...ev.at(-1), choices: [{ index: 0, delta: {}, finish_reason: "length" }] }], null, false];
		return [[{ ...ev[0], candidates: [{ content: { role: "model", parts: [] }, index: 0, finishReason: "MAX_TOKENS" }] }], null, false];
	}
	return [ev, null, false];
}
const prefixSeen = new Map();
function cachedPrefix(tag, raw) {
	let set = prefixSeen.get(tag);
	if (!set) prefixSeen.set(tag, (set = new Set()));
	let h1 = 0, h2 = 0, best = 0;
	const marks = [];
	for (let i = 0; i < raw.length; i++) {
		const c = raw.charCodeAt(i);
		h1 = (Math.imul(h1, 31) + c) | 0;
		h2 = (Math.imul(h2, 131) + c) | 0;
		if ((i + 1) % 1024 === 0) {
			const k = `${i + 1}:${h1}:${h2}`;
			if (set.has(k)) best = i + 1;
			marks.push(k);
		}
	}
	for (const k of marks) set.add(k);
	return best;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const estTokens = (s) => Math.ceil((s?.length ?? 0) / 4);

function flatText(x) {
	if (x == null) return "";
	if (typeof x === "string") return x;
	if (Array.isArray(x)) return x.map(flatText).join("\n");
	if (typeof x === "object") return Object.values(x).map(flatText).join("\n");
	return String(x);
}

function scenario(allText) {
	let best = null, pos = -1;
	for (const k of [...Object.keys(SCRIPTS), ...Object.keys(SEQ)]) {
		const i = allText.lastIndexOf(k);
		if (i > pos) { pos = i; best = k; }
	}
	return best;
}

// Conversation views per API: user texts (to find the keyword) and tool outputs (to find markers).
function viewAnthropic(b) {
	const users = [], outputs = [];
	let assistant = 0, mark = 0;
	for (const m of b.messages ?? []) {
		const parts = typeof m.content === "string" ? [{ type: "text", text: m.content }] : m.content ?? [];
		for (const p of parts) {
			if (p.type === "tool_result") outputs.push(flatText(p.content));
			else if (m.role === "user" && p.type === "text") { users.push(p.text); if (/\bHB[A-Z]+\b/.test(p.text)) mark = outputs.length; }
			else if (m.role === "assistant" && p.type === "text") assistant += p.text.length;
		}
	}
	return { users, outputs, assistant, since: outputs.length - mark, tools: (b.tools ?? []).map((t) => ({ name: t.name, schema: t.input_schema })) };
}
function viewResponses(b) {
	const users = [], outputs = [];
	let assistant = 0, mark = 0;
	const items = typeof b.input === "string" ? [{ role: "user", content: b.input }] : b.input ?? [];
	for (const it of items) {
		if (it.type === "function_call_output" || it.type === "custom_tool_call_output" || it.type === "local_shell_call_output") outputs.push(flatText(it.output));
		else if (it.role === "user") { const t = flatText(it.content); users.push(t); if (/\bHB[A-Z]+\b/.test(t)) mark = outputs.length; }
		else if (it.role === "assistant") assistant += flatText(it.content).length;
	}
	const tools = [];
	const add = (list) => { for (const t of list ?? []) t.type === "namespace" ? add(t.tools) : tools.push({ name: t.name ?? t.type, schema: t.parameters, type: t.type }); };
	add(b.tools);
	for (const it of items) if (it.type === "additional_tools") add(it.tools);
	return { users, outputs, tools, assistant, since: outputs.length - mark };
}
function viewChat(b) {
	const users = [], outputs = [];
	let mark = 0;
	for (const m of b.messages ?? []) {
		if (m.role === "tool") outputs.push(flatText(m.content));
		else if (m.role === "user") { const t = flatText(m.content); users.push(t); if (/\bHB[A-Z]+\b/.test(t)) mark = outputs.length; }
	}
	return { users, outputs, since: outputs.length - mark, tools: (b.tools ?? []).map((t) => ({ name: t.function?.name, schema: t.function?.parameters })) };
}

function viewGemini(b) {
	const users = [], outputs = [];
	let assistant = 0, mark = 0;
	for (const c of b.contents ?? []) {
		for (const p of c.parts ?? []) {
			if (p.functionResponse) outputs.push(flatText(p.functionResponse.response));
			else if (c.role === "user" && typeof p.text === "string") { users.push(p.text); if (/\bHB[A-Z]+\b/.test(p.text)) mark = outputs.length; }
			else if (c.role === "model" && typeof p.text === "string") assistant += p.text.length;
		}
	}
	const tools = [];
	for (const t of b.tools ?? []) for (const d of t.functionDeclarations ?? []) tools.push({ name: d.name, schema: d.parametersJsonSchema ?? d.parameters });
	return { users, outputs, assistant, since: outputs.length - mark, tools };
}

const SHELL_NAMES = ["bash", "shell", "exec_command", "shell_command", "run_shell_command", "execute_bash", "terminal"];
function shellTool(tools) {
	for (const n of SHELL_NAMES) {
		const t = tools.find((x) => x.name?.toLowerCase() === n);
		if (t) return t;
	}
	return null;
}
function shellArgs(tool, cmd) {
	const props = tool.schema?.properties ?? {};
	const args = {};
	const key = props.command ? "command" : props.cmd ? "cmd" : "command";
	args[key] = props[key]?.type === "array" ? ["bash", "-lc", cmd] : cmd;
	if (props.description) args.description = "run step";
	if (props.workdir && tool.schema?.required?.includes("workdir")) args.workdir = ".";
	if (props.timeout && tool.schema?.required?.includes("timeout")) {
		const t = props.timeout;
		const nullable = (t.anyOf ?? []).some((x) => x.type === "null") || (Array.isArray(t.type) && t.type.includes("null"));
		const desc = JSON.stringify(t).toLowerCase();
		args.timeout = nullable ? null : desc.includes("second") ? 120 : 120000;
	}
	return args;
}

// Minimal valid instance of a JSON schema, for structured-output side calls (routers, classifiers, next-speaker checks).
function fillSchema(sc) {
	if (!sc || typeof sc !== "object") return null;
	if (sc.enum?.length) return sc.enum.at(-1);
	const t = String(Array.isArray(sc.type) ? sc.type.find((x) => x !== "null") : sc.type ?? (sc.properties ? "object" : "string")).toLowerCase();
	if (t === "object") return Object.fromEntries(Object.entries(sc.properties ?? {}).map(([k, v]) => [k, fillSchema(v)]));
	if (t === "array") return [];
	if (t === "integer") return sc.minimum ?? 1;  // gemini router complexity_score (1-100): 1 = simple
	if (t === "number") return sc.minimum ?? 0;  // gemini loop detector confidence (0.0-1.0): 0 = not looping
	if (t === "boolean") return false;
	return "user";  // neutral: a judge (loop detector, next-speaker check) gets "no problem, user speaks next"
}

// Decide: {kind:"tool", name, args} or {kind:"text", text}
function plan(view) {
	const last = view.users.at(-1) ?? "";
	const usersAll = view.users.join("\n");
	const doneBefore = Math.max(0, ...[...usersAll.matchAll(/STEPS-DONE=(\d+)/g)].map((m) => Number(m[1])));
	const head = (last.slice(0, 1500) + "\n" + last.slice(-1500)).toLowerCase();
	const notice = /(was|has been|were) compacted|being continued from a previous conversation|another language model started|continue the conversation from where/.test(last.slice(0, 600).toLowerCase());
	const asksSummary = /summar|checkpoint compaction|state_snapshot/.test(head) && !notice && !/^HB[A-Z]+[ :]/.test(last);
	// some harnesses (goose) send the transcript to summarise inside the system prompt, with a bare "summarize" user turn
	const ctx = usersAll + "\n" + (asksSummary ? view.system ?? "" : "");
	if (asksSummary && /HBMARATHON|HBGROW/.test(ctx)) {
		const everything = ctx + "\n" + view.outputs.join("\n");
		const steps = Math.max(doneBefore, ...[...everything.matchAll(/(?:STEP|GROW)-(\d+)/g)].map((m) => Number(m[1]) + 1), doneBefore + view.outputs.length);
		const which = ctx.match(/HBGROW \d+/)?.[0] ?? "HBMARATHON";
		const who = ctx.match(/\[a\d{3}\]/)?.[0] ?? "";
		return { kind: "text", text: `Summary: the user asked ${which} (read files step by step). STEPS-DONE=${steps}. Continue with the next step. ${who}`, compaction: true };
	}
	const scSum = asksSummary ? scenario(ctx) : null;
	if (scSum === "HBFX") {
		// marker-script tasks (HBFX overflow): the summary carries the task keyword and the markers already reached, so the next step is not re-run
		const everything = ctx + "\n" + view.outputs.join("\n");
		const done = SCRIPTS[scSum].map(([, mk]) => mk).filter((mk) => everything.includes(mk));
		const who = ctx.match(/\[a\d{3}\]/)?.[0] ?? "";
		return { kind: "text", text: `Summary: the user asked ${scSum}: ${PROMPT_HINT[scSum] ?? "run the benchmark steps in order"}. MARKERS-DONE=${done.join(",")}. Continue with the next step. ${who}`, compaction: true };
	}
	if (!view.tools.length) return { kind: "text", text: "Benchmark task" };
	if (/\btitle\b/i.test(last.slice(0, 200))) return { kind: "text", text: "Benchmark task" };
	const sc = scenario(usersAll);
	if (!sc) return { kind: "text", text: "ok" };
	const tool = shellTool(view.tools);
	if (SEQ[sc]) {
		const i = (view.since ?? view.outputs.length) + (sc === "HBMARATHON" || sc === "HBGROW" ? doneBefore : 0);
		const growN = sc === "HBGROW" ? Number((usersAll.match(/HBGROW (\d+)/) ?? [0, 0])[1]) : 0;
		const cmd = sc === "HBLOOP" ? "true" : sc === "HBGROW" ? (i < growN ? `cat big_${String((i % 20) + 1).padStart(2, "0")}.txt; echo GROW-${i}` : null) : SEQ[sc][i];
		if (cmd == null) return { kind: "text", text: FINAL[sc], scenario: sc };
		const codeMode = view.tools.find((t) => t.type === "custom" && t.name === "exec");
		if (!tool && codeMode) return { kind: "custom", name: "exec", input: `const r = await tools.exec_command({cmd: ${JSON.stringify(cmd)}, yield_time_ms: 30000});\ntext(r.output ?? JSON.stringify(r));`, scenario: sc, step: `seq${i}` };
		if (!tool) return { kind: "text", text: "NO-SHELL-TOOL", scenario: sc };
		return { kind: "tool", name: tool.name, args: shellArgs(tool, cmd), scenario: sc, step: `seq${i}` };
	}
	const all = view.outputs.join("\n") + "\n" + [...usersAll.matchAll(/MARKERS-DONE=(\S*)/g)].map((m) => m[1].replaceAll(",", "\n")).join("\n");
	if (view.outputs.length > SCRIPTS[sc].length * 3 + 5) return { kind: "text", text: "GAVE-UP: step markers never seen", scenario: sc, gaveUp: true };
	const step = SCRIPTS[sc].find(([, marker]) => !all.includes(marker));
	if (!step) return { kind: "text", text: FINAL[sc], scenario: sc };
	const codeMode = view.tools.find((t) => t.type === "custom" && t.name === "exec");
	if (!tool && codeMode) return { kind: "custom", name: "exec", input: `const r = await tools.exec_command({cmd: ${JSON.stringify(step[0])}, yield_time_ms: 30000});\ntext(r.output ?? JSON.stringify(r));`, scenario: sc, step: step[1] };
	if (!tool) return { kind: "text", text: `NO-SHELL-TOOL among ${view.tools.map((t) => t.name).join(",")}`, scenario: sc };
	return { kind: "tool", name: tool.name, args: shellArgs(tool, step[0]), scenario: sc, step: step[1] };
}

function chunks(s, n = 12) {
	const out = [];
	for (let i = 0; i < s.length; i += n) out.push(s.slice(i, i + n));
	return out.length ? out : [""];
}
const id = (p) => p + randomBytes(10).toString("hex");

// Mid-stream faults (usage.fault): after MIDSTREAM_AFTER events: truncate = socket destroyed; stall = silence for the fault window, then destroyed;
// trickle = one event per 5 s; sse_error = provider error event, stream ends; malformed = one invalid data line, stream continues.
const MIDSTREAM_AFTER = 3;
const ERROR_EVENT = {
	anthropic: () => `event: error\ndata: ${JSON.stringify({ type: "error", error: { type: "overloaded_error", message: "Overloaded" } })}\n\n`,
	responses: () => `event: response.failed\ndata: ${JSON.stringify({ type: "response.failed", response: { id: id("resp_"), status: "failed", error: { code: "server_error", message: "injected mid-stream failure" } } })}\n\n`,
	chat: () => `data: ${JSON.stringify({ error: { message: "injected mid-stream failure", type: "server_error", code: "server_error" } })}\n\n`,
	gemini: () => `data: ${JSON.stringify({ error: { code: 503, message: "The model is overloaded. Please try again later.", status: "UNAVAILABLE" } })}\n\n`,
};
async function pace(res, outTokens, events, write, ttft, usage = {}) {
	await sleep(ttft);
	const gap = events.length ? (outTokens / tps) * 1000 / events.length : 0;
	let i = 0;
	for (const e of events) {
		if (res.destroyed) return false;
		if (usage.cutAt != null && i === usage.cutAt) { res.socket?.destroy(); return false; }
		if (usage.fault && i === Math.min(MIDSTREAM_AFTER, events.length - 1)) {
			const f = usage.fault;
			if (f === "truncate") { res.socket?.destroy(); return false; }
			if (f === "stall") { await sleep(usage.faultWindow ?? 120000); res.socket?.destroy(); return false; }
			if (f === "sse_error") { res.write(ERROR_EVENT[usage.api]()); res.end(); return false; }
			if (f === "malformed") res.write(usage.api === "chat" || usage.api === "gemini" ? "data: {not json\n\n" : "event: content_block_delta\ndata: {not json\n\n");
		}
		write(e);
		i++;
		if (usage.fault === "trickle" && i >= Math.min(MIDSTREAM_AFTER, events.length - 1)) await sleep(5000);
		else if (gap) await sleep(gap);
	}
	return true;
}

async function anthropic(req, res, b, p, usage) {
	const msgId = id("msg_");
	const block = p.kind === "tool" ? { type: "tool_use", id: id("toolu_"), name: p.name, input: {} } : { type: "text", text: "" };
	const body = p.kind === "tool" ? p.rawArgs ?? JSON.stringify(p.args) : p.text;
	const stop = p.kind === "tool" ? "tool_use" : "end_turn";
	const u = { input_tokens: usage.input - usage.cached, output_tokens: usage.output, cache_creation_input_tokens: 0, cache_read_input_tokens: usage.cached };
	if (!b.stream) {
		const content = p.kind === "tool" ? [{ ...block, input: p.args }] : [{ type: "text", text: p.text }];
		await sleep(usage.ttft);
		res.writeHead(200, { "content-type": "application/json", "request-id": id("req_") });
		return res.end(JSON.stringify({ id: msgId, type: "message", role: "assistant", model: b.model, content, stop_reason: stop, stop_sequence: null, usage: u }));
	}
	res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", "request-id": id("req_") });
	const ev = [];
	ev.push(["message_start", { type: "message_start", message: { id: msgId, type: "message", role: "assistant", model: b.model, content: [], stop_reason: null, stop_sequence: null, usage: { ...u, output_tokens: 1 } } }]);
	ev.push(["content_block_start", { type: "content_block_start", index: 0, content_block: block }]);
	for (const c of chunks(body, p.kind === "tool" ? 8 : 12)) ev.push(["content_block_delta", { type: "content_block_delta", index: 0, delta: p.kind === "tool" ? { type: "input_json_delta", partial_json: c } : { type: "text_delta", text: c } }]);
	ev.push(["content_block_stop", { type: "content_block_stop", index: 0 }]);
	ev.push(["message_delta", { type: "message_delta", delta: { stop_reason: stop, stop_sequence: null }, usage: { output_tokens: usage.output } }]);
	ev.push(["message_stop", { type: "message_stop" }]);
	if (await pace(res, usage.output, applyModelFault(ev, usage), ([t, d]) => res.write(`event: ${t}\ndata: ${JSON.stringify(d)}\n\n`), usage.ttft, usage)) res.end();
}

async function responses(req, res, b, p, usage) {
	const rid = id("resp_");
	const u = { input_tokens: usage.input, input_tokens_details: { cached_tokens: usage.cached }, output_tokens: usage.output, output_tokens_details: { reasoning_tokens: 0 }, total_tokens: usage.input + usage.output };
	let item;
	const ev = [{ type: "response.created", response: { id: rid, status: "in_progress", model: b.model } }];
	if (p.kind === "custom") {
		item = { type: "custom_tool_call", id: id("ctc_"), call_id: id("call_"), name: p.name, input: p.input, status: "completed" };
		ev.push({ type: "response.output_item.added", output_index: 0, item: { ...item, input: "", status: "in_progress" } });
		for (const c of chunks(p.input, 24)) ev.push({ type: "response.custom_tool_call_input.delta", output_index: 0, item_id: item.id, delta: c });
		ev.push({ type: "response.custom_tool_call_input.done", output_index: 0, item_id: item.id, input: p.input });
	} else if (p.kind === "tool") {
		const args = p.rawArgs ?? JSON.stringify(p.args);
		const callId = id("call_");
		item = { type: "function_call", id: id("fc_"), call_id: callId, name: p.name, arguments: args, status: "completed" };
		ev.push({ type: "response.output_item.added", output_index: 0, item: { ...item, arguments: "", status: "in_progress" } });
		for (const c of chunks(args, 8)) ev.push({ type: "response.function_call_arguments.delta", output_index: 0, item_id: item.id, delta: c });
		ev.push({ type: "response.function_call_arguments.done", output_index: 0, item_id: item.id, arguments: args });
	} else {
		item = { type: "message", id: id("msg_"), role: "assistant", status: "completed", content: [{ type: "output_text", text: p.text, annotations: [] }] };
		ev.push({ type: "response.output_item.added", output_index: 0, item: { ...item, status: "in_progress", content: [] } });
		ev.push({ type: "response.content_part.added", output_index: 0, item_id: item.id, content_index: 0, part: { type: "output_text", text: "", annotations: [] } });
		for (const c of chunks(p.text)) ev.push({ type: "response.output_text.delta", output_index: 0, item_id: item.id, content_index: 0, delta: c });
		ev.push({ type: "response.output_text.done", output_index: 0, item_id: item.id, content_index: 0, text: p.text });
		ev.push({ type: "response.content_part.done", output_index: 0, item_id: item.id, content_index: 0, part: item.content[0] });
	}
	ev.push({ type: "response.output_item.done", output_index: 0, item });
	ev.push({ type: "response.completed", response: { id: rid, object: "response", status: "completed", model: b.model, output: [item], usage: u } });
	if (b.stream === false) {
		await sleep(usage.ttft);
		res.writeHead(200, { "content-type": "application/json" });
		return res.end(JSON.stringify(ev.at(-1).response));
	}
	res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
	if (await pace(res, usage.output, applyModelFault(ev, usage), (e) => res.write(`event: ${e.type}\ndata: ${JSON.stringify(e)}\n\n`), usage.ttft, usage)) res.end();
}

async function chat(req, res, b, p, usage) {
	const cid = id("chatcmpl-");
	const base = { id: cid, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: b.model };
	const u = { prompt_tokens: usage.input, completion_tokens: usage.output, total_tokens: usage.input + usage.output };
	const callId = id("call_");
	if (!b.stream) {
		await sleep(usage.ttft);
		const msg = p.kind === "tool" ? { role: "assistant", content: null, tool_calls: [{ id: callId, type: "function", function: { name: p.name, arguments: JSON.stringify(p.args) } }] } : { role: "assistant", content: p.text };
		res.writeHead(200, { "content-type": "application/json" });
		return res.end(JSON.stringify({ ...base, object: "chat.completion", choices: [{ index: 0, message: msg, finish_reason: p.kind === "tool" ? "tool_calls" : "stop" }], usage: u }));
	}
	res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
	const ev = [];
	if (p.kind === "tool") {
		const args = p.rawArgs ?? JSON.stringify(p.args);
		ev.push({ ...base, choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 0, id: callId, type: "function", function: { name: p.name, arguments: "" } }] }, finish_reason: null }] });
		for (const c of chunks(args)) ev.push({ ...base, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: c } }] }, finish_reason: null }] });
		ev.push({ ...base, choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }], usage: u });
	} else {
		ev.push({ ...base, choices: [{ index: 0, delta: { role: "assistant", content: "" }, finish_reason: null }] });
		for (const c of chunks(p.text)) ev.push({ ...base, choices: [{ index: 0, delta: { content: c }, finish_reason: null }] });
		ev.push({ ...base, choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: u });
	}
	if (await pace(res, usage.output, applyModelFault(ev, usage), (e) => res.write(`data: ${JSON.stringify(e)}\n\n`), usage.ttft, usage)) res.end("data: [DONE]\n\n");
}

async function gemini(req, res, b, p, usage, model, stream) {
	const um = { promptTokenCount: usage.input, candidatesTokenCount: usage.output, totalTokenCount: usage.input + usage.output, cachedContentTokenCount: usage.cached };
	const base = { modelVersion: model, responseId: id("") };
	const cand = (parts, finish) => ({ candidates: [{ content: { role: "model", parts }, index: 0, ...(finish ? { finishReason: "STOP" } : {}) }] });
	const ev = [];
	// a mid-stream fault needs an event boundary before the call: lead with a short text chunk (gemini sends whole parts per chunk)
	if (p.kind === "tool" && usage.fault) ev.push({ ...cand([{ text: "Running the next step." }], false), ...base });
	if (p.kind === "tool") ev.push({ ...cand([{ functionCall: { name: p.name, args: p.args } }], true), usageMetadata: um, ...base });
	else {
		const cs = chunks(p.text, 48);
		cs.forEach((c, i) => ev.push({ ...cand([{ text: c }], i === cs.length - 1), ...(i === cs.length - 1 ? { usageMetadata: um } : {}), ...base }));
	}
	if (!stream) {
		await sleep(usage.ttft);
		res.writeHead(200, { "content-type": "application/json" });
		const parts = p.kind === "tool" ? [{ functionCall: { name: p.name, args: p.args } }] : [{ text: p.text }];
		return res.end(JSON.stringify({ ...cand(parts, true), usageMetadata: um, ...base }));
	}
	res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
	if (await pace(res, usage.output, applyModelFault(ev, usage), (e) => res.write(`data: ${JSON.stringify(e)}\n\n`), usage.ttft, usage)) res.end();
}

function applyModelFault(ev, usage) {
	if (!usage.modelFault) return ev;
	const [out, cut, na] = mutate(ev, usage.api, usage.modelFault);
	usage.cutAt = cut;
	return out;
}

function models(res, path) {
	res.writeHead(200, { "content-type": "application/json" });
	const ids = ["claude-sonnet-5-5", "claude-opus-5-5", "claude-haiku-5-5", "gpt-5.5", "gpt-5.5-codex", "hb-mock"];
	if (/\/v1(beta|alpha)?\/models$/.test(path) && path.includes("beta")) return res.end(JSON.stringify({ models: ["gemini-3.1-pro-preview", "gemini-3.5-flash-lite"].map((m) => ({ name: `models/${m}`, supportedGenerationMethods: ["generateContent", "countTokens"] })) }));
	return res.end(JSON.stringify({ data: ids.map((i) => ({ id: i, type: "model", object: "model", display_name: i, created_at: "2026-01-01T00:00:00Z", created: 1767225600, owned_by: "hb" })), has_more: false, object: "list" }));
}

const server = http.createServer(async (req, res) => {
	const chunksIn = [];
	for await (const c of req) chunksIn.push(c);
	const raw = Buffer.concat(chunksIn).toString("utf8");
	let path = req.url.split("?")[0];
	let tag = "untagged";
	const m = path.match(/^\/t\/([^/]+)(\/.*)?$/);
	if (m) { tag = decodeURIComponent(m[1]); path = m[2] || "/"; }
	if (path === "/ctl" && req.method === "POST") {
		const c = JSON.parse(raw || "{}");
		if (c.kind === "quota") { quotas.set(c.tag, { rpm: Number(c.rpm), retryAfter: c.retry_after !== false, times: [] }); res.writeHead(200); return res.end("{}"); }
		if (c.kind) faults.set(c.tag, { kind: String(c.kind), after: Number(c.after ?? 0), count: c.count == null ? Infinity : Number(c.count), window: c.window_s == null ? Infinity : Number(c.window_s) * 1000, seen: 0, start: null,
			retryAfter: c.retry_after, seq: c.seq ? c.seq.map(String) : null, period: Number(c.period_s ?? 20) * 1000, down: Number(c.down_s ?? 5) * 1000, threshold: null });
		else { faults.delete(c.tag); quotas.delete(c.tag); }
		res.writeHead(200, { "content-type": "application/json" });
		return res.end(JSON.stringify({ ok: true }));
	}
	if (req.method === "GET" && path.endsWith("/models")) return models(res, path);
	if (req.method === "GET" || req.method === "HEAD") { res.writeHead(200); return res.end("{}"); }
	let b = {};
	try { b = JSON.parse(raw || "{}"); } catch { }
	const gm = path.match(/\/models\/([^/:]+):(streamGenerateContent|generateContent|countTokens)$/);
	const api = gm ? (gm[2] === "countTokens" ? "gemini_count" : "gemini") : path.endsWith("/messages/count_tokens") ? "anthropic_count" : path.endsWith("/messages") ? "anthropic" : path.endsWith("/responses") ? "responses" : path.endsWith("/chat/completions") ? "chat" : "other";
	if (gm) b.model ??= gm[1];
	const n = ++seq;
	const rec = { n, t: Date.now() / 1000, tag, api, path, model: b.model, stream: b.stream ?? null, bytes: raw.length, ua: req.headers["user-agent"] };
	if (api === "anthropic_count" || api === "gemini_count") {
		rec.est_input = estTokens(raw);
		appendFileSync(log, JSON.stringify(rec) + "\n");
		res.writeHead(200, { "content-type": "application/json" });
		return res.end(JSON.stringify(api === "gemini_count" ? { totalTokens: rec.est_input } : { input_tokens: rec.est_input }));
	}
	if (api === "gemini") rec.stream = gm[2] === "streamGenerateContent";
	const view = api === "anthropic" ? viewAnthropic(b) : api === "responses" ? viewResponses(b) : api === "chat" ? viewChat(b) : api === "gemini" ? viewGemini(b) : null;
	if (!view) {
		appendFileSync(log, JSON.stringify(rec) + "\n");
		res.writeHead(404, { "content-type": "application/json" });
		return res.end(JSON.stringify({ error: { type: "not_found", message: path } }));
	}
	const sys = api === "anthropic" ? flatText(b.system) : api === "responses" ? flatText(b.instructions) : api === "gemini" ? flatText(b.systemInstruction ?? b.system_instruction) : flatText((b.messages ?? []).filter((x) => x.role === "system" || x.role === "developer").map((x) => x.content));
	const toolsJson = JSON.stringify(b.tools ?? []);
	Object.assign(rec, {
		sys_chars: sys.length, tools_n: view.tools.length, tools_chars: toolsJson.length,
		msgs_chars: raw.length - sys.length - toolsJson.length,
		est_input: estTokens(raw), cache_marks: (raw.match(/"cache_control"/g) ?? []).length,
		tool_names: view.tools.map((t) => t.name), n_outputs: view.outputs.length,
		users_tail: view.users.at(-1)?.slice(0, 120), assistant_chars: view.assistant ?? null,
	});
	if (keepBodies) writeFileSync(`${outDir}/bodies/${String(n).padStart(6, "0")}.json.gz`, gzipSync(raw));
	const q = quotas.get(tag);
	if (q) {
		const now = Date.now();
		q.times = q.times.filter((x) => now - x < 60000);
		if (q.times.length >= q.rpm) {
			const wait = Math.ceil((q.times[0] + 60000 - now) / 1000);
			rec.injected = "quota";
			rec.agent = (view.users.join(" ").match(/\[(a\d{3})\]/) ?? [])[1];
			appendFileSync(log, JSON.stringify(rec) + "\n");
			res.writeHead(429, { "content-type": "application/json", ...(q.retryAfter ? { "retry-after": String(wait) } : {}) });
			return res.end(JSON.stringify(api === "anthropic" ? { type: "error", error: { type: "rate_limit_error", message: "quota" } } : api === "gemini" ? { error: { code: 429, message: "Resource has been exhausted (e.g. check quota).", status: "RESOURCE_EXHAUSTED" } } : { error: { type: "rate_limit_exceeded", code: "rate_limit_exceeded", message: "quota" } }));
		}
		q.times.push(now);
	}
	const gc = b.generationConfig ?? b.generation_config;
	const schema = api === "gemini" && gc?.responseMimeType === "application/json" ? (gc.responseJsonSchema ?? gc.responseSchema ?? {}) : null;
	view.system = sys;
	let p = schema ? { kind: "text", text: JSON.stringify(fillSchema(schema)), side: "json" } : plan(view);
	// main = a turn request of the task (tools offered, not a JSON side call, not a title); only main requests advance fault counters
	const isMain = view.tools.length > 0 && !schema && !/\btitle\b/i.test((view.users.at(-1) ?? "").slice(0, 200));
	if (!isMain) rec.side = rec.side ?? (schema ? "json" : "aux");
	const f = faults.get(tag);
	let midFault = null, midWindow = null, modelFault = null;
	const fail = (kind) => {
		appendFileSync(log, JSON.stringify(rec) + "\n");
		res.on("close", () => appendFileSync(log, JSON.stringify({ n, t_end: Date.now() / 1000 }) + "\n"));
		if (kind === "reset") return req.socket.destroy();
		if (kind === "hang") return setTimeout(() => req.socket.destroy(), Math.max(0, f.start + f.window - Date.now()));
		if (kind === "html200") { res.writeHead(200, { "content-type": "text/html" }); return res.end("<html><head><title>502 Bad Gateway</title></head><body><center><h1>502 Bad Gateway</h1></center><hr><center>nginx</center></body></html>\n"); }
		if (kind === "empty200") { res.writeHead(200, { "content-type": rec.stream ? "text/event-stream" : "application/json" }); return res.end(); }
		if (kind === "overflow") {
			const lim = Math.floor(f.threshold / 4);
			res.writeHead(400, { "content-type": "application/json", ...(api === "anthropic" ? { "request-id": id("req_") } : {}) });
			return res.end(JSON.stringify(api === "anthropic" ? { type: "error", error: { type: "invalid_request_error", message: `prompt is too long: ${rec.est_input} tokens > ${lim} maximum` }, request_id: id("req_") }
				: api === "gemini" ? { error: { code: 400, message: `The input token count (${rec.est_input}) exceeds the maximum number of tokens allowed (${lim}).`, status: "INVALID_ARGUMENT" } }
				: { error: { message: `This model's maximum context length is ${lim} tokens. However, your messages resulted in ${rec.est_input} tokens. Please reduce the length of the messages.`, type: "invalid_request_error", param: "messages", code: "context_length_exceeded" } }));
		}
		const status = Number(kind);
		const anth = { 429: ["rate_limit_error", "Number of request tokens has exceeded your per-minute rate limit"], 401: ["authentication_error", "invalid x-api-key"], 500: ["api_error", "Internal server error"], 529: ["overloaded_error", "Overloaded"], 503: ["api_error", "Service unavailable"], 413: ["request_too_large", "Request exceeds the maximum allowed number of bytes"] }[status];
		const oai = { 429: ["requests", "rate_limit_exceeded", "Rate limit reached for requests"], 401: ["invalid_request_error", "invalid_api_key", "Incorrect API key provided."], 500: ["server_error", "server_error", "The server had an error while processing your request."], 503: ["server_error", "service_unavailable", "The server is overloaded or not ready yet."], 413: ["invalid_request_error", "request_too_large", "Request too large."] }[status] ?? ["server_error", "server_error", "error"];
		const gem = { 429: ["RESOURCE_EXHAUSTED", "Resource has been exhausted (e.g. check quota)."], 401: ["UNAUTHENTICATED", "API key not valid. Please pass a valid API key."], 500: ["INTERNAL", "An internal error has occurred."], 503: ["UNAVAILABLE", "The model is overloaded. Please try again later."], 413: ["INVALID_ARGUMENT", "Request payload size exceeds the limit."] }[status] ?? ["UNKNOWN", "error"];
		const ra = f.retryAfter === false ? null : f.retryAfter != null ? String(f.retryAfter) : status === 429 ? "2" : null;
		res.writeHead(status, { "content-type": "application/json", ...(ra ? { "retry-after": ra } : {}), ...(api === "anthropic" ? { "request-id": id("req_") } : {}) });
		return res.end(JSON.stringify(api === "anthropic" ? { type: "error", error: { type: anth[0], message: anth[1] }, request_id: id("req_") }
			: api === "gemini" ? { error: { code: status, message: gem[1], status: gem[0], ...(status === 429 && ra ? { details: [{ "@type": "type.googleapis.com/google.rpc.RetryInfo", retryDelay: `${ra}s` }] } : {}) } }
			: { error: { message: oai[2], type: oai[0], param: null, code: oai[1] } }));
	};
	const inWindow = () => f.start != null && Date.now() - f.start < f.window;
	if (f && f.kind === "flap") {
		// request-based flapping for the whole window: after `after` main requests, main requests go down, down, up, ...; side calls fail while down
		if (isMain && f.seen++ >= f.after) {
			f.start ??= Date.now();
			f.k = (f.k ?? -1) + 1;
			if (inWindow() && f.k % 3 !== 2) { rec.injected = "flap"; return fail("reset"); }
		}
	} else if (f && f.kind === "overflow") {
		// context limit = last accepted main request + 1 KB once `after` main requests passed: the next turn (one more 16 KB output) overflows
		if (isMain) {
			if (f.threshold == null && f.seen >= f.after) f.threshold = (f.lastMain ?? raw.length) + 1024;
			f.seen++;
			if (f.threshold != null && raw.length > f.threshold) { rec.injected = "overflow"; return fail("overflow"); }
			f.lastMain = raw.length;
		}
	} else if (f && MODEL_FAULTS.has(f.kind)) {
		if (isMain && rec.stream && f.seen >= f.after && f.count > 0 && (p.kind === "tool" || p.kind === "custom")) modelFault = f.kind;
		if (isMain) f.seen++;
	} else if (f) {
		const open = inWindow() && f.count > 0;
		const kind0 = f.seq ? f.seq[0] : f.kind;
		const streamOnly = MIDSTREAM.has(kind0) && !rec.stream;  // mid-stream faults need a stream: non-stream calls pass and do not consume the count
		if (isMain && !streamOnly && (open || (f.start == null && f.seen >= f.after && f.count > 0))) {
			f.start ??= Date.now();
			f.count--;
			const kind = f.seq ? f.seq.shift() : f.kind;
			if (f.seq && !f.seq.length) f.count = 0;
			rec.injected = kind;
			if (api !== "anthropic" && kind === "529") rec.fault_na = true;  // 529 exists only on Anthropic
			if (MIDSTREAM.has(kind)) {
				midFault = kind;
				midWindow = f.window === Infinity ? 120000 : f.window;
			} else return fail(kind);
		} else if (!isMain && open && f.window !== Infinity && !MIDSTREAM.has(kind0)) {
			rec.injected = kind0;  // outage windows hit side calls too (logged side, not counted)
			return fail(kind0);
		}
		if (isMain) f.seen++;
	}
	if (modelFault) {
		f.count--;
		rec.injected = modelFault;
		// gemini: each chunk carries whole parts, so a duplicated chunk is a valid second call, not a protocol duplicate
		if ((modelFault === "reorder" && api !== "anthropic" && api !== "responses") || ((modelFault === "truncate_tool" || modelFault === "dup_events") && api === "gemini")) rec.fault_na = true;
		p = planFault(p, modelFault, api);
	}
	const out = p.kind === "tool" ? JSON.stringify(p.args) : p.kind === "custom" ? p.input : p.text;
	const cached = Math.floor(cachedPrefix(tag, raw) / 4);
	const usage = { input: rec.est_input, cached, output: Math.max(1, estTokens(out)) };
	rec.est_cached = cached;
	usage.fault = midFault;
	usage.modelFault = modelFault;
	usage.rec = rec;
	usage.faultWindow = midWindow;
	usage.api = api;
	usage.ttft = ttftBase + ttftPerKtok * Math.max(0, rec.est_input - cached) / 1000;
	const scen = scenario(view.users.join("\n"));
	const allOut = view.outputs.join("\n");
	rec.markers = scen && SCRIPTS[scen] ? SCRIPTS[scen].filter(([, mk]) => allOut.includes(mk)).map(([, mk]) => mk) : [];
	const agent = (view.users.join(" ").match(/\[(a\d{3})\]/) ?? [])[1];
	Object.assign(rec, { agent, side: p.side ?? rec.side, compaction: p.compaction || undefined, gave_up: p.gaveUp || undefined, reply: p.kind, scenario: p.scenario ?? null, step: p.step ?? null, out_tokens: usage.output, text: p.kind === "text" ? p.text.slice(0, 80) : undefined });
	appendFileSync(log, JSON.stringify(rec) + "\n");
	try {
		if (api === "anthropic") await anthropic(req, res, b, p, usage);
		else if (api === "responses") await responses(req, res, b, p, usage);
		else if (api === "gemini") await gemini(req, res, b, p, usage, b.model, rec.stream);
		else await chat(req, res, b, p, usage);
		appendFileSync(log, JSON.stringify({ n, t_end: Date.now() / 1000 }) + "\n");
	} catch (e) {
		appendFileSync(log, JSON.stringify({ n, error: String(e) }) + "\n");
		try { res.end(); } catch { }
	}
});
server.keepAliveTimeout = 120000;
server.listen(port, "127.0.0.1", () => console.log(`hb mock on ${port} → ${outDir}`));
