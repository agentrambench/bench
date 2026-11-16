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
	HBSTREAM: [["printf 'ONE-%s\\n' DONE", "ONE-DONE"]],
	HBLONG: Array.from({ length: 20 }, (_, i) => { const k = String(i + 1).padStart(2, "0"); return [`printf 'BIG-%s-END\\n' ${k}; cat big_${k}.txt`, `BIG-${k}-END`]; }),
};
// Count-driven scripts: step i is issued when the conversation holds i tool outputs (no marker needed); HBLOOP never ends its turn.
const SEQ = {
	HBHANG: ["python3 -c 'input()'", "python3 -c 'import time; time.sleep(100000)'"],
	HBHUGE: ["head -c 50000000 /dev/zero | tr '\\0' a", "head -c 7500000 /dev/urandom | base64 -w0", "head -c 2000000 /dev/urandom"],
	HBLOOP: [],
	HBBURST: ["cat big_01.txt", "cat big_02.txt", "cat big_03.txt"],
	HBGROW: [],
	HBMARATHON: Array.from({ length: 300 }, (_, i) => `cat big_${String((i % 20) + 1).padStart(2, "0")}.txt; echo STEP-${i}`),
};
const REPORT_MD = ["# File review", "", "Read all twenty files. Summary:", "",
	...Array.from({ length: 20 }, (_, i) => `- **big_${String(i + 1).padStart(2, "0")}.txt**: maritime word list, ~16 KB, ends with marker \`BIG-${String(i + 1).padStart(2, "0")}-END\`.`),
	"", "## Counts", "", "| file | lines | bytes |", "|---|---|---|", ...Array.from({ length: 20 }, (_, i) => `| big_${String(i + 1).padStart(2, "0")}.txt | 190 | 16000 |`),
	"", "## Sample", "", "```python", "from pathlib import Path", "for f in sorted(Path('.').glob('big_*.txt')):", "    print(f.name, len(f.read_text().split()))", "```", ""].join("\n");
const FINAL = { HBTASK: "Fixed: add() now returns a + b and the tests pass.", HBSLOW: "All three steps done.", HBIDLE: "Ready.", HBLONG: REPORT_MD, HBHANG: "Hang steps done.", HBBURST: REPORT_MD, HBGROW: REPORT_MD, HBMARATHON: REPORT_MD, HBHUGE: "Huge outputs done.", HBSTREAM: "STREAM-BEGIN " + "the harness keeps streaming this long benchmark answer while the fault happens ".repeat(150) + "STREAM-END" };

const MIDSTREAM = new Set(["truncate", "stall", "trickle", "sse_error", "malformed"]);
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
	const all = view.outputs.join("\n");
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
		if (usage.fault && i === MIDSTREAM_AFTER) {
			const f = usage.fault;
			if (f === "truncate") { res.socket?.destroy(); return false; }
			if (f === "stall") { await sleep(usage.faultWindow ?? 120000); res.socket?.destroy(); return false; }
			if (f === "sse_error") { res.write(ERROR_EVENT[usage.api]()); res.end(); return false; }
			if (f === "malformed") res.write(usage.api === "chat" || usage.api === "gemini" ? "data: {not json\n\n" : "event: content_block_delta\ndata: {not json\n\n");
		}
		write(e);
		i++;
		if (usage.fault === "trickle" && i >= MIDSTREAM_AFTER) await sleep(5000);
		else if (gap) await sleep(gap);
	}
	return true;
}

async function anthropic(req, res, b, p, usage) {
	const msgId = id("msg_");
	const block = p.kind === "tool" ? { type: "tool_use", id: id("toolu_"), name: p.name, input: {} } : { type: "text", text: "" };
	const body = p.kind === "tool" ? JSON.stringify(p.args) : p.text;
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
	for (const c of chunks(body)) ev.push(["content_block_delta", { type: "content_block_delta", index: 0, delta: p.kind === "tool" ? { type: "input_json_delta", partial_json: c } : { type: "text_delta", text: c } }]);
	ev.push(["content_block_stop", { type: "content_block_stop", index: 0 }]);
	ev.push(["message_delta", { type: "message_delta", delta: { stop_reason: stop, stop_sequence: null }, usage: { output_tokens: usage.output } }]);
	ev.push(["message_stop", { type: "message_stop" }]);
	if (await pace(res, usage.output, ev, ([t, d]) => res.write(`event: ${t}\ndata: ${JSON.stringify(d)}\n\n`), usage.ttft, usage)) res.end();
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
		const args = JSON.stringify(p.args);
		const callId = id("call_");
		item = { type: "function_call", id: id("fc_"), call_id: callId, name: p.name, arguments: args, status: "completed" };
		ev.push({ type: "response.output_item.added", output_index: 0, item: { ...item, arguments: "", status: "in_progress" } });
		for (const c of chunks(args)) ev.push({ type: "response.function_call_arguments.delta", output_index: 0, item_id: item.id, delta: c });
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
	if (await pace(res, usage.output, ev, (e) => res.write(`event: ${e.type}\ndata: ${JSON.stringify(e)}\n\n`), usage.ttft, usage)) res.end();
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
		const args = JSON.stringify(p.args);
		ev.push({ ...base, choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 0, id: callId, type: "function", function: { name: p.name, arguments: "" } }] }, finish_reason: null }] });
		for (const c of chunks(args)) ev.push({ ...base, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: c } }] }, finish_reason: null }] });
		ev.push({ ...base, choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }], usage: u });
	} else {
		ev.push({ ...base, choices: [{ index: 0, delta: { role: "assistant", content: "" }, finish_reason: null }] });
		for (const c of chunks(p.text)) ev.push({ ...base, choices: [{ index: 0, delta: { content: c }, finish_reason: null }] });
		ev.push({ ...base, choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: u });
	}
	if (await pace(res, usage.output, ev, (e) => res.write(`data: ${JSON.stringify(e)}\n\n`), usage.ttft, usage)) res.end("data: [DONE]\n\n");
}

async function gemini(req, res, b, p, usage, model, stream) {
	const um = { promptTokenCount: usage.input, candidatesTokenCount: usage.output, totalTokenCount: usage.input + usage.output, cachedContentTokenCount: usage.cached };
	const base = { modelVersion: model, responseId: id("") };
	const cand = (parts, finish) => ({ candidates: [{ content: { role: "model", parts }, index: 0, ...(finish ? { finishReason: "STOP" } : {}) }] });
	const ev = [];
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
	if (await pace(res, usage.output, ev, (e) => res.write(`data: ${JSON.stringify(e)}\n\n`), usage.ttft, usage)) res.end();
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
		if (c.kind) faults.set(c.tag, { kind: String(c.kind), after: Number(c.after ?? 0), count: c.count == null ? Infinity : Number(c.count), window: c.window_s == null ? Infinity : Number(c.window_s) * 1000, seen: 0, start: null });
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
	const f = faults.get(tag);
	let midFault = null, midWindow = null;
	if (f && f.seen++ >= f.after && f.count > 0 && (f.start == null || Date.now() - f.start < f.window)) {
		f.start ??= Date.now();
		f.count--;
		rec.injected = f.kind;
		if (MIDSTREAM.has(f.kind)) {
			midFault = f.kind;
			midWindow = f.window === Infinity ? 120000 : f.window;
		} else {
			appendFileSync(log, JSON.stringify(rec) + "\n");
			if (f.kind === "reset") return req.socket.destroy();
			if (f.kind === "hang") return setTimeout(() => req.socket.destroy(), Math.max(0, f.start + f.window - Date.now()));
			const status = api === "gemini" && f.kind === "529" ? 503 : Number(f.kind);
			const anth = { 429: ["rate_limit_error", "injected 429"], 401: ["authentication_error", "invalid x-api-key"], 500: ["api_error", "injected 500"], 529: ["overloaded_error", "Overloaded"], 503: ["overloaded_error", "The model is overloaded"] }[status];
			const oai = { 429: "rate_limit_exceeded", 401: "invalid_api_key", 500: "server_error", 529: "server_error" }[status];
			res.writeHead(status, { "content-type": "application/json", ...(status === 429 ? { "retry-after": "2" } : {}) });
			const gem = { 429: "RESOURCE_EXHAUSTED", 401: "UNAUTHENTICATED", 500: "INTERNAL", 503: "UNAVAILABLE" }[status];
			return res.end(JSON.stringify(api === "anthropic" ? { type: "error", error: { type: anth[0], message: anth[1] } } : api === "gemini" ? { error: { code: status, message: anth[1], status: gem } } : { error: { type: oai, code: oai, message: anth[1] } }));
		}
	}
	const gc = b.generationConfig ?? b.generation_config;
	const schema = api === "gemini" && gc?.responseMimeType === "application/json" ? (gc.responseJsonSchema ?? gc.responseSchema ?? {}) : null;
	view.system = sys;
	const p = schema ? { kind: "text", text: JSON.stringify(fillSchema(schema)), side: "json" } : plan(view);
	const out = p.kind === "tool" ? JSON.stringify(p.args) : p.kind === "custom" ? p.input : p.text;
	const cached = Math.floor(cachedPrefix(tag, raw) / 4);
	const usage = { input: rec.est_input, cached, output: Math.max(1, estTokens(out)) };
	rec.est_cached = cached;
	usage.fault = midFault;
	usage.faultWindow = midWindow;
	usage.api = api;
	usage.ttft = ttftBase + ttftPerKtok * Math.max(0, rec.est_input - cached) / 1000;
	const scen = scenario(view.users.join("\n"));
	const allOut = view.outputs.join("\n");
	rec.markers = scen && SCRIPTS[scen] ? SCRIPTS[scen].filter(([, mk]) => allOut.includes(mk)).map(([, mk]) => mk) : [];
	const agent = (view.users.join(" ").match(/\[(a\d{3})\]/) ?? [])[1];
	Object.assign(rec, { agent, side: p.side, compaction: p.compaction || undefined, gave_up: p.gaveUp || undefined, reply: p.kind, scenario: p.scenario ?? null, step: p.step ?? null, out_tokens: usage.output, text: p.kind === "text" ? p.text.slice(0, 80) : undefined });
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
