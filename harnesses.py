"""Harness adapters: isolated HOME config pointing at the mock, argv for one-shot, resume and interactive TUI. Defaults kept except
FLAGS below (auto-approval of shell commands, pre-accepted first-run dialogs, model id, provider base URL)."""
import json
import os
from pathlib import Path

ROOT = Path(__file__).parent
BIN = ROOT / "harnesses/node_modules/.bin"
PI_CLI = str(ROOT / "harnesses/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js")
CLAUDE_MODEL = "claude-sonnet-5-5"
KEY = "sk-ant-api03-hbdummy-0000000000000000000000"
BASE_PATH = "/usr/local/bin:/usr/bin:/bin"


def base_env(home):
    return {"PATH": BASE_PATH, "HOME": str(home), "TERM": "xterm-256color", "LANG": "C.UTF-8", "USER": os.environ.get("USER", "sr"),
            "XDG_CONFIG_HOME": str(home / ".config"), "XDG_DATA_HOME": str(home / ".local/share"), "XDG_STATE_HOME": str(home / ".local/state"),
            "XDG_CACHE_HOME": str(home / ".cache"), "TMPDIR": str(home / "tmp")}


class Harness:
    name = ""
    version = ""

    def prepare(self, home, url, workspaces):
        home.mkdir(parents=True, exist_ok=True)
        (home / "tmp").mkdir(exist_ok=True)

    def env(self, home):
        return base_env(home)


class Claude(Harness):
    name = "claude"
    version = "claude-code 2.1.318"

    def prepare(self, home, url, workspaces):
        super().prepare(home, url, workspaces)
        self.url = url
        f = home / ".claude.json"
        cfg = json.loads(f.read_text()) if f.exists() else {}
        cfg.update({"hasCompletedOnboarding": True, "theme": "dark", "bypassPermissionsModeAccepted": True,
               "customApiKeyResponses": {"approved": [KEY[-20:]], "rejected": []},
               "projects": {str(w): {"hasTrustDialogAccepted": True, "hasCompletedProjectOnboarding": True, "allowedTools": []} for w in workspaces}})
        f.write_text(json.dumps(cfg))
        (home / ".claude").mkdir(exist_ok=True)
        (home / ".claude/settings.json").write_text(json.dumps({"skipDangerousModePermissionPrompt": True}))

    def env(self, home):
        return {**base_env(home), "ANTHROPIC_BASE_URL": self.url, "ANTHROPIC_API_KEY": KEY}

    def run(self, prompt):
        return [str(BIN / "claude"), "-p", prompt, "--model", CLAUDE_MODEL, "--dangerously-skip-permissions"]

    def resume(self, prompt):
        return [str(BIN / "claude"), "-p", prompt, "--continue", "--model", CLAUDE_MODEL, "--dangerously-skip-permissions"]

    def tui(self, prompt):
        return [str(BIN / "claude"), prompt, "--model", CLAUDE_MODEL, "--dangerously-skip-permissions"]

    def tui_resume(self, home, ws):
        # interactive --continue ignores sessions created with -p ("No conversation found to continue"): resume the newest session of this cwd by id
        proj = home / ".claude/projects" / str(ws).replace("/", "-").replace(".", "-")
        sid = max(proj.glob("*.jsonl"), key=lambda f: f.stat().st_mtime).stem
        return [str(BIN / "claude"), "--resume", sid, "--model", CLAUDE_MODEL, "--dangerously-skip-permissions"]

    session_dirs = (".claude/projects",)


class Codex(Harness):
    name = "codex"
    version = "codex-cli 0.167.0 (npm), default model gpt-6.1-sol"

    def prepare(self, home, url, workspaces):
        super().prepare(home, url, workspaces)
        (home / ".codex").mkdir(exist_ok=True)
        trust = "".join(f'[projects."{w}"]\ntrust_level = "trusted"\n' for w in workspaces)
        (home / ".codex/config.toml").write_text(f'model_provider = "hb"\nsandbox_mode = "workspace-write"\n[model_providers.hb]\nname = "hb"\nbase_url = "{url}/v1"\nenv_key = "HB_KEY"\nwire_api = "responses"\n{trust}')

    def env(self, home):
        return {**base_env(home), "HB_KEY": "dummy"}

    def run(self, prompt):
        return [str(BIN / "codex"), "exec", "-s", "workspace-write", "--skip-git-repo-check", prompt]

    def resume(self, prompt):
        return [str(BIN / "codex"), "exec", "-s", "workspace-write", "--skip-git-repo-check", "resume", "--last", prompt]

    def tui(self, prompt):
        return [str(BIN / "codex"), prompt]

    def tui_resume(self, home, ws):
        # interactive "resume --last" skips sessions created by "codex exec": resume the newest rollout of this cwd by id
        best = None
        for f in (home / ".codex/sessions").rglob("rollout-*.jsonl"):
            with open(f) as fh:
                first = json.loads(fh.readline() or "{}")
            if first.get("payload", {}).get("cwd") == str(ws) and (best is None or f.stat().st_mtime > best.stat().st_mtime):
                best = f
        return [str(BIN / "codex"), "resume", best.stem[-36:]]

    session_dirs = (".codex/sessions",)


class OpenCode(Harness):
    name = "opencode"
    version = "opencode 1.19.5"

    def prepare(self, home, url, workspaces):
        super().prepare(home, url, workspaces)
        d = home / ".config/opencode"
        d.mkdir(parents=True, exist_ok=True)
        (d / "opencode.json").write_text(json.dumps({"$schema": "https://opencode.ai/config.json", "model": f"anthropic/{CLAUDE_MODEL}",
                                                     "provider": {"anthropic": {"options": {"baseURL": f"{url}/v1", "apiKey": KEY}}}}))

    def run(self, prompt):
        return [str(BIN / "opencode"), "run", prompt]

    def resume(self, prompt):
        return [str(BIN / "opencode"), "run", "--continue", prompt]

    def tui(self, prompt):
        return [str(BIN / "opencode"), "--prompt", prompt]

    def tui_resume(self, home, ws):
        return [str(BIN / "opencode"), "--continue"]

    session_dirs = (".local/share/opencode",)


class Pi(Harness):
    name = "pi"
    version = "pi-coding-agent 1.4.0 (npm upstream)"

    def prepare(self, home, url, workspaces):
        super().prepare(home, url, workspaces)
        d = home / ".pi/agent"
        d.mkdir(parents=True, exist_ok=True)
        (d / "models.json").write_text(json.dumps({"providers": {"anthropic": {"baseUrl": url, "apiKey": KEY}}}))
        (d / "settings.json").write_text(json.dumps({"defaultProvider": "anthropic", "defaultModel": CLAUDE_MODEL}))

    def run(self, prompt):
        return ["node", PI_CLI, "-p", prompt]

    def resume(self, prompt):
        return ["node", PI_CLI, "-p", "--continue", prompt]

    def tui(self, prompt):
        return ["node", PI_CLI, prompt]

    def tui_resume(self, home, ws):
        return ["node", PI_CLI, "--continue"]

    session_dirs = (".pi/agent/sessions",)


GOOSE = str(ROOT / "harnesses/goose/goose")
CRUSH = str(ROOT / "harnesses/node_modules/@charmland/crush/bin/crush")  # the Go binary, not the npm node wrapper


class Crush(Harness):
    name = "crush"
    version = "crush 0.101.0"
    model = f"hb/{CLAUDE_MODEL}"

    def prepare(self, home, url, workspaces):
        super().prepare(home, url, workspaces)
        d = home / ".config/crush"
        d.mkdir(parents=True, exist_ok=True)
        (d / "crushrc").write_text(f'provider add hb --type messages --base-url "{url}" --api-key "{KEY}"\n'
                                   f'model add {self.model} --name "Sonnet (mock)" --context-window 1000000 --default-max-tokens 8000\n')
        (home / "run").mkdir(mode=0o700, exist_ok=True)
        for w in workspaces:
            (w / ".crush").mkdir(exist_ok=True)
            (w / ".crush/init").touch()

    def env(self, home):
        return {**base_env(home), "XDG_RUNTIME_DIR": str(home / "run")}

    def run(self, prompt):
        return [CRUSH, "run", "-y", "-m", self.model, prompt]

    def resume(self, prompt):
        return [CRUSH, "run", "-y", "-m", self.model, "--continue", prompt]

    def tui(self, prompt):
        return [CRUSH, "-y"]

    def tui_resume(self, home, ws):
        return [CRUSH, "-y", "--continue"]

    tui_prompt_by_keys = True
    session_dirs = ()
    ws_session_dirs = (".crush",)


class Goose(Harness):
    name = "goose"
    version = "goose 1.59.1"

    def env(self, home):
        return {**base_env(home), "GOOSE_PROVIDER": "anthropic", "GOOSE_MODEL": CLAUDE_MODEL, "ANTHROPIC_HOST": self.url, "ANTHROPIC_API_KEY": KEY,
                "GOOSE_DISABLE_KEYRING": "1", "GOOSE_MODE": "auto"}

    def prepare(self, home, url, workspaces):
        super().prepare(home, url, workspaces)
        self.url = url
        d = home / ".config/goose"
        d.mkdir(parents=True, exist_ok=True)
        f = d / "config.yaml"
        if not f.exists():
            f.write_text("GOOSE_TELEMETRY_ENABLED: true\n")  # first-run consent dialog answered with its default (Yes)

    def run(self, prompt):
        return [GOOSE, "run", "-t", prompt]

    def resume(self, prompt):
        return [GOOSE, "run", "-r", "-t", prompt]

    def tui(self, prompt):
        return [GOOSE, "run", "-s", "-t", prompt]

    def tui_resume(self, home, ws):
        return [GOOSE, "session", "-r"]

    session_dirs = (".local/share/goose/sessions",)


class Gemini(Harness):
    name = "gemini"
    version = "gemini-cli 0.68.0"

    def prepare(self, home, url, workspaces):
        super().prepare(home, url, workspaces)
        self.url = url
        d = home / ".gemini"
        d.mkdir(parents=True, exist_ok=True)
        f = d / "settings.json"
        cfg = json.loads(f.read_text()) if f.exists() else {}
        cfg.setdefault("security", {}).setdefault("auth", {})["selectedType"] = "gemini-api-key"
        f.write_text(json.dumps(cfg))
        (d / "trustedFolders.json").write_text(json.dumps({str(w): "TRUST_FOLDER" for w in workspaces}))

    def env(self, home):
        return {**base_env(home), "GEMINI_API_KEY": "hbdummy", "GOOGLE_GEMINI_BASE_URL": self.url}

    def run(self, prompt):
        return [str(BIN / "gemini"), "-y", "--skip-trust", "-p", prompt]

    def resume(self, prompt):
        return [str(BIN / "gemini"), "-y", "--skip-trust", "-r", "latest", "-p", prompt]

    def tui(self, prompt):
        return [str(BIN / "gemini"), "-y", "--skip-trust", "-i", prompt]

    def tui_resume(self, home, ws):
        return [str(BIN / "gemini"), "-y", "--skip-trust", "-r", "latest"]

    session_dirs = (".gemini/tmp",)


FLAGS = {
    "claude": "-p/--dangerously-skip-permissions, --model; ANTHROPIC_BASE_URL+ANTHROPIC_API_KEY; .claude.json onboarding/trust/api-key pre-accepted",
    "codex": "custom model_provider (base_url, env_key, wire_api=responses); sandbox_mode=workspace-write; projects trusted; exec --skip-git-repo-check",
    "opencode": "provider.anthropic.options baseURL+apiKey; model anthropic/claude-sonnet-5-5",
    "pi": "models.json anthropic baseUrl+key; settings defaultProvider/defaultModel",
    "crush": "crushrc: provider add (type messages, base-url, dummy key) + model add; -y (yolo); -m model; XDG_RUNTIME_DIR private; project-init dialog pre-dismissed (.crush/init); Go binary run directly, not the npm node wrapper",
    "goose": "GOOSE_PROVIDER=anthropic, GOOSE_MODEL, ANTHROPIC_HOST+ANTHROPIC_API_KEY, GOOSE_DISABLE_KEYRING=1, GOOSE_MODE=auto; telemetry consent pre-answered with its default (yes)",
    "gemini": "settings security.auth.selectedType=gemini-api-key; GEMINI_API_KEY dummy + GOOGLE_GEMINI_BASE_URL; -y (yolo), --skip-trust; trustedFolders",
}

CLI = {h.name: h for h in (Claude(), Codex(), OpenCode(), Pi(), Crush(), Goose(), Gemini())}
