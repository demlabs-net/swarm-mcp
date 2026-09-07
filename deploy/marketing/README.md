# Marketing deployment additions

Nonsecret deployment bundle captured 2026-09-07. This is an overlay for an existing
Hermes + SLC + swarm installation, not a full VM bootstrap or a vault backup.
It is intentionally separate from the swarm transport implementation.

## Provenance and layout

- Marketing VM only: SSH `root@atlant-llm0 -p 2223`.
- `agents/*/SOUL.md`: exact role instructions from `/opt/hermes/agents/` for
  overseer, writer, editor, communicator, librarian, thinker, designer and girl.
- `skills/swarm-telegram-operations/SKILL.md`: current canonical instructions
  from overseer's skill. Historical references and runtime documents excluded.
- `hooks/slc-context.py`, `hooks/slc_timer_check.py`: captured from
  `/opt/hermes/src/hooks/`; local review removes exception details from context-hook
  stderr. Other known hook limitations are listed below; no live hook was changed.
- `tooling/Dockerfile.designer`: exact `/opt/hermes/tooling/` build file.
- `backup/vm/`: captured `/usr/local/sbin/marketing-application-backup`, with local
  review fixes to reject missing/failed Compose inventories and include the installed
  ComfyUI client. These review fixes have not been deployed.
- `backup/atlant/`: exact marketing pull script and its service/timer from the
  atlant host (not the VM). No other host services included.
- `expiry/ai-router/`: exact marketing-only expiry service/timer from ai-router.
  No dev expiry units, key metadata, router environment or credentials included.
- `*.example`: sanitized templates, **not exact live configuration snapshots**.
  All secret and deployment-specific values are `${...}` placeholders.

## Install overlay

Perform these steps on the marketing VM only, in an approved maintenance window.
First make and verify a private backup; never copy `.env`, keys, agent sessions,
SLC history or rendered configs into this repository.

1. Put the eight SOUL files in `/opt/hermes/agents/<role>/SOUL.md`. Install the
   one canonical Telegram skill as `skills/swarm-telegram-operations/SKILL.md`
   inside each agent's directory. Do not copy obsolete skill references.
2. Install the two hook files in `/opt/hermes/src/hooks/`, readable by the
   containers. Install `Dockerfile.designer` in `/opt/hermes/tooling/`.
3. Render `config.agent.yaml.example` once per role and **merge** the relevant
   fields into its installed `config.yaml`; preserve existing integrations and
   role-specific settings. Render to private files (umask 077), not stdout.
   `${...}` is a template convention, not a promise of Hermes interpolation.
   Give every webhook a unique secret/port and every swarm role its own bearer.
   SLC uses `SLC_MCP_SEAT_TOKENS`, a JSON map of seat name to unique bearer
   token (see `slc-auth.env.example`, installed privately as `/opt/slc-mcp/auth.env`).
   Each role's `slc.env` supplies only its matching `SLC_MCP_TOKEN`; the MCP
   Authorization header and hooks use that same token. `X-Seat-ID` must match
   the authenticated seat, not confer identity by itself. REST authentication
   and seat binding are enforced too: do not expose unauthenticated REST as a
   workaround. Verify missing/wrong tokens and mismatched-seat access are rejected
   on both MCP and REST, using the installed server's actual routes.
4. Merge one rendered `compose.agent.yml.example` service per role into
   `/opt/hermes/docker-compose.yml`. Render the service key before Compose
   parses it; Compose does not interpolate mapping keys. Keep actual values in
   env files: **do not add `environment:` token overrides**. Put the hook's
   `SLC_MCP_TOKEN` in each role's protected `slc.env`, loaded last via env_file.
   The former shared `environment: SLC_MCP_TOKEN` override has been removed;
   do not restore it. Keep the shared inbound
   mount identical between agents and swarm. Add the designer-only build entry
   documented in the template. Remove provider.env entries for roles without
   that file. Provider-enabled roles include overseer/thinker/designer/communicator.
   For communicator merge `agents/communicator/primary.yaml`: exact model
   `alitp-intl/qwen3.8-flash`, provider `custom:alitp-intl`, endpoint
   `https://ai-router.swarm.demlabs.net/v1`, key variable `OPENAI_API_KEY` in its
   private `provider.env`. Preserve existing fallback and other provider entries;
   the captured model configuration declares no vision support.
5. Preserve/build the specified `HERMES_BASE` image before building designer.
   Its default is a locally retained pre-audit image, not a published artifact;
   a clean host needs an independently verified base image. Playwright 1.55.0,
   CairoSVG 2.8.2 and Pillow 11.3.0 are pinned, OS packages are not.
6. `compose.slc.yml.example` and `compose.swarm.yml.example` preserve the
   allowlisted hardening shape, not private settings. Supply paths for env
   files, data/SSH mounts, tmpfs and internal host mappings; set
   `SLC_VAULT_PATH=/opt/slc-vault` in the SLC env file. Keep SLC auth enabled,
   bind MCP only to required private interfaces and retain all role ACLs.
   The marketing swarm's existing container is named `dev-swarm-mcp`; this
   historical name does **not** authorize operations on another dev deployment.
7. Validate rendered YAML and `docker compose config --quiet`, then recreate
   only changed marketing services from their respective `/opt/...` directories.
   Full `docker compose config` output can expose secrets; do not log it.

Before rollout, verify installed Hermes recognizes these lifecycle events and
loads the canonical skills; check Python `httpx` exists in its interpreter.
Check authenticated SLC context loading/saving for all seats, negative auth/ACL
cases, and designer screenshot/PDF/SVG-to-PNG with `designer-python`. Never send
an external message merely to test deployment. Test an approved Telegram file
only with a known recipient and verify outbox delivery, not just `queued:true`.

## Backup installation and restore

The VM script was revised and installed after backing up its previous version;
the atlant scripts remain unchanged. Inspect hardcoded marketing paths and host
address before installing anywhere else. The revised archive was not executed
in this collection: allow ample free disk and review the 2h service timeout before
the next scheduled run, because Docker images substantially increase its size. VM backup needs Docker, Python 3,
SQLite support, GNU tar/coreutils, OpenSSL and a separately provisioned root-only
`/root/marketing-backup.key`. Install the VM script mode 0750 in
`/usr/local/sbin/`. It temporarily pauses **only** `slc-mcp` and `dev-swarm-mcp`,
then resumes them before compression. It contains no credential values, but its
output archive includes private env files, SSH material and vault history.
Never add generated archives or their decryption key to Git.

On atlant, precreate `/var/backups/marketing-swarm-1` and
`/root/marketing-backup-escrow` root-only; install the pull script mode 0750 and
service/timer in `/etc/systemd/system/`. Configure root SSH trust to the marketing
VM only. Run `systemctl daemon-reload`; after a successful manually authorized
`systemctl start marketing-application-backup.service`, enable its timer with
`systemctl enable --now marketing-application-backup.timer`. Inspect status and
journal without printing secret file contents. Timer is daily 03:40 UTC plus up
to 10 minutes jitter; host retention is 14 days by mtime, VM keeps newest archive
plus recently modified files. Existing paths must exist for ProtectSystem rules.

For an isolated restore test: verify archive SHA256, decrypt using OpenSSL
`enc -d -aes-256-cbc -pbkdf2 -iter 200000 -md sha256 -pass file:<private-key>`,
extract into a root-only temporary directory and check SQLite integrity and Git
vault presence. Never extract over live services. Coordinate downtime and restore
all matching DB/WAL files together for a real recovery. Decrypt-and-compare and
SQLite integrity checks are **not** a full application recovery test. Encryption
is CBC plus a separate checksum, not authenticated encryption; the escrow key is
on the same host as archives. Revised backups include each existing provider.env,
all eight slc.env files, server auth.env, tooling, hooks, installed skills and
static Comfy/provider config. Secrets exist only inside the encrypted published
archive; temporary plaintext staging is root-only and removed on normal exit
(but a crash/SIGKILL may leave it behind). Never commit backup output.

`runtime-images/images.tar` saves images of containers belonging to the three
marketing Compose projects and the local designer base if present. Restore with
`docker image load -i runtime-images/images.tar`, then use `tag-map.txt` to restore
required Compose tags if images were saved by ID. `references.txt` records image
IDs/base reference. Image export fails the backup on errors; install/start all
required marketing services before taking a recovery snapshot. Images and skills
may themselves contain sensitive material and remain encrypted. Live agent
sessions, external Comfy model storage and external provider/router secrets are
not captured. This is recovery of installed images, not a bit-reproducible rebuild
from fully pinned upstream sources.

## Marketing key expiry

Install only the two `marketing-9router-key-expiry.*` units on ai-router.
The generic root-owned (root:root 0755) helper is included unchanged in
`expiry/ai-router/expire-9router-api-keys.py`; it contains no embedded credentials.
Install at `/usr/local/sbin/expire-9router-api-keys.py` only after comparing any
existing shared helper: do not overwrite a newer version used by another service.
It requires private marketing metadata at
`/opt/9router/data/marketing-secrets/expiry.json` and the protected router env.
Metadata schema 2 is `{schema: 2, keys: [{id: <UUID>, name: <key-name>,
expiresAt: <timezone-aware ISO date>}]}`; no actual identifiers belong in Git.
The helper checks ID/name equality before deactivation and reads JWT_SECRET from
the private router env. Its default base URL is deployment-specific; inspect it
before installation. Do not substitute dev metadata or enable dev units. Review helper behavior before a
manual run: it expires credentials, so it is not a read-only health check.
After prerequisites are configured, daemon-reload and enable the marketing timer;
it runs hourly with up to 60 seconds jitter. Verify only marketing key identifiers
and expiry dates through an authorized private channel, never through this repo.

## Known limitations and final refresh gate

- Auth is finalized as the per-seat JSON map described above; actual tokens stay
  private. Provider and Comfy/Qwen work was concurrent with collection. Before
  deployment, the owning agents must confirm final provider
  `name`/`key_env`/model/endpoint, fallback and vision capabilities,
  Comfy endpoint, workflow/model availability and Qwen settings. Update examples
  with placeholders only. No captured previous key or credential is reusable.
- Designer ComfyUI was verified from its container at
  `http://192.168.122.1:8188` (host atlant-llm0). `/system_stats`, `/queue` and
  `/object_info` work without an API key on this internal endpoint. No `comfy`
  CLI is needed. Install `agents/designer/comfyui.json` as `/opt/data/comfyui.json`,
  `tooling/comfyui-client.py` as `/opt/data/bin/comfyui-client.py`, and
  `skills/comfyui-marketing` in designer skills (host bind mount
  `/opt/hermes/agents/designer`). Designer SOUL includes these instructions.
  Run `python3 /opt/data/bin/comfyui-client.py` for non-generating validation.
  The actual catalog has 904 nodes and checkpoint `sd_xl_base_1.0.safetensors`;
  the bundled SDXL graph validates against it. Models/output are mounted from
  host `/opt/comfyui/models` and `/opt/comfyui/output`. Generation uses explicit
  `--generate --seed 42 --prompt '...'`, preserves graph/history, and downloads
  PNGs under `/opt/data/inbound/comfyui/<prompt_id>/`. Shared GPUs had only
  approximately 3.1GB/0.8GB free; the tested 8GiB guard refused submission. No
  image was generated and no LM Studio model was evicted. Ask infrastructure
  owner for GPU availability before claiming end-to-end generation. Primary
  Luna and fallback settings were not changed by this ComfyUI deployment.
- Hook context loading does not drain SLC pagination; optional workflow lookups
  swallow failures. The timer uses `list_reminders` and text matching, which must
  be checked against the installed SLC catalog (some versions expose
  `reminder_list`), and is not a reliable due-time parser. Local review removes
  exception details from context-hook stderr; audit/summary redaction remains
  best-effort. Treat logs as sensitive. Hook protocol changes require separate
  integration testing against the installed SLC/Hermes versions.
- Role instructions refer to marketing_* SLC registers. Their live content,
  approvals, private correspondence, campaign assets and history are deliberately
  absent; provision approved clean documents separately. This bundle does not
  claim that runtime auto_load or delivery has been verified after deployment.
- The initial revised VM backup script was installed after a remote backup and
  syntax check; no revised archive was run. Subsequent repository-review fixes
  remain local to this bundle and need an approved rollout and restore test.
  Never stage the repository's untracked secret `.txt` files.
