# Paper Library Dev Environment

Use this branch with an isolated OpenClaw runtime so paper-library work does not affect the production Feishu bot.

Suggested local directories:

- `dev_env/library/`: isolated downloaded files and SQLite cache
- `dev_env/runtime/profile/`: isolated browser profile for login cookies
- `dev_env/runtime/agent-browser-session.json`: isolated session config
- `dev_env/media/`: optional isolated OpenClaw media root for attachments
- `dev_env/openclaw.paper-dev.example.json`: OpenClaw config template for a second local gateway

Recommended workflow:

1. Copy `dev_env/openclaw.paper-dev.example.json` to a machine-local file such as `dev_env/openclaw.paper-dev.local.json`.
2. Update the Feishu `appId` and `appSecret` to a dedicated test bot.
3. Point the plugin config at `dev_env/library`, `dev_env/runtime/profile`, and `dev_env/runtime/agent-browser-session.json`.
4. Start a second OpenClaw gateway with that config instead of reusing the production one.
5. Run `/bookfetch login` against the test bot to initialize the isolated browser profile.

Do not commit the local runtime, library, or media contents from this directory.
