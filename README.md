# The Watch

Real-time safety intelligence system with a Telegram listener, processing pipeline, and user-facing bot/CLI.

## Setup

1) Create a virtual environment (recommended) and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) Create a `.env` file from the template:

```bash
cp env.template .env
```

3) Set required environment variables in `.env`:

- `GOOGLE_API_KEY` (required for Gemini + geocoding)
- `TELEGRAM_API_ID` (required for Telegram listener)
- `TELEGRAM_API_HASH` (required for Telegram listener)
- `TELEGRAM_BOT_TOKEN` (required for Telegram bot mode)

## Run

All modes are driven from `main.py`:

- Full system (listener + processing):
```bash
python main.py
```

- Listener only (Telegram ingestion):
```bash
python main.py --listener-only
```

- Telegram bot interface:
```bash
python main.py --bot
```

- Analyst CLI (interactive query mode):
```bash
python main.py --analyst-cli
```

- Test pipeline with sample data:
```bash
python main.py --test-pipeline
```

## Notes

- Telegram session files (`*.session`) are created automatically when the listener runs.
- ChromaDB data is stored under `data/chroma_db/`.
