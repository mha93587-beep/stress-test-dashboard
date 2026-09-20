# 🔥 Stress Test Dashboard

A Streamlit-based frontend for the Python asyncio stress/load testing tool.

## Features

- 🎯 **Target URL Input** — Enter any URL to test
- 📊 **Customizable Load Stages** — Define ramp-up/hold/ramp-down stages
- ⚡ **Real-time Metrics** — Active users, RPS, error rate, response times
- 📜 **Live Logs** — See every tick of the test in real-time
- 🛑 **Start/Stop Controls** — Full control over the test lifecycle
- 🌙 **Dark Theme** — Beautiful dark gradient UI

## Deploy to Streamlit Cloud

1. Push this `streamlit_app/` folder to a GitHub repo
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your repo
4. Set the main file path to `app.py`
5. Deploy! 🚀

## Run Locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Files

| File | Purpose |
|------|---------|
| `app.py` | Main Streamlit application |
| `requirements.txt` | Python dependencies |
| `.streamlit/config.toml` | Theme & server configuration |

## ⚠️ Disclaimer

Only use this tool against servers you **OWN** or have **explicit written permission** to test. Unauthorized stress testing is illegal.
