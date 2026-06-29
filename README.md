
# BIDV Sanctions Processing System

🔒 Streamlit-based web application for processing sanctions data from multiple sources

## Live Demo

🌐 Access the Application: [https://bidv-sanctions-system.streamlit.app/](https://bidv-sanctions-system.streamlit.app/)

## Features

- 🔐 Secure Authentication

- 📄 PDF Processing with Gemini AI

- 🇺🇸 OFAC Data Integration

- 🌍 UN Sanctions Processing

- ⚙️ Batch Processing

- 📊 Real-time Dashboard

## Setup

1. Get one or more Gemini API keys from [Google AI Studio](https://aistudio.google.com/app/apikey)

2. Configure them in Streamlit Cloud secrets (App → Settings → Secrets). Multiple
   keys are pooled with round-robin load-balancing and automatic failover when a
   key hits its rate limit:

   ```toml
   GEMINI_API_KEYS = [
       "your_key_1",
       "your_key_2",
       "your_key_3",
   ]
   ```

   A single `GEMINI_API_KEY = "your_key"` is still supported for backward
   compatibility. See `.streamlit/secrets.toml.example` for the full template.

3. Deploy and enjoy!

