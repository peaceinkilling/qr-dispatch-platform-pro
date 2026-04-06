# Google Maps API Setup

To use Google Maps instead of OpenStreetMap tiles, set up an API key and add it to your environment.

## 1. Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable billing (required, but free tier includes $200/month credit)

## 2. Enable the Maps JavaScript API

1. In Cloud Console, go to **APIs & Services** → **Library**
2. Search for **Maps JavaScript API**
3. Click **Enable**

## 3. Create an API Key

1. Go to **APIs & Services** → **Credentials**
2. Click **Create Credentials** → **API Key**
3. Copy the key

## 4. Restrict the Key (Recommended)

1. Click your API key to edit
2. Under **Application restrictions**, select **HTTP referrers**
3. Add your site URLs, e.g. `http://localhost:8000/*`, `https://yourdomain.com/*`
4. Under **API restrictions**, select **Maps JavaScript API**

## 5. Add the Key to Your App

**Option A: Environment variable**

```bash
export GOOGLE_MAPS_API_KEY="your-api-key-here"
```

**Option B: .env file** (recommended)

A `.env` file already exists in the `qr_dispatch_platform_pro` folder. Add your key:

```
GOOGLE_MAPS_API_KEY=your-api-key-here
```

The app loads `.env` automatically if `python-dotenv` is installed (`pip install python-dotenv`).

**Option C: Windows**

```powershell
$env:GOOGLE_MAPS_API_KEY="your-api-key-here"
```

## 6. Restart the App

After setting the key, restart the FastAPI server. The dashboard will use Google Maps when the key is present; otherwise it falls back to Leaflet + static map.
