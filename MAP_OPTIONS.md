# Map Options for Dispatch Platform

This document explains the different approaches you can use for displaying maps, with advantages and disadvantages.

---

## 1. Static Image + Overlay (Current Fallback)

**How it works:** Use a high-quality static image of India (or your region) as the background. Overlay markers and lines using SVG or canvas, positioned by converting lat/lng to pixel coordinates.

**Advantages:**
- Always works (no network dependency)
- No API keys or usage limits
- Fast load, works offline
- Full control over appearance
- No third-party terms of service

**Disadvantages:**
- No zoom/pan (or limited if you implement it)
- Fixed resolution; zooming may pixelate
- Must update the base image if borders/roads change
- Coordinate-to-pixel math needed for each view

**Best for:** Offline-first, low bandwidth, or when external tile services are blocked.

---

## 2. Google Maps API

**How it works:** Embed Google Maps via JavaScript API. Use `google.maps.Map`, markers, polylines. Requires API key and billing account (free tier available).

**Advantages:**
- Excellent data quality and coverage
- Road routing, traffic, Street View
- Familiar UX for users
- Well-documented

**Disadvantages:**
- Requires API key and Google Cloud billing
- Usage limits; costs after free tier (~$200/month credit)
- Terms of service restrictions
- Vendor lock-in

**Best for:** Production apps with budget, when you need routing or premium features.

---

## 3. Open Source: Leaflet + OpenStreetMap (Current Primary)

**How it works:** Leaflet.js loads map tiles from OpenStreetMap (or other providers). Tiles are images fetched from tile servers. No API key for OSM.

**Advantages:**
- Free, no API key for OSM
- Open source, no vendor lock-in
- Good coverage in India
- Zoom, pan, markers, polylines
- Many tile providers (OSM, CARTO, Stamen, etc.)

**Disadvantages:**
- Depends on network; tiles can fail if blocked or slow
- Tile servers can be rate-limited
- Some networks block external tile URLs

**Best for:** Web apps with internet; cost-sensitive projects.

---

## 4. Mapbox

**How it works:** Similar to Leaflet but with Mapbox GL JS. Vector tiles, custom styling. Free tier: 50k map loads/month.

**Advantages:**
- Vector tiles (smooth zoom, smaller data)
- Custom map styles
- Good performance
- Free tier for moderate use

**Disadvantages:**
- API key required
- Costs after free tier
- Another vendor dependency

**Best for:** Apps needing custom styling or high-quality vector maps.

---

## 5. Hybrid (What We Use Now)

**Current setup:**
- **Primary:** Leaflet + OpenStreetMap tiles
- **Fallback:** Static India map image when tiles fail
- **Overlay:** SVG schematic with depot → station lines (always visible)

**Why:** Ensures something is always visible. If OSM tiles load, you get an interactive map. If not, the static map + schematic still show positions.

---

## Quick Comparison

| Option              | Cost      | Offline | API Key | Quality |
|---------------------|-----------|---------|---------|---------|
| Static + Overlay    | Free      | Yes     | No      | Good    |
| Google Maps         | Paid*     | Limited | Yes     | Best    |
| Leaflet + OSM      | Free      | No      | No      | Good    |
| Mapbox             | Free tier | No      | Yes     | Best    |

*Google offers $200/month free credit.

---

## Switching Options

To use a different approach, edit:
- `templates/dashboard_ui.html` – fleet map section
- `templates/detail.html` – single-dispatch map
- `static/styles.css` – map container styles
- `base.html` – add/remove Leaflet or other map library scripts
