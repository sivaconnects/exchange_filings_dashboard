# 📊 India Exchange Filings Dashboard
### NSE + BSE · Daily · Open Source · Free Forever

A free, open-source dashboard that aggregates **NSE and BSE exchange filings** daily and surfaces **multibagger investment opportunities** — before institutional investors enter.

> Built for independent Indian investors. No API keys. No subscriptions. No cost.

**🌐 Live Dashboard:** `https://YOUR_USERNAME.github.io/exchange_filings_dashboard`

---

## 🎯 What This Does

| Section | What You Get |
|---|---|
| 📋 Exchange Filings | All corporate filings from NSE + BSE, filtered by Equity / SME |
| ⭐ Important Filings | Board meetings, financial results, corporate actions combined |
| 🎯 Opportunities | AI-scored multibagger signals scored out of 10 |

### Filing Categories (Equity + SME both)
- **Corporate Announcements** — All press releases and disclosures
- **Board Meetings** — Notices with agenda (look for expansion/fundraise)
- **Financial Results** — Quarterly and annual results
- **Corporate Actions** — Dividends, bonus, splits, buybacks, rights issues
- **Shareholding Patterns** — Promoter / FII / DII / public holding changes

---

## 🔥 Multibagger Signal Engine

The opportunity analyzer scores every filing automatically:

| Signal | Score |
|---|---|
| Promoter holding > 70% | +4 |
| Bonus / Stock split | +3 |
| Buyback announced | +3 |
| Board meeting with fundraise/capex agenda | +3 |
| Turnaround (profit after loss) | +3 |
| Dividend announcement | +2 |
| High promoter holding (60-70%) | +2 |
| SME segment filing (under-researched) | +1 |
| Caution: SEBI notice / fraud / insolvency | -5 |

**Score ≥ 8** = 🔥 High Opportunity  
**Score 5-7** = ✅ Moderate  
**Score 3-4** = 👀 Watch  

---

## 🚀 Setup (One Time)

### 1. Fork this repository
Click **Fork** on GitHub

### 2. Enable GitHub Pages
- Go to Settings → Pages
- Source: `gh-pages` branch
- Your dashboard will be live at `https://YOUR_USERNAME.github.io/exchange_filings_dashboard`

### 3. Enable GitHub Actions
- Go to Actions tab → Enable workflows
- The scraper runs automatically at **11:30 PM IST every weekday**
- You can also trigger it manually: Actions → Daily Exchange Filings Scraper → Run workflow

### 4. That's it! ✅
GitHub Actions will scrape NSE + BSE nightly and update your dashboard automatically.

---

## 📁 Project Structure

```
exchange_filings_dashboard/
├── scrapers/
│   ├── main.py                  # Orchestrator — runs everything
│   ├── nse_scraper.py           # NSE web scraper
│   ├── bse_scraper.py           # BSE web scraper
│   └── opportunity_analyzer.py  # Multibagger signal engine
├── data/
│   ├── json/                    # Daily JSON files (nse_YYYY-MM-DD.json etc.)
│   └── csv/                     # Daily CSV files
├── docs/
│   ├── index.html               # Dashboard (GitHub Pages)
│   └── data.json                # Latest data (updated daily by scraper)
├── .github/
│   └── workflows/
│       └── daily_scraper.yml    # GitHub Actions automation
├── requirements.txt
└── README.md
```

---

## 🏃 Run Locally

```bash
git clone https://github.com/YOUR_USERNAME/exchange_filings_dashboard
cd exchange_filings_dashboard
pip install -r requirements.txt
cd scrapers
python main.py
```

Data will be saved to `data/json/`, `data/csv/`, and `docs/data.json`.

### View the dashboard locally (important)
Because the dashboard uses `fetch()` to load `docs/data.json`, most browsers will **block it** if you open `docs/index.html` directly via `file://`.

Serve the `docs/` folder with a local web server instead:

```bash
cd docs
python -m http.server 8000
```

Then open `http://localhost:8000` in your browser.

---

## 💡 Investment Philosophy

> "The best opportunities are found before the crowd discovers them."

This tool is inspired by how large investors do their research:
- Watch **exchange filings** that most retail investors ignore
- Track **promoter activity** (when founders buy more, it's a signal)
- Find **SME companies** before they grow into mid-caps
- Spot **board meeting agendas** that hint at big expansion plans
- Notice **shareholding changes** — FII/DII entry is a leading indicator

---

## 🤝 Contributing

This is open source. PRs welcome!

Ideas for future signals:
- [ ] Bulk/block deal tracker
- [ ] Insider trading disclosures
- [ ] Pledge creation/release tracking
- [ ] Revenue trend analysis from consecutive quarterly results
- [ ] Telegram bot alerts for high-score opportunities
- [ ] WhatsApp alert integration
- [ ] Historical data archiving (5 years)

---

## ⚠️ Disclaimer

This tool is for **informational and research purposes only**. It is **not financial advice**. Always do your own due diligence before investing. The opportunity scores are algorithmic and do not guarantee returns. Invest responsibly.

---

## 📄 License

MIT License — Free to use, fork, and improve. Please credit the project.

---

*Made with ❤️ for independent Indian investors.*
