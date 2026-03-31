"""
fetch_power_changes.py
----------------------
Scrapes 100+ international RSS feeds for headlines related to changes in
political power (coups, elections, uprisings, resignations, etc.) across
44 tracked countries. Non-English headlines are translated to English.
Outputs /docs/leadership-outputs.json and keeps a 14-day rolling archive.

No external APIs required — translation uses deep-translator (Google
Translate web scraping, free, no key).
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import feedparser
from dateutil import parser as dateutil_parser
from deep_translator import GoogleTranslator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DOCS_DIR = Path("docs")
OUTPUT_FILE = DOCS_DIR / "leadership-outputs.json"
ARCHIVE_DAYS = 14
REQUEST_TIMEOUT = 15  # seconds per feed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tracked countries
# ---------------------------------------------------------------------------

TRACKED_COUNTRIES = {
    "Algeria", "Argentina", "Armenia", "Azerbaijan", "Brazil", "Canada",
    "Chile", "China", "Colombia", "Cuba", "Denmark", "Egypt", "El Salvador",
    "France", "Germany", "India", "Indonesia", "Iran", "Israel", "Japan",
    "Libya", "Mexico", "Morocco", "Myanmar", "Nigeria", "North Korea",
    "Pakistan", "Palestine", "Panama", "Peru", "Russia", "Saudi Arabia",
    "Somalia", "South Korea", "Sudan", "Syria", "Taiwan", "Turkey", "UAE",
    "Ukraine", "United Kingdom", "Venezuela", "Vietnam", "Yemen",
}

# Country aliases / demonyms used in headlines
COUNTRY_ALIASES = {
    "algerian": "Algeria",
    "argentinian": "Argentina", "argentinean": "Argentina", "argentine": "Argentina",
    "armenian": "Armenia",
    "azerbaijani": "Azerbaijan", "azeri": "Azerbaijan",
    "brazilian": "Brazil",
    "canadian": "Canada",
    "chilean": "Chile",
    "chinese": "China", "beijing": "China",
    "colombian": "Colombia",
    "cuban": "Cuba",
    "danish": "Denmark",
    "egyptian": "Egypt",
    "salvadoran": "El Salvador", "salvadorean": "El Salvador",
    "french": "France", "paris": "France",
    "german": "Germany", "berlin": "Germany",
    "indian": "India", "new delhi": "India",
    "indonesian": "Indonesia", "jakarta": "Indonesia",
    "iranian": "Iran", "tehran": "Iran",
    "israeli": "Israel", "tel aviv": "Israel",
    "japanese": "Japan", "tokyo": "Japan",
    "libyan": "Libya",
    "mexican": "Mexico", "mexico city": "Mexico",
    "moroccan": "Morocco",
    "burmese": "Myanmar", "myanmar": "Myanmar",
    "nigerian": "Nigeria", "abuja": "Nigeria",
    "north korean": "North Korea", "pyongyang": "North Korea", "dprk": "North Korea",
    "pakistani": "Pakistan", "islamabad": "Pakistan",
    "palestinian": "Palestine", "gaza": "Palestine", "west bank": "Palestine",
    "panamanian": "Panama",
    "peruvian": "Peru",
    "russian": "Russia", "moscow": "Russia", "kremlin": "Russia",
    "saudi": "Saudi Arabia", "riyadh": "Saudi Arabia",
    "somali": "Somalia", "mogadishu": "Somalia",
    "south korean": "South Korea", "seoul": "South Korea",
    "sudanese": "Sudan", "khartoum": "Sudan",
    "syrian": "Syria", "damascus": "Syria",
    "taiwanese": "Taiwan", "taipei": "Taiwan",
    "turkish": "Turkey", "ankara": "Turkey",
    "emirati": "UAE", "dubai": "UAE", "abu dhabi": "UAE",
    "ukrainian": "Ukraine", "kyiv": "Ukraine", "kiev": "Ukraine",
    "british": "United Kingdom", "uk": "United Kingdom",
    "london": "United Kingdom", "parliament": "United Kingdom",
    "venezuelan": "Venezuela", "caracas": "Venezuela",
    "vietnamese": "Vietnam", "hanoi": "Vietnam",
    "yemeni": "Yemen", "sanaa": "Yemen",
}

# ---------------------------------------------------------------------------
# Power-change keywords (English — applied after translation)
# ---------------------------------------------------------------------------

POWER_KEYWORDS = [
    # Elections & referenda
    r"\belection\b", r"\belections\b", r"\bvote\b", r"\bvoting\b",
    r"\bballot\b", r"\bpoll\b", r"\breferendum\b", r"\bplebiscite\b",
    r"\bsnap election\b", r"\bby-election\b", r"\bbye-election\b",
    r"\bprimary\b", r"\brunoff\b", r"\brun-off\b",
    # Leadership transitions
    r"\bcoup\b", r"\bputsch\b", r"\bjunta\b",
    r"\bpresident\b", r"\bprime minister\b", r"\bpm\b",
    r"\bchancellor\b", r"\bpremier\b", r"\bminister\b",
    r"\bgovernment\b", r"\bregime\b", r"\bstate\b",
    r"\bparliament\b", r"\blegislature\b", r"\bcongress\b", r"\bsenate\b",
    r"\bkabinet\b", r"\bcabinet\b",
    r"\bterm limit\b", r"\bconstitution\b",
    r"\bsworn in\b", r"\bswearing.in\b", r"\binaugurat\b",
    r"\bappoint\b", r"\bappointed\b", r"\bnominated\b", r"\bnomination\b",
    r"\bresign\b", r"\bresigned\b", r"\bresignation\b",
    r"\bimpeach\b", r"\bimpeachment\b",
    r"\boverthrough\b", r"\boverthrow\b", r"\bdeposed\b", r"\bdepose\b",
    r"\bforced out\b", r"\bstep down\b", r"\bstepped down\b",
    r"\boutsted\b", r"\bousted\b", r"\bremoved from power\b",
    r"\bremoved from office\b", r"\btransition of power\b",
    r"\bpower transfer\b", r"\bsuccession\b", r"\bsuccessor\b",
    r"\bleader\b", r"\bleadership\b",
    r"\bhead of state\b", r"\bhead of government\b",
    # Uprisings & unrest
    r"\buprising\b", r"\brevolution\b", r"\bprotest\b", r"\bdemonstration\b",
    r"\banti-government\b", r"\bcrackdown\b", r"\bstate of emergency\b",
    r"\bmartial law\b", r"\bcurfew\b",
    r"\brebellion\b", r"\binsurrection\b", r"\bcivil war\b",
    r"\bmilitia\b", r"\bmilitary takeover\b", r"\barmed forces\b",
    r"\bopposition\b", r"\bdissent\b", r"\bdefection\b",
    # Parties & coalitions
    r"\bcoalition\b", r"\bparty\b", r"\brunning mate\b", r"\bcandidate\b",
    r"\bincumbent\b", r"\bopponent\b", r"\bopposition leader\b",
    # Sanctions / foreign pressure that triggers regime change
    r"\bsanctions\b", r"\bblockade\b", r"\bembargo\b",
]

POWER_RE = re.compile("|".join(POWER_KEYWORDS), re.IGNORECASE)

# ---------------------------------------------------------------------------
# RSS feed list — 100+ sources covering all 44 countries
# ---------------------------------------------------------------------------
# Format: (source_name, feed_url, primary_language_code)
# lang "en" = English (no translation needed)
# lang other = attempt translation via deep-translator

FEEDS = [
    # ── GLOBAL / MULTI-COUNTRY ──────────────────────────────────────────────
    ("Reuters World", "https://feeds.reuters.com/reuters/worldNews", "en"),
    ("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml", "en"),
    ("Al Jazeera English", "https://www.aljazeera.com/xml/rss/all.xml", "en"),
    ("Associated Press", "https://rsshub.app/apnews/topics/apf-intlnews", "en"),
    ("France 24 English", "https://www.france24.com/en/rss", "en"),
    ("Deutsche Welle English", "https://rss.dw.com/rdf/rss-en-world", "en"),
    ("VOA News World", "https://www.voanews.com/api/zyrqo$ekyq/feed.rss", "en"),
    ("NPR World", "https://feeds.npr.org/1004/rss.xml", "en"),
    ("The Guardian World", "https://www.theguardian.com/world/rss", "en"),
    ("Foreign Policy", "https://foreignpolicy.com/feed/", "en"),
    ("Politico World", "https://rss.politico.com/politics-news.xml", "en"),
    ("Axios World", "https://api.axios.com/feed/", "en"),

    # ── MIDDLE EAST & NORTH AFRICA ──────────────────────────────────────────
    # Egypt
    ("Egypt Independent", "https://egyptindependent.com/feed/", "en"),
    ("Al-Ahram Weekly", "https://english.ahram.org.eg/rss.aspx", "en"),
    ("Mada Masr", "https://www.madamasr.com/en/feed/", "en"),
    # Libya
    ("Libya Observer", "https://www.libyaobserver.ly/feed", "en"),
    ("Libya Herald", "https://www.libyaherald.com/feed/", "en"),
    # Morocco
    ("Morocco World News", "https://www.moroccoworldnews.com/feed/", "en"),
    ("Le Desk (Morocco)", "https://ledesk.ma/feed/", "fr"),
    # Algeria
    ("Algeria Press Service", "https://www.aps.dz/en/rss.html", "en"),
    ("El Watan (Algeria)", "https://www.elwatan.com/feed/", "fr"),
    # Saudi Arabia
    ("Arab News", "https://www.arabnews.com/rss.xml", "en"),
    ("Saudi Gazette", "https://saudigazette.com.sa/rss", "en"),
    # UAE
    ("The National (UAE)", "https://www.thenationalnews.com/rss.xml", "en"),
    ("Gulf News", "https://gulfnews.com/rss", "en"),
    # Palestine / Israel
    ("Haaretz English", "https://www.haaretz.com/cmlink/1.628765", "en"),
    ("Jerusalem Post", "https://www.jpost.com/rss/rssfeedsfrontpage.aspx", "en"),
    ("Middle East Eye", "https://www.middleeasteye.net/rss", "en"),
    ("Palestinian Chronicle", "https://www.palestinechronicle.com/feed/", "en"),
    # Iran
    ("Iran International", "https://www.iranintl.com/en/rss.xml", "en"),
    ("Press TV (Iran)", "https://www.presstv.ir/rssFeed/world/", "en"),
    # Syria / Yemen / Somalia / Sudan
    ("Syria Direct", "https://syriadirect.org/feed/", "en"),
    ("Yemen Monitor", "https://yemenmonitor.com/feed/", "en"),
    ("Garowe Online (Somalia)", "https://www.garoweonline.com/en/rss", "en"),
    ("Radio Dabanga (Sudan)", "https://www.dabangasudan.org/en/all-news/feed", "en"),

    # ── SUB-SAHARAN AFRICA ──────────────────────────────────────────────────
    # Nigeria
    ("Vanguard Nigeria", "https://www.vanguardngr.com/feed/", "en"),
    ("The Punch Nigeria", "https://punchng.com/feed/", "en"),
    ("Premium Times Nigeria", "https://www.premiumtimesng.com/feed", "en"),

    # ── EUROPE ──────────────────────────────────────────────────────────────
    # France
    ("Le Monde (France)", "https://www.lemonde.fr/rss/une.xml", "fr"),
    ("Le Figaro (France)", "https://www.lefigaro.fr/rss/figaro_actualites.xml", "fr"),
    # Germany
    ("Der Spiegel (Germany)", "https://www.spiegel.de/international/index.rss", "en"),
    ("DW Politics (Germany)", "https://rss.dw.com/xml/rss-en-pol", "en"),
    ("Frankfurter Allgemeine", "https://www.faz.net/rss/aktuell/politik/", "de"),
    # UK
    ("Sky News UK", "https://feeds.skynews.com/feeds/rss/world.xml", "en"),
    ("The Times UK", "https://www.thetimes.co.uk/feed/", "en"),
    ("The Telegraph", "https://www.telegraph.co.uk/rss.xml", "en"),
    # Denmark
    ("The Local Denmark", "https://www.thelocal.dk/feed/", "en"),
    ("Politiken (Denmark)", "https://politiken.dk/rss/senestenyt.rss", "da"),
    # Russia / Ukraine
    ("Moscow Times", "https://www.themoscowtimes.com/rss/news", "en"),
    ("Kyiv Independent", "https://kyivindependent.com/feed/", "en"),
    ("Ukrinform", "https://www.ukrinform.net/rss/block-lastnews", "en"),
    ("Meduza (Russia EN)", "https://meduza.io/rss/en/all", "en"),
    # Armenia / Azerbaijan
    ("Civilnet (Armenia)", "https://www.civilnet.am/en/feed/", "en"),
    ("Azerbaycan 24", "https://azerbaycan24.com/feed/", "az"),

    # ── ASIA ────────────────────────────────────────────────────────────────
    # China
    ("South China Morning Post", "https://www.scmp.com/rss/91/feed", "en"),
    ("Caixin Global (China)", "https://www.caixinglobal.com/rss/101096.xml", "en"),
    ("Xinhua World (China)", "https://www.xinhuanet.com/english/rss/worldrss.xml", "en"),
    # Japan
    ("Japan Times", "https://www.japantimes.co.jp/feed/", "en"),
    ("NHK World Japan", "https://www3.nhk.or.jp/rss/news/cat0.xml", "en"),
    ("Mainichi Shimbun English", "https://mainichi.jp/rss/etc/english.rss", "en"),
    # South Korea
    ("Korea Herald", "https://www.koreaherald.com/rss/020000000000.xml", "en"),
    ("Korea Times", "https://www.koreatimes.co.kr/www/rss/nation.xml", "en"),
    # North Korea
    ("NK News", "https://www.nknews.org/feed/", "en"),
    ("38 North", "https://www.38north.org/feed/", "en"),
    # Taiwan
    ("Taiwan News", "https://www.taiwannews.com.tw/rss/rss.xml", "en"),
    ("Focus Taiwan", "https://focustaiwan.tw/rss-feed", "en"),
    # India
    ("The Hindu Politics", "https://www.thehindu.com/news/national/feeder/default.rss", "en"),
    ("Hindustan Times", "https://www.hindustantimes.com/rss/topnews/rssfeed.xml", "en"),
    ("Indian Express", "https://indianexpress.com/feed/", "en"),
    # Pakistan
    ("Dawn Pakistan", "https://www.dawn.com/feeds/home", "en"),
    ("The News International", "https://www.thenews.com.pk/rss/1/8", "en"),
    # Indonesia
    ("Jakarta Post", "https://www.thejakartapost.com/news/rss", "en"),
    ("Tempo.co (Indonesia)", "https://en.tempo.co/rss/full", "en"),
    # Myanmar
    ("Irrawaddy (Myanmar)", "https://www.irrawaddy.com/feed", "en"),
    ("Myanmar Now", "https://myanmar-now.org/en/feed/", "en"),
    # Vietnam
    ("VN Express International", "https://e.vnexpress.net/rss/news/politics.rss", "en"),
    ("Thanh Nien News", "https://thanhnien.vn/rss/home.rss", "vi"),

    # ── AMERICAS ────────────────────────────────────────────────────────────
    # Argentina
    ("Buenos Aires Herald", "https://buenosairesherald.com/feed", "en"),
    ("La Nacion (Argentina)", "https://www.lanacion.com.ar/arc/outboundfeeds/rss/", "es"),
    ("Infobae (Argentina)", "https://www.infobae.com/feeds/rss/politics.xml", "es"),
    # Brazil
    ("Brasil de Fato", "https://brasildefato.com.br/feeds/all.atom.xml", "pt"),
    ("Folha de S.Paulo", "https://feeds.folha.uol.com.br/poder/rss091.xml", "pt"),
    ("O Globo", "https://oglobo.globo.com/rss.xml", "pt"),
    # Mexico
    ("El Universal (Mexico)", "https://www.eluniversal.com.mx/rss.xml", "es"),
    ("Reforma (Mexico)", "https://www.reforma.com/rss/portada.xml", "es"),
    ("Animal Politico (Mexico)", "https://animalpolitico.com/feed/", "es"),
    # Colombia
    ("El Colombiano", "https://www.elcolombiano.com/rss.xml", "es"),
    ("El Tiempo (Colombia)", "https://www.eltiempo.com/rss/politica.xml", "es"),
    # Chile
    ("La Tercera (Chile)", "https://www.latercera.com/feed/", "es"),
    ("El Mostrador (Chile)", "https://www.elmostrador.cl/feed/", "es"),
    # Venezuela
    ("El Nacional (Venezuela)", "https://www.elnacional.com/feed/", "es"),
    ("Efecto Cocuyo (Venezuela)", "https://efectococuyo.com/feed/", "es"),
    # Peru
    ("Peru Reports", "https://perureports.com/feed/", "en"),
    ("El Comercio (Peru)", "https://elcomercio.pe/rss/ultimas-noticias.xml", "es"),
    # Cuba
    ("14ymedio (Cuba)", "https://www.14ymedio.com/feed", "es"),
    ("CiberCuba", "https://www.cibercuba.com/feed", "es"),
    # Canada
    ("CBC News", "https://www.cbc.ca/cmlink/rss-world", "en"),
    ("Globe and Mail", "https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/politics/", "en"),
    # Panama / El Salvador
    ("La Estrella de Panama", "https://www.laestrella.com.pa/rss.xml", "es"),
    ("El Faro (El Salvador)", "https://elfaro.net/rss.xml", "es"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_translate(text: str, src_lang: str) -> str:
    """Translate text to English. Returns original on failure."""
    if src_lang == "en" or not text:
        return text
    try:
        translated = GoogleTranslator(source=src_lang, target="en").translate(text)
        return translated or text
    except Exception as exc:
        log.warning("Translation failed (%s → en): %s", src_lang, exc)
        return text


def is_power_change(title: str) -> bool:
    """Return True if title matches power-change keywords."""
    return bool(POWER_RE.search(title))


def mentions_tracked_country(title: str) -> bool:
    """Return True if the title mentions at least one tracked country."""
    title_lower = title.lower()
    for country in TRACKED_COUNTRIES:
        if country.lower() in title_lower:
            return True
    for alias in COUNTRY_ALIASES:
        if alias in title_lower:
            return True
    return False


def parse_published(entry) -> str:
    """Extract and normalise published date to ISO-8601 UTC string."""
    for attr in ("published", "updated", "created"):
        raw = getattr(entry, attr, None)
        if raw:
            try:
                dt = dateutil_parser.parse(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).isoformat()
            except Exception:
                pass
    return datetime.now(timezone.utc).isoformat()


def fetch_feed(source_name: str, url: str, lang: str) -> list[dict]:
    """Fetch one RSS feed and return matching power-change stories."""
    results = []
    try:
        feed = feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})
        if feed.bozo and not feed.entries:
            log.warning("[SKIP] %s — feed parse error: %s", source_name, feed.bozo_exception)
            return results

        for entry in feed.entries:
            raw_title = (getattr(entry, "title", "") or "").strip()
            if not raw_title:
                continue

            # Translate non-English titles
            title = safe_translate(raw_title, lang) if lang != "en" else raw_title

            # Filter: must match power-change keywords AND tracked country
            if not is_power_change(title):
                continue
            if not mentions_tracked_country(title):
                continue

            link = getattr(entry, "link", url)
            published = parse_published(entry)

            results.append({
                "title": title,
                "source": source_name,
                "url": link,
                "published_date": published,
            })

    except Exception as exc:
        log.warning("[SKIP] %s — unexpected error: %s", source_name, exc)

    return results


def cutoff_date() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=ARCHIVE_DAYS)


def load_existing(path: Path) -> list[dict]:
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
        except Exception as exc:
            log.warning("Could not load existing JSON: %s", exc)
    return []


def deduplicate(stories: list[dict]) -> list[dict]:
    """Deduplicate by URL, keeping newest occurrence."""
    seen = {}
    for s in stories:
        seen[s["url"]] = s
    return list(seen.values())


def prune_old(stories: list[dict], cutoff: datetime) -> list[dict]:
    """Remove stories older than cutoff."""
    kept = []
    for s in stories:
        try:
            pub = dateutil_parser.parse(s["published_date"])
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            if pub >= cutoff:
                kept.append(s)
        except Exception:
            kept.append(s)  # keep if unparseable
    return kept


def sort_stories(stories: list[dict]) -> list[dict]:
    def sort_key(s):
        try:
            return dateutil_parser.parse(s["published_date"])
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)
    return sorted(stories, key=sort_key, reverse=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=== Change-in-Power RSS Scraper starting ===")
    DOCS_DIR.mkdir(exist_ok=True)

    # Load existing archive
    existing = load_existing(OUTPUT_FILE)
    log.info("Loaded %d existing stories from archive.", len(existing))

    # Prune old stories first
    cutoff = cutoff_date()
    existing = prune_old(existing, cutoff)
    log.info("After 14-day prune: %d stories remain.", len(existing))

    # Fetch fresh stories
    fresh: list[dict] = []
    for source_name, url, lang in FEEDS:
        log.info("Fetching: %s", source_name)
        stories = fetch_feed(source_name, url, lang)
        log.info("  → %d matching stories", len(stories))
        fresh.extend(stories)
        time.sleep(0.3)  # gentle rate-limiting

    log.info("Fetched %d fresh matching stories total.", len(fresh))

    # Merge, deduplicate, prune, sort
    merged = existing + fresh
    merged = deduplicate(merged)
    merged = prune_old(merged, cutoff)
    merged = sort_stories(merged)

    # Build output JSON with metadata header
    output = {
        "_meta": {
            "description": (
                "Power-change news headlines from 100+ international RSS feeds. "
                "Covers elections, coups, uprisings, resignations, and other "
                "leadership transitions across 44 tracked countries. "
                "Archive window: 14 days. Updated twice daily at 06:00 and 18:00 EST."
            ),
            "tracked_countries": sorted(TRACKED_COUNTRIES),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "story_count": len(merged),
            "fields": {
                "title": "Headline in English",
                "source": "Name of the news outlet",
                "url": "Direct link to the article",
                "published_date": "ISO-8601 UTC publication timestamp",
            },
        },
        "stories": merged,
    }

    OUTPUT_FILE.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info("Wrote %d stories to %s", len(merged), OUTPUT_FILE)
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
