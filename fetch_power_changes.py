"""
fetch_power_changes.py
----------------------
Scrapes 100+ international RSS feeds for headlines related to changes in
political power (coups, elections, uprisings, resignations, etc.) across
44 tracked countries. Non-English headlines are translated to English.
Outputs /docs/leadership-outputs.json and keeps a 14-day rolling archive.

No external APIs required — translation uses deep-translator (Google
Translate web scraping, free, no key).

FILTERS (applied in order):
1. Headline must mention a tracked country or its alias/demonym/capital
2. Headline must match a power-change keyword
3. Headline must NOT match a noise exclusion list (corporate, sports, etc.)
"""

import io
import json
import logging
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import feedparser
import requests
from dateutil import parser as dateutil_parser
from deep_translator import GoogleTranslator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DOCS_DIR = Path("docs")
OUTPUT_FILE = DOCS_DIR / "leadership-outputs.json"
ARCHIVE_DAYS = 14
REQUEST_TIMEOUT = 20
RETRY_ATTEMPTS = 2
RETRY_BACKOFF = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}

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

# Country aliases / demonyms / capitals — lower-case, word-boundary matched
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
    "israeli": "Israel", "tel aviv": "Israel", "knesset": "Israel",
    "japanese": "Japan", "tokyo": "Japan",
    "libyan": "Libya", "tripoli": "Libya",
    "mexican": "Mexico", "mexico city": "Mexico",
    "moroccan": "Morocco", "rabat": "Morocco",
    "burmese": "Myanmar", "tatmadaw": "Myanmar", "junta": "Myanmar",
    "nigerian": "Nigeria", "abuja": "Nigeria",
    "north korean": "North Korea", "pyongyang": "North Korea", "dprk": "North Korea",
    "pakistani": "Pakistan", "islamabad": "Pakistan",
    "palestinian": "Palestine", "gaza": "Palestine", "west bank": "Palestine",
    "panamanian": "Panama",
    "peruvian": "Peru", "lima": "Peru",
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
    "british": "United Kingdom", "london": "United Kingdom",
    "venezuelan": "Venezuela", "caracas": "Venezuela",
    "vietnamese": "Vietnam", "hanoi": "Vietnam",
    "yemeni": "Yemen", "sanaa": "Yemen",
}

# Build a compiled regex for all aliases (word-boundary, case-insensitive)
_alias_patterns = "|".join(
    r"\b" + re.escape(a) + r"\b" for a in sorted(COUNTRY_ALIASES, key=len, reverse=True)
)
ALIAS_RE = re.compile(_alias_patterns, re.IGNORECASE)

# ---------------------------------------------------------------------------
# Power-change keywords (English — applied after translation)
# These are SPECIFIC enough to indicate actual leadership/power transitions.
# Deliberately avoid overly broad terms like "government" or "minister" alone.
# ---------------------------------------------------------------------------

POWER_KEYWORDS = [
    # Elections & voting
    r"\belection\b", r"\belections\b", r"\bvote\b", r"\bvoting\b",
    r"\bballot\b", r"\breferendum\b", r"\bplebiscite\b",
    r"\bsnap election\b", r"\bby-election\b", r"\bbye-election\b",
    r"\bprimary election\b", r"\brunoff\b", r"\brun-off\b",
    r"\bpoll results\b", r"\bvoter\b", r"\bvotes?\b",

    # Coups & forced takeovers
    r"\bcoup\b", r"\bputsch\b", r"\bjunta\b",
    r"\bmilitary takeover\b", r"\bmilitary seize[sd]\b",
    r"\btoppl(?:ed|ing)\b", r"\boverth(?:rew|row|rown|rowing)\b",
    r"\bseize[sd] power\b", r"\bseizing power\b",

    # Leadership change verbs — explicit transitions
    r"\bsworn in\b", r"\bswearing.in\b", r"\binaugurat(?:ed|ion)\b",
    r"\bappointed (?:as |to )?(?:president|prime minister|chancellor|premier|leader|minister|chief)\b",
    r"\bnominated (?:as |for )?(?:president|prime minister|chancellor|premier|leader)\b",
    r"\bresign(?:ed|ation|s)\b",
    r"\bimpeach(?:ed|ment)\b",
    r"\bdeposed?\b",
    r"\bousted\b",
    r"\bforced out\b",
    r"\bstep(?:ped|s|ping)? down\b",
    r"\bremoved from (?:power|office)\b",
    r"\btransition of power\b",
    r"\bpower transfer\b",
    r"\bsuccession\b",
    r"\bsuccessor\b",
    r"\bnew (?:president|prime minister|chancellor|premier|leader|government|regime)\b",
    r"\bdefected?\b",
    r"\bdefection\b",

    # Leadership titles in context of change
    r"\bhead of state\b",
    r"\bhead of government\b",
    r"\bsupreme leader\b",
    r"\bcommander.in.chief\b",

    # Uprisings & unrest
    r"\buprising\b", r"\brevolution\b",
    r"\banti.government (?:protest|demonstration|rally|movement)\b",
    r"\bstate of emergency\b",
    r"\bmartial law\b",
    r"\brebellion\b", r"\binsurrection\b", r"\bcivil war\b",
    r"\barmed (?:uprising|revolt|rebellion|takeover)\b",

    # Structural political change
    r"\bconstitution(?:al)? (?:amendment|revision|referendum|reform|change)\b",
    r"\bterm limit\b",
    r"\bdissolv(?:ed|ing) (?:parliament|government|assembly|congress)\b",
    r"\bparliament (?:vote|votes|voted|approves?|passes?|rejects?)\b",
    r"\blegislature (?:vote|passes?|approves?)\b",
    r"\bno.confidence (?:vote|motion)\b",

    # Coalition / party power shifts
    r"\bcoalition (?:government|deal|collapses?|formed?)\b",
    r"\bopposition (?:leader|win|wins|victory|takes? control)\b",
    r"\bincumbent (?:defeated?|loses?|ousted)\b",
    r"\blandslide (?:win|victory|defeat)\b",

    # Specific high-profile power-change terms
    r"\bcaptur(?:ed|ing) (?:president|prime minister|leader)\b",
    r"\barrest(?:ed)? (?:president|prime minister|leader)\b",
    r"\bexiled?\b",
    # Broader election / leadership patterns
    r"\breel(?:ect|ected|ects)\b",
    r"\belected (?:as |to )?(?:lead|president|prime minister|chancellor|leader)\b",
    r"\bprorogue\b",
    r"\bcaptur(?:ed|ing)\b",
    r"\bpresident(?:ial)? (?:election|vote|race|contest|candidate)\b",
    r"\bwon (?:election|vote|majority|seat|power)\b",
    r"\blose (?:election|seat|power)\b",
    r"\bpresident.{0,20}rival\b",
    r"\bnew .{0,15}regime\b",
    r"\bregime change\b",
    r"\bpolitical (?:transition|crisis|vacuum)\b",
]

POWER_RE = re.compile("|".join(POWER_KEYWORDS), re.IGNORECASE)

# ---------------------------------------------------------------------------
# NOISE EXCLUSION — headlines matching these are dropped even if they hit
# the power-change keywords. These catch false positives like:
#   - corporate exec resignations (Air Canada CEO, etc.)
#   - sports results ("votes" for MVP, etc.)
#   - cultural / entertainment news
#   - non-political government appointments (metro president, etc.)
# ---------------------------------------------------------------------------

NOISE_PATTERNS = [
    # Corporate / business leadership
    r"\bceo\b", r"\bchief executive\b", r"\bexecutive director\b",
    r"\bchief operating officer\b", r"\bcoo\b",
    r"\bchairman of the board\b",
    r"\bcorporat(?:e|ion)\b",
    r"\bstock(?:s| market| exchange)\b",
    r"\bearnings\b", r"\bquarterly\b",
    r"\bipo\b", r"\bmerger\b", r"\bacquisition\b",

    # Sports
    r"\bnba\b", r"\bnfl\b", r"\bnhl\b", r"\bnba\b", r"\bmlb\b",
    r"\bsoccer\b", r"\bfootball (?:club|team|player|match|game)\b",
    r"\bworld cup\b", r"\bolympic\b",
    r"\bmvp\b", r"\bplayoff\b",
    r"\bcoach\b", r"\bmanager\b.*\b(?:club|team|league)\b",

    # Entertainment / culture / religion (unless political)
    r"\bfilm\b", r"\bmovie\b", r"\balbum\b", r"\bsong\b",
    r"\bactress?\b", r"\bactor\b", r"\bcelebrit\b",
    r"\bnomination(?:s)?\b.*\b(?:award|oscar|grammy|emmy|bafta|amvca)\b",
    r"\baward\b.*\bnominat",
    r"\bpope\b.*\bnominated\b",  # papal appointments are religious, not political
    r"\bnuncio\b",  # apostolic nuncio = diplomatic/religious appointment
    r"\barchbishop\b.*\bnominated\b",
    r"\bbishop\b.*\bnominated\b",
    r"\barchbishop\b.*\bappointed\b",
    r"\bbishop\b.*\bappointed\b",

    # Infrastructure / public services
    r"\bmetro\b.*\bpresident\b",  # e.g. "president of the Caracas Metro"
    r"\bpresident\b.*\bmetro\b",
    r"\bairport\b", r"\bhighway\b", r"\bbridge\b",

    # Judicial / legal (not political power change)
    r"\bjudge\b.*\bappointed\b",
    r"\bjustice\b.*\bappointed\b",
    r"\bappointed\b.*\bjudge\b",
    r"\bcourt\b.*\bappointed\b",

    # Economic policy (not power change)
    r"\bcentral bank\b",
    r"\binterest rate\b",
    r"\binflation\b",
    r"\bgdp\b",

    # Military/police operations that aren't coups
    r"\bpolice (?:chief|commissioner)\b",
    r"\barmy (?:general|officer) (?:promoted|appointed)\b",
    r"\bmilitary (?:exercises|drills|maneuvers)\b",
]

NOISE_RE = re.compile("|".join(NOISE_PATTERNS), re.IGNORECASE)

# ---------------------------------------------------------------------------
# RSS feed list — 100+ sources covering all 44 countries
# ---------------------------------------------------------------------------

FEEDS = [
    # GLOBAL
    ("BBC World",               "https://feeds.bbci.co.uk/news/world/rss.xml",                    "en"),
    ("Al Jazeera English",      "https://www.aljazeera.com/xml/rss/all.xml",                      "en"),
    ("France 24 English",       "https://www.france24.com/en/rss",                                "en"),
    ("Deutsche Welle English",  "https://rss.dw.com/rdf/rss-en-world",                            "en"),
    ("NPR World",               "https://feeds.npr.org/1004/rss.xml",                             "en"),
    ("The Guardian World",      "https://www.theguardian.com/world/rss",                          "en"),
    ("Foreign Policy",          "https://foreignpolicy.com/feed/",                                "en"),
    ("Axios World",             "https://api.axios.com/feed/",                                    "en"),
    ("Reuters via FeedBurner",  "https://feeds.feedburner.com/reuters/worldNews",                 "en"),
    ("AP Top Headlines",        "https://feeds.apnews.com/apnews/topnews",                        "en"),
    ("VOA News",                "https://www.voanews.com/feeds/world-news",                       "en"),
    ("RFI English",             "https://en.rfi.fr/feed/",                                        "en"),
    ("Euronews",                "https://www.euronews.com/rss",                                   "en"),
    # MIDDLE EAST / NORTH AFRICA
    ("Egypt Independent",       "https://egyptindependent.com/feed/",                            "en"),
    ("Ahram Online",            "https://english.ahram.org.eg/NewsContent/2/0/rss.aspx",          "en"),
    ("Mada Masr",               "https://www.madamasr.com/en/feed/",                              "en"),
    ("Libya Herald",            "https://www.libyaherald.com/feed/",                              "en"),
    ("Libya Observer",          "https://www.libyaobserver.ly/rss.xml",                          "en"),
    ("Morocco World News",      "https://www.moroccoworldnews.com/feed",                          "en"),
    ("Le Desk Morocco",         "https://ledesk.ma/feed/",                                       "fr"),
    ("Algeria Press Service",   "https://www.aps.dz/en/feed",                                    "en"),
    ("El Watan Algeria",        "https://www.elwatan.com/feed/",                                  "fr"),
    ("Arab News",               "https://www.arabnews.com/rss.xml",                               "en"),
    ("Saudi Gazette",           "https://saudigazette.com.sa/rss",                                "en"),
    ("The National UAE",        "https://www.thenationalnews.com/rss.xml",                        "en"),
    ("Gulf News",               "https://gulfnews.com/rss",                                      "en"),
    ("Haaretz English",         "https://www.haaretz.com/cmlink/1.628765",                        "en"),
    ("Jerusalem Post",          "https://www.jpost.com/rss/rssfeedsfrontpage.aspx",               "en"),
    ("Middle East Eye",         "https://www.middleeasteye.net/rss",                              "en"),
    ("Palestinian Chronicle",   "https://www.palestinechronicle.com/feed/",                      "en"),
    ("Iran International",      "https://www.iranintl.com/en/rss.xml",                           "en"),
    ("Radio Farda",             "https://www.radiofarda.com/api/epiq-eityp/feed.rss",             "fa"),
    ("Syria Direct",            "https://syriadirect.org/feed/",                                 "en"),
    ("Yemen Monitor",           "https://yemenmonitor.com/feed/",                                "en"),
    ("Garowe Online Somalia",   "https://www.garoweonline.com/en/rss",                           "en"),
    ("Radio Dabanga Sudan",     "https://www.dabangasudan.org/en/all-news/feed",                  "en"),
    # SUB-SAHARAN AFRICA
    ("Vanguard Nigeria",        "https://www.vanguardngr.com/feed/",                             "en"),
    ("The Punch Nigeria",       "https://punchng.com/feed/",                                     "en"),
    ("Premium Times Nigeria",   "https://www.premiumtimesng.com/feed",                           "en"),
    # EUROPE
    ("Le Monde France",         "https://www.lemonde.fr/rss/une.xml",                            "fr"),
    ("Le Figaro France",        "https://www.lefigaro.fr/rss/figaro_actualites.xml",              "fr"),
    ("France 24 FR",            "https://www.france24.com/fr/rss",                               "fr"),
    ("Der Spiegel Intl",        "https://www.spiegel.de/international/index.rss",                "en"),
    ("DW Politics",             "https://rss.dw.com/xml/rss-en-pol",                             "en"),
    ("FAZ Germany",             "https://www.faz.net/rss/aktuell/politik/",                      "de"),
    ("Sky News World",          "https://feeds.skynews.com/feeds/rss/world.xml",                 "en"),
    ("The Independent UK",      "https://www.independent.co.uk/news/world/rss",                  "en"),
    ("The Telegraph UK",        "https://www.telegraph.co.uk/rss.xml",                           "en"),
    ("The Local Denmark",       "https://www.thelocal.dk/feed/",                                 "en"),
    ("DR News Denmark",         "https://www.dr.dk/nyheder/service/feeds/allenyheder",           "da"),
    ("Moscow Times",            "https://www.themoscowtimes.com/rss/news",                       "en"),
    ("Kyiv Independent",        "https://kyivindependent.com/feed/",                             "en"),
    ("Ukrinform",               "https://www.ukrinform.net/rss/block-lastnews",                  "en"),
    ("Meduza EN",               "https://meduza.io/rss/en/all",                                  "en"),
    ("Civilnet Armenia",        "https://www.civilnet.am/en/feed/",                              "en"),
    ("OC Media Armenia",        "https://oc-media.org/feed/",                                    "en"),
    ("Turan Azerbaijan",        "https://turan.az/ext/news/rss.xml",                             "az"),
    # ASIA
    ("South China Morning Post","https://www.scmp.com/rss/91/feed",                              "en"),
    ("Caixin Global",           "https://www.caixinglobal.com/rss/101096.xml",                   "en"),
    ("Xinhua World",            "https://feeds.feedburner.com/xinhuanet/news",                   "en"),
    ("Japan Times",             "https://www.japantimes.co.jp/feed/",                            "en"),
    ("NHK World",               "https://www3.nhk.or.jp/rss/news/cat0.xml",                      "en"),
    ("Mainichi English",        "https://mainichi.jp/rss/etc/english.rss",                       "en"),
    ("Korea Herald",            "https://www.koreaherald.com/rss/020000000000.xml",              "en"),
    ("Korea Times",             "https://www.koreatimes.co.kr/www/rss/nation.xml",               "en"),
    ("NK News",                 "https://www.nknews.org/feed/",                                  "en"),
    ("38 North",                "https://www.38north.org/feed/",                                 "en"),
    ("Taiwan News",             "https://www.taiwannews.com.tw/rss/index.rss",                   "en"),
    ("Focus Taiwan",            "https://focustaiwan.tw/rss-feed",                               "en"),
    ("The Hindu Politics",      "https://www.thehindu.com/news/national/feeder/default.rss",     "en"),
    ("Hindustan Times",         "https://www.hindustantimes.com/rss/topnews/rssfeed.xml",        "en"),
    ("Indian Express",          "https://indianexpress.com/feed/",                               "en"),
    ("Dawn Pakistan",           "https://www.dawn.com/feeds/home",                               "en"),
    ("The News International",  "https://www.thenews.com.pk/rss/1/8",                            "en"),
    ("Jakarta Post",            "https://www.thejakartapost.com/news/rss",                       "en"),
    ("Tempo.co Indonesia",      "https://en.tempo.co/rss/full",                                  "en"),
    ("Irrawaddy Myanmar",       "https://www.irrawaddy.com/feed",                                "en"),
    ("Myanmar Now",             "https://myanmar-now.org/en/feed/",                              "en"),
    ("VN Express International","https://e.vnexpress.net/rss/news/politics.rss",                 "en"),
    # AMERICAS
    ("Buenos Aires Herald",     "https://buenosairesherald.com/feed",                            "en"),
    ("La Nacion Argentina",     "https://www.lanacion.com.ar/arc/outboundfeeds/rss/",            "es"),
    ("Infobae Argentina",       "https://www.infobae.com/feeds/rss/politics.xml",                "es"),
    ("Folha de S Paulo",        "https://feeds.folha.uol.com.br/poder/rss091.xml",               "pt"),
    ("O Globo Brazil",          "https://oglobo.globo.com/rss.xml",                              "pt"),
    ("Agencia Brasil",          "https://agenciabrasil.ebc.com.br/rss/politica/feed.xml",        "pt"),
    ("El Universal Mexico",     "https://www.eluniversal.com.mx/rss.xml",                        "es"),
    ("Animal Politico Mexico",  "https://animalpolitico.com/feed/",                              "es"),
    ("Proceso Mexico",          "https://www.proceso.com.mx/rss/",                               "es"),
    ("El Colombiano",           "https://www.elcolombiano.com/rss.xml",                          "es"),
    ("El Tiempo Colombia",      "https://www.eltiempo.com/rss/politica.xml",                     "es"),
    ("La Tercera Chile",        "https://www.latercera.com/feed/",                               "es"),
    ("El Mostrador Chile",      "https://www.elmostrador.cl/feed/",                              "es"),
    ("El Nacional Venezuela",   "https://www.elnacional.com/feed/",                              "es"),
    ("Efecto Cocuyo Venezuela", "https://efectococuyo.com/feed/",                                "es"),
    ("Peru Reports",            "https://perureports.com/feed/",                                 "en"),
    ("El Comercio Peru",        "https://elcomercio.pe/rss/ultimas-noticias.xml",                "es"),
    ("14ymedio Cuba",           "https://www.14ymedio.com/feed",                                 "es"),
    ("CiberCuba",               "https://www.cibercuba.com/feed",                                "es"),
    ("CBC News World",          "https://www.cbc.ca/cmlink/rss-world",                           "en"),
    ("Globe and Mail Politics", "https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/politics/", "en"),
    ("La Estrella Panama",      "https://www.laestrella.com.pa/rss.xml",                         "es"),
    ("El Faro El Salvador",     "https://elfaro.net/rss.xml",                                    "es"),
]


# ---------------------------------------------------------------------------
# Robust feed fetching
# ---------------------------------------------------------------------------

def fetch_raw(url: str) -> bytes | None:
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.HTTPError as exc:
            log.warning("HTTP error %s (attempt %d/%d): %s", url, attempt, RETRY_ATTEMPTS, exc)
        except requests.exceptions.ConnectionError as exc:
            log.warning("Connection error %s (attempt %d/%d): %s", url, attempt, RETRY_ATTEMPTS, exc)
        except requests.exceptions.Timeout:
            log.warning("Timeout %s (attempt %d/%d)", url, attempt, RETRY_ATTEMPTS)
        except Exception as exc:
            log.warning("Error %s (attempt %d/%d): %s", url, attempt, RETRY_ATTEMPTS, exc)
        if attempt < RETRY_ATTEMPTS:
            time.sleep(RETRY_BACKOFF)
    return None


def parse_feed_bytes(raw: bytes):
    """Three-strategy XML parser to handle bozo/malformed feeds."""
    result = feedparser.parse(io.BytesIO(raw))
    if not result.bozo or result.entries:
        return result
    # Strip illegal XML 1.0 control characters
    try:
        text = raw.decode("utf-8", errors="replace")
        clean = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
        result2 = feedparser.parse(clean)
        if result2.entries:
            return result2
    except Exception:
        pass
    # latin-1 fallback
    try:
        text = raw.decode("latin-1", errors="replace")
        result3 = feedparser.parse(text)
        if result3.entries:
            return result3
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# Filtering helpers
# ---------------------------------------------------------------------------

def safe_translate(text: str, src_lang: str) -> str:
    if src_lang == "en" or not text:
        return text
    try:
        translated = GoogleTranslator(source=src_lang, target="en").translate(text)
        return translated or text
    except Exception as exc:
        log.warning("Translation failed (%s->en): %s", src_lang, exc)
        return text


def get_matched_country(title: str) -> str | None:
    """Return the first tracked country matched in the title, or None."""
    title_lower = title.lower()
    # Check full country names first
    for country in TRACKED_COUNTRIES:
        if country.lower() in title_lower:
            return country
    # Check aliases
    m = ALIAS_RE.search(title_lower)
    if m:
        return COUNTRY_ALIASES.get(m.group(0).lower())
    return None


def is_power_change(title: str) -> bool:
    return bool(POWER_RE.search(title))


def is_noise(title: str) -> bool:
    """Return True if the headline is a known false-positive category."""
    return bool(NOISE_RE.search(title))


def passes_filters(title: str) -> tuple[bool, str | None]:
    """
    Returns (passes, matched_country).
    A headline passes if:
      1. It mentions a tracked country
      2. It matches a power-change keyword
      3. It does NOT match the noise exclusion list
    """
    country = get_matched_country(title)
    if not country:
        return False, None
    if not is_power_change(title):
        return False, None
    if is_noise(title):
        log.debug("Noise filtered: %s", title)
        return False, None
    return True, country


def parse_published(entry) -> str:
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


def fetch_feed(source_name: str, url: str, lang: str) -> list:
    results = []

    raw = fetch_raw(url)
    if raw is None:
        log.warning("[SKIP] %s — could not retrieve feed after %d attempts", source_name, RETRY_ATTEMPTS)
        return results

    feed = parse_feed_bytes(raw)
    if not feed.entries:
        if feed.bozo:
            log.warning("[SKIP] %s — unparseable feed: %s", source_name, feed.bozo_exception)
        else:
            log.warning("[SKIP] %s — feed returned 0 entries", source_name)
        return results

    if feed.bozo:
        log.warning("[WARN] %s — bozo feed (processing %d entries anyway): %s",
                    source_name, len(feed.entries), feed.bozo_exception)

    for entry in feed.entries:
        raw_title = (getattr(entry, "title", "") or "").strip()
        if not raw_title:
            continue
        title = safe_translate(raw_title, lang) if lang != "en" else raw_title

        passes, country = passes_filters(title)
        if not passes:
            continue

        results.append({
            "title": title,
            "source": source_name,
            "country": country,
            "url": getattr(entry, "link", url),
            "published_date": parse_published(entry),
        })

    return results


# ---------------------------------------------------------------------------
# Archive management
# ---------------------------------------------------------------------------

def cutoff_date() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=ARCHIVE_DAYS)


def load_existing(path: Path) -> list:
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and "stories" in data:
                return data["stories"]
        except Exception as exc:
            log.warning("Could not load existing JSON: %s", exc)
    return []


def deduplicate(stories: list) -> list:
    seen = {}
    for s in stories:
        seen[s["url"]] = s
    return list(seen.values())


def prune_old(stories: list, cutoff: datetime) -> list:
    kept = []
    for s in stories:
        try:
            pub = dateutil_parser.parse(s["published_date"])
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            if pub >= cutoff:
                kept.append(s)
        except Exception:
            kept.append(s)
    return kept


def sort_stories(stories: list) -> list:
    def key(s):
        try:
            return dateutil_parser.parse(s["published_date"])
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)
    return sorted(stories, key=key, reverse=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=== Change-in-Power RSS Scraper starting ===")
    DOCS_DIR.mkdir(exist_ok=True)

    existing = load_existing(OUTPUT_FILE)
    log.info("Loaded %d existing stories from archive.", len(existing))

    cutoff = cutoff_date()
    existing = prune_old(existing, cutoff)
    log.info("After 14-day prune: %d stories remain.", len(existing))

    fresh = []
    for source_name, url, lang in FEEDS:
        log.info("Fetching: %s", source_name)
        stories = fetch_feed(source_name, url, lang)
        log.info("  -> %d matching stories", len(stories))
        fresh.extend(stories)
        time.sleep(0.3)

    log.info("Fetched %d fresh matching stories total.", len(fresh))

    merged = deduplicate(existing + fresh)
    merged = prune_old(merged, cutoff)
    merged = sort_stories(merged)

    output = {
        "_meta": {
            "description": (
                "Power-change news headlines from 100+ international RSS feeds. "
                "Covers elections, coups, uprisings, resignations, impeachments, "
                "and other leadership transitions across 44 tracked countries. "
                "Archive window: 14 days. Updated twice daily at 06:00 and 18:00 EST. "
                "Each story includes a 'country' field identifying which tracked country "
                "the headline pertains to, enabling rapid country-by-country triage."
            ),
            "tracked_countries": sorted(TRACKED_COUNTRIES),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "story_count": len(merged),
            "fields": {
                "title": "Headline in English",
                "source": "Name of the news outlet",
                "country": "Tracked country this story pertains to",
                "url": "Direct link to the article",
                "published_date": "ISO-8601 UTC publication timestamp",
            },
            "agent_guidance": (
                "To assess significance: group stories by 'country', then look for "
                "clusters of multiple independent sources covering the same event "
                "(e.g. coup, election result, leader arrest). A single story may be noise; "
                "3+ sources on the same event within 48 hours signals a major change. "
                "Key current contexts as of early 2026: Iran — new Supreme Leader "
                "Mojtaba Khamenei (appointed March 8, 2026) following assassination of "
                "Ali Khamenei; Venezuela — Maduro captured by US forces Jan 3 2026, "
                "interim president Delcy Rodriguez; Myanmar — junta chief Min Aung Hlaing "
                "transitioning to civilian presidency (March 2026); Canada — PM Mark Carney "
                "minority Liberal government (since April 2025 election)."
            ),
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
