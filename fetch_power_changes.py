"""
fetch_power_changes.py
----------------------
Scrapes 100+ international RSS feeds for headlines related to changes in
political power across 44 tracked countries. Non-English headlines are
translated to English. Outputs /docs/leadership-outputs.json with a 14-day
rolling archive.

All country recognition data (aliases, legislative bodies, parties, capitals,
abbreviations) is inlined below for single-file deployment.

FILTER PIPELINE:
  1. Country match  — checked against COUNTRY_ALIASES (687 entries) covering
                      demonyms, capitals, legislature names, party names,
                      leader names, and abbreviations
  2. Power keyword  — specific leadership-transition phrases including title
                      abbreviations (PM, Pres., FM, etc.)
  3. Noise veto     — drops known false-positive categories
  4. Significance   — bare structural terms require a second qualifying anchor
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
# COUNTRY ALIASES, LEGISLATIVE BODIES, PARTIES, CAPITALS, TITLE ABBREVIATIONS
# (formerly country_aliases.py — inlined for single-file deployment)
# ---------------------------------------------------------------------------

COUNTRY_ALIASES = {

    # =========================================================
    # ALGERIA
    # =========================================================
    "algerian": "Algeria",
    "algerians": "Algeria",
    "algiers": "Algeria",
    # Legislature: People's National Assembly (APN) + Council of the Nation
    "apn": "Algeria",                    # Assemblée Populaire Nationale
    "council of the nation": "Algeria",
    "people's national assembly": "Algeria",
    # Parties
    "fln": "Algeria",                    # Front de Libération Nationale (ruling)
    "rnd": "Algeria",                    # Rassemblement National Démocratique
    "msp": "Algeria",                    # Mouvement de la Société pour la Paix
    # Head of state
    "tebboune": "Algeria",               # President Abdelmadjid Tebboune

    # =========================================================
    # ARGENTINA
    # =========================================================
    "argentinian": "Argentina",
    "argentinean": "Argentina",
    "argentine": "Argentina",
    "argentines": "Argentina",
    "buenos aires": "Argentina",
    "bsas": "Argentina",                 # common abbreviation
    "casa rosada": "Argentina",          # seat of government
    # Legislature: Congreso / Congress — Chamber of Deputies + Senate
    "congreso nacional": "Argentina",
    "camara de diputados": "Argentina",  # lower house
    "diputados": "Argentina",
    "senado argentino": "Argentina",
    # Parties
    "peronist": "Argentina",
    "peronismo": "Argentina",
    "kirchnerist": "Argentina",
    "kirchnerism": "Argentina",
    "la libertad avanza": "Argentina",   # Milei's party
    "union por la patria": "Argentina",
    "pro party": "Argentina",            # Propuesta Republicana
    "milei": "Argentina",                # President Javier Milei

    # =========================================================
    # ARMENIA
    # =========================================================
    "armenian": "Armenia",
    "armenians": "Armenia",
    "yerevan": "Armenia",
    "erevan": "Armenia",
    # Legislature: National Assembly / Azgayin Zhoghov
    "azgayin zhoghov": "Armenia",
    "national assembly of armenia": "Armenia",
    # Parties
    "civil contract": "Armenia",         # ruling party
    "pashinyan": "Armenia",              # PM Nikol Pashinyan
    "hhk": "Armenia",                    # Republican Party of Armenia

    # =========================================================
    # AZERBAIJAN
    # =========================================================
    "azerbaijani": "Azerbaijan",
    "azeri": "Azerbaijan",
    "azeris": "Azerbaijan",
    "baku": "Azerbaijan",
    # Legislature: Milli Majlis (National Assembly)
    "milli majlis": "Azerbaijan",
    "milli məclis": "Azerbaijan",
    # Parties
    "yap": "Azerbaijan",                 # New Azerbaijan Party (ruling)
    "new azerbaijan party": "Azerbaijan",
    "aliyev": "Azerbaijan",              # President Ilham Aliyev

    # =========================================================
    # BRAZIL
    # =========================================================
    "brazilian": "Brazil",
    "brazilians": "Brazil",
    "brasilia": "Brazil",
    "brasília": "Brazil",
    "planalto": "Brazil",                # Presidential Palace / seat of power
    # Legislature: Congresso Nacional — Câmara dos Deputados + Senado Federal
    "congresso nacional": "Brazil",
    "camara dos deputados": "Brazil",
    "câmara dos deputados": "Brazil",
    "senado federal": "Brazil",
    "camara federal": "Brazil",
    # Parties
    "pt": "Brazil",                      # Partido dos Trabalhadores (Workers' Party)
    "partido dos trabalhadores": "Brazil",
    "workers party brazil": "Brazil",
    "pl": "Brazil",                      # Partido Liberal (Bolsonaro's party)
    "partido liberal brasil": "Brazil",
    "psd brasil": "Brazil",
    "mdb": "Brazil",                     # Movimento Democrático Brasileiro
    "lula": "Brazil",                    # President Luiz Inácio Lula da Silva
    "bolsonaro": "Brazil",

    # =========================================================
    # CANADA
    # =========================================================
    "canadian": "Canada",
    "canadians": "Canada",
    "ottawa": "Canada",
    "rideau": "Canada",                  # Rideau Hall / Governor General residence
    # Legislature: Parliament — House of Commons + Senate
    "house of commons canada": "Canada",
    "commons canada": "Canada",
    "senate canada": "Canada",
    # Executive abbreviations
    "pm canada": "Canada",
    # Parties
    "liberal party": "Canada",
    "liberals canada": "Canada",
    "conservative party canada": "Canada",
    "conservatives canada": "Canada",
    "cpc": "Canada",                     # Conservative Party of Canada
    "ndp": "Canada",                     # New Democratic Party
    "new democratic party": "Canada",
    "bloc québécois": "Canada",
    "bloc quebecois": "Canada",
    "green party canada": "Canada",
    "carney": "Canada",                  # PM Mark Carney
    "poilievre": "Canada",               # Conservative leader

    # =========================================================
    # CHILE
    # =========================================================
    "chilean": "Chile",
    "chileans": "Chile",
    "santiago": "Chile",
    "valparaiso": "Chile",               # seat of congress
    "la moneda": "Chile",                # presidential palace
    # Legislature: Congreso Nacional — Cámara de Diputados + Senado
    "camara de diputados chile": "Chile",
    "senado chile": "Chile",
    # Parties
    "republican party chile": "Chile",
    "partido republicano chile": "Chile",
    "apruebo dignidad": "Chile",
    "chile vamos": "Chile",
    "kast": "Chile",                     # President José Antonio Kast
    "boric": "Chile",                    # former President Gabriel Boric

    # =========================================================
    # CHINA
    # =========================================================
    "chinese": "China",
    "beijing": "China",
    "peking": "China",                   # historical/alternate name
    "zhongnanhai": "China",              # leadership compound
    "great hall of the people": "China",
    # Legislature: National People's Congress (NPC)
    "npc": "China",                      # National People's Congress
    "national people's congress": "China",
    "cppcc": "China",                    # advisory body often cited
    "standing committee npc": "China",
    # Parties
    "ccp": "China",                      # Chinese Communist Party
    "chinese communist party": "China",
    "communist party of china": "China",
    "pla": "China",                      # People's Liberation Army (political actor)
    "politburo": "China",
    "xi jinping": "China",
    "li qiang": "China",                 # Premier

    # =========================================================
    # COLOMBIA
    # =========================================================
    "colombian": "Colombia",
    "colombians": "Colombia",
    "bogota": "Colombia",
    "bogotá": "Colombia",
    "casa de nariño": "Colombia",        # presidential palace
    # Legislature: Congreso — Cámara de Representantes + Senado
    "camara de representantes colombia": "Colombia",
    "senado colombia": "Colombia",
    # Parties
    "pacto historico": "Colombia",
    "historic pact": "Colombia",
    "partido conservador colombia": "Colombia",
    "partido liberal colombia": "Colombia",
    "centro democratico": "Colombia",
    "petro": "Colombia",                 # President Gustavo Petro

    # =========================================================
    # CUBA
    # =========================================================
    "cuban": "Cuba",
    "cubans": "Cuba",
    "havana": "Cuba",
    "la habana": "Cuba",
    # Legislature: National Assembly of People's Power (ANPP)
    "anpp": "Cuba",                      # Asamblea Nacional del Poder Popular
    "national assembly cuba": "Cuba",
    "asamblea nacional cuba": "Cuba",
    # Parties — single party
    "pcc": "Cuba",                       # Partido Comunista de Cuba
    "partido comunista de cuba": "Cuba",
    "diaz-canel": "Cuba",
    "díaz-canel": "Cuba",               # President / First Secretary

    # =========================================================
    # DENMARK
    # =========================================================
    "danish": "Denmark",
    "danes": "Denmark",
    "copenhagen": "Denmark",
    "kobenhavn": "Denmark",
    "københavn": "Denmark",
    "christiansborg": "Denmark",         # parliament building
    # Legislature: Folketing
    "folketing": "Denmark",
    "the folketing": "Denmark",
    # Parties
    "socialdemokratiet": "Denmark",      # Social Democrats
    "social democrats denmark": "Denmark",
    "venstre denmark": "Denmark",        # Liberal party
    "dansk folkeparti": "Denmark",       # Danish People's Party
    "dfp": "Denmark",
    "moderates denmark": "Denmark",
    "sf denmark": "Denmark",             # Socialist People's Party
    "red-green alliance denmark": "Denmark",
    "enhedslisten": "Denmark",
    "frederiksen": "Denmark",            # PM Mette Frederiksen
    "løkke": "Denmark",
    "lokke": "Denmark",                  # Lars Løkke Rasmussen
    "formateur": "Denmark",              # specific to Danish coalition process
    "kingmaker denmark": "Denmark",

    # =========================================================
    # EGYPT
    # =========================================================
    "egyptian": "Egypt",
    "egyptians": "Egypt",
    "cairo": "Egypt",
    "el-sisi": "Egypt",
    "sisi": "Egypt",                     # President Abdel Fattah el-Sisi
    # Legislature: bicameral — Senate + House of Representatives
    "house of representatives egypt": "Egypt",
    "senate egypt": "Egypt",
    "majlis al-nuwwab": "Egypt",         # House of Representatives (Arabic)
    # Parties
    "nour party": "Egypt",
    "wafd": "Egypt",

    # =========================================================
    # EL SALVADOR
    # =========================================================
    "salvadoran": "El Salvador",
    "salvadorean": "El Salvador",
    "salvadoreño": "El Salvador",
    "san salvador": "El Salvador",
    # Legislature: Asamblea Legislativa
    "asamblea legislativa": "El Salvador",
    # Parties
    "nuevas ideas": "El Salvador",       # Bukele's party
    "bukele": "El Salvador",             # President Nayib Bukele

    # =========================================================
    # FRANCE
    # =========================================================
    "french": "France",
    "france's": "France",
    "paris": "France",
    "elysee": "France",
    "élysée": "France",                  # presidential palace
    "matignon": "France",                # PM's residence
    # Legislature: Parliament — Assemblée Nationale + Sénat
    "assemblée nationale": "France",
    "assemblee nationale": "France",
    "national assembly france": "France",
    "sénat france": "France",
    "senat france": "France",
    # Parties
    "renaissance france": "France",      # Macron's party (formerly LREM)
    "lrem": "France",
    "en marche": "France",
    "rassemblement national": "France",
    "rn": "France",                      # National Rally (Le Pen)
    "les republicains": "France",
    "lr france": "France",
    "la france insoumise": "France",
    "lfi": "France",
    "parti socialiste france": "France",
    "ps france": "France",
    "macron": "France",
    "le pen": "France",                  # Marine Le Pen
    "melenchon": "France",
    "mélenchon": "France",
    "barnier": "France",
    "bayrou": "France",                  # PM François Bayrou

    # =========================================================
    # GERMANY
    # =========================================================
    "german": "Germany",
    "germany's": "Germany",
    "berlin": "Germany",
    "bundestag": "Germany",              # lower house
    "bundesrat": "Germany",             # upper house / states chamber
    "bundeskanzler": "Germany",         # Federal Chancellor
    "reichstag": "Germany",             # building name (often used for Bundestag)
    "chancellery": "Germany",           # seat of chancellor
    # Parties
    "cdu": "Germany",                    # Christian Democratic Union
    "csu": "Germany",                    # Christian Social Union (Bavaria)
    "cdu/csu": "Germany",
    "union germany": "Germany",
    "spd": "Germany",                    # Social Democratic Party
    "sozialdemokraten": "Germany",
    "fdp germany": "Germany",            # Free Democratic Party
    "afd": "Germany",                    # Alternative für Deutschland
    "alternative for germany": "Germany",
    "grunen": "Germany",                 # The Greens
    "die grünen": "Germany",
    "bsw": "Germany",                    # Bündnis Sahra Wagenknecht
    "linke": "Germany",                  # Die Linke
    "merz": "Germany",                   # Chancellor Friedrich Merz
    "scholz": "Germany",                 # former Chancellor Olaf Scholz

    # =========================================================
    # INDIA
    # =========================================================
    "indian": "India",
    "indians": "India",
    "new delhi": "India",
    "rashtrapati bhavan": "India",       # Presidential palace
    "south block": "India",              # PM's office area
    # Legislature: Parliament / Sansad — Lok Sabha + Rajya Sabha
    "lok sabha": "India",
    "rajya sabha": "India",
    "sansad": "India",
    "parliament india": "India",
    # Parties
    "bjp": "India",                      # Bharatiya Janata Party
    "bharatiya janata party": "India",
    "indian national congress": "India",
    "inc india": "India",
    "congress party india": "India",
    "aap": "India",                      # Aam Aadmi Party
    "aam aadmi party": "India",
    "samajwadi party": "India",
    "trinamool congress": "India",
    "tmc india": "India",
    "india alliance": "India",           # INDIA opposition alliance
    "nda india": "India",                # National Democratic Alliance (ruling)
    "modi": "India",
    "rahul gandhi": "India",
    "yogi": "India",                     # CM Yogi Adityanath (often in national news)

    # =========================================================
    # INDONESIA
    # =========================================================
    "indonesian": "Indonesia",
    "indonesians": "Indonesia",
    "jakarta": "Indonesia",
    "nusantara": "Indonesia",            # new capital under construction
    "istana merdeka": "Indonesia",       # State Palace
    # Legislature: Dewan Perwakilan Rakyat (DPR) + DPD (senate equivalent)
    "dpr": "Indonesia",                  # House of Representatives
    "dewan perwakilan rakyat": "Indonesia",
    "dpd": "Indonesia",                  # Regional Representatives Council
    "mpr": "Indonesia",                  # People's Consultative Assembly
    # Parties
    "golkar": "Indonesia",
    "pdip": "Indonesia",                 # PDI-P / Indonesian Democratic Party-Struggle
    "pdi-p": "Indonesia",
    "gerindra": "Indonesia",             # Great Indonesia Movement Party
    "pkb": "Indonesia",                  # National Awakening Party
    "prabowo": "Indonesia",              # President Prabowo Subianto
    "jokowi": "Indonesia",               # former president (often cited in context)

    # =========================================================
    # IRAN
    # =========================================================
    "iranian": "Iran",
    "iranians": "Iran",
    "tehran": "Iran",
    "tehran's": "Iran",
    "khamenei": "Iran",                  # Supreme Leader family name
    "mojtaba khamenei": "Iran",
    "ali khamenei": "Iran",
    "irgc": "Iran",                      # Islamic Revolutionary Guard Corps
    "revolutionary guard": "Iran",
    "basij": "Iran",                     # paramilitary
    "ayatollah": "Iran",
    "pezeshkian": "Iran",               # President Masoud Pezeshkian
    "majlis": "Iran",                    # Parliament / Islamic Consultative Assembly
    "islamic consultative assembly": "Iran",
    "assembly of experts": "Iran",       # selects supreme leader
    "guardian council": "Iran",          # vets candidates
    "snsc": "Iran",                      # Supreme National Security Council
    "rouhani": "Iran",
    "raisi": "Iran",                     # former president
    "jalili": "Iran",
    "larijani": "Iran",

    # =========================================================
    # ISRAEL
    # =========================================================
    "israeli": "Israel",
    "israelis": "Israel",
    "tel aviv": "Israel",
    "jerusalem": "Israel",               # internationally contested but used in headlines
    "knesset": "Israel",                 # Parliament
    "the knesset": "Israel",
    "netanyahu": "Israel",               # PM Benjamin Netanyahu
    "bibi": "Israel",                    # Netanyahu nickname
    "gantz": "Israel",
    "ben gvir": "Israel",
    "smotrich": "Israel",
    "lapid": "Israel",
    # Parties
    "likud": "Israel",                   # ruling party
    "labor israel": "Israel",
    "meretz": "Israel",
    "yesh atid": "Israel",
    "national unity israel": "Israel",
    "otzma yehudit": "Israel",
    "religious zionism": "Israel",

    # =========================================================
    # JAPAN
    # =========================================================
    "japanese": "Japan",
    "tokyo": "Japan",
    "nagatacho": "Japan",                # political district / seat of power
    # Legislature: National Diet / Kokkai — House of Representatives + House of Councillors
    "diet japan": "Japan",
    "national diet": "Japan",
    "kokkai": "Japan",
    "house of representatives japan": "Japan",
    "house of councillors": "Japan",
    # Parties
    "ldp": "Japan",                      # Liberal Democratic Party
    "liberal democratic party japan": "Japan",
    "komeito": "Japan",                  # coalition partner
    "cdp japan": "Japan",                # Constitutional Democratic Party
    "constitutional democratic party japan": "Japan",
    "nippon ishin": "Japan",
    "ishiba": "Japan",                   # PM Shigeru Ishiba

    # =========================================================
    # LIBYA
    # =========================================================
    "libyan": "Libya",
    "libyans": "Libya",
    "tripoli": "Libya",
    "benghazi": "Libya",                 # seat of rival government
    "tobruk": "Libya",                   # HoR location
    # Legislature: two rival — House of Representatives (HoR) + High State Council (HSC)
    "house of representatives libya": "Libya",
    "hor libya": "Libya",
    "high state council libya": "Libya",
    "hsc libya": "Libya",
    "gnu": "Libya",                      # Government of National Unity
    "gnc": "Libya",                      # former General National Congress
    # Factions
    "haftar": "Libya",                   # Field Marshal Khalifa Haftar
    "lna": "Libya",                      # Libyan National Army
    "dbeibah": "Libya",                  # PM Abdulhamid Dbeibah

    # =========================================================
    # MEXICO
    # =========================================================
    "mexican": "Mexico",
    "mexicans": "Mexico",
    "mexico city": "Mexico",
    "cdmx": "Mexico",                    # Ciudad de México abbreviation
    "ciudad de mexico": "Mexico",
    "ciudad de méxico": "Mexico",
    "chapultepec": "Mexico",             # presidential residence
    "los pinos": "Mexico",               # former presidential palace, still cited
    # Legislature: Congreso de la Unión — Cámara de Diputados + Senado
    "camara de diputados mexico": "Mexico",
    "senado mexico": "Mexico",
    "congreso mexico": "Mexico",
    # Parties
    "morena": "Mexico",                  # Movimiento Regeneración Nacional (ruling)
    "pan mexico": "Mexico",              # Partido Acción Nacional
    "pri mexico": "Mexico",              # Partido Revolucionario Institucional
    "prd mexico": "Mexico",
    "frente amplio mexico": "Mexico",
    "claudia sheinbaum": "Mexico",       # President Claudia Sheinbaum
    "sheinbaum": "Mexico",
    "obrador": "Mexico",                 # former president AMLO (still cited)
    "amlo": "Mexico",

    # =========================================================
    # MOROCCO
    # =========================================================
    "moroccan": "Morocco",
    "moroccans": "Morocco",
    "rabat": "Morocco",
    # Legislature: Parliament — House of Representatives + House of Councillors
    "parliament morocco": "Morocco",
    "house of representatives morocco": "Morocco",
    # Parties
    "pjd": "Morocco",                    # Justice and Development Party (formerly ruling)
    "rnimar": "Morocco",                 # National Rally of Independents
    "istiqlal": "Morocco",
    "mohammed vi": "Morocco",            # King

    # =========================================================
    # MYANMAR
    # =========================================================
    "burmese": "Myanmar",
    "burma": "Myanmar",
    "myanmar's": "Myanmar",
    "naypyidaw": "Myanmar",
    "naypyitaw": "Myanmar",
    "rangoon": "Myanmar",                # former capital / still used
    "yangon": "Myanmar",
    "tatmadaw": "Myanmar",               # Myanmar military
    "min aung hlaing": "Myanmar",
    "ye win oo": "Myanmar",              # new military commander
    # Legislature: Pyidaungsu Hluttaw (Union Assembly)
    "pyidaungsu hluttaw": "Myanmar",
    "pyithu hluttaw": "Myanmar",         # lower house
    "amyotha hluttaw": "Myanmar",        # upper house
    "hluttaw": "Myanmar",
    # Parties / political actors
    "usdp": "Myanmar",                   # Union Solidarity and Development Party (junta-backed)
    "nld": "Myanmar",                    # National League for Democracy (Suu Kyi's party)
    "national unity government": "Myanmar",  # shadow government
    "nug": "Myanmar",
    "pdf myanmar": "Myanmar",            # People's Defence Force
    "sac": "Myanmar",                    # State Administration Council (junta)
    "aung san suu kyi": "Myanmar",

    # =========================================================
    # NIGERIA
    # =========================================================
    "nigerian": "Nigeria",
    "nigerians": "Nigeria",
    "abuja": "Nigeria",
    "aso rock": "Nigeria",               # presidential villa
    # Legislature: National Assembly — House of Representatives + Senate
    "national assembly nigeria": "Nigeria",
    "house of representatives nigeria": "Nigeria",
    "senate nigeria": "Nigeria",
    # Parties
    "apc nigeria": "Nigeria",            # All Progressives Congress (ruling)
    "all progressives congress": "Nigeria",
    "pdp nigeria": "Nigeria",            # Peoples Democratic Party
    "peoples democratic party nigeria": "Nigeria",
    "labour party nigeria": "Nigeria",
    "tinubu": "Nigeria",                 # President Bola Tinubu

    # =========================================================
    # NORTH KOREA
    # =========================================================
    "north korean": "North Korea",
    "north koreans": "North Korea",
    "pyongyang": "North Korea",
    "dprk": "North Korea",
    "kim jong un": "North Korea",
    "kim jong-un": "North Korea",
    "kim jong il": "North Korea",        # sometimes cited in historical context
    "kim dynasty": "North Korea",
    "wonsan": "North Korea",
    # Legislature: Supreme People's Assembly (SPA)
    "supreme people's assembly": "North Korea",
    "spa north korea": "North Korea",
    "korean workers party": "North Korea",  # ruling party
    "kwp": "North Korea",
    "korean people's army": "North Korea",
    "kpa": "North Korea",

    # =========================================================
    # PAKISTAN
    # =========================================================
    "pakistani": "Pakistan",
    "pakistanis": "Pakistan",
    "islamabad": "Pakistan",
    "rawalpindi": "Pakistan",            # GHQ location — military power
    "lahore": "Pakistan",                # political centre
    # Legislature: Parliament — National Assembly + Senate
    "national assembly pakistan": "Pakistan",
    "senate pakistan": "Pakistan",
    "parliament pakistan": "Pakistan",
    # Parties
    "pti": "Pakistan",                   # Pakistan Tehreek-e-Insaf (Imran Khan)
    "pakistan tehreek-e-insaf": "Pakistan",
    "pmln": "Pakistan",                  # Pakistan Muslim League-Nawaz (ruling)
    "pml-n": "Pakistan",
    "ppp pakistan": "Pakistan",          # Pakistan Peoples Party
    "jui-f": "Pakistan",
    "imran khan": "Pakistan",
    "shehbaz sharif": "Pakistan",        # PM
    "nawaz sharif": "Pakistan",
    "asim munir": "Pakistan",            # Army Chief (major political actor)

    # =========================================================
    # PALESTINE
    # =========================================================
    "palestinian": "Palestine",
    "palestinians": "Palestine",
    "gaza": "Palestine",
    "west bank": "Palestine",
    "ramallah": "Palestine",             # PA seat
    "nablus": "Palestine",
    "jenin": "Palestine",
    # Legislative / political bodies
    "palestinian authority": "Palestine",
    "pa ": "Palestine",                  # Palestinian Authority (with space to avoid "pa" in "Japan")
    "palestinian legislative council": "Palestine",
    "plc": "Palestine",
    # Factions
    "hamas": "Palestine",
    "fatah": "Palestine",
    "plo": "Palestine",
    "islamic jihad palestine": "Palestine",
    "abu mazen": "Palestine",            # Mahmoud Abbas
    "mahmoud abbas": "Palestine",
    "sinwar": "Palestine",               # Hamas leader
    "yahya sinwar": "Palestine",

    # =========================================================
    # PANAMA
    # =========================================================
    "panamanian": "Panama",
    "panamanians": "Panama",
    "panama city": "Panama",
    # Legislature: Asamblea Nacional
    "asamblea nacional panama": "Panama",
    # Parties
    "molirena": "Panama",
    "partido revolucionario democratico": "Panama",
    "prd panama": "Panama",
    "mulino": "Panama",                  # President José Raúl Mulino

    # =========================================================
    # PERU
    # =========================================================
    "peruvian": "Peru",
    "peruvians": "Peru",
    "lima": "Peru",
    "palacio de gobierno peru": "Peru",
    # Legislature: unicameral Congress (Congreso de la República)
    "congreso peru": "Peru",
    "congreso de la republica peru": "Peru",
    # Parties
    "fuerza popular": "Peru",            # Keiko Fujimori's party
    "accion popular": "Peru",
    "gana peru": "Peru",
    "boluarte": "Peru",                  # President Dina Boluarte

    # =========================================================
    # RUSSIA
    # =========================================================
    "russian": "Russia",
    "russians": "Russia",
    "moscow": "Russia",
    "moscow's": "Russia",
    "kremlin": "Russia",
    "the kremlin": "Russia",
    "st. petersburg": "Russia",          # second city, political significance
    # Legislature: Federal Assembly — State Duma + Federation Council
    "state duma": "Russia",
    "duma": "Russia",
    "federation council russia": "Russia",
    "federal assembly russia": "Russia",
    # Parties
    "united russia": "Russia",           # ruling party
    "edinaya rossiya": "Russia",
    "ldpr": "Russia",                    # Liberal Democratic Party of Russia
    "kprf": "Russia",                    # Communist Party of Russian Federation
    "just russia": "Russia",
    "a just russia": "Russia",
    "new people party russia": "Russia",
    "putin": "Russia",
    "medvedev": "Russia",
    "mishustin": "Russia",               # PM Mikhail Mishustin
    "patrushev": "Russia",

    # =========================================================
    # SAUDI ARABIA
    # =========================================================
    "saudi": "Saudi Arabia",
    "saudis": "Saudi Arabia",
    "saudi arabian": "Saudi Arabia",
    "riyadh": "Saudi Arabia",
    "mecca": "Saudi Arabia",             # sometimes cited in political context
    "al saud": "Saudi Arabia",           # royal family
    # Legislature: Majlis al-Shura / Shura Council (advisory, not elected)
    "shura council": "Saudi Arabia",
    "majlis al-shura": "Saudi Arabia",
    # Key figures
    "mbs": "Saudi Arabia",               # Crown Prince Mohammed bin Salman
    "mohammed bin salman": "Saudi Arabia",
    "bin salman": "Saudi Arabia",
    "salman": "Saudi Arabia",            # King Salman

    # =========================================================
    # SOMALIA
    # =========================================================
    "somali": "Somalia",
    "somalis": "Somalia",
    "mogadishu": "Somalia",
    # Legislature: Federal Parliament — House of the People + Upper House
    "federal parliament somalia": "Somalia",
    "house of the people somalia": "Somalia",
    "upper house somalia": "Somalia",
    # Parties / factions
    "al-shabaab": "Somalia",             # major non-state actor
    "al shabaab": "Somalia",
    "nusantara somalia": "Somalia",
    "sfg": "Somalia",                    # Somali Federal Government
    "hassan sheikh": "Somalia",          # President Hassan Sheikh Mohamud

    # =========================================================
    # SOUTH KOREA
    # =========================================================
    "south korean": "South Korea",
    "south koreans": "South Korea",
    "seoul": "South Korea",
    "cheong wa dae": "South Korea",      # Blue House / presidential palace
    "yongsan": "South Korea",            # presidential office district
    # Legislature: National Assembly (Gukhoe)
    "national assembly korea": "South Korea",
    "gukhoe": "South Korea",
    # Parties
    "ppp south korea": "South Korea",    # People Power Party (ruling)
    "people power party": "South Korea",
    "dpk": "South Korea",                # Democratic Party of Korea
    "democratic party korea": "South Korea",
    "yoon": "South Korea",               # President Yoon Suk-yeol
    "lee jae-myung": "South Korea",
    "han duck-soo": "South Korea",       # PM

    # =========================================================
    # SUDAN
    # =========================================================
    "sudanese": "Sudan",
    "khartoum": "Sudan",
    "omdurman": "Sudan",                 # major city, alternate seat
    "port sudan": "Sudan",               # current de facto capital
    # Legislature: currently suspended / Transitional Sovereignty Council
    "sovereignty council sudan": "Sudan",
    "transitional sovereignty council": "Sudan",
    # Factions
    "saf": "Sudan",                      # Sudanese Armed Forces
    "rsf": "Sudan",                      # Rapid Support Forces
    "rapid support forces": "Sudan",
    "dagalo": "Sudan",                   # RSF commander Hemeti
    "hemeti": "Sudan",
    "al-burhan": "Sudan",                # SAF chief / de facto head of state
    "burhan": "Sudan",

    # =========================================================
    # SYRIA
    # =========================================================
    "syrian": "Syria",
    "syrians": "Syria",
    "damascus": "Syria",
    "aleppo": "Syria",
    # Legislature: People's Assembly (suspended post-2024)
    "people's assembly syria": "Syria",
    "majlis al-shaab": "Syria",
    # Parties / factions
    "hts": "Syria",                      # Hayat Tahrir al-Sham (now governing)
    "hayat tahrir al-sham": "Syria",
    "jolani": "Syria",                   # Ahmad al-Sharaa / Abu Mohammad al-Jolani
    "al-sharaa": "Syria",
    "sdf": "Syria",                      # Syrian Democratic Forces
    "baath party syria": "Syria",
    "fsa": "Syria",                      # Free Syrian Army
    "sna": "Syria",                      # Syrian National Army

    # =========================================================
    # TAIWAN
    # =========================================================
    "taiwanese": "Taiwan",
    "taipei": "Taiwan",
    "taiwan's": "Taiwan",
    "republic of china": "Taiwan",       # official name
    "roc": "Taiwan",                     # Republic of China abbreviation
    "formosa": "Taiwan",                 # historical / alternate name
    # Legislature: Legislative Yuan
    "legislative yuan": "Taiwan",
    "yuan taiwan": "Taiwan",
    # Parties
    "dpp": "Taiwan",                     # Democratic Progressive Party (ruling)
    "democratic progressive party taiwan": "Taiwan",
    "kmt": "Taiwan",                     # Kuomintang / Nationalist Party
    "kuomintang": "Taiwan",
    "tpp taiwan": "Taiwan",              # Taiwan People's Party
    "lai ching-te": "Taiwan",            # President
    "lai": "Taiwan",
    "william lai": "Taiwan",
    "han kuo-yu": "Taiwan",

    # =========================================================
    # TURKEY
    # =========================================================
    "turkish": "Turkey",
    "turks": "Turkey",
    "ankara": "Turkey",
    "istanbul": "Turkey",
    "cumhurbaskanligi": "Turkey",        # presidential palace complex
    # Legislature: Grand National Assembly (Büyük Millet Meclisi)
    "grand national assembly": "Turkey",
    "buyuk millet meclisi": "Turkey",
    "büyük millet meclisi": "Turkey",
    "tbmm": "Turkey",                    # Türkiye Büyük Millet Meclisi
    # Parties
    "akp": "Turkey",                     # Justice and Development Party (ruling)
    "adalet ve kalkinma partisi": "Turkey",
    "chp": "Turkey",                     # Republican People's Party (main opposition)
    "mhp": "Turkey",                     # Nationalist Movement Party
    "hdp": "Turkey",                     # Peoples' Democratic Party (Kurdish)
    "deva turkey": "Turkey",
    "iyi parti": "Turkey",
    "erdogan": "Turkey",
    "erdoğan": "Turkey",
    "imamoglu": "Turkey",                # Istanbul mayor / opposition figure
    "imamoğlu": "Turkey",
    "kilicdaroglu": "Turkey",

    # =========================================================
    # UAE
    # =========================================================
    "emirati": "UAE",
    "emiratis": "UAE",
    "dubai": "UAE",
    "abu dhabi": "UAE",
    "sharjah": "UAE",
    # Legislature: Federal National Council (FNC) — advisory
    "federal national council": "UAE",
    "fnc uae": "UAE",
    # Key figures
    "mbn": "UAE",                        # President Mohamed bin Zayed
    "mohamed bin zayed": "UAE",
    "bin zayed": "UAE",
    "mbs dubai": "UAE",                  # Mohammed bin Salman Dubai visit context

    # =========================================================
    # UKRAINE
    # =========================================================
    "ukrainian": "Ukraine",
    "ukrainians": "Ukraine",
    "kyiv": "Ukraine",
    "kiev": "Ukraine",
    "ukraine's": "Ukraine",
    "mariyinsky palace": "Ukraine",      # presidential palace
    # Legislature: Verkhovna Rada
    "verkhovna rada": "Ukraine",
    "rada": "Ukraine",
    # Parties
    "servant of the people": "Ukraine",  # Zelensky's party
    "sluga narodu": "Ukraine",
    "european solidarity ukraine": "Ukraine",
    "opposition platform ukraine": "Ukraine",
    "zelensky": "Ukraine",
    "zelenskyy": "Ukraine",
    "poroshenko": "Ukraine",
    "zaluzhny": "Ukraine",               # former military chief / political figure
    "syrsky": "Ukraine",                 # current military chief

    # =========================================================
    # UNITED KINGDOM
    # =========================================================
    "british": "United Kingdom",
    "britain": "United Kingdom",
    "britain's": "United Kingdom",
    "uk": "United Kingdom",              # careful — broad but widely used in headlines
    "london": "United Kingdom",
    "westminster": "United Kingdom",
    "downing street": "United Kingdom",  # No. 10 / PM residence
    "buckingham": "United Kingdom",
    "whitehall": "United Kingdom",
    # Legislature: Parliament — House of Commons + House of Lords
    "house of commons": "United Kingdom",
    "house of lords": "United Kingdom",
    # Parties
    "labour party uk": "United Kingdom",
    "labour uk": "United Kingdom",
    "conservative party uk": "United Kingdom",
    "tories": "United Kingdom",
    "tory": "United Kingdom",
    "lib dems": "United Kingdom",
    "liberal democrats uk": "United Kingdom",
    "snp": "United Kingdom",             # Scottish National Party
    "plaid cymru": "United Kingdom",
    "reform uk": "United Kingdom",
    "starmer": "United Kingdom",         # PM Keir Starmer
    "farage": "United Kingdom",

    # =========================================================
    # VENEZUELA
    # =========================================================
    "venezuelan": "Venezuela",
    "venezuelans": "Venezuela",
    "caracas": "Venezuela",
    "miraflores": "Venezuela",           # presidential palace
    # Legislature: Asamblea Nacional
    "asamblea nacional venezuela": "Venezuela",
    "national assembly venezuela": "Venezuela",
    # Parties
    "psuv": "Venezuela",                 # Partido Socialista Unido de Venezuela (ruling)
    "mud": "Venezuela",                  # Mesa de la Unidad Democrática (opposition)
    "plataforma unitaria": "Venezuela",  # opposition coalition
    "maduro": "Venezuela",
    "delcy rodriguez": "Venezuela",      # interim president
    "rodríguez venezuela": "Venezuela",
    "guaido": "Venezuela",               # former interim president (cited in context)
    "edmundo gonzalez": "Venezuela",     # opposition candidate
    "maria corina machado": "Venezuela",

    # =========================================================
    # VIETNAM
    # =========================================================
    "vietnamese": "Vietnam",
    "hanoi": "Vietnam",
    "ho chi minh city": "Vietnam",
    "hcmc": "Vietnam",                   # Ho Chi Minh City abbreviation
    "saigon": "Vietnam",                 # former name, still used
    # Legislature: National Assembly (Quoc Hoi)
    "quoc hoi": "Vietnam",
    "national assembly vietnam": "Vietnam",
    # Parties — single party
    "cpv": "Vietnam",                    # Communist Party of Vietnam
    "communist party vietnam": "Vietnam",
    "dang cong san viet nam": "Vietnam",
    "to lam": "Vietnam",                 # General Secretary
    "luong cuong": "Vietnam",            # President

    # =========================================================
    # YEMEN
    # =========================================================
    "yemeni": "Yemen",
    "yemenis": "Yemen",
    "sanaa": "Yemen",
    "sana'a": "Yemen",
    "aden": "Yemen",                     # seat of internationally recognized government
    # Factions
    "houthis": "Yemen",
    "houthi": "Yemen",
    "ansarallah": "Yemen",               # Houthi's official name
    "ansar allah": "Yemen",
    "presidential leadership council": "Yemen",
    "plc yemen": "Yemen",
    "southern transitional council": "Yemen",
    "stc yemen": "Yemen",
    "islah party": "Yemen",

}

# =========================================================
# HEAD-OF-STATE / HEAD-OF-GOVERNMENT TITLE ABBREVIATIONS
# These are ADDED to the power-change keywords (not country aliases)
# so that "PM X resigns" or "Pres. Y ousted" triggers correctly.
# =========================================================

TITLE_ABBREVIATIONS = [
    # English
    r"\bPM\b",           # Prime Minister
    r"\bPMs\b",
    r"\bPMO\b",          # Prime Minister's Office
    r"\bPres\.",         # President (abbreviated with period)
    r"\bpres\.",
    r"\bSec\.\s*Gen\.",  # Secretary-General
    r"\bSec\s+Gen\b",
    r"\bFM\b",           # Foreign Minister
    r"\bDefMin\b",       # Defence Minister
    r"\bIntMin\b",       # Interior Minister
    r"\bFinMin\b",       # Finance Minister
    # Spanish/Portuguese
    r"\bpdte\.",         # presidente (abbrev.)
    r"\bpdte\b",
    r"\bpresidente\b",
    r"\bprimer\s+ministro\b",
    r"\bjefe\s+de\s+estado\b",    # head of state
    r"\bjefe\s+de\s+gobierno\b",  # head of government
    # French
    r"\bpremier\s+ministre\b",
    r"\bchef\s+de\s+l'état\b",
    r"\bchef\s+du\s+gouvernement\b",
    # German
    r"\bBundeskanzler\b",
    r"\bBundespräsident\b",
    r"\bBundesrat\b",
    r"\bAußenminister\b",
    # Arabic
    r"\brais\b",         # رئيس — president/chairman
    # Turkish
    r"\bcumhurbaskani\b",
    r"\bcumhurbaşkanı\b",
    # General
    r"\bhead\s+of\s+state\b",
    r"\bhead\s+of\s+government\b",
    r"\bchief\s+of\s+state\b",
    r"\bcommander[-\s]in[-\s]chief\b",
    r"\bsupreme\s+leader\b",
    r"\bsupreme\s+commander\b",
    r"\bacting\s+president\b",
    r"\bacting\s+prime\s+minister\b",
    r"\binterim\s+president\b",
    r"\binterim\s+prime\s+minister\b",
    r"\binterim\s+leader\b",
    r"\bpresident[-\s]elect\b",
    r"\bpm[-\s]elect\b",
    r"\bpresident\s+in\s+exile\b",
    r"\bgovernment[-\s]in[-\s]exile\b",
]



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

# Build compiled alias regex — longest keys first to avoid partial matches
_alias_pat = "|".join(
    r"\b" + re.escape(k) + r"\b"
    for k in sorted(COUNTRY_ALIASES, key=len, reverse=True)
)
ALIAS_RE = re.compile(_alias_pat, re.IGNORECASE)

# ---------------------------------------------------------------------------
# POWER-CHANGE KEYWORDS
#
# Core transition verbs + structural terms. TITLE_ABBREVIATIONS from
# TITLE_ABBREVIATIONS (inlined) are appended so "PM resigns" / "Pres. ousted" work.
# ---------------------------------------------------------------------------

_POWER_CORE = [
    # Elections & voting
    r"\belections?\b",
    r"\bsnap\s+election\b",
    r"\bby-election\b",
    r"\bbye-election\b",
    r"\breferendum\b",
    r"\bplebiscite\b",
    r"\brunoff\b",
    r"\brun-off\b",
    r"\bexit\s+poll\b",
    r"\bvoter\s+turnout\b",
    r"\belectoral\s+(crisis|fraud|reform|college)\b",
    r"\bpresidential\s+election\b",
    r"\bpresidential\s+candidate\b",
    r"\bpresidential\s+race\b",
    r"\bpresidential\s+runoff\b",
    r"\bgeneral\s+election\b",
    r"\bparliamentary\s+election\b",
    r"\blegislative\s+election\b",
    r"\bmidterm\s+election\b",

    # Coalition & majority
    r"\bcoalition\s+(talks|negotiations|formed?|deal|government|collapses?|building|partner)\b",
    r"\bkingmaker\b",
    r"\bmajority\s+government\b",
    r"\bminority\s+government\b",
    r"\bno-confidence\b",
    r"\bno\s+confidence\s+(vote|motion)\b",
    r"\bvote\s+of\s+confidence\b",
    r"\bformateur\b",
    r"\bbloc\s+(wins?|loses?|majority|minority|sweeps?|collapses?)\b",
    r"\blandslide\s+(win|victory|defeat|loss)\b",
    r"\bwon\s+(the\s+)?election\b",
    r"\blost?\s+(the\s+)?election\b",
    r"\bwins\s+(the\s+)?election\b",

    # Parliament maneuvers
    r"\bprorogue\b",
    r"\bdissolv(?:ed|ing|es)\s+(parliament|government|assembly|congress|legislature)\b",
    r"\bparliament(?:ary)?\s+(?:vote|voted|votes|passes?|approves?|rejects?|session|convened?)\b",
    r"\bparty\s+switch(?:es|ed)?\b",
    r"\bcross(?:es|ed)?\s+the\s+floor\b",      # floor crossing = party switch
    r"\bfloor\s+cross(?:es|ed|ing)?\b",
    r"\bparty\s+defect(?:s|ed|ion)?\b",

    # Sworn in / inaugurated
    r"\bsworn\s+in\b",
    r"\bswearing[-\s]in\b",
    r"\binaugurat(?:ed|ion|es)\b",

    # Elected / nominated / appointed
    r"\b(?:re)?elected\s+(?:as\s+|to\s+)?(?:president|prime\s+minister|chancellor|premier|leader|speaker|pm)\b",
    r"\belected\s+to\s+lead\b",
    r"\breelect(?:ed|s|ion)\b",
    r"\bnominated\s+(?:as\s+|for\s+)?(?:president|prime\s+minister|chancellor|premier|leader|pm)\b",
    r"\bappointed\s+(?:as\s+|to\s+)?(?:president|prime\s+minister|chancellor|premier|leader|pm|foreign\s+minister|defence\s+minister|defense\s+minister|interior\s+minister|pm)\b",
    r"\bnew\s+(?:president|prime\s+minister|pm|chancellor|premier|leader|government|regime)\b",
    r"\bforms?\s+(?:a\s+)?(?:new\s+)?government\b",
    r"\btransition\s+of\s+power\b",
    r"\bpower\s+transfer\b",
    r"\bsuccession\b",
    r"\bsuccessor\b",

    # Forced / contested removal
    r"\bcoup\b",
    r"\bputsch\b",
    r"\bjunta\b",
    r"\bmilitary\s+takeover\b",
    r"\bseiz(?:ed|ing|es)\s+power\b",
    r"\btoppl(?:ed|ing|es)\b",
    r"\boverthrew\b",
    r"\boverthrown\b",
    r"\boverthrow\b",
    r"\bousted\b",
    r"\bdeposed?\b",
    r"\bforced\s+out\b",
    r"\bstep(?:ped|s|ping)?\s+down\b",
    r"\bresign(?:ed|ation|s|ing)\b",
    r"\bimpeach(?:ed|ment|ing|es)\b",
    r"\bremoved\s+from\s+(?:power|office)\b",
    r"\bno\s+longer\s+(?:president|prime\s+minister|chancellor|leader|pm)\b",
    r"\bexil(?:ed|e)\b",
    r"\bdefect(?:ed|ion|s)\b",
    r"\bcaptur(?:ed|ing)\b",
    r"\barrest(?:ed)?\s+(?:the\s+)?(?:president|prime\s+minister|chancellor|leader|dictator|pm)\b",
    r"\bregime\s+change\b",
    r"\bnew\s+.{0,20}regime\b",

    # Structural political change
    r"\bconstitutional\s+(?:amendment|referendum|reform|revision|change|crisis)\b",
    r"\bconstitution\s+(?:amended|rewritten|revised|referendum)\b",
    r"\bterm\s+limits?\b",
    r"\bhead\s+of\s+(?:state|government)\b",
    r"\bsupreme\s+leader\b",
    r"\bcommander[-\s]in[-\s]chief\b",
    r"\bacting\s+(?:president|prime\s+minister|pm|leader|head)\b",
    r"\binterim\s+(?:president|prime\s+minister|pm|leader|government)\b",
    r"\bpresident[-\s]elect\b",
    r"\bpm[-\s]elect\b",
    r"\bgovernment[-\s]in[-\s]exile\b",
    r"\bpresident\s+in\s+exile\b",

    # Uprisings & instability
    r"\buprising\b",
    r"\binsurrection\b",
    r"\brebellion\b",
    r"\bcivil\s+war\b",
    r"\banti[-\s]government\s+(?:protest|demonstration|rally|movement|unrest)\b",
    r"\banti[-\s]regime\s+(?:protest|demonstration|rally|movement)\b",
    r"\bstate\s+of\s+emergency\b",
    r"\bmartial\s+law\b",

    # Electoral competition / rivals
    r"\bopposition\s+(?:leader|wins?|victory|defeats?|party)\b",
    r"\bincumbent\s+(?:defeated?|loses?|ousted|wins?)\b",
    r"\bpresident.{0,30}rivals?\b",
    r"\bpolitical\s+(?:transition|crisis|succession|vacuum)\b",
    r"\bpower\s+struggle\b",
    r"\bcrisis\s+talks\b",
    r"\bpolitical\s+crisis\b",
    r"\bno[-\s]confidence\b",
    r"\bemergency\s+powers?\b",
    r"\bvotes?\s+on\s+(?:constitutional|reform|motion|bill|resolution)\b",
    r"\bconstitutional\s+(?:reform|amendment|revision|referendum|vote|crisis)\b",
    r"\breshuffle\b",
    r"\bleadership\s+(?:race|contest|election|crisis|change|reshuffle|shake[-\s]?up)\b",
    r"\bcrosses?\s+the\s+floor\b",
    r"\bdefects?\b",
    r"\bleadership\s+(?:race|contest|election|crisis|change|transition|challenge)\b",
]

# Append title abbreviations (inlined above)
_POWER_ALL = _POWER_CORE + TITLE_ABBREVIATIONS
POWER_RE = re.compile("|".join(_POWER_ALL), re.IGNORECASE)

# ---------------------------------------------------------------------------
# NOISE VETO — high-precision false-positive signatures
# ---------------------------------------------------------------------------

NOISE_RE = re.compile(r"""
  \bceo\b | \bchief\s+executive\b | \bexecutive\s+director\b |
  \b(oscar|grammy|emmy|bafta|amvca|golden\s+globe)\b |
  \b(actress?|actor|celebrity|pop\s+star)\b |
  \baward\s+(ceremony|season|nomination)\b |
  \bnominated\s+for\s+(best|the\s).{0,30}(award|oscar|grammy|emmy)\b |
  \b(apostolic\s+)?nuncio\b |
  \b(arch)?bishop\s+.{0,25}appointed\b |
  \bpresident\s+of\s+the\s+(metro|subway|transit|railway|airport|port|stadium|chamber\s+of\s+commerce)\b |
  \b(nba|nfl|nhl|mlb|epl|fifa|uefa|icc)\b |
  \b(soccer|football)\s+(club|team|match|game|league|season)\b |
  \b(world\s+cup|olympics|olympic\s+games)\b |
  \b(coach|manager)\s+.{0,20}(appointed|resigned|sacked|fired)\b |
  \b(interest\s+rate|central\s+bank|gdp|stock\s+market|bond\s+yield)\b |
  \b(earnings|quarterly\s+results|ipo|merger|acquisition)\b |
  \b(judge|justice|magistrate)\s+.{0,20}appointed\b |
  \b(fake|false|debunk|misinformation|fact.?check)\b.{0,50}\b(claim|image|video|photo|report)\b |
  \b(disneyland|disney\s+(world|adventure|resort|park))\b |
  \boil\s+(tanker|shipment|price|exports?|imports?|barrel|supply)\b |
  \bsanctions\s+(breach|violation|evasion|busting)\b
""", re.IGNORECASE | re.VERBOSE | re.DOTALL)

# ---------------------------------------------------------------------------
# SIGNIFICANCE GATE
# A few terms are structural words that appear in countless non-political
# headlines. If the ONLY power match is one of these weak terms, require
# a second qualifying anchor to confirm actual power change context.
# ---------------------------------------------------------------------------

WEAK_POWER_TERMS = re.compile(
    r"^(government|minister|parliament(?:ary)?|congress|senate|assembly|leader|"
    r"president|pm|pres\.|FM|DefMin|FinMin|IntMin)$",
    re.IGNORECASE,
)

SIGNIFICANCE_ANCHORS = re.compile(
    r"\b(crisis|collapses?|falls?|toppled|dissolved|formed|transition|"
    r"majority|minority|coalition|election|vote|resign|oust|depose|coup|"
    r"impeach|emergency|uprising|revolution|takeover|captured|arrested|"
    r"defect|exile|succession|successor|sworn|inaugurated|prorogue|"
    r"landslide|runoff|formateur|kingmaker|bloc)\b",
    re.IGNORECASE,
)

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
    ("Google News World",       "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx1YlY4U0FtVnVHZ0pWVXlnQVAB?hl=en-US&gl=US&ceid=US:en",                        "en"),
    ("VOA World",               "https://www.voanews.com/api/z-botl-vomx-tpertmq",                       "en"),
    ("RFI English",             "https://www.rfi.fr/en/rss",                                        "en"),
    ("Euronews",                "https://www.euronews.com/rss",                                   "en"),
    # MIDDLE EAST / NORTH AFRICA
    ("Egypt Independent",       "https://egyptindependent.com/feed/",                            "en"),
    ("Ahram Online",            "https://english.ahram.org.eg/rss.aspx",          "en"),
    ("Mada Masr",               "https://www.madamasr.com/en/feed/",                              "en"),
    ("Libya Herald",            "https://www.libyaherald.com/feed/",                              "en"),
    ("Libya Observer",          "https://libyaobserver.ly/feed",                          "en"),
    ("Morocco World News",      "https://www.moroccoworldnews.com/feed/",                          "en"),
    ("Le Desk Morocco",         "https://ledesk.ma/feed/",                                       "fr"),
    ("Algeria Press Service",   "https://www.aps.dz/en/rss",                                    "en"),
    ("El Watan Algeria",        "https://elwatan.com/feed",                                  "fr"),
    ("Arab News",               "https://www.arabnews.com/feed/",                               "en"),
    ("Saudi Gazette",           "https://saudigazette.com.sa/feed",                                "en"),
    ("The National UAE",        "https://www.thenationalnews.com/arc/outboundfeeds/rss/",                        "en"),
    ("Gulf News",               "https://gulfnews.com/rss/world",                                      "en"),
    ("Haaretz English",         "https://www.haaretz.com/srv/haaretz-feeds?feedName=headlines",                        "en"),
    ("Jerusalem Post",          "https://www.jpost.com/rss/rssfeedsfrontpage.aspx",               "en"),
    ("Middle East Eye",         "https://www.middleeasteye.net/rss",                              "en"),
    ("Palestinian Chronicle",   "https://www.palestinechronicle.com/feed",                      "en"),
    ("Iran International",      "https://www.iranintl.com/en/rss",                           "en"),
    ("Radio Farda",             "https://www.radiofarda.com/api/zyrqo_ekyq/feed.rss",             "fa"),
    ("Syria Direct",            "https://syriadirect.org/feed/",                                 "en"),
    ("Yemen Monitor",           "https://yemenmonitor.com/rss",                                "en"),
    ("Garowe Online Somalia",   "https://garoweonline.com/en/rss.xml",                           "en"),
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
    ("DW World Politics",       "https://rss.dw.com/rdf/rss-en-pol",                             "en"),
    ("FAZ Germany",             "https://www.faz.net/rss/aktuell/politik/",                      "de"),
    ("Sky News World",          "https://feeds.skynews.com/feeds/rss/world.xml",                 "en"),
    ("The Independent UK",      "https://www.independent.co.uk/news/world/rss",                  "en"),
    ("The Telegraph UK",        "https://www.telegraph.co.uk/rss.xml",                           "en"),
    ("The Local Denmark",       "https://www.thelocal.dk/rss/",                                 "en"),
    ("DR News Denmark",         "https://www.dr.dk/nyheder/service/feeds/allenyheder",           "da"),
    ("Politiken Denmark",       "https://politiken.dk/rss/senestenyt.rss",                                     "da"),
    ("Moscow Times",            "https://www.themoscowtimes.com/rss/news",                       "en"),
    ("Kyiv Independent",        "https://kyivindependent.com/feed",                             "en"),
    ("Ukrinform",               "https://www.ukrinform.net/rss/block-lastnews",                  "en"),
    ("Meduza EN",               "https://meduza.io/rss/en/all",                                  "en"),
    ("Civilnet Armenia",        "https://www.civilnet.am/en/feed/",                              "en"),
    ("OC Media Caucasus",       "https://oc-media.org/feed/",                                    "en"),
    ("Turan Azerbaijan",        "https://turan.az/en/rss.xml",                             "az"),
    # ASIA
    ("South China Morning Post","https://www.scmp.com/rss/91/feed",                              "en"),
    ("Caixin Global",           "https://www.caixinglobal.com/rss/",                   "en"),
    ("Xinhua World",            "https://english.news.cn/rss/world_news.xml",                   "en"),
    ("Japan Times",             "https://www.japantimes.co.jp/feed/",                            "en"),
    ("NHK World",               "https://www3.nhk.or.jp/rss/news/cat0.xml",                      "en"),
    ("Mainichi English",        "https://mainichi.jp/english/rss/",                       "en"),
    ("Korea Herald",            "https://www.koreaherald.com/rss/020000000000.xml",              "en"),
    ("Korea Times",             "https://www.koreatimes.co.kr/www/rss/nation.xml",               "en"),
    ("NK News",                 "https://www.nknews.org/feed/",                                  "en"),
    ("38 North",                "https://www.38north.org/feed",                                 "en"),
    ("Taiwan News",             "https://www.taiwannews.com.tw/rss/news.xml",                   "en"),
    ("Focus Taiwan",            "https://focustaiwan.tw/feed/rss",                               "en"),
    ("The Hindu Politics",      "https://www.thehindu.com/news/national/feeder/default.rss",     "en"),
    ("Hindustan Times",         "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",        "en"),
    ("Indian Express",          "https://indianexpress.com/feed/",                               "en"),
    ("Dawn Pakistan",           "https://www.dawn.com/feeds/home",                               "en"),
    ("The News International",  "https://www.thenews.com.pk/rss/1/8",                            "en"),
    ("Jakarta Post",            "https://www.thejakartapost.com/rss/news.rss",                       "en"),
    ("Tempo.co Indonesia",      "https://en.tempo.co/rss",                                  "en"),
    ("Irrawaddy Myanmar",       "https://www.irrawaddy.com/news/feed",                                "en"),
    ("Myanmar Now",             "https://myanmar-now.org/en/feed/",                              "en"),
    ("VN Express International","https://e.vnexpress.net/rss/world.rss",                 "en"),
    # AMERICAS
    ("Buenos Aires Herald",     "https://buenosairesherald.com/rss",                            "en"),
    ("La Nacion Argentina",     "https://www.lanacion.com.ar/arc/outboundfeeds/rss/",            "es"),
    ("Infobae Argentina",       "https://www.infobae.com/feeds/rss/",                "es"),
    ("Folha de S Paulo",        "https://feeds.folha.uol.com.br/poder/rss091.xml",               "pt"),
    ("O Globo Brazil",          "https://oglobo.globo.com/rss.xml",                              "pt"),
    ("Agencia Brasil",          "https://agenciabrasil.ebc.com.br/rss/politica/feed.xml",        "pt"),
    ("El Universal Mexico",     "https://www.eluniversal.com.mx/rss.xml?section=politica",                        "es"),
    ("Animal Politico Mexico",  "https://animalpolitico.com/feed",                              "es"),
    ("Proceso Mexico",          "https://www.proceso.com.mx/rss/",                               "es"),
    ("El Colombiano",           "https://www.elcolombiano.com/feeds/rss/rss.xml",                          "es"),
    ("El Tiempo Colombia",      "https://www.eltiempo.com/rss/politica.xml",                     "es"),
    ("La Tercera Chile",        "https://www.latercera.com/rss/",                               "es"),
    ("El Mostrador Chile",      "https://www.elmostrador.cl/rss",                              "es"),
    ("El Nacional Venezuela",   "https://www.elnacional.com/feed/",                              "es"),
    ("Efecto Cocuyo Venezuela", "https://efectococuyo.com/rss",                                "es"),
    ("Peru Reports",            "https://perureports.com/feed/",                                 "en"),
    ("El Comercio Peru",        "https://elcomercio.pe/rss/",                "es"),
    ("14ymedio Cuba",           "https://14ymedio.com/feed/",                                 "es"),
    ("CiberCuba",               "https://www.cibercuba.com/rss.xml",                                "es"),
    ("CBC News World",          "https://www.cbc.ca/cmlink/rss-world",                           "en"),
    ("Globe and Mail Politics", "https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/politics/", "en"),
    ("La Estrella Panama",      "https://laestrella.com.pa/rss.xml",                         "es"),
    ("El Faro El Salvador",     "https://elfaro.net/es/rss",                                    "es"),
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
        except requests.exceptions.HTTPError as e:
            log.warning("HTTP %s attempt %d/%d: %s", url, attempt, RETRY_ATTEMPTS, e)
        except requests.exceptions.ConnectionError as e:
            log.warning("Conn %s attempt %d/%d: %s", url, attempt, RETRY_ATTEMPTS, e)
        except requests.exceptions.Timeout:
            log.warning("Timeout %s attempt %d/%d", url, attempt, RETRY_ATTEMPTS)
        except Exception as e:
            log.warning("Error %s attempt %d/%d: %s", url, attempt, RETRY_ATTEMPTS, e)
        if attempt < RETRY_ATTEMPTS:
            time.sleep(RETRY_BACKOFF)
    return None


def parse_feed_bytes(raw: bytes):
    """Three-strategy XML parser to survive bozo/malformed feeds."""
    result = feedparser.parse(io.BytesIO(raw))
    if not result.bozo or result.entries:
        return result
    try:
        text = raw.decode("utf-8", errors="replace")
        clean = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
        r2 = feedparser.parse(clean)
        if r2.entries:
            return r2
    except Exception:
        pass
    try:
        r3 = feedparser.parse(raw.decode("latin-1", errors="replace"))
        if r3.entries:
            return r3
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def safe_translate(text: str, src_lang: str) -> str:
    if src_lang == "en" or not text:
        return text
    try:
        return GoogleTranslator(source=src_lang, target="en").translate(text) or text
    except Exception as e:
        log.warning("Translation fail (%s->en): %s", src_lang, e)
        return text


def resolve_country(title: str) -> str | None:
    """
    Return the tracked country a headline is about, or None.
    Priority: full country name > specific alias.
    """
    tl = title.lower()

    # 1. Full country name (highest priority — avoids mis-tagging)
    for country in TRACKED_COUNTRIES:
        if re.search(r"\b" + re.escape(country.lower()) + r"\b", tl):
            return country

    # 2. Specific aliases (inlined COUNTRY_ALIASES dict)
    m = ALIAS_RE.search(tl)
    if m:
        return COUNTRY_ALIASES.get(m.group(0).lower())

    return None


def passes_filters(title: str) -> tuple[bool, str | None]:
    """Returns (passes, country_or_None). All four stages must pass."""

    # Stage 1: country
    country = resolve_country(title)
    if not country:
        return False, None

    # Stage 2: power keyword (includes title abbreviations)
    pw = POWER_RE.search(title)
    if not pw:
        return False, None

    # Stage 3: noise veto
    if NOISE_RE.search(title):
        log.debug("Noise veto: %s", title)
        return False, None

    # Stage 4: significance gate
    matched_term = pw.group(0).strip()
    if WEAK_POWER_TERMS.match(matched_term):
        if not SIGNIFICANCE_ANCHORS.search(title):
            log.debug("Significance gate: %s", title)
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
        log.warning("[SKIP] %s — unreachable", source_name)
        return results

    feed = parse_feed_bytes(raw)
    if not feed.entries:
        if feed.bozo:
            log.warning("[SKIP] %s — unparseable: %s", source_name, feed.bozo_exception)
        return results

    if feed.bozo:
        log.warning("[WARN] %s — bozo, processing %d entries: %s",
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
            stories = data.get("stories", data) if isinstance(data, dict) else data
            if isinstance(stories, list):
                for s in stories:
                    if "country" not in s:
                        c = resolve_country(s.get("title", ""))
                        if c:
                            s["country"] = c
                return stories
        except Exception as e:
            log.warning("Could not load existing JSON: %s", e)
    return []


def deduplicate(stories: list) -> list:
    seen: dict[str, dict] = {}
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
    log.info("Loaded %d country aliases, %d title patterns",
             len(COUNTRY_ALIASES), len(TITLE_ABBREVIATIONS))
    DOCS_DIR.mkdir(exist_ok=True)

    existing = load_existing(OUTPUT_FILE)
    log.info("Loaded %d existing stories from archive.", len(existing))

    cutoff = cutoff_date()
    existing = prune_old(existing, cutoff)
    # Retroactively clean noise from old archive entries
    existing = [s for s in existing if not NOISE_RE.search(s.get("title", ""))]
    log.info("After prune+noise-clean: %d stories remain.", len(existing))

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
                "party switches, and other leadership transitions across 44 tracked "
                "countries. Archive window: 14 days. Updated twice daily at 06:00 "
                "and 18:00 EST. Country resolution uses 687 aliases covering "
                "demonyms, capitals, legislature names, party names, leader names, "
                "and political abbreviations."
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
                "Group stories by 'country' to triage. Signal threshold: 3+ independent "
                "sources on the same event within 48 hours = major change. "
                "Established 2026 baselines (do not re-flag as new): "
                "Iran — Mojtaba Khamenei appointed Supreme Leader March 8 2026 after "
                "assassination of Ali Khamenei Feb 28; "
                "Venezuela — Maduro captured by US forces Jan 3 2026, interim president "
                "Delcy Rodriguez in place; "
                "Myanmar — junta chief Min Aung Hlaing nominated civilian president "
                "March 30 2026, new army chief Ye Win Oo; "
                "Denmark — inconclusive election March 24 2026, Frederiksen caretaker PM, "
                "coalition talks ongoing, Moderates are kingmakers; "
                "Canada — PM Mark Carney minority Liberal government since April 2025."
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
