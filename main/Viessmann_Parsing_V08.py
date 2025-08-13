# === IMPORTS ===
from __future__ import annotations
import csv
import json
import logging
import shutil
import sys
import time
import random 
import requests 
import os
import atexit
import argparse
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import signal

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException, WebDriverException, ElementClickInterceptedException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys

from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from webdriver_manager.chrome import ChromeDriverManager

# =============================================================================
# === LOCK-DATEI IMPLEMENTIERUNG ===
# =============================================================================
def ensure_single_instance(script_name: str, stale_lock_timeout: int = 300):
    """Verhindert das gleichzeitige Ausführen mehrerer Instanzen des Skripts."""
    script_dir = Path(__file__).parent
    lock_file_path = script_dir / f"{script_name}.lock"
    
    def remove_lock_file():
        try:
            if lock_file_path.exists():
                lock_file_path.unlink()
                print("🔓 Lock-Datei entfernt - Skript beendet.")
        except Exception as e:
            print(f"⚠️  Warnung beim Entfernen der Lock-Datei: {e}")
    
    def signal_handler(signum, frame):
        print(f"\n📡 Signal {signum} empfangen. Beende sauber...")
        remove_lock_file()
        sys.exit(0)
    
    if lock_file_path.exists():
        try:
            lock_age = time.time() - lock_file_path.stat().st_mtime
            if lock_age > stale_lock_timeout:
                print(f"⚠️  Veraltete Lock-Datei gefunden (Alter: {lock_age:.0f}s). Entferne sie...")
                lock_file_path.unlink()
            else:
                with open(lock_file_path, 'r') as f:
                    pid = f.read().strip()
                print(f"⚠️  Lock-Datei gefunden: {lock_file_path}")
                print(f"📋 Prozess-ID: {pid}")
                print("❌ Andere Instanz läuft bereits. Beende Programm.")
                sys.exit(1)
        except Exception as e:
            try:
                lock_file_path.unlink()
                print("🧹 Beschädigte Lock-Datei entfernt.")
            except Exception as e2:
                print(f"❌ Konnte beschädigte Lock-Datei nicht entfernen: {e2}")
                sys.exit(1)
    
    try:
        with open(lock_file_path, "w") as f:
            f.write(str(os.getpid()))
        
        atexit.register(remove_lock_file)
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        print("🔒 Lock-Datei erstellt. Skript startet...")
        return lock_file_path
        
    except Exception as e:
        print(f"❌ Fehler beim Erstellen der Lock-Datei: {e}")
        raise

# =============================================================================
# === 1. KONFIGURATION ===
# =============================================================================
@dataclass
class Paths:
    BASE_DIR: Path = Path(r"S:\PyScarper\Viesmann")
    ARCHIV_DIR: Path = field(init=False)
    LOG_DIR: Path = field(init=False)
    INPUT_DIR: Path = field(init=False)
    PD_DATA_DIR: Path = field(init=False)
    COOKIE_DIR: Path = field(init=False)
    PROXY_DIR: Path = Path(r"S:\PyScarper\Proxy") 
    CHROMEDRIVER: Path = Path(r"S:\PyScarper\chromedriver-win64\chromedriver.exe")
    USER_DATA_DIR: Path = field(init=False) 
    pd_file: Path = field(init=False)
    logfile_path: Path = field(init=False)
    input_file: Path = field(init=False)
    cookie_file: Path = field(init=False) 
    login_credentials_file: Path = field(init=False)
    proxy_file: Path = field(init=False)

    def __post_init__(self):
        self.ARCHIV_DIR = self.BASE_DIR / "Archiv"; self.LOG_DIR = self.BASE_DIR / "Log"
        self.INPUT_DIR = self.BASE_DIR / "Input"; self.PD_DATA_DIR = self.BASE_DIR / "PD_Data"
        self.COOKIE_DIR = self.BASE_DIR / "cookie"
        self.USER_DATA_DIR = self.BASE_DIR / "user-data-dir" 
        self.input_file = self.INPUT_DIR / "VS_DN_Täglich.csv"
        self.cookie_file = self.COOKIE_DIR / "cookies.json"
        self.login_credentials_file = self.INPUT_DIR / "viessmann.txt"
        self.proxy_file = self.PROXY_DIR / "Proxy_IP.txt"
        timestamp = datetime.now().strftime("%Y.%m.%d_%H%M%S")
        self.pd_file = self.PD_DATA_DIR / f"shop.viessmann_PD_{timestamp}.csv"
        self.logfile_path = self.LOG_DIR / "log_Viessmann_Shop.txt"

@dataclass
class ScraperSettings: 
    START_URL: str = "https://shop.viessmann.com/de/de/home"
    ARTICLE_URL_TEMPLATE: str = "https://shop.viessmann.com/details/{artikelnummer}?origins=O,M,E&searchTerm={artikelnummer}&page=0&origins=O,M,E&selectedDetailsTab=description"
    SHOP_BASE_DOMAIN: str = "shop.viessmann.com" 
    PROXY_TEST_URL: str = "https://httpbin.org/ip" 
    MAX_RETRIES: int = 3; RETRY_DELAY_SECONDS: int = 5; HEADLESS_BROWSER: bool = True

@dataclass
class Timeouts: 
    PAGE_LOAD: int = 35; ELEMENT_SEARCH: int = 10; FIELD_SEARCH_QUICK: int = 3 
    PROXY_TEST_TIMEOUT: int = 10; IMPLICIT_WAIT: int = 0
    SESSION_CHECK_TIMEOUT: int = 8 

@dataclass
class Selectors: # Unverändert
    LOGIN_BUTTON_STARTPAGE: str = "button[data-testid='login-button'], button.login-button, a[href*='login'], a[href*='Login'], button[aria-label*='Login'], a[href*='account/login'], a[href*='my-account']"
    EMAIL_FIELD: str = "input[name='isiwebuserid'], input[type='text'][name*='user'], input[class*='mdc-text-field__input']"
    PASSWORD_FIELD: str = "input#isiwebpasswd"; SUBMIT_BUTTON: str = "button#submitButton:not([disabled])"
    LOGIN_SUCCESS_INDICATOR: str = "div.user-menu, a.account-link, span.user-name, div[class*='user'], div[class*='account'], div[class*='logged-in'], div[class*='success']"
    CART_NAVIGATION_INDICATOR: str = "div.cart-navigation-price-and-commission"
    COOKIE_ACCEPT_SHADOW_HOST: str = "#usercentrics-root"
    COOKIE_ACCEPT_BUTTON: str = 'button[data-testid="uc-accept-all-button"]' 
    MATERIAL_NUMBER: str = 'app-info[data-cy="product_detail.info_bar.material_number"] div.grey-blue-font'
    STATUS: str = 'app-info[data-cy="product_detail.info_bar.status"] div.availability-text > div'
    PRICE_GROSS: str = "span[data-cy='product_detail.info_bar.gross_price']"; PRICE_NET: str = "span[data-cy='product_detail.info_bar.net_price'] b"
    SALE_NOTE: str = 'app-info[data-cy="product_detail.info_bar.sale_note"] div.grey-blue-font > div'
    ALT_MATERIAL_NUMBER: str = 'table#product-table tbody tr:first-child td[data-cy="product_overview.product_table.material_number_row_data"] app-copy-to-clipboard div'
    ALT_STATUS: str = 'table#product-table tbody tr:first-child td[data-cy="product_overview.product_table.availability_row_data"] app-availability div.availability-text div'
    ALT_BRUTTO: str = 'table#product-table tbody tr:first-child td p[data-cy="product_overview.product_table.price_row_data.gross_price"]'
    ALT_NETTO: str = 'table#product-table tbody tr:first-child td p[data-cy="product_overview.product_table.price_row_data.net_price"]'
    ARTICLE_NOT_FOUND_INDICATOR: str = "div.page-not-found-component" 

@dataclass
class Config: 
    paths: Paths = field(default_factory=Paths); settings: ScraperSettings = field(default_factory=ScraperSettings)
    timeouts: Timeouts = field(default_factory=Timeouts); selectors: Selectors = field(default_factory=Selectors)

# =============================================================================
# === 2. LOGGER SETUP (unverändert) ===
# =============================================================================
def setup_logger(log_dir: Path, log_file: Path, archiv_dir: Path):
    try:
        log_dir.mkdir(exist_ok=True)
        archiv_dir.mkdir(exist_ok=True)
        if log_file.exists():
            timestamp = datetime.now().strftime("%Y.%m.%d_%H%M%S")
            archive_path = archiv_dir / f"log_viessmann_{timestamp}.txt"
            shutil.copy2(log_file, archive_path)
            log_file.unlink()
    except Exception as e:
        print(f"Warnung Log-Archivierung: {e}")
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.WARNING)  # Nur Warnung und Fehler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.WARNING)     # Nur Warnung und Fehler
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    root_logger.setLevel(logging.WARNING)      # Nur Warnung und Fehler
    logging.warning("="*50)
    logging.warning("🚀 Viessmann Scraper gestartet")
    logging.warning("="*50)

# =============================================================================
# === 3. BROWSER MANAGER (unverändert) ===
# =============================================================================
class BrowserManager:
    def __init__(self, config: Config): self.config = config; self.driver_path = config.paths.CHROMEDRIVER
    def create_driver(self, proxy_address: str | None = None) -> webdriver.Chrome:
        logging.info("Initialisiere Chrome-WebDriver...")
        if proxy_address:
            logging.info(f"Verwende Proxy: {proxy_address}")
        
        chrome_options = Options()
        
        # Grundlegende Optionen
        if self.config.settings.HEADLESS_BROWSER:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument(f"--user-data-dir={str(self.config.paths.USER_DATA_DIR)}")
        
        if proxy_address:
            chrome_options.add_argument(f"--proxy-server=http://{proxy_address}")
        
        # Stabilität und Performance
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-field-trial-config")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        
        # Browser-Verhalten
        chrome_options.add_argument("--window-size=640,480")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        
        # Anti-Detection
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_experimental_option("detach", False)
        
        # Preferences
        prefs = {
            "profile.default_content_setting_values": {
                "notifications": 2,
                "geolocation": 2,
                "media_stream": 2,
                "images": 2
            },
            "profile.managed_default_content_settings.images": 2
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Service mit Retry-Logik
        max_retries = 3
        for attempt in range(max_retries):
            try:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
                driver.set_page_load_timeout(self.config.timeouts.PAGE_LOAD)
                driver.implicitly_wait(self.config.timeouts.IMPLICIT_WAIT)
                
                # Teste Verbindung
                driver.get("data:text/html,<html><body>Test</body></html>")
                logging.info("✅ WebDriver erfolgreich erstellt und getestet.")
                return driver
                
            except Exception as e:
                logging.warning(f"Versuch {attempt + 1}/{max_retries} fehlgeschlagen: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)  # Warte vor erneutem Versuch
                else:
                    logging.error(f"❌ Fehler beim Erstellen des WebDriver nach {max_retries} Versuchen: {e}")
                    raise

# =============================================================================
# === 4. HAUPT-SCRAPER ===
# =============================================================================
class ViessmannScraper:
    CSV_FIELD_ORDER = ['timestamp', 'artikelnummer', 'material_nr', 'status', 'brutto', 'netto', 'verkaufshinweis', 'extraktions_quelle', 'status_text']
    CSV_DISPLAY_HEADERS = ['Zeitstempel', 'Artikelnummer', 'Material_Nr', 'Status', 'Brutto', 'Netto', 'Verkaufshinweis', 'Extraktions_Quelle', 'status_text']

    def __init__(self, config: Config): # Unverändert
        self.config = config; self.browser_manager = BrowserManager(config)
        self.driver = None ; self.current_proxy = None 
        self._load_credentials(); self.proxies = self._load_proxies()

    def _load_credentials(self): # Unverändert
        try:
            creds = {};
            with open(self.config.paths.login_credentials_file, "r", encoding='utf-8') as f:
                for line in f:
                    if '=' in line: key, value = line.split('=', 1); creds[key.strip()] = value.strip()
            self.username = creds.get('email', '').strip().strip('"'); self.password = creds.get('password', '').strip().strip('"')
            if not self.username or not self.password: raise ValueError("E-Mail/Passwort nicht in Datei gefunden/leer.")
        except (FileNotFoundError, ValueError) as e: logging.error(f"❌ Fehler Anmeldedaten: {e}"); raise

    def _load_proxies(self) -> list[str]: # Unverändert
        proxy_file = self.config.paths.proxy_file
        if not proxy_file.exists(): logging.warning(f"Proxy-Datei nicht gefunden: {proxy_file}."); return []
        try:
            with open(proxy_file, "r", encoding="utf-8") as f: proxies = [line.strip() for line in f if line.strip() and not line.startswith("#")]
            logging.info(f"💠 {len(proxies)} Proxys aus {proxy_file} geladen."); return proxies
        except Exception as e: logging.error(f"Fehler Laden Proxys {proxy_file}: {e}"); return []

    def _test_proxy(self, proxy_address: str) -> bool:
        if not proxy_address: return False
        test_url = self.config.settings.PROXY_TEST_URL
        proxies_dict = {"http": f"http://{proxy_address}", "https": f"http://{proxy_address}"}
        
        try:
            response = requests.get(
                test_url, 
                proxies=proxies_dict, 
                timeout=self.config.timeouts.PROXY_TEST_TIMEOUT,
                verify=False
            )
            if response.status_code == 200:
                logging.info(f"✅ Proxy {proxy_address} aktiv. IP: {response.json().get('origin', 'N/A')}")
                return True
            logging.warning(f"Proxy {proxy_address} Test fehlgeschlagen. Status: {response.status_code}")
            return False
        except requests.exceptions.RequestException as e:
            logging.warning(f"Proxy {proxy_address} Test fehlgeschlagen: {e}")
            return False

    def _get_and_test_initial_proxy(self) -> str | None: # Unverändert
        if not self.proxies: logging.info("Keine Proxys konfiguriert. Fahre ohne Proxy fort."); return None
        random.shuffle(self.proxies)
        for proxy_to_test in self.proxies:
            logging.info(f"Initialer Test für Proxy {proxy_to_test}...")
            if self._test_proxy(proxy_to_test): return proxy_to_test
            else: logging.warning(f"Initialer Test für Proxy {proxy_to_test} fehlgeschlagen.")
        logging.error("❌ Keiner der Proxys hat initialen Test bestanden. Skript wird beendet."); sys.exit(1)

    def run(self): # Unverändert
        try:
            self.current_proxy = self._get_and_test_initial_proxy()
            self.driver = self.browser_manager.create_driver(proxy_address=self.current_proxy)
            self._ensure_login(); articles = self._load_articles_from_csv(); self._process_articles(articles)
        except SystemExit as e: logging.info(f"Skript beendet: {e}")
        except Exception as e: logging.error(f"Schwerwiegender Fehler Hauptprozess: {e}", exc_info=True)
        finally:
            if self.driver:
                try: self.driver.quit()
                except Exception as e_q: logging.debug(f"Fehler beim Beenden des finalen WebDrivers: {e_q}")
            logging.info("🏁 Scraper-Lauf beendet.")

    def _ensure_login(self):
        logging.info("Versuche Sitzungswiederherstellung über gespeichertes Nutzerprofil...")
        self.driver.get(self.config.settings.START_URL)
        time.sleep(3)
        
        if self._is_logged_in():
            logging.info("✅ Erfolgreich über bestehendes Nutzerprofil eingeloggt.")
            return
            
        logging.info("Keine gültige Session durch Nutzerprofil. Lösche altes Profil und führe manuellen Login durch...")
        
        # Beende WebDriver sauber
        if self.driver:
            try:
                self.driver.quit()
                logging.debug("Alter WebDriver beendet.")
            except Exception as e_quit:
                logging.warning(f"Fehler beim Beenden des alten WebDrivers: {e_quit}")
        
        # Warte kurz, damit alle Verbindungen geschlossen werden
        time.sleep(2)
        
        # Lösche Nutzerprofil mit robuster Methode
        user_data_dir_path = self.config.paths.USER_DATA_DIR
        if user_data_dir_path.exists():
            try:
                self._force_delete_directory(user_data_dir_path)
                logging.info(f"Nutzerprofil '{user_data_dir_path}' gelöscht.")
            except Exception as e_del:
                logging.error(f"❌ Fehler Löschen Nutzerprofil '{user_data_dir_path}': {e_del}")
                # Versuche es mit umbenennen statt löschen
                try:
                    backup_path = user_data_dir_path.with_suffix('.old')
                    if backup_path.exists():
                        self._force_delete_directory(backup_path)
                    user_data_dir_path.rename(backup_path)
                    logging.info(f"Nutzerprofil umbenannt zu '{backup_path}'")
                except Exception as e_rename:
                    logging.error(f"❌ Auch Umbenennen fehlgeschlagen: {e_rename}")
                    sys.exit(1)
        
        # Warte vor Erstellung des neuen WebDrivers
        time.sleep(3)
        
        logging.info("Erstelle neuen WebDriver für frischen Login...")
        self.driver = self.browser_manager.create_driver(proxy_address=self.current_proxy)
        
        self._perform_login()
        
        if not self._is_logged_in():
            raise RuntimeError("Login fehlgeschlagen, auch nach Löschen Profils.")
            
        logging.info("✅ Manueller Login erfolgreich. Session im neuen Nutzerprofil gespeichert.")
        self._save_cookies()

    def _force_delete_directory(self, path: Path):
        """Lösche Verzeichnis mit robuster Methode"""
        import stat
        
        def on_rm_error(func, path, exc_info):
            # Ändere Berechtigungen und versuche erneut
            try:
                os.chmod(path, stat.S_IWRITE)
                os.unlink(path)
            except:
                pass
        
        # Versuche normale Löschung
        try:
            shutil.rmtree(path, onerror=on_rm_error)
        except Exception as e:
            logging.warning(f"Normale Löschung fehlgeschlagen: {e}")
            # Fallback: Versuche Datei für Datei zu löschen
            try:
                for root, dirs, files in os.walk(path, topdown=False):
                    for name in files:
                        file_path = os.path.join(root, name)
                        try:
                            os.chmod(file_path, stat.S_IWRITE)
                            os.unlink(file_path)
                        except:
                            pass
                    for name in dirs:
                        dir_path = os.path.join(root, name)
                        try:
                            os.chmod(dir_path, stat.S_IWRITE)
                            os.rmdir(dir_path)
                        except:
                            pass
                os.rmdir(str(path))
            except Exception as e2:
                raise Exception(f"Alle Löschversuche fehlgeschlagen: {e2}")

    def _is_logged_in(self) -> bool: # Unverändert
        logging.debug("Starte Autorisierungsprüfung (Warenkorb-Text)...")
        try:
            cart_element = WebDriverWait(self.driver, self.config.timeouts.SESSION_CHECK_TIMEOUT).until(EC.presence_of_element_located((By.CSS_SELECTOR, self.config.selectors.CART_NAVIGATION_INDICATOR)))
            text = cart_element.text 
            if "Neuer Warenkorb" in text: logging.info("✅ Session aktiv: 'Neuer Warenkorb' Text gefunden."); return True
            else: logging.info(f"ℹ️ Session nicht aktiv: Text '{text[:50].replace(chr(10), ' ')}...' enthält nicht 'Neuer Warenkorb'."); return False
        except TimeoutException: logging.info(f"ℹ️ Session nicht aktiv: Warenkorb-Indikator ('{self.config.selectors.CART_NAVIGATION_INDICATOR}') nicht innerhalb {self.config.timeouts.SESSION_CHECK_TIMEOUT}s gefunden."); return False
        except Exception as e: logging.warning(f"❌ Fehler bei Autorisierungsprüfung (Warenkorb-Text): {e}"); return False

    def _handle_cookie_banner(self): # Unverändert
        try:
            logging.info("⏳ Warte auf Cookie-Banner..."); shadow_host_selector = self.config.selectors.COOKIE_ACCEPT_SHADOW_HOST
            button_selector_in_shadow_dom = self.config.selectors.COOKIE_ACCEPT_BUTTON
            WebDriverWait(self.driver, 7).until(EC.presence_of_element_located((By.CSS_SELECTOR, shadow_host_selector)))
            logging.debug(f"Cookie-Banner Shadow Host ('{shadow_host_selector}') gefunden."); time.sleep(1)
            script_to_execute = """
                const shadowHost = arguments[0]; const buttonSelector = arguments[1];
                if (shadowHost && shadowHost.shadowRoot) {
                    const btn = shadowHost.shadowRoot.querySelector(buttonSelector);
                    if (btn) { btn.click(); return true; }
                    else { console.warn("Cookie-Button NICHT im Shadow DOM. Selektor: " + buttonSelector); return false; }
                } else { console.warn("Usercentrics Root ('" + shadowHost + "') oder ShadowRoot NICHT gefunden."); return false; }"""
            shadow_host_element = self.driver.find_element(By.CSS_SELECTOR, shadow_host_selector)
            clicked_successfully = self.driver.execute_script(script_to_execute, shadow_host_element, button_selector_in_shadow_dom)
            if clicked_successfully: logging.info("🍪 'Alle akzeptieren' per JS geklickt."); time.sleep(2)
            else: logging.warning("Cookie-Button im JS nicht gefunden/geklickt.")
        except TimeoutException: logging.debug(f"Kein Cookie-Banner (Host '{self.config.selectors.COOKIE_ACCEPT_SHADOW_HOST}').")
        except Exception as e: logging.warning(f"⚠️ Fehler Cookie-Banner: {e}")

    def _perform_login(self):
        max_retries = 3
        for retry in range(max_retries):
            try:
                logging.info("🔐 Starte direkten Login-Prozess...")
                
                # Navigate to start page
                if self.config.settings.START_URL not in self.driver.current_url:
                    self.driver.get(self.config.settings.START_URL)
                    time.sleep(5)
                
                WebDriverWait(self.driver, self.config.timeouts.ELEMENT_SEARCH).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                logging.info("✅ Startseite für Login geladen")
                
                # Handle cookie popup
                self._handle_cookie_banner()
                
                # Find and click login button with multiple strategies
                login_button = self._find_login_button()
                if not login_button:
                    logging.error("❌ Login-Button nicht gefunden")
                    raise NoSuchElementException("Login button not found with any selector")
                
                self._safe_click(login_button, "login button")
                logging.info("✅ Login-Button geklickt")
                
                # Find and fill email field
                email_field = self._find_and_fill_email_field(self.username)
                if not email_field:
                    logging.error("❌ E-Mail-Feld nicht gefunden")
                    raise NoSuchElementException("Email field not found")
                
                # Click continue and wait for password page
                self._click_continue_and_wait_for_password_page()
                logging.info("✅ Weiter-Button geklickt und Seitenwechsel erkannt")
                
                # Enter password and submit
                self._enter_password_and_submit(self.password)
                logging.info("✅ Passwort eingegeben und Login abgeschickt")
                
                # Wait for successful redirect
                WebDriverWait(self.driver, self.config.timeouts.PAGE_LOAD).until(
                    EC.url_contains(self.config.settings.SHOP_BASE_DOMAIN)
                )
                logging.info(f"✅ Weiterleitung zu '{self.config.settings.SHOP_BASE_DOMAIN}' erfolgt.")
                time.sleep(3)
                
                # Verify login success
                self._verify_login_success()
                logging.info("✅ Login erfolgreich abgeschlossen!")
                return
                
            except Exception as e:
                logging.error(f"❌ Fehler beim Login (Versuch {retry + 1}/{max_retries}): {str(e)}")
                if retry < max_retries - 1:
                    wait_time = 300  # 5 Minuten
                    logging.info(f"⏳ Warte {wait_time} Sekunden vor nächstem Versuch...")
                    time.sleep(wait_time)
                    
                    # Versuche Browser neu zu starten
                    try:
                        if self.driver:
                            self.driver.quit()
                    except:
                        pass
                        
                    # Neuen Browser starten
                    self.driver = self.browser_manager.create_driver(proxy_address=self.current_proxy)
                else:
                    logging.error("Maximale Anzahl an Login-Versuchen erreicht. Fahre trotzdem fort...")
                    return  # Fahre trotz Fehler fort

    def _safe_click(self, element, description: str):
        """Sicherer Klick mit JavaScript-Fallback"""
        try:
            if not element.is_displayed():
                self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(1)
            element.click()
        except (ElementClickInterceptedException, ElementNotInteractableException):
            self.driver.execute_script("arguments[0].click();", element)

    def _find_login_button(self):
        """Finde Login-Button mit mehreren Strategien"""
        login_selectors = [
            "button[data-testid='login-button']",
            "button.login-button",
            "a[href*='login']",
            "a[href*='Login']",
            "button[aria-label*='Login']",
            "a[href*='account/login']",
            "a[href*='my-account']"
        ]
        
        for selector in login_selectors:
            try:
                element = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                if element and element.is_displayed():
                    logging.info(f"Login-Button gefunden mit Selektor: {selector}")
                    return element
            except TimeoutException:
                continue
        return None

    def _find_and_fill_email_field(self, email: str):
        """Finde E-Mail-Feld und fülle es aus"""
        email_selectors = [
            "input[name='isiwebuserid']",
            "input[type='text'][name*='user']",
            "input[class*='mdc-text-field__input']",
            "input[type='email']",
            "input[name*='email']",
            "input[name*='user']",
            "input[placeholder*='email']",
            "input[placeholder*='Email']"
        ]
        
        for selector in email_selectors:
            try:
                email_field = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                if email_field and email_field.is_displayed():
                    email_field.clear()
                    email_field.send_keys(email)
                    logging.info(f"E-Mail eingegeben mit Selektor: {selector}")
                    return email_field
            except TimeoutException:
                continue
        return None

    def _click_continue_and_wait_for_password_page(self):
        """Klicke Weiter-Button und warte auf Passwort-Seite"""
        continue_selectors = [
            "//button[contains(., 'Weiter')]",
            "//button[contains(., 'Continue')]",
            "//button[contains(., 'Next')]",
            "//input[@type='submit']",
            "//button[@type='submit']",
            "button[type='submit']",
            "input[type='submit']"
        ]
        
        continue_clicked = False
        for selector in continue_selectors:
            try:
                if selector.startswith("//"):
                    continue_button = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                else:
                    continue_button = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                
                if continue_button and continue_button.is_displayed():
                    self._safe_click(continue_button, "continue button")
                    continue_clicked = True
                    logging.info(f"Weiter-Button geklickt mit Selektor: {selector}")
                    break
            except TimeoutException:
                continue
        
        if not continue_clicked:
            raise NoSuchElementException("Continue button not found")
        
        # Wait for page transition
        logging.info("Warte auf Seitenwechsel...")
        try:
            # Wait for URL change
            WebDriverWait(self.driver, 30).until(
                lambda driver: "iam.viessmann.com" in driver.current_url or 
                              "login" in driver.current_url.lower() or
                              "auth" in driver.current_url.lower()
            )
            logging.info("Seitenwechsel über URL-Änderung erkannt")
        except TimeoutException:
            # If URL doesn't change, wait for password field to appear
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
                )
                logging.info("Seitenwechsel über Passwort-Feld erkannt")
            except TimeoutException:
                logging.warning("Kein klarer Seitenwechsel erkannt, fahre fort...")

    def _enter_password_and_submit(self, password: str):
        """Gebe Passwort ein und sende Login-Formular ab"""
        password_selectors = [
            "input#isiwebpasswd",
            "input[name='isiwebpasswd']",
            "input[type='password']",
            "input[name*='password']",
            "input[name*='pass']"
        ]
        
        password_field = None
        for selector in password_selectors:
            try:
                password_field = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                if password_field and password_field.is_displayed():
                    break
            except TimeoutException:
                continue
        
        if not password_field:
            raise NoSuchElementException("Password field not found")
        
        password_field.clear()
        password_field.send_keys(password)
        logging.info("Passwort eingegeben")
        
        # Submit login with multiple strategies
        submit_selectors = [
            "button#submitButton:not([disabled])",
            "button[type='submit']:not([disabled])",
            "input[type='submit']",
            "button:contains('Login')",
            "button:contains('Anmelden')",
            "button:contains('Sign In')"
        ]
        
        submit_clicked = False
        for selector in submit_selectors:
            try:
                if selector.startswith("button:contains("):
                    text = selector.split("'")[1] if "'" in selector else selector.split('"')[1]
                    xpath = f"//button[contains(text(), '{text}')]"
                    submit_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, xpath))
                    )
                else:
                    submit_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                
                if submit_button and submit_button.is_displayed():
                    self._safe_click(submit_button, "login submit button")
                    submit_clicked = True
                    logging.info(f"Login abgeschickt mit Selektor: {selector}")
                    break
            except TimeoutException:
                continue
        
        if not submit_clicked:
            raise NoSuchElementException("Login submit button not found")

    def _verify_login_success(self):
        """Überprüfe erfolgreichen Login"""
        success_selectors = [
            "div.user-menu",
            "a.account-link",
            "span.user-name",
            "div[class*='user']",
            "div[class*='account']",
            "div[class*='logged-in']",
            "div[class*='success']",
            "a[href*='logout']",
            "button[data-testid*='logout']",
            "div[class*='welcome']",
            "span[class*='user']"
        ]
        
        for selector in success_selectors:
            try:
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                if element and element.is_displayed():
                    logging.info(f"Login-Erfolg erkannt mit Selektor: {selector}")
                    return
            except TimeoutException:
                continue
        
        # Check URL for success indicators
        current_url = self.driver.current_url
        if any(indicator in current_url.lower() for indicator in ['account', 'dashboard', 'profile', 'my-account']):
            logging.info("Login-Erfolg über URL-Änderung erkannt")
            return
        
        raise Exception("Login success verification failed")

    def _load_articles_from_csv(self) -> list[str]: # Unverändert
        try:
            # Versuche verschiedene Encodings
            encodings = ['utf-8', 'utf-16', 'windows-1252', 'latin1']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(self.config.paths.input_file, sep=';', dtype=str, header=None, encoding=encoding)
                    logging.info(f"CSV erfolgreich mit Encoding {encoding} gelesen")
                    break
                except UnicodeDecodeError:
                    continue
                    
            if df is None:
                raise ValueError("Konnte CSV mit keinem der versuchten Encodings lesen")
                
            articles_raw = df.iloc[:, 0].dropna().unique().tolist(); articles = []
            if articles_raw:
                first_item_lower = str(articles_raw[0]).lower()
                if "artikelnummer" in first_item_lower or "art.-nr" in first_item_lower:
                    logging.info(f"Möglicher Header '{articles_raw[0]}' in CSV gefunden, wird übersprungen."); articles = articles_raw[1:]
                else: articles = articles_raw
            logging.info(f"📄 {len(articles)} einzigartige Artikel aus CSV geladen."); return articles
        except FileNotFoundError: logging.error(f"❌ Input-Datei nicht gefunden: {self.config.paths.input_file}"); return []
        except pd.errors.EmptyDataError: logging.error(f"❌ Input-Datei ist leer: {self.config.paths.input_file}"); return []
        except Exception as e: logging.error(f"❌ Fehler beim Lesen der CSV: {e}"); return []

    def _process_articles(self, articles: list[str]):
        if not articles: 
            logging.info("Keine Artikelnummern zum Verarbeiten.")
            return
        
        for i, article_number_raw in enumerate(articles):
            article_number = str(article_number_raw).strip()
            if not article_number: 
                logging.warning(f"Überspringe leere Artikelnummer Zeile {i+1}.")
                continue
            
            logging.info(f"Prüfe Session vor Verarbeitung von Artikel: {article_number} ({i+1}/{len(articles)})")
            if self.config.settings.SHOP_BASE_DOMAIN not in self.driver.current_url:
                logging.debug(f"Nicht auf Shop-Domain. Navigiere zu Startseite für Session-Check: {self.config.settings.START_URL}")
                self.driver.get(self.config.settings.START_URL)
                time.sleep(1)

            if not self._is_logged_in():
                logging.warning(f"Session für Artikel {article_number} nicht mehr gültig. Führe Relogin durch...")
                self._ensure_login()
                
            logging.info(f"--- Verarbeitung Artikel {i+1}/{len(articles)}: {article_number} (Direkte URL) ---")
            for attempt in range(self.config.settings.MAX_RETRIES):
                try:
                    data = self._scrape_article_data_by_url(article_number)
                    self._write_to_csv(data)
                    break
                except Exception as e:
                    logging.warning(f"Versuch {attempt+1}/{self.config.settings.MAX_RETRIES} für {article_number} fehlgeschlagen: {e}")
                    if attempt == self.config.settings.MAX_RETRIES - 1:
                        logging.error(f"❌ Konnte {article_number} nicht verarbeiten.")
                        self._write_to_csv(self._get_empty_data(article_number, error="Extraction Failed via Direct URL"))
                        wait_time = 300  # 5 Minuten
                        logging.info(f"⏳ Warte {wait_time} Sekunden vor nächstem Artikel...")
                        time.sleep(wait_time)
                    else:
                        time.sleep(self.config.settings.RETRY_DELAY_SECONDS)

    def _wait_for_element_safely(self, by_type, selector_str, timeout=None): # Unverändert
        if timeout is None: timeout = self.config.timeouts.FIELD_SEARCH_QUICK
        try: return WebDriverWait(self.driver, timeout).until(EC.presence_of_element_located((by_type, selector_str)))
        except TimeoutException: logging.debug(f"Timeout ({timeout}s): {by_type}='{selector_str}'"); return None
        except Exception as e: logging.warning(f"Anderer Fehler Warten {by_type}='{selector_str}': {e}"); return None
    
    # --- HIER DIE NEUE LOGIK FÜR MATERIAL_NR RETRY ---
    def _scrape_article_data_by_url(self, artikelnummer: str) -> dict:
        url = self.config.settings.ARTICLE_URL_TEMPLATE.format(artikelnummer=artikelnummer)
        
        data = {
            'artikelnummer': artikelnummer, 'material_nr': 'N/A', 'status': 'N/A', 
            'brutto': 'N/A', 'netto': 'N/A', 'verkaufshinweis': 'N/A', 
            'extraktions_quelle': 'N/A', 
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
            'status_text': 'Pending'
        }

        # Variable, um zu steuern, ob die alternativen Selektoren versucht werden sollen
        try_alternative_extraction = False

        # Schleife für maximal 3 Versuche (1 initial + 2 Wiederholungen) für die Haupt-Extraktion der material_nr
        for attempt_count in range(1, 4): 
            logging.info(f"Navigiere zu URL (Versuch {attempt_count}/3 für MaterialNr): {url}")
            try:
                self.driver.get(url)
                logging.debug(f"Prüfe auf Cookie-Banner auf Artikelseite (Versuch {attempt_count}): {artikelnummer}")
                self._handle_cookie_banner()
                time.sleep(0.5) # Kurze Pause nach Laden/Banner
            except TimeoutException as e:
                logging.error(f"Timeout URL {url} (Versuch {attempt_count}) mit Proxy {self.current_proxy}. Fehler: {e.msg}")
                s_path = self.config.paths.LOG_DIR / f"error_url_timeout_{artikelnummer.replace('/', '_')}_{datetime.now():%Y%m%d_%H%M%S}.png"; self.driver.save_screenshot(str(s_path)); logging.error(f"Screenshot: {s_path}")
                if attempt_count == 3: # Letzter Versuch
                    data['status_text'] = f"Failed (Timeout URL Load nach {attempt_count} Versuchen)"
                    return data # Daten mit Fehlerstatus zurückgeben
                time.sleep(self.config.settings.RETRY_DELAY_SECONDS); continue # Nächster Versuch der Schleife
            except WebDriverException as e:
                proxy_error_indicators = ["net::ERR_PROXY_CONNECTION_FAILED", "net::ERR_TUNNEL_CONNECTION_FAILED", "net::ERR_NAME_NOT_RESOLVED", "Unable to connect to the proxy server"]
                if any(indicator in e.msg for indicator in proxy_error_indicators):
                    logging.error(f"WebDriverException URL {url} (Versuch {attempt_count}) mit Proxy {self.current_proxy}. Proxy-Fehler: {e.msg}")
                    s_path = self.config.paths.LOG_DIR / f"error_webdriver_proxy_{artikelnummer.replace('/', '_')}_{datetime.now():%Y%m%d_%H%M%S}.png"; self.driver.save_screenshot(str(s_path)); logging.error(f"Screenshot: {s_path}")
                    sys.exit(f"Skript gestoppt: Proxy-Fehler ({e.msg}) mit {self.current_proxy} bei {url}.")
                else: 
                    logging.warning(f"WebDriverException URL {url} (Versuch {attempt_count}): {e.msg}. Nicht sicher ob Proxy."); 
                    if attempt_count == 3: data['status_text'] = f"Failed (WebDriverEx URL Load nach {attempt_count} Versuchen)"; return data
                    time.sleep(self.config.settings.RETRY_DELAY_SECONDS); continue
            except Exception as e_page_load:
                 logging.error(f"Allg. Fehler beim Laden von {url} (Versuch {attempt_count}): {e_page_load}")
                 if attempt_count == 3: data['status_text'] = f"Failed (Page Load Error nach {attempt_count} Versuchen)"; return data
                 time.sleep(self.config.settings.RETRY_DELAY_SECONDS); continue
            
            # Haupt-Extraktion versuchen
            try:
                logging.debug(f"Starte Haupt-Extraktion (Versuch {attempt_count})...")
                material_nr_element = self._wait_for_element_safely(By.CSS_SELECTOR, self.config.selectors.MATERIAL_NUMBER, timeout=self.config.timeouts.ELEMENT_SEARCH)
                if material_nr_element and material_nr_element.text.strip():
                    data['material_nr'] = material_nr_element.text.strip().split()[0]
                    data['extraktions_quelle'] = f'Haupt (Versuch {attempt_count})'
                    logging.debug(f"Haupt-MaterialNr: {data['material_nr']}")

                    status_element = self._wait_for_element_safely(By.CSS_SELECTOR, self.config.selectors.STATUS)
                    if status_element: data['status'] = status_element.text.strip()
                    
                    brutto_element = self._wait_for_element_safely(By.CSS_SELECTOR, self.config.selectors.PRICE_GROSS)
                    if brutto_element: data['brutto'] = brutto_element.text.strip()

                    netto_element = self._wait_for_element_safely(By.CSS_SELECTOR, self.config.selectors.PRICE_NET)
                    if netto_element: data['netto'] = netto_element.text.strip()
                    
                    sale_note_element = self._wait_for_element_safely(By.CSS_SELECTOR, self.config.selectors.SALE_NOTE)
                    if sale_note_element: data['verkaufshinweis'] = sale_note_element.text.strip()
                    
                    data['status_text'] = f"Success (Haupt - Versuch {attempt_count})"
                    return data # MaterialNr gefunden und Daten extrahiert, Funktion hier beenden
                else:
                    logging.warning(f"MaterialNr für {artikelnummer} bei Versuch {attempt_count} nicht gefunden oder leer.")
                    if attempt_count == 3: # Wenn dies der letzte Versuch der Haupt-Extraktion war
                        try_alternative_extraction = True # Flag setzen, um Alternative zu versuchen
                        data['extraktions_quelle'] = 'Fehler Haupt (alle Versuche)'
                        data['status_text'] = 'Failed (Haupt-MaterialNr N/A nach 3 Versuchen)'
                    # Kein break hier, damit die Schleife für den nächsten Versuch weiterläuft, falls material_nr N/A ist

            except Exception as e_main_extract:
                logging.warning(f"Fehler bei Haupt-Extraktion (Versuch {attempt_count}) für {artikelnummer}: {e_main_extract}")
                if attempt_count == 3: # Wenn dies der letzte Versuch war
                    try_alternative_extraction = True # Flag setzen
                    data['extraktions_quelle'] = f'Fehler Haupt Extraktion (Versuch {attempt_count})'
                    data['status_text'] = f'Error (Haupt-Extraktion Versuch {attempt_count})'
            
            if data['material_nr'] != 'N/A': # Sollte eigentlich durch return oben abgefangen werden
                 break # Aus der Retry-Schleife ausbrechen, wenn material_nr gefunden wurde

            if attempt_count < 3: # Nur schlafen, wenn weitere Versuche folgen
                 logging.debug(f"Warte {self.config.settings.RETRY_DELAY_SECONDS}s vor nächstem MaterialNr-Versuch für {artikelnummer}...")
                 time.sleep(self.config.settings.RETRY_DELAY_SECONDS)


        if try_alternative_extraction or data['material_nr'] == 'N/A':
            logging.warning(f"MaterialNr für {artikelnummer} nach Haupt-Extraktion (inkl. Wiederholungen) immer noch N/A oder Fehler. Versuche Alternative...")
            try:
                # Erneutes Laden der Seite für den alternativen Versuch, falls nicht schon im letzten Retry geschehen
                # Diese Bedingung ist wichtig, um doppeltes Laden zu vermeiden, wenn der letzte Hauptversuch die Seite schon geladen hat
                # Aber zur Sicherheit laden wir hier nochmal, falls der letzte Versuch ein reiner Extraktionsfehler war.
                logging.info(f"Lade URL {url} erneut für alternative Extraktion von {artikelnummer}")
                self.driver.get(url)
                self._handle_cookie_banner()
                time.sleep(1)

                logging.debug("Starte alternative Extraktion...")
                alt_material_nr_element = self._wait_for_element_safely(By.CSS_SELECTOR, self.config.selectors.ALT_MATERIAL_NUMBER, timeout=self.config.timeouts.ELEMENT_SEARCH)
                if alt_material_nr_element and alt_material_nr_element.text.strip():
                    data['material_nr'] = alt_material_nr_element.text.strip().split()[0]
                    data['extraktions_quelle'] = 'Alternative'
                    logging.debug(f"Alternative MaterialNr gefunden: {data['material_nr']}")

                    alt_status_element = self._wait_for_element_safely(By.CSS_SELECTOR, self.config.selectors.ALT_STATUS)
                    if alt_status_element: data['status'] = alt_status_element.text.strip()
                    
                    alt_brutto_element = self._wait_for_element_safely(By.CSS_SELECTOR, self.config.selectors.ALT_BRUTTO)
                    if alt_brutto_element: data['brutto'] = alt_brutto_element.text.strip()

                    alt_netto_element = self._wait_for_element_safely(By.CSS_SELECTOR, self.config.selectors.ALT_NETTO)
                    if alt_netto_element: data['netto'] = alt_netto_element.text.strip()
                    
                    alt_sale_note_element = self._wait_for_element_safely(By.CSS_SELECTOR, self.config.selectors.SALE_NOTE)
                    if alt_sale_note_element: data['verkaufshinweis'] = alt_sale_note_element.text.strip()
                    
                    data['status_text'] = "Success (Alternative)"
                else:
                    logging.warning(f"Alternative Materialnummer für {artikelnummer} ebenfalls nicht gefunden oder leer.")
                    if data['status_text'] == 'Pending' or "Failed (Haupt-MaterialNr N/A" in data['status_text'] or "Fehler Haupt" in data['extraktions_quelle']:
                         data['status_text'] = "Failed (Alle Pfade MaterialNr N/A)"
                    data['extraktions_quelle'] = 'Fehler Beide' # Überschreibt ggf. 'Fehler Haupt'
            except Exception as e_alt:
                logging.error(f"❌ Fehler bei alternativer Extraktion für {artikelnummer}: {e_alt}")
                data['status_text'] = "Error (Alternative Extraction)"
                data['extraktions_quelle'] = 'Fehler Alternative'
        
        if data['status_text'] == 'Pending': # Fallback-Status, falls Logik oben nicht greift
            data['status_text'] = "Failed (N/A nach allen Versuchen)" if data['material_nr'] == 'N/A' else "Success (Unspezifisch)"
            if data['material_nr'] == 'N/A' and data['extraktions_quelle'] == 'N/A':
                data['extraktions_quelle'] = 'Fehler Alle Pfade'
        
        logging.info(f"Finales Extraktionsergebnis {artikelnummer}: Quelle='{data['extraktions_quelle']}', Status='{data['status_text']}', MaterialNr='{data['material_nr']}'")
        return data
    # --- Ende der neuen Extraktionslogik ---
        
    def _get_empty_data(self, article_number: str, error: str) -> dict: # Unverändert
        return {"artikelnummer": article_number, "material_nr": "N/A", "status": "N/A", "brutto": "N/A", "netto": "N/A", 
                "verkaufshinweis": "N/A", "extraktions_quelle": "N/A",
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "status_text": error}

    def _write_to_csv(self, data: dict): # Unverändert
        out_file = self.config.paths.pd_file; out_file.parent.mkdir(exist_ok=True, parents=True)
        file_exists_and_has_content = out_file.exists() and out_file.stat().st_size > 0
        with open(out_file, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_FIELD_ORDER, delimiter=';', extrasaction='ignore')
            if not file_exists_and_has_content:
                header_writer = csv.writer(f, delimiter=';')
                header_writer.writerow(self.CSV_DISPLAY_HEADERS)
            writer.writerow(data) 
        logging.debug(f"Daten für Artikel {data['artikelnummer']} in CSV geschrieben.")

    def _save_cookies(self): # Unverändert
        c_file = self.config.paths.cookie_file; c_file.parent.mkdir(exist_ok=True, parents=True)
        with open(c_file, 'w') as f: json.dump(self.driver.get_cookies(), f, indent=4)
        logging.debug(f"Cookies zusätzlich gespeichert: {c_file}")

    def _load_cookies(self): # Unverändert
        c_file = self.config.paths.cookie_file
        if c_file.exists():
            logging.debug(f"Versuche Cookies aus {c_file} zu laden (Fallback für Profil)...")
            try:
                with open(c_file, 'r') as f: cookies = json.load(f)
                for cookie in cookies:
                    try: self.driver.add_cookie(cookie)
                    except Exception as e_add: logging.warning(f"Cookie nicht ladbar: {cookie.get('name', 'N/A')}. Fehler: {e_add}")
                logging.debug("Cookies aus JSON-Datei geladen (Fallback).")
            except Exception as e_load: logging.warning(f"Fehler beim Laden der Cookies aus JSON: {e_load}")
        else: logging.debug(f"Keine separate Cookie-Datei ({c_file}) gefunden.")

# =============================================================================
# === 5. HAUPTPROGRAMM (verbessert) ===
# =============================================================================
def parse_arguments():
    """Parse command line arguments for headless mode control"""
    parser = argparse.ArgumentParser(description='Viessmann Scraper mit Headless-Modus Kontrolle')
    parser.add_argument('--headless', action='store_true', 
                       help='Browser im Headless-Modus starten (Standard: True)')
    parser.add_argument('--no-headless', action='store_true',
                       help='Browser mit GUI starten (überschreibt --headless)')
    return parser.parse_args()

def main():
    config = None
    lock_file_path = None
    try:
        # Kommandozeilenargumente parsen
        args = parse_arguments()
        
        # Lock-Datei erstellen
        lock_file_path = ensure_single_instance("mein_skript", stale_lock_timeout=300)
        
        config = Config()
        
        # Headless-Modus basierend auf Argumenten setzen
        if args.no_headless:
            config.settings.HEADLESS_BROWSER = False
            print("🖥️  Browser wird mit GUI gestartet (--no-headless)")
        elif args.headless:
            config.settings.HEADLESS_BROWSER = True
            print("👻 Browser wird im Headless-Modus gestartet (--headless)")
        else:
            # Standard: Headless-Modus (wie in ScraperSettings definiert)
            print(f"👻 Browser wird im Headless-Modus gestartet (Standard: {config.settings.HEADLESS_BROWSER})")
        
        setup_logger(config.paths.LOG_DIR, config.paths.logfile_path, config.paths.ARCHIV_DIR)
        scraper = ViessmannScraper(config)
        scraper.run()
        logging.info("✅ Skript erfolgreich abgeschlossen")
        
    except SystemExit as e: 
        logging.info(f"Skript beendet mit Status: {e}")
        # Lock-Datei wird automatisch durch atexit entfernt
        
    except KeyboardInterrupt:
        logging.info("⚠️  Skript durch Benutzer unterbrochen (Ctrl+C)")
        # Lock-Datei wird automatisch durch signal_handler entfernt
        
    except Exception as e:
        if config and logging.getLogger().hasHandlers(): 
            logging.critical(f"❌ Kritischer Fehler: {e}", exc_info=True)
        else: 
            print(f"[{datetime.now()}] ❌ KRITISCH: Anwendung nicht startbar. Fehler: {e}")
        
        # Lock-Datei manuell entfernen bei Fehlern
        if lock_file_path and lock_file_path.exists():
            try:
                lock_file_path.unlink()
                print("🔓 Lock-Datei bei Fehler entfernt.")
            except Exception as cleanup_error:
                print(f"⚠️  Konnte Lock-Datei bei Fehler nicht entfernen: {cleanup_error}")
        
        raise  # Re-raise für Debugging

if __name__ == "__main__":
    main()