# Projekt: Viessmann Shop Scraper

## 1. Übersicht

* **Zweck:** Das Skript automatisiert die Extraktion von Produktdaten aus dem Viessmann Online-Shop. Es löst das Problem der manuellen Datensammlung von Artikelinformationen wie Materialnummern, Preisen, Verfügbarkeiten und Verkaufshinweisen.
* **Prozess:** Das Skript verbindet sich mit dem Viessmann Shop, führt eine automatische Anmeldung durch, lädt Artikelnummern aus einer CSV-Datei, navigiert zu den jeweiligen Produktseiten und extrahiert systematisch alle relevanten Produktdaten. Die Ergebnisse werden in eine strukturierte CSV-Datei mit Zeitstempel gespeichert.

## 2. Funktionalität

* **Automatische Anmeldung:** Robuste Login-Prozedur mit Session-Management und Cookie-Handling
* **Proxy-Unterstützung:** Verwendung von Proxy-Servern für stabile Verbindungen
* **CSV-Datenverarbeitung:** Einlesen von Artikelnummern aus strukturierten CSV-Dateien
* **Web-Scraping:** Automatische Navigation und Datenextraktion von Produktseiten
* **Fehlerbehandlung:** Umfassende Retry-Logik und Fehlerbehandlung bei Netzwerkproblemen
* **Lock-Mechanismus:** Verhindert gleichzeitige Ausführung mehrerer Instanzen
* **Logging-System:** Detaillierte Protokollierung aller Vorgänge
* **Headless-Browser:** Optionale Ausführung ohne GUI für Server-Umgebungen
* **Datenvalidierung:** Überprüfung der extrahierten Daten auf Vollständigkeit

## 3. Voraussetzungen

* **System:** Windows, Linux, macOS (plattformunabhängig dank pathlib)
* **Software:**
  * Python 3.10+
  * Chrome Browser (für WebDriver)
  * pip (Python Package Manager)

## 4. Installation & Konfiguration

### 4.1 Repository klonen

```bash
git clone [repository-url]
cd Viesmann
```

### 4.2 Virtuelle Umgebung erstellen

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate     # Windows
```

### 4.3 Abhängigkeiten installieren

```bash
pip install -r requirements.txt
```

### 4.4 Projektstruktur einrichten

Erstelle folgende Verzeichnisstruktur:

```text
Viesmann/
├── main/
│   └── Viessmann_Parsing_V08.py
├── Input/
│   ├── VS_DN_Täglich.csv          # Artikelnummern (eine pro Zeile)
│   └── viessmann.txt              # Login-Credentials
├── PD_Data/                       # Ausgabedateien
├── Log/                           # Log-Dateien
├── Archiv/                        # Archivierte Logs
├── cookie/                        # Cookie-Speicherung
├── user-data-dir/                 # Browser-Profil
└── Proxy/
    └── Proxy_IP.txt               # Proxy-Liste (optional)
```

### 4.5 Konfigurationsdateien einrichten

#### Login-Credentials (`Input/viessmann.txt`)

```txt
email=ihre.email@domain.com
password=ihr_passwort
```

#### Artikelnummern (`Input/VS_DN_Täglich.csv`)

```csv
Artikelnummer
12345678
87654321
11223344
```

#### Proxy-Liste (`Proxy/Proxy_IP.txt`) - Optional

```txt
# Kommentarzeilen beginnen mit #
proxy1.example.com:8080
proxy2.example.com:3128
proxy3.example.com:80
```

## 5. Verwendung

### 5.1 Standardausführung

```bash
python main/Viessmann_Parsing_V08.py
```

### 5.2 Kommandozeilenargumente

```bash
# Browser mit GUI starten (für Debugging)
python main/Viessmann_Parsing_V08.py --no-headless

# Browser im Headless-Modus starten (Standard)
python main/Viessmann_Parsing_V08.py --headless
```

**Argumente:**

* `--headless`: Startet Browser im Headless-Modus (Standard)
* `--no-headless`: Startet Browser mit GUI (überschreibt --headless)

## 6. Code-Struktur

* `main/Viessmann_Parsing_V08.py`: Hauptskript mit allen Scraping-Funktionen
* `Input/`: Eingabedateien (CSV mit Artikelnummern, Login-Credentials)
* `PD_Data/`: Ausgabedateien (CSV mit extrahierten Produktdaten)
* `Log/`: Protokollierungsdateien
* `Archiv/`: Archivierte Log-Dateien
* `cookie/`: Gespeicherte Browser-Cookies
* `user-data-dir/`: Chrome-Browser-Profil
* `Proxy/`: Proxy-Konfigurationsdateien

## 7. Abhängigkeiten

### 7.1 requirements.txt

```txt
pandas>=1.5.0
selenium>=4.0.0
webdriver-manager>=3.8.0
requests>=2.28.0
urllib3>=1.26.0
```

### 7.2 Verwendete Bibliotheken

* **pandas**: CSV-Dateiverarbeitung und Datenmanipulation
* **selenium**: Web-Browser-Automatisierung
* **webdriver-manager**: Automatische ChromeDriver-Verwaltung
* **requests**: HTTP-Anfragen für Proxy-Tests
* **urllib3**: HTTP-Client-Bibliothek
* **pathlib**: Plattformunabhängige Pfadbehandlung
* **dataclasses**: Datenklassen für Konfiguration
* **logging**: Protokollierung
* **csv**: CSV-Datei-Operationen
* **json**: JSON-Dateiverarbeitung
* **time**: Zeitverzögerungen und Timestamps
* **random**: Zufällige Proxy-Auswahl
* **os**: Betriebssystem-Operationen
* **sys**: System-spezifische Parameter
* **shutil**: Dateioperationen
* **argparse**: Kommandozeilenargumente
* **atexit**: Aufräumfunktionen beim Beenden
* **signal**: Signalbehandlung
* **datetime**: Datums- und Zeitfunktionen

## 8. Ausgabeformat

### 8.1 CSV-Struktur

Die Ausgabedatei enthält folgende Spalten:

* **Zeitstempel**: Zeitpunkt der Extraktion
* **Artikelnummer**: Ursprüngliche Artikelnummer
* **Material_Nr**: Extrahierte Materialnummer
* **Status**: Verfügbarkeitsstatus
* **Brutto**: Bruttopreis
* **Netto**: Nettopreis
* **Verkaufshinweis**: Zusätzliche Verkaufsinformationen
* **Extraktions_Quelle**: Quelle der Datenextraktion (Haupt/Alternative)
* **status_text**: Detaillierter Status der Extraktion

### 8.2 Dateiname

Ausgabedateien werden mit Zeitstempel benannt:

```text
shop.viessmann_PD_2024.01.15_143022.csv
```

## 9. Fehlerbehandlung

### 9.1 Automatische Wiederholungen

* **URL-Laden**: 3 Versuche bei Timeout-Fehlern
* **Datenextraktion**: 3 Versuche für Materialnummern
* **Login-Prozess**: 3 Versuche bei Anmeldefehlern

### 9.2 Proxy-Fehler

Bei Proxy-Verbindungsfehlern wird das Skript automatisch beendet, um Datenverlust zu vermeiden.

### 9.3 Lock-Mechanismus

Verhindert gleichzeitige Ausführung mehrerer Instanzen durch Lock-Dateien.

## 10. Monitoring

### 10.1 Log-Dateien

* **Aktuelle Logs**: `Log/log_Viessmann_Shop.txt`
* **Archivierte Logs**: `Archiv/log_viessmann_YYYY.MM.DD_HHMMSS.txt`

### 10.2 Log-Level

* **WARNING**: Wichtige Informationen und Warnungen
* **ERROR**: Fehler und Ausnahmen
* **DEBUG**: Detaillierte Debug-Informationen

## 11. Sicherheitshinweise

* Login-Credentials werden in separater Datei gespeichert
* Proxy-Konfiguration ist optional
* Lock-Dateien verhindern Datenkonflikte
* Automatische Bereinigung bei Skript-Beendigung

## 12. Troubleshooting

### 12.1 Häufige Probleme

* **ChromeDriver-Fehler**: Automatische Installation über webdriver-manager
* **Proxy-Verbindungsfehler**: Überprüfung der Proxy-Liste
* **Login-Fehler**: Überprüfung der Credentials-Datei
* **CSV-Lesefehler**: Überprüfung der Datei-Encodings

### 12.2 Debug-Modus

Verwende `--no-headless` für visuelle Fehlerdiagnose.

## 13. Wartung

* Regelmäßige Überprüfung der Proxy-Liste
* Archivierung alter Log-Dateien
* Aktualisierung der ChromeDriver-Version
* Überprüfung der Login-Credentials
