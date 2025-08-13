import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import subprocess
import csv
import urllib3
import certifi
import logging
import shutil
from pathlib import Path
from typing import List, Dict, Any
import glob

# Konfiguration
config = {
    'paths': {
        'base': Path('S:/PyScarper/Viesmann'),
        'bd': Path('S:/PyScarper/Viesmann/BD'),
        'pd_data': Path('S:/PyScarper/Viesmann/PD_Data'),
        'archiv': Path('S:/PyScarper/Viesmann/Archiv'),
        'input': Path('S:/PyScarper/Viesmann/Input'),
        'log': Path('S:/PyScarper/Viesmann/Log')
    },
    'files': {
        'main_db': 'shop.viessmann_BD.csv',
        'daily_file': 'VS_DN_Täglich.csv',
        'search_pattern': 'shop.viessmann_PD_*.csv'
    },
    'columns': {
        'article_id': 'Artikelnummer',
        'material_id': 'Material_Nr', # NEU: Spalte für Materialnummer hinzugefügt
        'timestamp': 'Zeitstempel'
    }
}

# Logging-Konfiguration
config['paths']['log'].mkdir(exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config['paths']['log'] / f'shop_Viessmann_BD_{timestamp}.txt', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def archiviere_datei(quellpfad: Path, archiv_ordner: Path, praefix: str) -> Path:
    """Archiviert eine Datei mit Zeitstempel."""
    if not quellpfad.exists():
        logging.warning(f"Quelldatei {quellpfad} nicht gefunden")
        return None
        
    archiv_ordner.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    archiv_name = f"{praefix}_{timestamp}{quellpfad.suffix}"
    ziel_pfad = archiv_ordner / archiv_name
    
    shutil.copy2(quellpfad, ziel_pfad)
    logging.info(f"Datei nach '{ziel_pfad}' archiviert")
    return ziel_pfad

def sammle_neue_daten(daten_ordner: Path, suchmuster: str) -> pd.DataFrame:
    """Sammelt und kombiniert Daten aus mehreren CSVs."""
    dateien = list(daten_ordner.glob(suchmuster))
    if not dateien:
        logging.warning(f"Keine Dateien mit Muster '{suchmuster}' gefunden")
        return pd.DataFrame()
        
    df_list = []
    for datei in dateien:
        try:
            # --- ANPASSUNG HIER ---
            # keep_default_na=False sorgt dafür, dass "N/A" als normaler Text gelesen wird.
            # na_values=[''] stellt sicher, dass echte leere Zellen weiterhin als fehlend erkannt werden.
            df_temp = pd.read_csv(datei, sep=';', decimal=',', dtype=str, keep_default_na=False, na_values=[''])
            if df_temp.shape[1] == 1:
                # Fallback für Komma-getrennte Dateien
                df_temp = pd.read_csv(datei, sep=',', decimal='.', dtype=str, keep_default_na=False, na_values=[''])
            # --- ENDE DER ANPASSUNG ---
            
            df_temp['source_file'] = datei.name
            df_list.append(df_temp)
        except Exception as e:
            logging.error(f"Fehler beim Lesen von {datei}: {e}")
            
    if not df_list:
        return pd.DataFrame()
        
    return pd.concat(df_list, ignore_index=True)

def verarbeite_und_dedupliziere_daten(df: pd.DataFrame, artikel_spalte: str, zeit_spalte: str) -> pd.DataFrame:
    """Sortiert und entfernt Duplikate basierend auf dem neuesten Zeitstempel."""
    if df.empty:
        return df
        
    # Konvertiere Artikelnummer zu Großbuchstaben, um Duplikate wie 'a1' und 'A1' zu finden
    df[artikel_spalte] = df[artikel_spalte].astype(str).str.upper()
    df[zeit_spalte] = pd.to_datetime(df[zeit_spalte], errors='coerce')
    
    # Sortieren, sodass die neuesten Einträge für jeden Artikel oben stehen
    df = df.sort_values(by=[artikel_spalte, zeit_spalte], ascending=[True, False])
    
    # Entferne Duplikate und behalte nur den neuesten Eintrag (den ersten nach der Sortierung)
    return df.drop_duplicates(subset=artikel_spalte, keep='first')

def aktualisiere_haupt_db(haupt_db_pfad: Path, neue_daten: pd.DataFrame, artikel_spalte: str) -> pd.DataFrame:
    """
    Aktualisiert die Haupt-CSV-Datei. Konvertiert relevante Spalten zu Großbuchstaben,
    kombiniert Daten und behält nur den neuesten Eintrag pro Artikel.
    """
    spalten_gross = [config['columns']['article_id'], config['columns']['material_id']]

    # Konvertiere Artikelnummer in neuen Daten zu Großbuchstaben für den Abgleich
    if not neue_daten.empty:
        neue_daten[artikel_spalte] = neue_daten[artikel_spalte].astype(str).str.upper()

    updated_df = pd.DataFrame()

    if haupt_db_pfad.exists():
        main_df = pd.read_csv(haupt_db_pfad, sep=';', decimal=',', dtype=str, keep_default_na=False, na_values=[''])
        # Konvertiere Artikelnummer in bestehenden Daten zu Großbuchstaben für den Abgleich
        if artikel_spalte in main_df.columns:
            main_df[artikel_spalte] = main_df[artikel_spalte].astype(str).str.upper()

        zeit_spalte = config['columns']['timestamp']
        if zeit_spalte not in main_df.columns:
            logging.error(f"Spalte '{zeit_spalte}' nicht in Hauptdatenbank gefunden. Überspringe Zusammenführung.")
            updated_df = neue_daten
        else:
            main_df[zeit_spalte] = pd.to_datetime(main_df[zeit_spalte], errors='coerce')
            if not neue_daten.empty:
                 neue_daten[zeit_spalte] = pd.to_datetime(neue_daten[zeit_spalte], errors='coerce')

            # Kombiniere alte und neue Daten
            combined_df = pd.concat([main_df, neue_daten], ignore_index=True)
            
            # Sortiere nach Artikelnummer und Zeitstempel (neueste zuerst)
            combined_df = combined_df.sort_values(by=[artikel_spalte, zeit_spalte], ascending=[True, False])
            
            # Behalte nur den neuesten Eintrag pro Artikelnummer
            updated_df = combined_df.drop_duplicates(subset=[artikel_spalte], keep='first')
            
            # Konvertiere Zeitstempel zurück in ein lesbares Format
            updated_df.loc[:, zeit_spalte] = pd.to_datetime(updated_df[zeit_spalte]).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            logging.info(f"Anzahl Artikel in BD: {len(updated_df)}")
            if not neue_daten.empty:
                logging.info(f"Anzahl aktualisierter Artikel: {len(updated_df[updated_df[artikel_spalte].isin(neue_daten[artikel_spalte])])}")

    else:
        logging.info("Hauptdatenbank existiert nicht, wird neu erstellt.")
        updated_df = neue_daten
    
    if not updated_df.empty:
        # Finale Konvertierung zu Großbuchstaben für alle relevanten Spalten im finalen DataFrame
        for spalte in spalten_gross:
            if spalte in updated_df.columns:
                updated_df.loc[:, spalte] = updated_df[spalte].astype(str).str.upper()
                logging.info(f"Spalte '{spalte}' in {haupt_db_pfad.name} final zu Großbuchstaben konvertiert.")

        updated_df.to_csv(haupt_db_pfad, sep=';', decimal=',', index=False, encoding='utf-8-sig', na_rep='N/A')
    
    return updated_df


def verschiebe_verarbeitete_dateien(dateiliste: List[Path], archiv_ordner: Path) -> None:
    """Verschiebt die Quelldateien nach der Verarbeitung."""
    archiv_ordner.mkdir(exist_ok=True)
    for datei in dateiliste:
        try:
            # Erstelle einen eindeutigeren Archivnamen
            timestamp = datetime.now().strftime('%Y%m%d')
            archiv_name = f"Archiv_{timestamp}_{datei.name}"
            ziel_pfad = archiv_ordner / archiv_name
            shutil.move(datei, ziel_pfad)
            logging.info(f"'{datei.name}' nach '{ziel_pfad}' verschoben")
        except Exception as e:
            logging.error(f"Fehler beim Verschieben von {datei}: {e}")

def read_csv_with_encoding(file_path: Path, **kwargs) -> pd.DataFrame:
    encodings = ['utf-16', 'utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    for encoding in encodings:
        try:
            return pd.read_csv(file_path, encoding=encoding, **kwargs)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Konnte Datei {file_path} mit keinem der Encodings {encodings} lesen")

def aktualisiere_und_sortiere_tagesdatei(tagesdatei_pfad: Path, haupt_db_pfad: Path, 
                                          artikel_spalte: str, zeit_spalte: str) -> None:
    """
    Aktualisiert und sortiert die Tagesdatei. Konvertiert relevante Spalten 
    zu Großbuchstaben, um Konsistenz zu gewährleisten.
    """
    if not tagesdatei_pfad.exists():
        logging.warning(f"Tagesdatei {tagesdatei_pfad} nicht gefunden")
        return
        
    try:
        daily_df = read_csv_with_encoding(tagesdatei_pfad, sep=';', low_memory=False, dtype=str)
        
        logging.info(f"Spalten in {tagesdatei_pfad.name}: {list(daily_df.columns)}")
        
        # Überprüfe und normalisiere Spaltennamen
        spalten_mapping = {
            'Artikelnummer': ['Artikelnummer', 'Artikel-Nr', 'ArtNr', 'Artikel_Nr']
        }
        
        # Definiere die Spalten, die in Großbuchstaben konvertiert werden sollen
        spalten_gross = [config['columns']['article_id'], config['columns']['material_id']]
        
        for standard_name, varianten in spalten_mapping.items():
            gefunden = False
            for variante in varianten:
                if variante in daily_df.columns:
                    if variante != standard_name:
                        daily_df = daily_df.rename(columns={variante: standard_name})
                    gefunden = True
                    break
            if not gefunden:
                logging.error(f"Spalte '{standard_name}' oder eine ihrer Varianten nicht in Tagesdatei gefunden")
                return
                
        # Material_Nr nur umbenennen, falls vorhanden
        if 'Material_Nr' in daily_df.columns:
            daily_df['Material_Nr'] = daily_df['Material_Nr'].astype(str).str.upper()
            logging.info(f"Spalte 'Material_Nr' in {tagesdatei_pfad.name} zu Großbuchstaben konvertiert.")
        else:
            logging.info("Spalte 'Material_Nr' nicht in Tagesdatei – wird übersprungen.")

        # Text in den relevanten Spalten in Großbuchstaben umwandeln
        for spalte in spalten_gross:
            if spalte in daily_df.columns:
                daily_df[spalte] = daily_df[spalte].astype(str).str.upper()
                logging.info(f"Spalte '{spalte}' in {tagesdatei_pfad.name} zu Großbuchstaben konvertiert.")

        # Duplikat-Prüfung (jetzt case-insensitive, da alles großgeschrieben ist)
        if daily_df.duplicated(subset=[artikel_spalte]).any():
            duplikate = daily_df[daily_df.duplicated(subset=[artikel_spalte], keep=False)]
            logging.warning(f"Gefundene Duplikate in {tagesdatei_pfad.name}:")
            logging.warning(f"Anzahl Duplikate: {len(duplikate)}")
            logging.warning(f"Betroffene Artikelnummern: {duplikate[artikel_spalte].unique().tolist()}")
            
            # Behalte nur den ersten Eintrag pro Artikelnummer
            daily_df = daily_df.drop_duplicates(subset=[artikel_spalte], keep='first')
            logging.info(f"Duplikate entfernt. Neue Anzahl Einträge: {len(daily_df)}")
        
        # Zeitstempel aus der Haupt-DB hinzufügen oder aktualisieren
        if haupt_db_pfad.exists():
            bd_df = pd.read_csv(haupt_db_pfad, sep=';', decimal=',', dtype=str, usecols=[artikel_spalte, zeit_spalte])
            
            # Alte Zeitstempel-Spalte entfernen, falls vorhanden, um Konflikte zu vermeiden
            if zeit_spalte in daily_df.columns:
                daily_df = daily_df.drop(columns=[zeit_spalte])

            daily_df = daily_df.merge(bd_df, on=artikel_spalte, how='left')
        else:
            # Sicherstellen, dass die Zeitstempel-Spalte existiert, auch wenn die DB leer ist
            if zeit_spalte not in daily_df.columns:
                daily_df[zeit_spalte] = None

        # Sortierlogik
        daily_df['sort_date'] = pd.to_datetime(daily_df[zeit_spalte], errors='coerce')
        daily_df['has_date'] = daily_df['sort_date'].notna()
        
        daily_df = daily_df.sort_values(by=['has_date', 'sort_date'], ascending=[True, True])
        
        logging.info(f"Sortierstatistik für {tagesdatei_pfad.name}:")
        logging.info(f"Gesamtanzahl Einträge: {len(daily_df)}")
        logging.info(f"Einträge ohne Datum: {len(daily_df[~daily_df['has_date']])}")
        logging.info(f"Einträge mit Datum: {len(daily_df[daily_df['has_date']])}")
        
        daily_df = daily_df.drop(columns=['sort_date', 'has_date'])
        daily_df.to_csv(tagesdatei_pfad, sep=';', index=False, encoding='utf-8-sig')
        
    except Exception as e:
        logging.error(f"Fehler bei der Verarbeitung der Tagesdatei: {e}")
        return


def main():
    """Hauptfunktion für den ETL-Prozess."""
    try:
        # Schritt 1: Hauptdatenbank archivieren
        main_db_pfad = config['paths']['bd'] / config['files']['main_db']
        if main_db_pfad.exists():
            archiviere_datei(main_db_pfad, config['paths']['archiv'], 'archiv_shop.viessmann_BD')
            
        # Schritt 2: Neue Daten aus PD_Data sammeln
        dateien_zu_verarbeiten = list(config['paths']['pd_data'].glob(config['files']['search_pattern']))
        if dateien_zu_verarbeiten:
            neue_daten = sammle_neue_daten(
                config['paths']['pd_data'], 
                config['files']['search_pattern']
            )
            
            if not neue_daten.empty:
                # Schritt 3: Daten verarbeiten (Duplikate innerhalb der neuen Daten entfernen)
                verarbeitete_daten = verarbeite_und_dedupliziere_daten(
                    neue_daten,
                    config['columns']['article_id'],
                    config['columns']['timestamp']
                )
                
                # Schritt 4: Hauptdatenbank aktualisieren
                aktualisiere_haupt_db(
                    main_db_pfad,
                    verarbeitete_daten,
                    config['columns']['article_id']
                )
                
            # Schritt 5: Verarbeitete Dateien verschieben
            verschiebe_verarbeitete_dateien(dateien_zu_verarbeiten, config['paths']['archiv'])
        else:
            logging.info("Keine neuen PD-Dateien zur Verarbeitung gefunden.")
            # Dennoch die Haupt-DB "aktualisieren", um Großschreibung sicherzustellen
            aktualisiere_haupt_db(main_db_pfad, pd.DataFrame(), config['columns']['article_id'])


        # Schritt 6: Tagesdatei aktualisieren und sortieren
        aktualisiere_und_sortiere_tagesdatei(
            config['paths']['input'] / config['files']['daily_file'],
            main_db_pfad,
            config['columns']['article_id'],
            config['columns']['timestamp']
        )
        
        logging.info("Alle Aufgaben erfolgreich abgeschlossen")
        
    except Exception as e:
        logging.error(f"Ein schwerwiegender Fehler ist aufgetreten: {e}", exc_info=True)

if __name__ == '__main__':
    main()