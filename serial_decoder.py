"""
Serial Number & Model Number Decoder for HVAC Equipment
Extracts manufacture date, capacity, and equipment specs from serial/model numbers.
Supports: Trane, Carrier, Bryant, Lennox, York, Goodman, Rheem, Daikin, Ingersoll Rand,
          Greenheck, Loren Cook, Reznor, Heil, American Standard, Payne, Modine
"""

import re
from datetime import datetime
from typing import Optional


def decode_serial(serial: str, brand: str, model: str = "") -> dict:
    """Main entry point. Returns dict with manufacture_year, manufacture_date, capacity_tons, etc."""
    if not serial or not brand:
        return {}
    serial = str(serial).strip().upper()
    brand = str(brand).strip().upper()
    model = str(model).strip().upper() if model else ""

    brand_map = {
        "TRANE": _decode_trane,
        "AMERICAN STANDARD": _decode_trane,  # Same parent company, same serial format
        "CARRIER": _decode_carrier,
        "BRYANT": _decode_carrier,  # Same as Carrier
        "PAYNE": _decode_carrier,  # Same as Carrier
        "LENNOX": _decode_lennox,
        "YORK": _decode_york,
        "GOODMAN": _decode_goodman,
        "AMANA": _decode_goodman,  # Same parent
        "RHEEM": _decode_rheem,
        "RUUD": _decode_rheem,  # Same as Rheem
        "DAIKIN": _decode_daikin,
        "DAKIN": _decode_daikin,  # Common misspelling in data
        "HEIL": _decode_heil,
        "REZNOR": _decode_reznor,
        "INGERSOLL RAND": _decode_ingersoll_rand,
        "GREENHECK": _decode_greenheck,
        "LOREN COOK": _decode_loren_cook,
        "COOK": _decode_loren_cook,
        "MODINE": _decode_modine,
    }

    decoder = brand_map.get(brand)
    result = {}
    if decoder:
        result = decoder(serial) or {}

    model_info = decode_model_number(model, brand)
    if model_info:
        for k, v in model_info.items():
            if k not in result or result[k] is None:
                result[k] = v

    return result


# --- TRANE ---
def _decode_trane(serial: str) -> dict:
    """Trane serials: Year code in first digit or first few chars.
    Modern format (2002+): YWWxxxxxxx where Y=year letter, WW=week
    Year letters: B=2002,C=2003,...,Y=2024 (skipping I,O,Q,U)
    Older format: digits with year embedded."""
    result = {}
    trane_year_letters = {}
    skip = set("IOQU")
    year = 2002
    for c in "ABCDEFGHJKLMNPRSTUVWXYZ":
        if c not in skip:
            trane_year_letters[c] = year
            year += 1
            if year > 2030:
                break

    if serial and serial[0].isalpha() and serial[0] in trane_year_letters:
        result["manufacture_year"] = trane_year_letters[serial[0]]
        if len(serial) > 2 and serial[1:3].isdigit():
            week = int(serial[1:3])
            if 1 <= week <= 52:
                result["manufacture_week"] = week
    elif len(serial) >= 9:
        # Older Trane: positions vary, try extracting year from digits
        for i in range(len(serial) - 3):
            chunk = serial[i:i+4]
            if chunk.isdigit():
                yr = int(chunk)
                if 1980 <= yr <= 2030:
                    result["manufacture_year"] = yr
                    break
    return result


# --- CARRIER / BRYANT / PAYNE ---
def _decode_carrier(serial: str) -> dict:
    """Carrier serials: Weekly format post-2000.
    Format: WWYYXXXXXX where WW=week, YY=year
    Some older: first 4 digits = week+year"""
    result = {}
    cleaned = re.sub(r'[^A-Z0-9]', '', serial)

    if len(cleaned) >= 4 and cleaned[:4].isdigit():
        ww = int(cleaned[:2])
        yy = int(cleaned[2:4])
        if 1 <= ww <= 52 and 0 <= yy <= 99:
            yr = 2000 + yy if yy < 50 else 1900 + yy
            result["manufacture_year"] = yr
            result["manufacture_week"] = ww
            return result

    # Try alternate: some Carrier use position 2-5
    if len(cleaned) >= 6 and cleaned[2:6].isdigit():
        ww = int(cleaned[2:4])
        yy = int(cleaned[4:6])
        if 1 <= ww <= 52 and 0 <= yy <= 99:
            yr = 2000 + yy if yy < 50 else 1900 + yy
            result["manufacture_year"] = yr
            result["manufacture_week"] = ww

    return result


# --- LENNOX ---
def _decode_lennox(serial: str) -> dict:
    """Lennox serials: Format varies.
    Modern (5900-series+): digits 3-4 = year, digit 5 = month letter
    Older: First 4 digits encode week/year."""
    result = {}
    cleaned = re.sub(r'[^A-Z0-9]', '', serial)

    if len(cleaned) >= 4:
        # Try: positions 1-2 = year
        for start in [0, 2]:
            if start + 2 <= len(cleaned) and cleaned[start:start+2].isdigit():
                yy = int(cleaned[start:start+2])
                if 0 <= yy <= 40:
                    result["manufacture_year"] = 2000 + yy
                    return result
                elif 70 <= yy <= 99:
                    result["manufacture_year"] = 1900 + yy
                    return result

    # 5900/5901 format
    match = re.match(r'(\d{4})(\w)(\d+)', cleaned)
    if match:
        prefix = match.group(1)
        if prefix.startswith("59"):
            yy = int(prefix[2:4])
            result["manufacture_year"] = 2000 + yy if yy < 50 else 1900 + yy

    return result


# --- YORK ---
def _decode_york(serial: str) -> dict:
    """York serials: Format W[M/G]MMBYYNNN
    B = build code, YY = year, or
    First letter = plant, next chars vary.
    Common: letters 3-4 encode month, 5-6 encode year."""
    result = {}
    cleaned = re.sub(r'[^A-Z0-9]', '', serial)

    york_month = {"A":1,"B":2,"C":3,"D":4,"E":5,"F":6,"G":7,"H":8,"J":9,"K":10,"L":11,"M":12}

    if len(cleaned) >= 6:
        # Try: position 2 = month letter, position 3-4 = year
        if cleaned[2] in york_month and cleaned[3:5].isdigit():
            yy = int(cleaned[3:5])
            result["manufacture_year"] = 2000 + yy if yy < 50 else 1900 + yy
            result["manufacture_month"] = york_month[cleaned[2]]
            return result

    # Try numeric patterns
    if len(cleaned) >= 4:
        for i in range(len(cleaned) - 3):
            if cleaned[i:i+2].isdigit():
                yy = int(cleaned[i:i+2])
                if 0 <= yy <= 30:
                    result["manufacture_year"] = 2000 + yy
                    return result

    return result


# --- GOODMAN / AMANA ---
def _decode_goodman(serial: str) -> dict:
    """Goodman serials: Format YYMMNNNNN
    YY = year, MM = month. Since ~2006."""
    result = {}
    cleaned = re.sub(r'[^0-9]', '', serial)

    if len(cleaned) >= 4:
        yy = int(cleaned[:2])
        mm = int(cleaned[2:4])
        if 0 <= yy <= 40 and 1 <= mm <= 12:
            result["manufacture_year"] = 2000 + yy
            result["manufacture_month"] = mm
            return result

    return result


# --- RHEEM / RUUD ---
def _decode_rheem(serial: str) -> dict:
    """Rheem serials: Various formats.
    Modern: First letter = month (A-M skipping I), next 2 digits = year.
    Older: MMYY at start."""
    result = {}
    cleaned = re.sub(r'[^A-Z0-9]', '', serial)
    rheem_month = {"A":1,"B":2,"C":3,"D":4,"E":5,"F":6,"G":7,"H":8,"J":9,"K":10,"L":11,"M":12}

    if cleaned and cleaned[0] in rheem_month and len(cleaned) >= 3 and cleaned[1:3].isdigit():
        yy = int(cleaned[1:3])
        result["manufacture_year"] = 2000 + yy if yy < 50 else 1900 + yy
        result["manufacture_month"] = rheem_month[cleaned[0]]
        return result

    # Fallback: MMYY
    if len(cleaned) >= 4 and cleaned[:4].isdigit():
        mm = int(cleaned[:2])
        yy = int(cleaned[2:4])
        if 1 <= mm <= 12:
            result["manufacture_year"] = 2000 + yy if yy < 50 else 1900 + yy
            result["manufacture_month"] = mm

    return result


# --- DAIKIN ---
def _decode_daikin(serial: str) -> dict:
    """Daikin serials: Format varies. Often XYYMMNNNNN or letter-based year."""
    result = {}
    cleaned = re.sub(r'[^A-Z0-9]', '', serial)

    daikin_year = {"E":2004,"F":2005,"G":2006,"H":2007,"J":2008,"K":2009,
                   "L":2010,"M":2011,"N":2012,"P":2013,"R":2014,"S":2015,
                   "T":2016,"U":2017,"V":2018,"W":2019,"X":2020,"Y":2021,"A":2022,"B":2023,"C":2024}

    if cleaned and cleaned[0] in daikin_year:
        result["manufacture_year"] = daikin_year[cleaned[0]]
        if len(cleaned) >= 3 and cleaned[1:3].isdigit():
            mm = int(cleaned[1:3])
            if 1 <= mm <= 12:
                result["manufacture_month"] = mm
        return result

    # Numeric fallback
    if len(cleaned) >= 4 and cleaned[:4].isdigit():
        yy = int(cleaned[:2])
        if 0 <= yy <= 30:
            result["manufacture_year"] = 2000 + yy

    return result


# --- HEIL ---
def _decode_heil(serial: str) -> dict:
    """Heil (ICP/Carrier family): Similar to Carrier format."""
    return _decode_carrier(serial)


# --- REZNOR ---
def _decode_reznor(serial: str) -> dict:
    """Reznor: Often YYMM or YYMMNNNN format."""
    result = {}
    cleaned = re.sub(r'[^0-9]', '', serial)
    if len(cleaned) >= 4:
        yy = int(cleaned[:2])
        mm = int(cleaned[2:4])
        if 0 <= yy <= 40 and 1 <= mm <= 12:
            result["manufacture_year"] = 2000 + yy
            result["manufacture_month"] = mm
    return result


# --- INGERSOLL RAND ---
def _decode_ingersoll_rand(serial: str) -> dict:
    """Ingersoll Rand: Trane parent company. Try Trane format first, then generic."""
    result = _decode_trane(serial)
    if result:
        return result
    cleaned = re.sub(r'[^0-9]', '', serial)
    if len(cleaned) >= 4:
        yy = int(cleaned[:2])
        if 0 <= yy <= 30:
            result["manufacture_year"] = 2000 + yy
    return result


# --- GREENHECK ---
def _decode_greenheck(serial: str) -> dict:
    """Greenheck fans: Serial often contains date code.
    Common: YYWWNNNNN or similar."""
    result = {}
    cleaned = re.sub(r'[^0-9]', '', serial)
    if len(cleaned) >= 4:
        yy = int(cleaned[:2])
        ww = int(cleaned[2:4])
        if 0 <= yy <= 40 and 1 <= ww <= 52:
            result["manufacture_year"] = 2000 + yy
            result["manufacture_week"] = ww
            return result
    # Fallback: look for 4-digit year in serial
    match = re.search(r'(20[0-2]\d)', serial)
    if match:
        result["manufacture_year"] = int(match.group(1))
    return result


# --- LOREN COOK / COOK ---
def _decode_loren_cook(serial: str) -> dict:
    """Cook fans: Serial varies widely. Look for date patterns."""
    result = {}
    match = re.search(r'(20[0-2]\d)', serial)
    if match:
        result["manufacture_year"] = int(match.group(1))
        return result
    cleaned = re.sub(r'[^0-9]', '', serial)
    if len(cleaned) >= 4:
        yy = int(cleaned[:2])
        if 0 <= yy <= 30:
            result["manufacture_year"] = 2000 + yy
    return result


# --- MODINE ---
def _decode_modine(serial: str) -> dict:
    """Modine heaters: Often YYWW or YYMM prefix."""
    result = {}
    cleaned = re.sub(r'[^0-9]', '', serial)
    if len(cleaned) >= 4:
        yy = int(cleaned[:2])
        mm = int(cleaned[2:4])
        if 0 <= yy <= 40 and 1 <= mm <= 12:
            result["manufacture_year"] = 2000 + yy
            result["manufacture_month"] = mm
    return result


# --- MODEL NUMBER DECODER ---
def decode_model_number(model: str, brand: str = "") -> dict:
    """Extract capacity (tons) and unit type from model numbers."""
    if not model:
        return {}
    result = {}
    model = str(model).upper().strip()

    # Extract tonnage from model number
    # Common pattern: 024=2ton, 030=2.5ton, 036=3ton, 042=3.5ton, 048=4ton, 060=5ton, etc.
    # These are BTU/1000 values, divide by 12 to get tons
    btu_match = re.search(r'(?<![0-9])(\d{3})(?![0-9])', model)
    if btu_match:
        btu_code = int(btu_match.group(1))
        btu_to_tons = {
            18: 1.5, 24: 2, 30: 2.5, 36: 3, 42: 3.5, 48: 4, 60: 5,
            72: 6, 90: 7.5, 102: 8.5, 120: 10, 150: 12.5, 180: 15,
            210: 17.5, 240: 20, 300: 25, 360: 30, 480: 40, 600: 50
        }
        if btu_code in btu_to_tons:
            result["capacity_tons"] = btu_to_tons[btu_code]

    # Extract SEER from model if present
    seer_match = re.search(r'(\d{2})(?:SEER|SE)', model)
    if seer_match:
        seer = int(seer_match.group(1))
        if 8 <= seer <= 30:
            result["seer_rating"] = seer

    return result


# --- ASHRAE EXPECTED LIFESPAN TABLE ---
ASHRAE_LIFESPAN = {
    "HVAC UNIT": 20,
    "RTU": 20,
    "ROOFTOP UNIT": 20,
    "SPLIT SYSTEM": 18,
    "PACKAGE UNIT": 20,
    "CHILLER": 25,
    "BOILER": 25,
    "FURNACE": 20,
    "AIR HANDLER": 20,
    "AHU": 20,
    "EXHAUST FAN": 20,
    "SUPPLY FAN": 25,
    "FAN": 20,
    "CONDENSER": 20,
    "HEAT PUMP": 16,
    "MINI SPLIT": 15,
    "GENERATOR": 30,
    "ICE MACHINE": 10,
    "LIFT": 20,
    "AIR COMPRESSOR": 15,
    "SHOP AIR COMPRESSOR": 15,
    "AIR BAG SYSTEM": 15,
    "GRINDER": 15,
    "COOLING TOWER": 25,
    "UNIT HEATER": 18,
    "MAKE UP AIR": 20,
    "MAU": 20,
    "EF": 20,  # Exhaust Fan
    "SF": 25,  # Supply Fan
}


def get_expected_lifespan(asset_type: str, description: str = "") -> int:
    """Return expected lifespan in years based on asset type and description."""
    asset_type = str(asset_type).upper().strip() if asset_type else ""
    description = str(description).upper().strip() if description else ""

    # Check description first for more specificity
    for key, years in ASHRAE_LIFESPAN.items():
        if key in description:
            return years

    # Then check asset type
    for key, years in ASHRAE_LIFESPAN.items():
        if key in asset_type:
            return years

    return 20  # Default for unknown HVAC equipment
