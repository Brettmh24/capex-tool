"""
XOi Dynamic Data API Client
Handles OAuth2 authentication, equipment creation, and specification retrieval.
API docs: https://integrations-api.xoi.io/docs
"""

import requests
import time
from typing import Optional


XOI_BASE_URL = "https://integrations-api.xoi.io"
XOI_TOKEN_URL = f"{XOI_BASE_URL}/oauth2/token"
XOI_SCOPES = "customers/read customers/write sites/read sites/write equipment/read equipment/write specifications/read"


class XOiClient:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expires_at = 0

    def _ensure_token(self):
        """Get or refresh OAuth2 access token."""
        if self.access_token and time.time() < self.token_expires_at - 60:
            return
        resp = requests.post(XOI_TOKEN_URL, data={
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": XOI_SCOPES,
        }, headers={"Content-Type": "application/x-www-form-urlencoded"})
        resp.raise_for_status()
        data = resp.json()
        self.access_token = data["access_token"]
        self.token_expires_at = time.time() + data.get("expires_in", 86400)

    def _headers(self) -> dict:
        self._ensure_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    # --- Customer ---
    def create_customer(self, name: str, external_id: str, identifier: str = "") -> dict:
        resp = requests.post(f"{XOI_BASE_URL}/customers", json={
            "external_id": external_id,
            "name": name,
            "identifier": identifier or external_id[:20],
        }, headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    def get_customers(self) -> list:
        resp = requests.get(f"{XOI_BASE_URL}/customers", headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    # --- Site ---
    def create_site(self, customer_id: str, name: str, external_id: str,
                    address: str = "", city: str = "", state: str = "", postal_code: str = "") -> dict:
        payload = {
            "external_id": external_id,
            "name": name,
        }
        if address:
            payload["address_line_1"] = address
        if city:
            payload["city"] = city
        if state:
            payload["state"] = state
        if postal_code:
            payload["postal_code"] = postal_code

        resp = requests.post(f"{XOI_BASE_URL}/customers/{customer_id}/sites",
                             json=payload, headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    # --- Equipment ---
    def create_equipment(self, site_id: str, name: str, make: str, model: str,
                         serial: str, external_id: str) -> dict:
        """Create an equipment record. XOi will asynchronously match specs."""
        resp = requests.post(f"{XOI_BASE_URL}/sites/{site_id}/equipment", json={
            "external_id": external_id,
            "name": name,
            "make": make,
            "model": model,
            "serial": serial,
        }, headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    def get_specification(self, equipment_id: str) -> Optional[dict]:
        """Retrieve specification data for an equipment record.
        Returns None if specs aren't ready yet."""
        resp = requests.get(f"{XOI_BASE_URL}/equipment/{equipment_id}/specification",
                            headers=self._headers())
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def get_equipment(self, equipment_id: str) -> dict:
        resp = requests.get(f"{XOI_BASE_URL}/equipment/{equipment_id}",
                            headers=self._headers())
        resp.raise_for_status()
        return resp.json()


def enrich_with_xoi(client: XOiClient, customer_id: str, site_id: str,
                    assets: list, progress_callback=None) -> list:
    """Send a batch of assets to XOi and retrieve specs.

    Args:
        client: Authenticated XOiClient
        customer_id: XOi customer UUID
        site_id: XOi site UUID
        assets: List of dicts with keys: brand, model_no, serial_no, tag_id, asset_description
        progress_callback: Optional callable(current, total, message) for UI updates

    Returns:
        List of dicts with enriched data per asset (keyed by tag_id or serial_no)
    """
    results = []
    total = len(assets)
    equipment_ids = {}

    # Step 1: Create all equipment records
    for i, asset in enumerate(assets):
        tag = str(asset.get("tag_id", asset.get("serial_no", f"asset-{i}")))
        brand = str(asset.get("brand", "")).strip()
        model = str(asset.get("model_no", "")).strip()
        serial = str(asset.get("serial_no", "")).strip()
        name = str(asset.get("asset_description", f"Unit {tag}")).strip()

        if not brand or not serial or brand.lower() in ("nan", "") or serial.lower() in ("nan", ""):
            continue

        if progress_callback:
            progress_callback(i + 1, total, f"Sending {brand} {serial[:15]}...")

        try:
            equip = client.create_equipment(
                site_id=site_id,
                name=name[:100],
                make=brand,
                model=model if model.lower() != "nan" else "",
                serial=serial,
                external_id=f"capex-{tag}",
            )
            equipment_ids[tag] = equip["id"]
        except requests.exceptions.HTTPError as e:
            # Skip duplicates or errors, continue with rest
            if e.response.status_code == 409:
                # Equipment already exists — try to find it
                pass
            continue
        except Exception:
            continue

    if not equipment_ids:
        return results

    # Step 2: Wait for XOi to process, then retrieve specs
    if progress_callback:
        progress_callback(0, len(equipment_ids), "Waiting for XOi to process specs...")

    # Give XOi time to process — poll with backoff
    time.sleep(5)

    for i, (tag, equip_id) in enumerate(equipment_ids.items()):
        if progress_callback:
            progress_callback(i + 1, len(equipment_ids), f"Retrieving specs for {tag}...")

        spec = None
        for attempt in range(3):
            spec = client.get_specification(equip_id)
            if spec:
                break
            time.sleep(2 * (attempt + 1))

        if spec:
            enriched = {"tag_id": tag, "xoi_equipment_id": equip_id}

            # Extract classification
            classification = spec.get("classification", {})
            enriched["xoi_equipment_type"] = classification.get("type_display", "")
            enriched["xoi_equipment_subtype"] = classification.get("subtype_display", "")
            enriched["xoi_domain"] = classification.get("domain", "")

            # Extract dataplate match quality
            dataplate = spec.get("dataplate", {})
            enriched["xoi_match_status"] = dataplate.get("match_status", "")

            # Extract specifications
            specifications = spec.get("specifications", {})

            # Lifecycle — asset age
            for item in specifications.get("Lifecycle", []):
                if item.get("field_key") == "assetAge":
                    enriched["xoi_asset_age"] = item.get("field_value")

            # Cooling — capacity in tons
            for item in specifications.get("Cooling", []):
                if item.get("field_key") == "coolingNominalOutputTons":
                    enriched["xoi_capacity_tons"] = item.get("field_value")

            # Heating
            for item in specifications.get("Heating", []):
                if "btu" in item.get("field_key", "").lower():
                    enriched["xoi_heating_btu"] = item.get("field_value")

            # Electrical
            for item in specifications.get("Electrical", []):
                if item.get("field_key") == "voltage":
                    enriched["xoi_voltage"] = item.get("field_value")

            # Refrigerant
            for item in specifications.get("Refrigerant", []):
                if "type" in item.get("field_key", "").lower():
                    enriched["xoi_refrigerant"] = item.get("field_value")

            # Grab all other specs as a flat dict
            for category, items in specifications.items():
                for item in items:
                    key = f"xoi_{item.get('field_key', 'unknown')}"
                    if key not in enriched:
                        enriched[key] = item.get("field_value")

            results.append(enriched)

    return results
