# OVAJ KOD SYNCHRONIZIRA STANJE ZALIHA OD WHOLESALER-ovog CSV-a U SHOPIFY PRODAVAONICU PREMA SKU-u

import os
import io
import requests
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

STORE = os.getenv("SHOPIFY_STORE")
TOKEN = os.getenv("SHOPIFY_TOKEN")
CSV_URL = os.getenv("SUPPLIER_FEED")
SKU_COLUMN = "sku"
QTY_COLUMN = "available_stock"

HEADERS = {"X-Shopify-Access-Token": TOKEN}

def send_mail(subject: str, body: str):
    """Send an email notice after sync completes"""
    import smtplib
    from email.mime.text import MIMEText

    sender = os.getenv("MAIL_FROM")
    password = os.getenv("SMTP_PASSWORD")
    receiver = os.getenv("MAIL_TO")
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT", 587))

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = receiver

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())
        print("📧 Email sent successfully!")
    except Exception as e:
        print(f"⚠️ Failed to send email: {e}")

def get_all_shopify_variants():
    """Fetch all product variants (with SKU + inventory_item_id) from Shopify"""
    all_variants = []
    page_info = None

    print("🔄 Fetching all Shopify products...")

    while True:
        url = f"https://{STORE}/admin/api/2024-10/variants.json?limit=250"
        if page_info:
            url += f"&page_info={page_info}"

        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        data = r.json()
        variants = data.get("variants", [])
        all_variants.extend(variants)

        link_header = r.headers.get("Link")
        if link_header and 'rel="next"' in link_header:
            import re
            match = re.search(r"page_info=([^&>]+)", link_header)
            page_info = match.group(1) if match else None
        else:
            break

    print(f"✅ Found {len(all_variants)} variants in your Shopify store")
    return all_variants


def get_first_location_id():
    """Fetch your first (default) Shopify location ID"""
    r = requests.get(f"https://{STORE}/admin/api/2024-10/locations.json", headers=HEADERS)
    r.raise_for_status()
    return r.json()["locations"][0]["id"]


def set_inventory(location_id: int, inventory_item_id: int, qty: int):
    """Update the stock quantity for the given item"""
    payload = {
        "location_id": location_id,
        "inventory_item_id": inventory_item_id,
        "available": int(qty)
    }
    r = requests.post(f"https://{STORE}/admin/api/2024-10/inventory_levels/set.json", headers=HEADERS, json=payload)
    if r.status_code != 200:
        print(f"⚠️ Error updating inventory for item {inventory_item_id}: {r.text}")


def load_csv_data(csv_url: str):
    print("🔽 Downloading CSV...")
    r = requests.get(csv_url)
    r.raise_for_status()
    csv_data = r.text
    df = pd.read_csv(io.StringIO(csv_data), sep=';', dtype=str)
    df[QTY_COLUMN] = df[QTY_COLUMN].fillna(0).astype(int)
    print(f"✅ Loaded {len(df)} products from Dreamlove CSV")
    return df


def main():
    df = load_csv_data(CSV_URL)
    shopify_variants = get_all_shopify_variants()
    location_id = get_first_location_id()

    shopify_lookup = {
        v["sku"].strip(): v["inventory_item_id"]
        for v in shopify_variants
        if v["sku"]
    }

    matched_rows = df[df[SKU_COLUMN].isin(shopify_lookup.keys())]
    print(f"🔍 Matched {len(matched_rows)} SKUs between Dreamlove CSV and Shopify store")

    updated_count = 0

    for _, row in matched_rows.iterrows():
        sku = row[SKU_COLUMN].strip()
        qty = int(row[QTY_COLUMN])
        inventory_item_id = shopify_lookup.get(sku)
        if inventory_item_id:
            set_inventory(location_id, inventory_item_id, qty)
            updated_count += 1
            print(f"✅ Updated {sku} → qty={qty}")

    print(f"\n🏁 Done! Updated {updated_count} matching products.")

    # Send email notification
    subject = "Shopify Dreamlove's Stock Sync Completed"
    body = (
        f"Shopify–Dreamlove stock sync finished successfully.\n\n"
        f"Updated products: {updated_count}"
    )
    send_mail(subject, body)

if __name__ == "__main__":
    main()
