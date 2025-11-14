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
NAME_COLUMN = "name"

HEADERS = {"X-Shopify-Access-Token": TOKEN}


# ---------------------------------------------
# EMAIL SENDING
# ---------------------------------------------
def send_mail(subject: str, body: str):
    import smtplib
    import ssl
    from email.mime.text import MIMEText

    sender = os.getenv("MAIL_FROM")
    receiver = os.getenv("MAIL_TO")
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    smtp_user = os.getenv("SMTP_USERNAME")
    smtp_pass = os.getenv("SMTP_PASSWORD")

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = receiver

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls(context=context)
            server.login(smtp_user, smtp_pass)
            server.sendmail(sender, [receiver], msg.as_string())
        print("📧 Email sent successfully!")
    except Exception as e:
        print(f"⚠️ Failed to send email: {e}")


# ---------------------------------------------
# LOAD SHOPIFY VARIANTS (with sku + inventory ID)
# ---------------------------------------------
def get_all_shopify_variants():
    all_variants = []
    page_info = None

    print("🔄 Fetching all Shopify variants...")

    while True:
        url = (
            f"https://{STORE}/admin/api/2024-10/variants.json"
            f"?limit=250&fields=id,sku,inventory_item_id"
        )
        if page_info:
            url += f"&page_info={page_info}"

        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        variants = r.json().get("variants", [])
        all_variants.extend(variants)

        # pagination
        link_header = r.headers.get("Link")
        if link_header and 'rel="next"' in link_header:
            import re
            match = re.search(r"page_info=([^&>]+)", link_header)
            page_info = match.group(1) if match else None
        else:
            break

    print(f"✅ Found {len(all_variants)} variants in your Shopify store")
    return all_variants


# ---------------------------------------------
# OTHER SHOPIFY HELPERS
# ---------------------------------------------
def get_first_location_id():
    r = requests.get(f"https://{STORE}/admin/api/2024-10/locations.json", headers=HEADERS)
    r.raise_for_status()
    return r.json()["locations"][0]["id"]


def set_inventory(location_id: int, inventory_item_id: int, qty: int):
    payload = {
        "location_id": location_id,
        "inventory_item_id": inventory_item_id,
        "available": int(qty),
    }
    r = requests.post(
        f"https://{STORE}/admin/api/2024-10/inventory_levels/set.json",
        headers=HEADERS,
        json=payload,
    )
    if r.status_code != 200:
        print(f"⚠️ Error updating inventory for item {inventory_item_id}: {r.text}")


# ---------------------------------------------
# LOAD SUPPLIER CSV
# ---------------------------------------------
def load_csv_data(csv_url: str):
    print("🔽 Downloading CSV...")
    r = requests.get(csv_url)
    r.raise_for_status()
    csv_data = r.text

    df = pd.read_csv(io.StringIO(csv_data), sep=";", dtype=str)

    df[QTY_COLUMN] = df[QTY_COLUMN].fillna(0).astype(int)
    df[NAME_COLUMN] = df[NAME_COLUMN].fillna("Unknown Product")
    df[SKU_COLUMN] = df[SKU_COLUMN].fillna("")

    print(f"✅ Loaded {len(df)} products from Dreamlove CSV")
    return df


# ---------------------------------------------
# MAIN SYNC
# ---------------------------------------------
def main():
    df = load_csv_data(CSV_URL)
    shopify_variants = get_all_shopify_variants()
    location_id = get_first_location_id()

    # Shopify sku → variant info
    shopify_lookup = {
        v["sku"].strip(): v
        for v in shopify_variants
        if v.get("sku")
    }

    matched_rows = df[df[SKU_COLUMN].isin(shopify_lookup.keys())]
    print(f"🔍 Matched {len(matched_rows)} SKUs")

    updated_items = []
    updated_count = 0

    for _, row in matched_rows.iterrows():
        sku = row[SKU_COLUMN].strip()
        qty = int(row[QTY_COLUMN])
        name = row[NAME_COLUMN].strip()

        variant = shopify_lookup.get(sku)
        if variant:
            inventory_item_id = variant["inventory_item_id"]
            set_inventory(location_id, inventory_item_id, qty)
            updated_count += 1

            updated_items.append(f"{name} – {sku} → {qty}")
            print(f"✅ Updated {name} ({sku}) → qty={qty}")

    print(f"\n🏁 Done! Updated {updated_count} products.")

    # EMAIL CONTENT
    body = (
        f"Shopify–Dreamlove stock sync finished successfully.\n"
        f"Updated products: {updated_count}\n\n"
        + "\n".join(updated_items)
    )

    send_mail("Shopify Stock Sync Completed", body)


if __name__ == "__main__":
    main()
