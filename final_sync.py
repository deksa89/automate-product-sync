import os
import io
import re
import time
import requests
import pandas as pd
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------
# ENV SETTINGS
# ---------------------------------------------
STORE = os.getenv("SHOPIFY_STORE")
TOKEN = os.getenv("SHOPIFY_TOKEN")
CSV_URL = os.getenv("SUPPLIER_FEED")

API_VERSION = "2024-10"

SKU_COLUMN = "sku"
QTY_COLUMN = "available_stock"
NAME_COLUMN = "name"
PRICE_COLUMN = "recommended_sale_price_with_taxes"

HEADERS = {
    "X-Shopify-Access-Token": TOKEN,
    "Content-Type": "application/json",
}


# ---------------------------------------------
# BASIC ENV VALIDATION
# ---------------------------------------------
def validate_env():
    missing = []

    if not STORE:
        missing.append("SHOPIFY_STORE")

    if not TOKEN:
        missing.append("SHOPIFY_TOKEN")

    if not CSV_URL:
        missing.append("SUPPLIER_FEED")

    if missing:
        raise Exception(f"Missing required .env values: {missing}")


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

    if not all([sender, receiver, smtp_server, smtp_user, smtp_pass]):
        print("⚠️ Email settings are missing. Skipping email.")
        return

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
# PRICE CLEANING
# ---------------------------------------------
def clean_price(value):
    """
    Converts supplier price to Shopify-compatible price string.

    Examples:
    84.99 -> "84.99"
    "84.99" -> "84.99"
    "84,99" -> "84.99"
    "1.234,56" -> "1234.56"
    "€84.99" -> "84.99"
    """

    if value is None or pd.isna(value):
        return None

    price = str(value).strip()

    if price == "":
        return None

    price = price.replace("€", "")
    price = price.replace("EUR", "")
    price = price.replace(" ", "")

    # European format: 1.234,56
    if "." in price and "," in price:
        price = price.replace(".", "").replace(",", ".")
    else:
        # Format: 84,99
        price = price.replace(",", ".")

    # Remove anything that is not a digit or dot
    price = re.sub(r"[^0-9.]", "", price)

    # If somehow there are multiple dots, price is invalid
    if price.count(".") > 1:
        return None

    try:
        decimal_price = Decimal(price).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP
        )
    except (InvalidOperation, ValueError):
        return None

    if decimal_price <= 0:
        return None

    return str(decimal_price)


# ---------------------------------------------
# QUANTITY CLEANING
# ---------------------------------------------
def clean_quantity(value):
    if value is None or pd.isna(value):
        return 0

    value = str(value).strip()

    if value == "":
        return 0

    try:
        return int(float(value))
    except ValueError:
        return 0


# ---------------------------------------------
# SHOPIFY REQUEST HELPER
# ---------------------------------------------
def shopify_request(method, url, **kwargs):
    """
    Small retry helper for Shopify API.
    Useful if Shopify returns 429 rate limit.
    """

    max_retries = 5

    for attempt in range(max_retries):
        response = requests.request(method, url, headers=HEADERS, **kwargs)

        if response.status_code == 429:
            wait_seconds = 2 + attempt
            print(f"⏳ Shopify rate limit. Waiting {wait_seconds}s...")
            time.sleep(wait_seconds)
            continue

        return response

    return response


# ---------------------------------------------
# LOAD SHOPIFY VARIANTS
# ---------------------------------------------
def get_all_shopify_variants():
    all_variants = []
    page_info = None

    print("🔄 Fetching all Shopify variants...")

    while True:
        url = (
            f"https://{STORE}/admin/api/{API_VERSION}/variants.json"
            f"?limit=250&fields=id,sku,inventory_item_id,price"
        )

        if page_info:
            url += f"&page_info={page_info}"

        response = shopify_request("GET", url)
        response.raise_for_status()

        variants = response.json().get("variants", [])
        all_variants.extend(variants)

        link_header = response.headers.get("Link")

        if link_header and 'rel="next"' in link_header:
            match = re.search(r"page_info=([^&>]+)", link_header)
            page_info = match.group(1) if match else None
        else:
            break

    print(f"✅ Found {len(all_variants)} variants in your Shopify store")

    return all_variants


# ---------------------------------------------
# SHOPIFY HELPERS
# ---------------------------------------------
def get_first_location_id():
    print("🔄 Fetching Shopify location...")

    url = f"https://{STORE}/admin/api/{API_VERSION}/locations.json"

    response = shopify_request("GET", url)
    response.raise_for_status()

    locations = response.json().get("locations", [])

    if not locations:
        raise Exception("No Shopify locations found.")

    location_id = locations[0]["id"]

    print(f"✅ Using location ID: {location_id}")

    return location_id


def set_inventory(location_id: int, inventory_item_id: int, qty: int):
    payload = {
        "location_id": location_id,
        "inventory_item_id": inventory_item_id,
        "available": int(qty),
    }

    url = f"https://{STORE}/admin/api/{API_VERSION}/inventory_levels/set.json"

    response = shopify_request("POST", url, json=payload)

    if response.status_code != 200:
        print(f"⚠️ Error updating inventory for item {inventory_item_id}: {response.text}")
        return False

    return True


def set_variant_price(variant_id: int, price: str):
    payload = {
        "variant": {
            "id": variant_id,
            "price": price,
        }
    }

    url = f"https://{STORE}/admin/api/{API_VERSION}/variants/{variant_id}.json"

    response = shopify_request("PUT", url, json=payload)

    if response.status_code != 200:
        print(f"⚠️ Error updating price for variant {variant_id}: {response.text}")
        return False

    return True


# ---------------------------------------------
# LOAD SUPPLIER CSV
# ---------------------------------------------
def load_csv_data(csv_url: str):
    print("🔽 Downloading supplier CSV...")

    response = requests.get(csv_url)
    response.raise_for_status()

    csv_data = response.text

    df = pd.read_csv(io.StringIO(csv_data), sep=";", dtype=str)

    required_columns = [
        SKU_COLUMN,
        QTY_COLUMN,
        NAME_COLUMN,
        PRICE_COLUMN,
    ]

    missing_columns = [column for column in required_columns if column not in df.columns]

    if missing_columns:
        raise Exception(f"Missing required columns in CSV: {missing_columns}")

    df[SKU_COLUMN] = df[SKU_COLUMN].fillna("").astype(str).str.strip()
    df[NAME_COLUMN] = df[NAME_COLUMN].fillna("Unknown Product").astype(str).str.strip()
    df[QTY_COLUMN] = df[QTY_COLUMN].apply(clean_quantity)
    df[PRICE_COLUMN] = df[PRICE_COLUMN].fillna("").astype(str).str.strip()

    # Remove rows without SKU
    df = df[df[SKU_COLUMN] != ""]

    print(f"✅ Loaded {len(df)} products from supplier CSV")

    print("\n🔎 CSV price preview:")
    print(df[[SKU_COLUMN, NAME_COLUMN, PRICE_COLUMN]].head(10).to_string(index=False))

    return df


# ---------------------------------------------
# MAIN SYNC
# ---------------------------------------------
def main():
    validate_env()

    df = load_csv_data(CSV_URL)
    shopify_variants = get_all_shopify_variants()
    location_id = get_first_location_id()

    # Shopify SKU → variant info
    shopify_lookup = {
        v["sku"].strip(): v
        for v in shopify_variants
        if v.get("sku")
    }

    matched_rows = df[df[SKU_COLUMN].isin(shopify_lookup.keys())]

    print(f"\n🔍 Matched {len(matched_rows)} SKUs")

    updated_items = []

    updated_stock_count = 0
    failed_stock_count = 0

    updated_price_count = 0
    unchanged_price_count = 0
    skipped_price_count = 0
    failed_price_count = 0

    for _, row in matched_rows.iterrows():
        sku = row[SKU_COLUMN].strip()
        name = row[NAME_COLUMN].strip()
        qty = int(row[QTY_COLUMN])

        supplier_price = clean_price(row[PRICE_COLUMN])

        variant = shopify_lookup.get(sku)

        if not variant:
            continue

        variant_id = variant["id"]
        inventory_item_id = variant["inventory_item_id"]
        current_shopify_price = clean_price(variant.get("price"))

        # ---------------------------------------------
        # UPDATE STOCK
        # ---------------------------------------------
        stock_ok = set_inventory(location_id, inventory_item_id, qty)

        if stock_ok:
            updated_stock_count += 1
        else:
            failed_stock_count += 1

        # ---------------------------------------------
        # UPDATE PRICE
        # ---------------------------------------------
        price_message = ""

        if supplier_price is None:
            skipped_price_count += 1
            price_message = "price skipped - invalid supplier price"

            print(f"⚠️ {name} ({sku}) → qty={qty}, invalid price skipped")

        elif current_shopify_price == supplier_price:
            unchanged_price_count += 1
            price_message = f"price unchanged {supplier_price}"

            print(f"✅ {name} ({sku}) → qty={qty}, price unchanged={supplier_price}")

        else:
            price_ok = set_variant_price(variant_id, supplier_price)

            if price_ok:
                updated_price_count += 1
                price_message = f"price {current_shopify_price} → {supplier_price}"

                print(
                    f"✅ {name} ({sku}) → "
                    f"qty={qty}, price {current_shopify_price} → {supplier_price}"
                )
            else:
                failed_price_count += 1
                price_message = f"price update failed, wanted {supplier_price}"

                print(
                    f"⚠️ {name} ({sku}) → "
                    f"qty={qty}, price update failed, wanted {supplier_price}"
                )

        updated_items.append(
            f"{name} – {sku} → qty: {qty}, {price_message}"
        )

    # ---------------------------------------------
    # FINAL REPORT
    # ---------------------------------------------
    print("\n🏁 Done!")
    print(f"Matched SKUs: {len(matched_rows)}")
    print(f"Updated stock: {updated_stock_count}")
    print(f"Failed stock updates: {failed_stock_count}")
    print(f"Updated prices: {updated_price_count}")
    print(f"Unchanged prices: {unchanged_price_count}")
    print(f"Skipped prices: {skipped_price_count}")
    print(f"Failed price updates: {failed_price_count}")

    body = (
        "Shopify–Dreamlove stock and price sync finished.\n\n"
        f"Matched SKUs: {len(matched_rows)}\n"
        f"Updated stock: {updated_stock_count}\n"
        f"Failed stock updates: {failed_stock_count}\n"
        f"Updated prices: {updated_price_count}\n"
        f"Unchanged prices: {unchanged_price_count}\n"
        f"Skipped prices: {skipped_price_count}\n"
        f"Failed price updates: {failed_price_count}\n\n"
        + "\n".join(updated_items[:500])
    )

    if len(updated_items) > 500:
        body += f"\n\n...and {len(updated_items) - 500} more items."

    send_mail("Shopify Stock & Price Sync Completed", body)


if __name__ == "__main__":
    main()
