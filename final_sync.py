import os
import io
import json
import re
import time
import requests
import pandas as pd
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------
# ENV SETTINGS
# ---------------------------------------------
STORE = os.getenv("SHOPIFY_STORE")
TOKEN = os.getenv("SHOPIFY_TOKEN")
CSV_URL = os.getenv("SUPPLIER_FEED")

API_VERSION = "2024-10"
GRAPHQL_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-07")
GRAPHQL_URL = f"https://{STORE}/admin/api/{GRAPHQL_API_VERSION}/graphql.json"

ANCHOR_REFERENCE_DATE = date.fromisoformat(
    os.getenv("ANCHOR_REFERENCE_DATE", "2026-09-10")
)
ANCHOR_CURRENCY = os.getenv("ANCHOR_CURRENCY", "EUR")
LOCAL_TZ = ZoneInfo("Europe/Zagreb")

SKU_COLUMN = "sku"
QTY_COLUMN = "available_stock"
NAME_COLUMN = "name"
PRICE_COLUMN = "recommended_sale_price_with_taxes"
DISCOUNT_MULTIPLIER = Decimal("0.90")  # 10% cheaper than recommended price

HEADERS = {
    "X-Shopify-Access-Token": TOKEN,
    "Content-Type": "application/json",
}


# ---------------------------------------------
# ENV VALIDATION
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
    Converts price values to Shopify-compatible string.

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

    price = re.sub(r"[^0-9.]", "", price)

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


def price_to_decimal(value):
    cleaned = clean_price(value)

    if cleaned is None:
        return None

    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None

def calculate_discounted_price(value):
    """
    Calculates Shopify price as 10% cheaper than supplier recommended price.

    Example:
    recommended_sale_price_with_taxes = 84.99
    Shopify price = 76.49
    """

    original_price = price_to_decimal(value)

    if original_price is None:
        return None

    discounted_price = (original_price * DISCOUNT_MULTIPLIER).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP
    )

    if discounted_price <= 0:
        return None

    return str(discounted_price)


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
    Retry helper for Shopify API.
    Handles basic 429 rate-limit responses.
    """

    max_retries = 5
    response = None

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
# GRAPHQL HELPERS FOR NEW-PRODUCT ANCHOR PRICES
# ---------------------------------------------
ANCHOR_VARIANTS_QUERY = """
query AnchorVariants($first: Int!, $after: String) {
  productVariants(first: $first, after: $after) {
    nodes {
      id
      legacyResourceId
      sku
      createdAt
      price
      compareAtPrice
      anchorPrice: metafield(namespace: "custom", key: "anchor_price") { value }
      anchorDate: metafield(namespace: "custom", key: "anchor_date") { value }
      anchorSource: metafield(namespace: "custom", key: "anchor_source") { value }
      product {
        id
        title
        status
        publishedAt
        onlineStoreUrl
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

SET_ANCHOR_METAFIELDS_MUTATION = """
mutation SetAnchorMetafields($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key value type }
    userErrors { field message code }
  }
}
"""


def graphql_request(query, variables=None):
    payload = {"query": query, "variables": variables or {}}

    for attempt in range(6):
        response = requests.post(
            GRAPHQL_URL,
            headers=HEADERS,
            json=payload,
            timeout=60,
        )

        if response.status_code == 429 or response.status_code >= 500:
            wait_seconds = min(2 ** attempt, 20)
            print(f"⏳ Shopify GraphQL retry in {wait_seconds}s...")
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        data = response.json()

        errors = data.get("errors") or []
        if errors:
            throttled = all(
                error.get("extensions", {}).get("code") == "THROTTLED"
                for error in errors
            )
            if throttled:
                wait_seconds = min(2 ** attempt, 20)
                time.sleep(wait_seconds)
                continue
            raise RuntimeError(
                "Shopify GraphQL error: "
                + json.dumps(errors, ensure_ascii=False)
            )

        return data.get("data") or {}

    raise RuntimeError("Shopify GraphQL failed after retries.")


def get_anchor_variant_contexts():
    contexts = []
    after = None

    while True:
        data = graphql_request(
            ANCHOR_VARIANTS_QUERY,
            {"first": 250, "after": after},
        )
        connection = data["productVariants"]
        contexts.extend(connection["nodes"])

        if not connection["pageInfo"]["hasNextPage"]:
            break

        after = connection["pageInfo"]["endCursor"]

    return contexts


def parse_shopify_datetime(value):
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def first_offer_datetime(variant):
    """
    Best available Shopify timestamp for first online-store offering:
    - product publishedAt for a newly published product;
    - variant createdAt when a new variant is added to an already published product.

    Using the later timestamp covers both cases.
    """
    product = variant.get("product") or {}
    published_at = parse_shopify_datetime(product.get("publishedAt"))
    variant_created_at = parse_shopify_datetime(variant.get("createdAt"))

    timestamps = [
        value for value in (published_at, variant_created_at)
        if value is not None
    ]
    return max(timestamps) if timestamps else None


def set_anchor_metafields(variant_gid, price, anchor_date):
    source = (
        f"First online-store listing {anchor_date.isoformat()} "
        f"after {ANCHOR_REFERENCE_DATE.isoformat()} - auto"
    )

    metafields = [
        {
            "ownerId": variant_gid,
            "namespace": "custom",
            "key": "anchor_price",
            "type": "money",
            "value": json.dumps(
                {
                    "amount": price,
                    "currency_code": ANCHOR_CURRENCY,
                },
                separators=(",", ":"),
            ),
        },
        {
            "ownerId": variant_gid,
            "namespace": "custom",
            "key": "anchor_date",
            "type": "date",
            "value": anchor_date.isoformat(),
        },
        {
            "ownerId": variant_gid,
            "namespace": "custom",
            "key": "anchor_source",
            "type": "single_line_text_field",
            "value": source,
        },
    ]

    data = graphql_request(
        SET_ANCHOR_METAFIELDS_MUTATION,
        {"metafields": metafields},
    )
    result = data["metafieldsSet"]

    if result["userErrors"]:
        raise RuntimeError(
            "Anchor metafield update failed: "
            + json.dumps(result["userErrors"], ensure_ascii=False)
        )


def process_new_product_anchors():
    """
    Create anchor data only for genuinely new online-store offers after
    the fixed reference date.

    Existing anchor_price/anchor_date values are never modified.

    Ambiguous cases are left untouched and reported for manual review:
    - only one of anchor_price / anchor_date exists;
    - an older product is missing anchor data;
    - the first observed price is already a sale price;
    - first-offer timestamp or current price is unavailable.
    """
    created = []
    review = []
    failed = []

    contexts = get_anchor_variant_contexts()

    for variant in contexts:
        product = variant.get("product") or {}
        sku = (variant.get("sku") or "").strip() or "(no SKU)"
        title = (product.get("title") or "").strip() or "Unknown product"

        # Only products currently offered through the Online Store.
        if product.get("status") != "ACTIVE":
            continue
        if not product.get("onlineStoreUrl"):
            continue

        anchor_price = ((variant.get("anchorPrice") or {}).get("value") or "").strip()
        anchor_date = ((variant.get("anchorDate") or {}).get("value") or "").strip()

        # Never change a complete existing anchor record.
        if anchor_price and anchor_date:
            continue

        if anchor_price or anchor_date:
            review.append(
                f"{sku} | {title} | incomplete anchor data; "
                "existing metafield(s) left unchanged"
            )
            continue

        first_offer_at = first_offer_datetime(variant)
        if first_offer_at is None:
            review.append(
                f"{sku} | {title} | no reliable first-offer timestamp"
            )
            continue

        first_offer_date = first_offer_at.astimezone(LOCAL_TZ).date()

        # Products/variants already offered on or before the reference date
        # must not be backfilled from today's price.
        if first_offer_date <= ANCHOR_REFERENCE_DATE:
            review.append(
                f"{sku} | {title} | missing anchor for offer dated "
                f"{first_offer_date.isoformat()}; manual historical check required"
            )
            continue

        current_price = clean_price(variant.get("price"))
        compare_at_price = clean_price(variant.get("compareAtPrice"))

        if current_price is None:
            review.append(
                f"{sku} | {title} | current Shopify price is invalid"
            )
            continue

        # Do not guess the regular first-listing price if the product is already
        # presented as a special/sale price at first detection.
        if is_variant_on_sale(current_price, compare_at_price):
            review.append(
                f"{sku} | {title} | first detected after reference date but "
                f"already on sale ({current_price} / compare-at {compare_at_price}); "
                "anchor not written"
            )
            continue

        try:
            set_anchor_metafields(
                variant["id"],
                current_price,
                first_offer_date,
            )
            created.append(
                f"{sku} | {title} | {current_price} EUR | "
                f"{first_offer_date.isoformat()}"
            )
            print(
                f"⚓ Anchor created for {sku}: "
                f"{current_price} EUR on {first_offer_date.isoformat()}"
            )
        except Exception as exc:
            failed.append(
                f"{sku} | {title} | {exc}"
            )
            print(f"⚠️ Anchor creation failed for {sku}: {exc}")

    return created, review, failed


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
            f"?limit=250&fields=id,sku,inventory_item_id,price,compare_at_price"
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
# SALE CHECK
# ---------------------------------------------
def is_variant_on_sale(current_price, compare_at_price):
    """
    Returns True if Shopify variant is currently on sale.

    Shopify sale logic:
    price < compare_at_price

    Example:
    price = 72.24
    compare_at_price = 84.99
    Product is on sale, so script should not overwrite price.
    """

    current_price_decimal = price_to_decimal(current_price)
    compare_at_price_decimal = price_to_decimal(compare_at_price)

    if current_price_decimal is None:
        return False

    if compare_at_price_decimal is None:
        return False

    return current_price_decimal < compare_at_price_decimal


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

    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

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

    anchor_created = []
    anchor_review = []
    anchor_failed = []

    # Capture first-listing anchor data before this sync can change prices.
    # Anchor failures do not block stock/price synchronization; they are
    # reported prominently and the daily price-list validation remains a
    # second compliance guard.
    try:
        anchor_created, anchor_review, anchor_failed = process_new_product_anchors()
    except Exception as exc:
        anchor_failed.append(f"Anchor pre-check failed: {exc}")
        print(f"⚠️ Anchor pre-check failed: {exc}")

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
    skipped_sale_price_count = 0
    skipped_invalid_price_count = 0
    failed_price_count = 0

    for _, row in matched_rows.iterrows():
        sku = row[SKU_COLUMN].strip()
        name = row[NAME_COLUMN].strip()
        qty = int(row[QTY_COLUMN])

        supplier_recommended_price = clean_price(row[PRICE_COLUMN])
        supplier_price = calculate_discounted_price(row[PRICE_COLUMN])

        variant = shopify_lookup.get(sku)

        if not variant:
            continue

        variant_id = variant["id"]
        inventory_item_id = variant["inventory_item_id"]

        current_shopify_price = clean_price(variant.get("price"))
        current_compare_at_price = clean_price(variant.get("compare_at_price"))

        product_is_on_sale = is_variant_on_sale(
            current_shopify_price,
            current_compare_at_price
        )

        # ---------------------------------------------
        # UPDATE STOCK - always update stock
        # ---------------------------------------------
        stock_ok = set_inventory(location_id, inventory_item_id, qty)

        if stock_ok:
            updated_stock_count += 1
        else:
            failed_stock_count += 1

        # ---------------------------------------------
        # UPDATE PRICE - but skip if product is on sale
        # ---------------------------------------------
        price_message = ""

        if product_is_on_sale:
            skipped_sale_price_count += 1

            price_message = (
                f"price skipped - product is on sale "
                f"{current_shopify_price} / compare-at {current_compare_at_price}"
            )

            print(
                f"🏷️ {name} ({sku}) → "
                f"qty={qty}, price skipped because product is on sale "
                f"{current_shopify_price} / compare-at {current_compare_at_price}"
            )

        elif supplier_price is None:
            skipped_invalid_price_count += 1

            price_message = "price skipped - invalid supplier price"

            print(
                f"⚠️ {name} ({sku}) → "
                f"qty={qty}, invalid supplier price skipped"
            )

        elif current_shopify_price == supplier_price:
            unchanged_price_count += 1
        
            price_message = (
                f"price unchanged {supplier_price} "
                f"(10% below recommended {supplier_recommended_price})"
            )
        
            print(
                f"✅ {name} ({sku}) → "
                f"qty={qty}, price unchanged={supplier_price} "
                f"(recommended {supplier_recommended_price})"
            )

        else:
            price_ok = set_variant_price(variant_id, supplier_price)

            if price_ok:
                updated_price_count += 1

                price_message = (
                    f"price {current_shopify_price} → {supplier_price} "
                    f"(10% below recommended {supplier_recommended_price})"
                )
                
                print(
                    f"✅ {name} ({sku}) → "
                    f"qty={qty}, price {current_shopify_price} → {supplier_price} "
                    f"(recommended {supplier_recommended_price})"
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
    print(f"Skipped sale prices: {skipped_sale_price_count}")
    print(f"Skipped invalid prices: {skipped_invalid_price_count}")
    print(f"Failed price updates: {failed_price_count}")
    print(f"New anchors created: {len(anchor_created)}")
    print(f"Anchors requiring review: {len(anchor_review)}")
    print(f"Anchor failures: {len(anchor_failed)}")

    anchor_section = (
        "\n\nANCHOR PRICE AUTOMATION\n"
        f"New anchors created: {len(anchor_created)}\n"
        f"Requires manual review: {len(anchor_review)}\n"
        f"Anchor failures: {len(anchor_failed)}\n"
    )

    if anchor_created:
        anchor_section += "\nCreated:\n" + "\n".join(anchor_created[:100]) + "\n"
    if anchor_review:
        anchor_section += "\nManual review required:\n" + "\n".join(anchor_review[:100]) + "\n"
    if anchor_failed:
        anchor_section += "\nFailures:\n" + "\n".join(anchor_failed[:100]) + "\n"

    body = (
        "Shopify–Dreamlove stock and price sync finished.\n\n"
        f"Matched SKUs: {len(matched_rows)}\n"
        f"Updated stock: {updated_stock_count}\n"
        f"Failed stock updates: {failed_stock_count}\n"
        f"Updated prices: {updated_price_count}\n"
        f"Unchanged prices: {unchanged_price_count}\n"
        f"Skipped sale prices: {skipped_sale_price_count}\n"
        f"Skipped invalid prices: {skipped_invalid_price_count}\n"
        f"Failed price updates: {failed_price_count}\n"
        + anchor_section
        + "\nSYNC DETAILS\n"
        + "\n".join(updated_items[:500])
    )

    if len(updated_items) > 500:
        body += f"\n\n...and {len(updated_items) - 500} more items."

    subject = (
        "Shopify Stock & Price Sync Completed - ANCHOR REVIEW NEEDED"
        if anchor_review or anchor_failed
        else "Shopify Stock & Price Sync Completed"
    )
    send_mail(subject, body)


if __name__ == "__main__":
    main()
