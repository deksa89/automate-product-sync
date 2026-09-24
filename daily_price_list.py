import csv
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

STORE = os.getenv("SHOPIFY_STORE")
TOKEN = os.getenv("SHOPIFY_TOKEN")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-07")
PAGE_HANDLE = os.getenv("PRICE_LIST_PAGE_HANDLE", "cjenici")
STORE_CODE = os.getenv("PRICE_LIST_STORE_CODE", "WEB-01")
ADDRESS_SLUG = os.getenv("PRICE_LIST_ADDRESS_SLUG", "ulica-sv-mateja-127_10000-zagreb")
ARCHIVE_DAYS = int(os.getenv("PRICE_LIST_ARCHIVE_DAYS", "35"))
DRY_RUN = os.getenv("PRICE_LIST_DRY_RUN", "false").lower() in {"1", "true", "yes", "y"}
OUTPUT_DIR = Path(os.getenv("PRICE_LIST_OUTPUT_DIR", "price_lists"))
TZ = ZoneInfo("Europe/Zagreb")

GRAPHQL_URL = f"https://{STORE}/admin/api/{API_VERSION}/graphql.json"
HEADERS = {"X-Shopify-Access-Token": TOKEN or "", "Content-Type": "application/json"}

UNIT_PRICE_ML_BY_SKU = {
    "D-224035": 240, "D-195073": 150, "D-230976": 100, "D-234934": 100,
    "D-229363": 250, "D-201644": 100, "D-201646": 100, "D-210394": 120,
    "D-230654": 100, "D-230670": 100, "D-230673": 100, "D-230676": 100,
    "D-230685": 100, "D-236039": 100, "D-211758": 100, "D-244664": 100,
}

REVIEWED_NO_UNIT_PRICE_SKUS = {
    "D11-201360", "D-194152", "D-196500", "D-201266", "D-201405",
    "D-201836", "D-205290", "D-207281", "D-210392", "D-213109",
    "D-213899", "D-221678", "D-224135", "D-229297", "D-230658",
    "D-230660", "D-241200", "D-241697", "D-245255",
}

UNIT_PRICE_REVIEW_KEYWORDS = (
    "lubric", "lube", "massage oil", "massage gel", "moistur", "cream",
    "creme", "lotion", "body oil", "bodylube", "intimate gel",
    "sliding gel", "powder",
)

CSV_HEADERS = [
    "naziv", "sifra", "marka", "jedinica_mjere",
    "cijena_za_jedinicu_mjere", "maloprodajna_cijena",
    "posebni_oblik_prodaje", "naziv_posebnog_oblika_prodaje",
    "sidrena_cijena", "barkod", "dostupnost",
]

VARIANTS_QUERY = """
query PriceListVariants($first: Int!, $after: String) {
  productVariants(first: $first, after: $after) {
    nodes {
      sku title barcode price compareAtPrice availableForSale
      anchorPrice: metafield(namespace: "custom", key: "anchor_price") { value }
      product { title vendor productType status }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

PAGE_QUERY = """
query PriceListPage($first: Int!, $query: String!) {
  pages(first: $first, query: $query) {
    nodes {
      id title handle isPublished
      manifest: metafield(namespace: "custom", key: "price_list_manifest") { value }
    }
  }
}
"""

STAGE_MUTATION = """
mutation StagePriceList($input: [StagedUploadInput!]!) {
  stagedUploadsCreate(input: $input) {
    stagedTargets { url resourceUrl parameters { name value } }
    userErrors { field message }
  }
}
"""

FILE_CREATE_MUTATION = """
mutation CreatePriceListFile($files: [FileCreateInput!]!) {
  fileCreate(files: $files) {
    files {
      id fileStatus fileErrors { code message }
      ... on GenericFile { url }
    }
    userErrors { field message code }
  }
}
"""

FILE_STATUS_QUERY = """
query PriceListFileStatus($id: ID!) {
  node(id: $id) {
    ... on GenericFile {
      id fileStatus url fileErrors { code message }
    }
  }
}
"""

PAGE_UPDATE_MUTATION = """
mutation UpdatePriceListPage($id: ID!, $page: PageUpdateInput!) {
  pageUpdate(id: $id, page: $page) {
    page { id handle isPublished }
    userErrors { field message code }
  }
}
"""


def send_mail(subject, body):
    import smtplib
    import ssl
    from email.mime.text import MIMEText

    sender = os.getenv("MAIL_FROM")
    receiver = os.getenv("MAIL_TO")
    server_name = os.getenv("SMTP_SERVER")
    port = int(os.getenv("SMTP_PORT", "587"))
    username = os.getenv("SMTP_USERNAME")
    password = os.getenv("SMTP_PASSWORD")

    if not all([sender, receiver, server_name, username, password]):
        print("Email settings missing; skipping email.")
        return

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"], msg["From"], msg["To"] = subject, sender, receiver
    try:
        with smtplib.SMTP(server_name, port, timeout=30) as server:
            server.starttls(context=ssl.create_default_context())
            server.login(username, password)
            server.sendmail(sender, [receiver], msg.as_string())
    except Exception as exc:
        print(f"Email failed: {exc}")


def validate_env():
    missing = [name for name, value in {
        "SHOPIFY_STORE": STORE, "SHOPIFY_TOKEN": TOKEN
    }.items() if not value]
    if missing:
        raise RuntimeError(f"Missing environment variables: {missing}")


def gql(query, variables=None):
    for attempt in range(6):
        response = requests.post(
            GRAPHQL_URL,
            headers=HEADERS,
            json={"query": query, "variables": variables or {}},
            timeout=60,
        )
        if response.status_code == 429 or response.status_code >= 500:
            time.sleep(min(2 ** attempt, 20))
            continue
        response.raise_for_status()
        payload = response.json()
        errors = payload.get("errors") or []
        if errors:
            if all(e.get("extensions", {}).get("code") == "THROTTLED" for e in errors):
                time.sleep(min(2 ** attempt, 20))
                continue
            raise RuntimeError(json.dumps(errors, ensure_ascii=False, indent=2))
        return payload.get("data") or {}
    raise RuntimeError("Shopify GraphQL failed after retries.")


def money(value):
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid money value: {value!r}") from exc


def anchor_amount(raw):
    if not raw:
        return None
    try:
        return money(json.loads(raw)["amount"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def is_sale(price, compare_at):
    if compare_at in (None, ""):
        return False
    try:
        return money(price) < money(compare_at)
    except ValueError:
        return False


def get_active_variants():
    variants, after = [], None
    while True:
        data = gql(VARIANTS_QUERY, {"first": 250, "after": after})
        connection = data["productVariants"]
        variants.extend(connection["nodes"])
        if not connection["pageInfo"]["hasNextPage"]:
            break
        after = connection["pageInfo"]["endCursor"]

    active = [v for v in variants if v.get("product", {}).get("status") == "ACTIVE"]
    active.sort(key=lambda v: (v.get("sku") or "").casefold())
    return active


def validate_variants(variants):
    if not variants:
        raise RuntimeError("No ACTIVE variants found.")

    seen, problems = set(), []
    for v in variants:
        sku = (v.get("sku") or "").strip()
        vendor = (v.get("product", {}).get("vendor") or "").strip()
        barcode = (v.get("barcode") or "").strip()
        anchor = anchor_amount((v.get("anchorPrice") or {}).get("value"))

        if not sku:
            problems.append(f"Missing SKU: {v.get('product', {}).get('title')}")
        elif sku in seen:
            problems.append(f"Duplicate ACTIVE SKU: {sku}")
        else:
            seen.add(sku)

        if not vendor:
            problems.append(f"Missing vendor/marka: {sku}")
        if not barcode:
            problems.append(f"Missing barcode: {sku}")
        if anchor is None:
            problems.append(f"Missing custom.anchor_price: {sku}")

    if problems:
        raise RuntimeError("Price-list validation failed:\n- " + "\n- ".join(problems))


def parse_measurement(title):
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(ML|GR|G)\b", title or "", re.I)
    if not m:
        return None
    qty = Decimal(m.group(1).replace(",", "."))
    unit = "ml" if m.group(2).upper() == "ML" else "g"
    return qty, unit


def unit_price_warnings(variants):
    warnings = []
    for v in variants:
        sku = (v.get("sku") or "").strip()
        product = v.get("product") or {}
        title = (product.get("title") or "").strip()
        product_type = (product.get("productType") or "").strip()

        if not sku or sku in UNIT_PRICE_ML_BY_SKU or sku in REVIEWED_NO_UNIT_PRICE_SKUS:
            continue
        if product_type.casefold() == "toy cleaner":
            continue

        measurement = parse_measurement(title)
        if measurement and measurement[0] < Decimal("50"):
            continue

        lower = title.casefold()
        relevant = product_type.casefold() == "lubricants" or any(
            key in lower for key in UNIT_PRICE_REVIEW_KEYWORDS
        )
        if not relevant:
            continue

        if measurement:
            reason = f"{measurement[0].normalize()} {measurement[1]}"
        else:
            reason = "package size not found in title"

        warnings.append({"sku": sku, "title": title, "reason": reason})
    return warnings


def display_name(v):
    title = v["product"]["title"].strip()
    variant = (v.get("title") or "").strip()
    return f"{title} - {variant}" if variant and variant != "Default Title" else title


def build_rows(variants):
    rows = []
    for v in variants:
        sku = v["sku"].strip()
        price = money(v["price"])
        anchor = anchor_amount((v.get("anchorPrice") or {}).get("value"))
        sale = is_sale(v["price"], v.get("compareAtPrice"))
        ml = UNIT_PRICE_ML_BY_SKU.get(sku)

        if ml:
            unit = "l"
            unit_price = (price / (Decimal(ml) / Decimal("1000"))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            unit_price_text = f"{unit_price:.2f}"
        else:
            unit, unit_price_text = "", ""

        rows.append([
            display_name(v),
            sku,
            v["product"]["vendor"].strip(),
            unit,
            unit_price_text,
            f"{price:.2f}",
            "DA" if sale else "NE",
            "Akcijska prodaja" if sale else "",
            f"{anchor:.2f}",
            v["barcode"].strip(),
            "dostupno" if v["availableForSale"] else "nedostupno",
        ])
    return rows


def write_csv(rows, filename):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / filename
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh, delimiter=";", quoting=csv.QUOTE_ALL, lineterminator="\n")
        writer.writerow(CSV_HEADERS)
        writer.writerows(rows)
    return path


def append_summary(lines):
    path = os.getenv("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")


def get_page_and_manifest():
    data = gql(PAGE_QUERY, {"first": 10, "query": f"handle:{PAGE_HANDLE}"})
    pages = [p for p in data["pages"]["nodes"] if p.get("handle") == PAGE_HANDLE]
    if not pages:
        raise RuntimeError(f'Page "{PAGE_HANDLE}" not found.')
    page = pages[0]

    raw = (page.get("manifest") or {}).get("value")
    if not raw:
        return page, {"sequence": 0, "items": []}

    manifest = json.loads(raw)
    if not isinstance(manifest.get("items", []), list):
        raise RuntimeError("Invalid price_list_manifest.")
    return page, {"sequence": int(manifest.get("sequence", 0)), "items": manifest.get("items", [])}


def stage_file(filename):
    data = gql(STAGE_MUTATION, {"input": [{
        "resource": "FILE", "filename": filename, "mimeType": "text/csv", "httpMethod": "POST"
    }]})
    result = data["stagedUploadsCreate"]
    if result["userErrors"]:
        raise RuntimeError(str(result["userErrors"]))
    return result["stagedTargets"][0]


def upload_staged(target, path):
    fields = {p["name"]: p["value"] for p in target["parameters"]}
    with path.open("rb") as fh:
        response = requests.post(
            target["url"],
            data=fields,
            files={"file": (path.name, fh, "text/csv")},
            timeout=120,
        )
    response.raise_for_status()


def create_file(filename, resource_url):
    data = gql(FILE_CREATE_MUTATION, {"files": [{
        "contentType": "FILE",
        "originalSource": resource_url,
        "filename": filename,
        "duplicateResolutionMode": "RAISE_ERROR",
    }]})
    result = data["fileCreate"]
    if result["userErrors"]:
        raise RuntimeError(str(result["userErrors"]))
    return result["files"][0]


def wait_ready(file_id):
    deadline = time.time() + 120
    while time.time() < deadline:
        node = gql(FILE_STATUS_QUERY, {"id": file_id}).get("node")
        if not node:
            raise RuntimeError("Uploaded file could not be read.")
        if node.get("fileErrors"):
            raise RuntimeError(str(node["fileErrors"]))
        if node.get("fileStatus") == "READY" and node.get("url"):
            return node["url"]
        if node.get("fileStatus") == "FAILED":
            raise RuntimeError("Shopify file processing failed.")
        time.sleep(4)
    raise TimeoutError("Shopify file did not become READY within 120 seconds.")


def clean_archive(items, now):
    cutoff = now - timedelta(days=ARCHIVE_DAYS)
    cleaned = []
    for item in items:
        try:
            dt = datetime.fromisoformat(item["created_at"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ)
        except (KeyError, TypeError, ValueError):
            continue
        if dt >= cutoff and item.get("url") and item.get("filename"):
            cleaned.append(item)
    cleaned.sort(key=lambda x: x["created_at"], reverse=True)
    return cleaned


def page_body(items):
    latest, *archive = items
    dt = datetime.fromisoformat(latest["created_at"]).astimezone(TZ)
    parts = [
        "<h1>Cjenici</h1>",
        "<p>Na ovoj stranici dostupni su javni dnevni cjenici internetske trgovine LuvMechanics u strojno čitljivom CSV formatu.</p>",
        "<h2>Najnoviji cjenik</h2>",
        f'<p><a href="{html.escape(latest["url"], quote=True)}">Preuzmi najnoviji CSV cjenik</a></p>',
        f'<p>Objavljeno: {dt.strftime("%d.%m.%Y. %H:%M")} &middot; Broj stavki: {latest["rows"]}</p>',
        "<h2>Arhiva cjenika</h2>",
    ]
    if archive:
        parts.append("<ul>")
        for item in archive:
            item_dt = datetime.fromisoformat(item["created_at"]).astimezone(TZ)
            label = f'{item_dt.strftime("%d.%m.%Y. %H:%M")} ({item["rows"]} stavki)'
            parts.append(f'<li><a href="{html.escape(item["url"], quote=True)}">{html.escape(label)}</a></li>')
        parts.append("</ul>")
    else:
        parts.append("<p>Prethodni cjenici još nisu dostupni.</p>")
    return "\n".join(parts)


def update_page(page_id, items, sequence):
    manifest = {"sequence": sequence, "items": items}
    data = gql(PAGE_UPDATE_MUTATION, {"id": page_id, "page": {
        "body": page_body(items),
        "isPublished": True,
        "metafields": [{
            "namespace": "custom",
            "key": "price_list_manifest",
            "type": "json",
            "value": json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
        }],
    }})
    result = data["pageUpdate"]
    if result["userErrors"]:
        raise RuntimeError(str(result["userErrors"]))
    if not result["page"]["isPublished"]:
        raise RuntimeError("Cjenici page is still unpublished.")


def main():
    validate_env()
    now = datetime.now(TZ)

    variants = get_active_variants()
    validate_variants(variants)
    warnings = unit_price_warnings(variants)
    rows = build_rows(variants)

    sales = sum(is_sale(v["price"], v.get("compareAtPrice")) for v in variants)
    unavailable = sum(not v["availableForSale"] for v in variants)
    unit_rows = sum(v["sku"] in UNIT_PRICE_ML_BY_SKU for v in variants)

    if DRY_RUN:
        filename = f"DRY-RUN_{now.strftime('%Y%m%d_%H%M')}.csv"
        path = write_csv(rows, filename)
        append_summary([
            "## LuvMechanics daily price list — DRY RUN",
            f"- CSV rows: **{len(rows)}**",
            f"- Sale rows: **{sales}**",
            f"- Unit-price rows: **{unit_rows}**",
            f"- Unavailable rows: **{unavailable}**",
            f"- Unit-price review warnings: **{len(warnings)}**",
            f"- Generated file: `{path}`",
        ])
        if warnings:
            append_summary(["", "### Products requiring unit-price review"] + [
                f"- `{w['sku']}` — {w['title']} — {w['reason']}" for w in warnings
            ])
        print(f"DRY RUN OK: {len(rows)} rows; warnings={len(warnings)}")
        return

    page, manifest = get_page_and_manifest()
    sequence = manifest["sequence"] + 1
    filename = (
        f"internetska-trgovina_{ADDRESS_SLUG}_{STORE_CODE}_"
        f"{sequence:04d}_{now.strftime('%Y%m%d_%H%M')}.csv"
    )
    path = write_csv(rows, filename)

    target = stage_file(filename)
    upload_staged(target, path)
    created = create_file(filename, target["resourceUrl"])
    public_url = wait_ready(created["id"])

    new_item = {
        "filename": filename, "url": public_url,
        "created_at": now.isoformat(), "rows": len(rows),
        "storage_number": sequence,
    }
    items = clean_archive([new_item] + manifest["items"], now)
    update_page(page["id"], items, sequence)

    warning_text = "\n".join(
        f"- {w['sku']} | {w['title']} | {w['reason']}" for w in warnings
    ) or "None"

    body = (
        "LuvMechanics daily public price list published successfully.\n\n"
        f"File: {filename}\nRows: {len(rows)}\nSale rows: {sales}\n"
        f"Unit-price rows: {unit_rows}\nUnavailable rows: {unavailable}\n"
        f"Unit-price review warnings: {len(warnings)}\n"
        f"Public CSV URL: {public_url}\n"
        f"Page: https://www.luvmechanics.com/pages/{PAGE_HANDLE}\n\n"
        f"Warnings:\n{warning_text}\n"
    )
    subject = (
        "LuvMechanics Daily Price List Published - UNIT PRICE REVIEW NEEDED"
        if warnings else "LuvMechanics Daily Price List Published"
    )
    send_mail(subject, body)
    append_summary([
        "## LuvMechanics daily price list",
        f"- CSV rows: **{len(rows)}**",
        f"- Unit-price review warnings: **{len(warnings)}**",
        f"- Public CSV: {public_url}",
        f"- Page: https://www.luvmechanics.com/pages/{PAGE_HANDLE}",
    ])


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        message = f"LuvMechanics daily public price list FAILED.\n\nError: {exc}\n"
        print(message, file=sys.stderr)
        send_mail("LuvMechanics Daily Price List FAILED", message)
        raise
