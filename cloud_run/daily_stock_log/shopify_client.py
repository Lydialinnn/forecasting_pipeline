"""Shopify inventory fetch helpers.

Adapted from the Valor inventory-tracker job (fetch_shopify_product_updated.py):
paginated product pull + real-time inventory levels, with retry/backoff and a
persistent session (avoids Cloud Run SSL handshake latency per request).
"""

import json
import logging
import random
import time

import requests

logger = logging.getLogger(__name__)


def get_all_product_inventory(api_shop, api_version, headers):
    """Fetch all products (title, status, variants) with pagination + retries."""
    fields_param = "title,status,variants"
    product_base_url = (
        f"https://{api_shop}.myshopify.com/admin/api/{api_version}"
        f"/products.json?limit=250&fields={fields_param}"
    )
    all_products_data = []
    next_page_url = product_base_url

    max_retries = 5
    base_backoff_seconds = 2
    request_timeout = 30

    session = requests.Session()
    session.headers.update(headers)

    while next_page_url:
        current_page_data = None
        for attempt in range(max_retries):
            try:
                logger.info(
                    "Fetching page: %s... (Attempt %s/%s)",
                    next_page_url.split("page_info=")[-1][:20], attempt + 1, max_retries,
                )
                response = session.get(next_page_url, timeout=request_timeout)

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", base_backoff_seconds * (2 ** attempt)))
                    logger.warning("Rate limited (429). Retrying after %s seconds.", retry_after)
                    time.sleep(retry_after)

                response.raise_for_status()
                current_page_data = response.json()
                break

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    retry_after = int(e.response.headers.get("Retry-After", base_backoff_seconds * (2 ** attempt)))
                    if attempt < max_retries - 1:
                        time.sleep(retry_after)
                        continue
                    logger.error("Max retries reached for rate limit on %s.", next_page_url)
                elif 500 <= e.response.status_code < 600:
                    logger.warning("Server error (%s) on %s. Attempt %s/%s.", e.response.status_code, next_page_url, attempt + 1, max_retries)
                    if attempt < max_retries - 1:
                        time.sleep((base_backoff_seconds * (2 ** attempt)) + random.uniform(0, 1))
                        continue
                    logger.error("Max retries reached for server error on %s.", next_page_url)
                else:
                    logger.error("Non-retryable HTTP error fetching %s: %s", next_page_url, e)
                    next_page_url = None
                    break

            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                if attempt < max_retries - 1:
                    delay = (base_backoff_seconds * (2 ** attempt)) + random.uniform(0, 1)
                    logger.info("Error: %s. Retrying in %.2f seconds...", e, delay)
                    time.sleep(delay)
                    continue
                logger.error("Max retries reached on %s: %s", next_page_url, e)

            if attempt == max_retries - 1:
                logger.error("All %s retries failed for %s.", max_retries, next_page_url)
                next_page_url = None

        if current_page_data:
            all_products_data.extend(current_page_data.get("products", []))
            link_header = response.headers.get("Link")
            next_page_url = None
            if link_header:
                for link in link_header.split(","):
                    if 'rel="next"' in link:
                        next_page_url = link.split(";")[0].strip("<> ")
                        break
        else:
            break

    logger.info("Total products retrieved: %s", len(all_products_data))
    return all_products_data


def extract_inventory_data(products):
    """Flatten products into one row per variant."""
    inventory_summary = []
    for product in products:
        product_title = product.get("title")
        sku_status = product.get("status")
        for variant in product.get("variants", []):
            inventory_summary.append({
                "product_title": product_title,
                "variant_sku": variant.get("sku"),
                "variant_id": variant.get("id"),
                "inventory_item_id": variant.get("inventory_item_id"),
                "sku_status": sku_status,
                # inventory_quantity is deprecated/unreliable — kept as fallback only
                "legacy_inventory_quantity": variant.get("inventory_quantity"),
            })
    return inventory_summary


def get_inventory_levels(api_shop, api_version, headers, inventory_item_ids):
    """Real-time inventory levels for inventory_item_ids, batched by 50."""
    all_levels = []
    chunk_size = 50

    session = requests.Session()
    session.headers.update(headers)

    total_chunks = (len(inventory_item_ids) + chunk_size - 1) // chunk_size
    logger.info("Splitting %s items into %s chunks...", len(inventory_item_ids), total_chunks)

    for chunk_idx, i in enumerate(range(0, len(inventory_item_ids), chunk_size), 1):
        chunk = inventory_item_ids[i:i + chunk_size]
        ids_string = ",".join(map(str, [x for x in chunk if x]))
        if not ids_string:
            continue

        url = (
            f"https://{api_shop}.myshopify.com/admin/api/{api_version}"
            f"/inventory_levels.json?inventory_item_ids={ids_string}"
        )
        if chunk_idx % 10 == 0:
            logger.info("Processing inventory chunk %s/%s...", chunk_idx, total_chunks)

        for attempt in range(5):
            try:
                response = session.get(url, timeout=30)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 2 ** attempt))
                    logger.warning("Chunk %s: rate limit (429). Sleeping %ss...", chunk_idx, retry_after)
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                all_levels.extend(response.json().get("inventory_levels", []))
                break
            except Exception as e:
                logger.warning("Error fetching chunk %s: %s. Attempt %s/5", chunk_idx, e, attempt + 1)
                time.sleep(2 ** attempt)

    return all_levels
