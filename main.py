import os
import time
import json
import requests
from google.cloud import bigquery
from datetime import datetime, timezone
from fastapi import FastAPI

app = FastAPI()

API_BASE = "https://migranier.pansgranier.com/api"
TOKEN = os.environ["MIGRANIER_TOKEN"]
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

BQ_PROJECT = "business-intelligence-444511"
BQ_DATASET = "granier_app_migranier"
TABLE_CTRL = f"{BQ_PROJECT}.{BQ_DATASET}.Control_Descarga_Pedidos"
TABLE_RAW  = f"{BQ_PROJECT}.{BQ_DATASET}.Pedidos_App_Raw"
TABLE_ITMS = f"{BQ_PROJECT}.{BQ_DATASET}.Pedidos_App_Items"
TABLE_STORES = f"{BQ_PROJECT}.{BQ_DATASET}.Master_Tiendas"

MAX_CONSECUTIVE_MISS = 50
BATCH_MAX_IDS = 500

bq = bigquery.Client(project=BQ_PROJECT)


def get_last_id():
    q = f"""
    SELECT last_order_id
    FROM `{TABLE_CTRL}`
    WHERE nombre='orders_full'
    LIMIT 1
    """
    rows = list(bq.query(q).result())
    return rows[0].last_order_id if rows else 0


def set_last_id(new_id):
    q = f"""
    UPDATE `{TABLE_CTRL}`
    SET last_order_id = @new_id, updated_at = CURRENT_TIMESTAMP()
    WHERE nombre='orders_full'
    """
    bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("new_id", "INT64", new_id)]
    )).result()


def fetch_order_full(order_id: int):
    url = f"{API_BASE}/orders/{order_id}/full"
    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.status_code == 200:
        return r.json()
    if r.status_code == 404:
        return None
    if r.status_code == 401:
        raise RuntimeError("401 Unauthorized: token inválido o caducado.")
    time.sleep(1)
    r.raise_for_status()


def insert_order(order_json: dict):
    order = order_json.get("order", {})
    store = order_json.get("store", {})
    suppliers = order_json.get("suppliers", [])

    row_raw = {
        "order_id": order.get("id"),
        "created_at": datetime.utcnow().isoformat(),
        "store_id": store.get("id"),
        "amount": order.get("amount"),
        "status": order.get("status"),
        "payload": json.dumps(order_json, ensure_ascii=False),
    }

    errors = bq.insert_rows_json(TABLE_RAW, [row_raw])
    if errors:
        raise RuntimeError(f"Error insertando RAW: {errors}")

    items = []
    for s in suppliers:
        sid = s.get("id")
        sname = s.get("name")
        for p in s.get("products", []):
            items.append({
                "order_id": order.get("id"),
                "supplier_id": sid,
                "supplier_name": sname,
                "product_id": p.get("product_id"),
                "product_name": p.get("product_name"),
                "product_description": p.get("product_description"),
                "count": p.get("count"),
                "price": p.get("price"),
                "total": p.get("total"),
            })

    if items:
        errors2 = bq.insert_rows_json(TABLE_ITMS, items)
        if errors2:
            raise RuntimeError(f"Error insertando ITEMS: {errors2}")



@app.get("/test_order/{order_id}")
def test_order(order_id: int):
    """Prueba directa para ver qué responde la API del pedido."""
    url = f"{API_BASE}/orders/{order_id}/full"
    r = requests.get(url, headers=HEADERS, timeout=30)

    return {
        "url": url,
        "status_code": r.status_code,
        "text_snippet": r.text[:500],  # primeros 500 caracteres
    }


@app.get("/descargar_pedidos")
def run_incremental():
    last_id = get_last_id()
    consecutive_miss = 0
    max_inserted_id = last_id

    for order_id in range(last_id + 1, last_id + BATCH_MAX_IDS + 1):
        data = fetch_order_full(order_id)
        if data is None:
            consecutive_miss += 1
            if consecutive_miss >= MAX_CONSECUTIVE_MISS:
                break
            continue

        consecutive_miss = 0
        insert_order(data)
        if order_id > max_inserted_id:
            max_inserted_id = order_id

    if max_inserted_id > last_id:
        set_last_id(max_inserted_id)

    return {"status": "ok", "desde_id": last_id, "hasta_id": max_inserted_id}

@app.get("/sync_tiendas")
def sync_tiendas():
    """Descarga todas las tiendas desde la API Mi Granier y sincroniza con BigQuery."""
    url = f"{API_BASE}/master-tiendas?per_page=5000"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    data = r.json().get("data", [])

    rows = []
    now = datetime.utcnow().isoformat()

    for t in data:
        zona_raw = t.get("zona_ventas")
        zona_str = ",".join(zona_raw) if isinstance(zona_raw, list) else zona_raw

        rows.append({
            "store_id": t.get("id"),
            "email": t.get("email"),
            "marketing_emails": t.get("marketing_emails"),
            "dest_merc": t.get("dest_merc"),
            "zona_ventas": zona_str,
            "cliente_unico": t.get("cliente_unico"),
            "store_name": t.get("store_name"),
            "address": t.get("address"),
            "city": t.get("city"),
            "franquiciado": t.get("franquiciado"),
            "phone1": t.get("phone1"),
            "phone2": t.get("phone2"),
            "updated_at": now
        })

    # Vaciar tabla manteniendo el schema
    bq.query(f"TRUNCATE TABLE `{TABLE_STORES}`").result()

    # Insertar filas
    errors = bq.insert_rows_json(TABLE_STORES, rows)

    if errors:
        return {"error": errors}

    return {"ok": True, "rows": len(rows)}


    if errors:
        return {"error": errors}

    return {"ok": True, "rows": len(rows)}
