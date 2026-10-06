"""Productor de pagos simulados hacia PAGOS_STREAM vía Snowpipe Streaming.

pip install snowpipe-streaming
python productor_pagos.py --tps 20 --minutos 15 --p-fraude 0.003

profile.json junto al script (basado en el ejemplo oficial de Snowflake):
{"account": "ORG-CUENTA", "user": "...", "url": "https://ORG-CUENTA.snowflakecomputing.com:443",
 "private_key_file": "rsa_key.p8", "role": "ACCOUNTADMIN"}
"""
import argparse
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("SS_LOG_LEVEL", "warn")
from snowflake.ingest.streaming import StreamingIngestClient  # noqa: E402

# profile.json y la llave se leen siempre de la carpeta del script, no de donde se lance
AQUI = Path(__file__).resolve().parent
os.chdir(AQUI)

DB, SCHEMA, TABLE = "STREAMING_LAB", "PAGOS", "PAGOS_STREAM"

SUCURSALES = ["MTY-CENTRO", "MTY-APODACA", "SALTILLO", "CHIHUAHUA", "LIMA", "GUAYAQUIL"]
CANALES = ["TRADICIONAL", "MODERNO", "B2B-APP"]


def pago(pago_id: int, p_fraude: float) -> dict:
    fraude = random.random() < p_fraude
    monto = random.uniform(55_000, 120_000) if fraude else random.lognormvariate(5.5, 0.9)
    return {
        "PAGO_ID": pago_id,
        "TS_EVENTO": datetime.now(timezone.utc),
        "CLIENTE_ID": random.randint(1, 5_000),
        "SUCURSAL": random.choice(SUCURSALES),
        "CANAL": random.choice(CANALES),
        "MONTO": round(monto, 2),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tps", type=float, default=20, help="pagos por segundo")
    ap.add_argument("--minutos", type=float, default=15)
    ap.add_argument("--p-fraude", type=float, default=0.003)
    ap.add_argument("--canal", default="CANAL_PAGOS_01")
    ap.add_argument("--db", default=DB, help="base de datos (por defecto STREAMING_LAB)")
    ap.add_argument("--schema", default=SCHEMA, help="schema (por defecto PAGOS)")
    ap.add_argument("--tabla", default=TABLE, help="tabla destino (por defecto PAGOS_STREAM)")
    ap.add_argument("--fila-mala-en", type=int, default=0,
                    help="Reto 2: en este pago_id manda MONTO no numérico")
    args = ap.parse_args()

    with StreamingIngestClient(
        client_name="productor-pagos",
        db_name=args.db,
        schema_name=args.schema,
        pipe_name=f"{args.tabla}-STREAMING",  # pipe por defecto: Snowflake lo crea al abrir el primer canal
        profile_json=str(AQUI / "profile.json"),
    ) as client:
        channel, status = client.open_channel(args.canal)
        with channel:
            # Canal con nombre = orden y exactly-once vía offset token.
            # Si el productor se cae y se relanza con el mismo --canal, retoma
            # desde el último pago_id que Snowflake confirmó.
            ultimo = status.latest_committed_offset_token
            pago_id = int(ultimo) if ultimo else 0
            print(f"Canal {channel.channel_name} abierto, retomando desde pago_id={pago_id}")

            fin = time.time() + args.minutos * 60
            while time.time() < fin:
                pago_id += 1
                fila = pago(pago_id, args.p_fraude)
                if pago_id == args.fila_mala_en:
                    fila["MONTO"] = "no-es-un-numero"
                channel.append_row(fila, str(pago_id))
                if pago_id % 200 == 0:
                    print(f"enviados={pago_id}")
                time.sleep(1 / args.tps)

            channel.wait_for_commit(lambda t: t is not None and int(t) >= pago_id, timeout_seconds=60)
            st = channel.get_channel_status()
            print(f"confirmado={st.latest_committed_offset_token}  insertadas={st.rows_inserted_count}  "
                  f"errores={st.rows_error_count}  "
                  f"latencia_servidor={st.server_avg_processing_latency.total_seconds():.2f}s")
            if st.rows_error_count:
                print("último error:", st.last_error_message)


if __name__ == "__main__":
    main()
