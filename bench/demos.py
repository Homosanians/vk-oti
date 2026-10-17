import pathlib

from trino_client import query, scalar

ROOT = pathlib.Path(__file__).resolve().parent.parent
T = "silver.lakehouse.dim_customer"


class Log:
    def __init__(self, name):
        self.path = ROOT / "results" / name
        self.lines = []

    def run(self, sql, show=10):
        r = query(sql)
        self.lines.append(f"\ntrino> {sql.strip()};")
        if r["columns"]:
            self.lines.append(" | ".join(r["columns"]))
            self.lines += [" | ".join(map(str, row)) for row in r["rows"][:show]]
            if len(r["rows"]) > show:
                self.lines.append(f"... ({len(r['rows'])} строк)")
        return r

    def note(self, text):
        self.lines.append(f"\n-- {text}")

    def save(self):
        self.path.write_text("\n".join(self.lines) + "\n", encoding="utf-8")
        print("\n".join(self.lines))


def schema_evolution():
    log = Log("demo_schema_evolution.txt")
    log.note("Исходная схема Silver-таблицы и её текущий снапшот")
    log.run(f"DESCRIBE {T}")
    s0 = scalar(f'SELECT snapshot_id FROM silver.lakehouse."dim_customer$snapshots" ORDER BY committed_at DESC LIMIT 1')
    log.note(f"snapshot до эволюции = {s0}")

    log.note("1) ADD COLUMN: новая колонка-сегмент клиента. Меняются только метаданные (новый schema-id), "
             "parquet-файлы НЕ переписываются; в старых файлах колонки нет -> читается как NULL")
    log.run(f"ALTER TABLE {T} ADD COLUMN customer_tier varchar")
    log.run(f"SELECT custkey, acctbal, customer_tier FROM {T} ORDER BY custkey LIMIT 3")
    log.run(f"UPDATE {T} SET customer_tier = CASE WHEN acctbal >= 5000 THEN 'GOLD' "
            f"WHEN acctbal >= 0 THEN 'STANDARD' ELSE 'DEBTOR' END")
    log.run(f"SELECT customer_tier, count(*) AS cnt FROM {T} GROUP BY customer_tier ORDER BY 1")

    log.note("2) RENAME COLUMN: Iceberg адресует колонки по field-id, а не по имени -> "
             "переименование мгновенное, данные не трогаются")
    log.run(f"ALTER TABLE {T} RENAME COLUMN phone_country_code TO phone_cc")
    log.run(f"SELECT custkey, phone, phone_cc FROM {T} ORDER BY custkey LIMIT 3")

    log.note("3) Старый снапшот читается со СТАРОЙ схемой (time travel учитывает schema-id): "
             "нет customer_tier, колонка ещё называется phone_country_code")
    log.run(f"SELECT * FROM {T} FOR VERSION AS OF {s0} ORDER BY custkey LIMIT 2")

    log.note("История изменений таблицы")
    log.run(f'SELECT committed_at, snapshot_id, operation, summary[\'total-records\'] AS total_records '
            f'FROM silver.lakehouse."dim_customer$snapshots" ORDER BY committed_at')
    log.run(f"DESCRIBE {T}")

    log.note("Откат эволюции, чтобы Silver снова соответствовал пайплайну")
    log.run(f"ALTER TABLE {T} RENAME COLUMN phone_cc TO phone_country_code")
    log.run(f"ALTER TABLE {T} DROP COLUMN customer_tier")
    log.save()


def time_travel():
    log = Log("demo_time_travel.txt")
    t = "gold.lakehouse.q10_top20_returned_customers"
    log.note("Состояние Gold до «аварии»")
    log.run(f"SELECT count(*) AS rows, sum(revenue) AS total FROM {t}")
    good = scalar(f'SELECT snapshot_id FROM gold.lakehouse."q10_top20_returned_customers$snapshots" '
                  f"ORDER BY committed_at DESC LIMIT 1")
    log.note(f"Запоминаем «хороший» снапшот: {good}")

    log.note("Авария: ошибочный DELETE удалил половину витрины")
    log.run(f"DELETE FROM {t} WHERE revenue < (SELECT approx_percentile(revenue, 0.5) FROM {t})")
    log.run(f"SELECT count(*) AS rows, sum(revenue) AS total FROM {t}")

    log.note("Time travel: читаем витрину на момент хорошего снапшота (FOR VERSION AS OF / FOR TIMESTAMP AS OF)")
    log.run(f"SELECT count(*) AS rows, sum(revenue) AS total FROM {t} FOR VERSION AS OF {good}")
    log.run(f'SELECT committed_at, snapshot_id, parent_id, operation '
            f'FROM gold.lakehouse."q10_top20_returned_customers$snapshots" ORDER BY committed_at')

    log.note("Восстановление: откат указателя текущего снапшота (данные не копируются, "
             "старые файлы ещё на месте, пока не сделан expire_snapshots)")
    log.run(f"ALTER TABLE {t} EXECUTE rollback_to_snapshot({good})")
    log.run(f"SELECT count(*) AS rows, sum(revenue) AS total FROM {t}")
    log.run(f'SELECT made_current_at, snapshot_id, is_current_ancestor '
            f'FROM gold.lakehouse."q10_top20_returned_customers$history" ORDER BY made_current_at')
    log.save()


if __name__ == "__main__":
    schema_evolution()
    time_travel()
