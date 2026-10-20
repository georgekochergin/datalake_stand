"""Модульные тесты конфигурации источников bank и broker и DAG-ов обновления."""
import json
from pathlib import Path
import unittest


class TestBankBrokerSourcesAndDags(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).resolve().parent.parent

    def test_sources_manifest(self):
        conf_path = self.root / "conf" / "sydafa" / "sources.json"
        self.assertTrue(conf_path.exists(), "conf/sydafa/sources.json must exist")
        with open(conf_path, "r", encoding="utf-8") as f:
            conf = json.load(f)

        sources = conf.get("sources", {})
        self.assertIn("bank", sources)
        self.assertIn("broker", sources)

        # Bank checks
        bank = sources["bank"]
        self.assertEqual(bank["type"], "mssql")
        self.assertEqual(bank["external_port"], 14330)

        bank_tables = bank["schema"]["tables"]
        self.assertIn("retail_bank.clients", bank_tables)
        self.assertIn("retail_bank.accounts", bank_tables)
        self.assertIn("retail_bank.deals", bank_tables)
        self.assertIn("retail_bank.transactions", bank_tables)

        tx_fks = bank_tables["retail_bank.transactions"]["foreign_keys"]
        ref_entities = {fk["ref_entity_fqn"] for fk in tx_fks}
        self.assertEqual(
            ref_entities,
            {"retail_bank.accounts", "retail_bank.clients", "retail_bank.deals"},
        )

        bank_rules = bank["rules"]
        tx_rules = bank_rules["retail_bank.transactions"]["columns"]
        self.assertEqual(tx_rules["currency"]["kind"], "choice")
        self.assertEqual(tx_rules["currency"]["values"], ["RUB"])
        self.assertEqual(tx_rules["currency"]["null_ratio"], 0.0)
        self.assertEqual(tx_rules["amount"]["null_ratio"], 0.0)
        self.assertEqual(tx_rules["posting_date"]["null_ratio"], 0.0)
        self.assertEqual(tx_rules["description"]["null_ratio"], 0.05)

        deals_rules = bank_rules["retail_bank.deals"]["columns"]
        self.assertEqual(deals_rules["end_date"]["kind"], "date_offset")
        self.assertEqual(deals_rules["end_date"]["ref_column"], "start_date")
        self.assertGreater(deals_rules["end_date"]["min_offset"], 0)

        bank_invariants = bank_rules["retail_bank.transactions"]["invariants"]
        self.assertEqual(len(bank_invariants), 1)
        self.assertEqual(bank_invariants[0]["kind"], "cumulative_balance")
        self.assertEqual(bank_invariants[0]["amount_column"], "amount")
        self.assertEqual(bank_invariants[0]["partition_by"], ["account_id"])
        self.assertEqual(bank_invariants[0]["min_balance"], 0.0)
        self.assertFalse(bank_invariants[0]["validate_existing"])

        # Broker checks
        broker = sources["broker"]
        self.assertEqual(broker["type"], "oracle")
        self.assertEqual(broker["external_port"], 15211)

        broker_tables = broker["schema"]["tables"]
        self.assertIn("trading_core.clients", broker_tables)
        self.assertIn("trading_core.accounts", broker_tables)
        self.assertIn("trading_core.deals", broker_tables)
        self.assertIn("trading_core.transactions", broker_tables)

        broker_rules = broker["rules"]
        broker_tx_rules = broker_rules["trading_core.transactions"]["columns"]
        self.assertEqual(broker_tx_rules["security_type"]["kind"], "choice")
        self.assertEqual(broker_tx_rules["security_type"]["null_ratio"], 0.0)
        self.assertEqual(broker_tx_rules["quantity"]["null_ratio"], 0.0)
        self.assertEqual(broker_tx_rules["posting_date"]["null_ratio"], 0.0)
        self.assertEqual(broker_tx_rules["ticker"]["null_ratio"], 0.05)

        broker_invariants = broker_rules["trading_core.transactions"]["invariants"]
        self.assertEqual(len(broker_invariants), 1)
        self.assertEqual(broker_invariants[0]["kind"], "cumulative_balance")
        self.assertEqual(broker_invariants[0]["amount_column"], "quantity")
        self.assertEqual(broker_invariants[0]["partition_by"], ["account_id"])
        self.assertEqual(broker_invariants[0]["min_balance"], 0.0)
        self.assertFalse(broker_invariants[0]["validate_existing"])

    def test_migration_payloads(self):
        bank_mig_path = self.root / "airflow" / "migrations" / "bank_update.json"
        broker_mig_path = self.root / "airflow" / "migrations" / "broker_update.json"

        with open(bank_mig_path, "r", encoding="utf-8") as f:
            bank_ops = json.load(f)["operations"]

        # Bank: append 25000, 3 updates of 100 rows, no delete
        self.assertEqual(len(bank_ops), 4)
        append_ops = [op for op in bank_ops if op["op"] == "append"]
        update_ops = [op for op in bank_ops if op["op"] == "update"]
        delete_ops = [op for op in bank_ops if op["op"] == "delete"]

        self.assertEqual(len(append_ops), 1)
        self.assertEqual(append_ops[0]["entity_fqn"], "retail_bank.transactions")
        self.assertEqual(append_ops[0]["rows"], 25000)

        self.assertEqual(len(update_ops), 3)
        for u in update_ops:
            self.assertEqual(u["limit"], 100)
            self.assertEqual(len(u["set"]), 1)

        self.assertEqual(len(delete_ops), 0)

        # Broker: append 25000, 3 updates of 100 rows, 1 delete of 1000 rows
        with open(broker_mig_path, "r", encoding="utf-8") as f:
            broker_ops = json.load(f)["operations"]

        self.assertEqual(len(broker_ops), 5)
        b_append_ops = [op for op in broker_ops if op["op"] == "append"]
        b_update_ops = [op for op in broker_ops if op["op"] == "update"]
        b_delete_ops = [op for op in broker_ops if op["op"] == "delete"]

        self.assertEqual(len(b_append_ops), 1)
        self.assertEqual(b_append_ops[0]["entity_fqn"], "trading_core.transactions")
        self.assertEqual(b_append_ops[0]["rows"], 25000)

        self.assertEqual(len(b_update_ops), 3)
        for u in b_update_ops:
            self.assertEqual(u["limit"], 100)
            self.assertEqual(len(u["set"]), 1)

        self.assertEqual(len(b_delete_ops), 1)
        self.assertEqual(b_delete_ops[0]["entity_fqn"], "trading_core.transactions")
        self.assertEqual(b_delete_ops[0]["limit"], 1000)
        self.assertFalse(b_delete_ops[0].get("cascade", True))

    def test_dags_definition(self):
        # Проверяем файлы DAG
        bank_dag_file = self.root / "airflow" / "dags" / "update_source_bank.py"
        broker_dag_file = self.root / "airflow" / "dags" / "update_source_broker.py"

        self.assertTrue(bank_dag_file.exists())
        self.assertTrue(broker_dag_file.exists())

        content_bank = bank_dag_file.read_text(encoding="utf-8")
        self.assertIn("update_source_bank", content_bank)
        self.assertIn("SydafaMigrateOperator", content_bank)
        self.assertIn('source="bank"', content_bank)
        self.assertIn('conn_id="sydafa_default"', content_bank)

        content_broker = broker_dag_file.read_text(encoding="utf-8")
        self.assertIn("update_source_broker", content_broker)
        self.assertIn("SydafaMigrateOperator", content_broker)
        self.assertIn('source="broker"', content_broker)
        self.assertIn('conn_id="sydafa_default"', content_broker)


if __name__ == "__main__":
    unittest.main()
