import unittest

from transport_coordination.acceptance_hub import run


class HubAcceptanceTest(unittest.TestCase):
    def test_offline_hub_acceptance(self):
        result = run()
        self.assertEqual("ok", result["status"])
        self.assertTrue(result["audit_before_valid"])
        self.assertTrue(result["audit_after_valid"])
        self.assertTrue(result["occupancies_preserved"])
        self.assertEqual(4, result["committed_occupancies"])
        self.assertEqual(18, result["kpi_total_units"])
        self.assertEqual(10, result["kpi_on_time_units"])


if __name__ == "__main__":
    unittest.main()
