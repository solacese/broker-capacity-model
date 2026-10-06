from __future__ import annotations

import math
import unittest

from broker_capacity.config_writer import ConfigAccess
from broker_capacity.experiments import DeliveryMode, Workload


class SafetyTests(unittest.TestCase):
    def test_config_access_rejects_other_service(self) -> None:
        with self.assertRaisesRegex(ValueError, "dedicated 5K"):
            ConfigAccess("wrong", "usfos7cfqge", "bcm-20261005-5k", "https://x", "u", "p")

    def test_config_access_rejects_other_vpn(self) -> None:
        with self.assertRaisesRegex(ValueError, "dedicated 5K VPN"):
            ConfigAccess("4qn20a1u6ny", "usfos7cfqge", "wrong", "https://x", "u", "p")

    def test_workload_rejects_non_finite_rate(self) -> None:
        with self.assertRaisesRegex(ValueError, "finite"):
            Workload(DeliveryMode.DIRECT, "SMF", "SMF", 1024, 1, ("d",), math.inf)


if __name__ == "__main__":
    unittest.main()
