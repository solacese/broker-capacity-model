import copy
import json
import tempfile
import unittest
from pathlib import Path

from build import load_model_config


class ModelConfigTests(unittest.TestCase):
    def test_versioned_config_is_valid(self):
        config = load_model_config()
        self.assertEqual(config["status"], "ILLUSTRATIVE")
        self.assertEqual(len(config["features"]), 20)

    def test_new_feature_requires_metadata_and_unique_id(self):
        config = load_model_config()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "calibration.json"
            updated = copy.deepcopy(config)
            updated["features"].append({
                "id": "new_feature", "label": "New broker feature", "group": "Queue behavior",
                "factor": 1.02, "doc_url": "https://docs.solace.com/Messaging/Guaranteed-Msg/Queues.htm",
                "doc_help": "Description of the feature's conditional performance mechanism."
            })
            updated["model_version"] = "illustrative-next"
            path.write_text(json.dumps(updated))
            self.assertEqual(len(load_model_config(path)["features"]), 21)
            updated["features"][-1]["id"] = "tls"
            path.write_text(json.dumps(updated))
            with self.assertRaises(ValueError):
                load_model_config(path)


if __name__ == "__main__":
    unittest.main()
