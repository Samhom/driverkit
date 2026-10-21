import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import check_compatibility as contract


class CompatibilityContractTest(unittest.TestCase):
    def fixture(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        shutil.copy(contract.ROOT / "pom.xml", root / "pom.xml")
        (root / "docs").mkdir()
        shutil.copy(contract.ROOT / "docs/compatibility.json", root / "docs/compatibility.json")
        for pom in contract.ROOT.glob("driverkit-*/pom.xml"):
            (root / pom.parent.name).mkdir()
            shutil.copy(pom, root / pom.parent.name / "pom.xml")
        return root

    def test_verified_server_cannot_be_declared_without_evidence(self):
        root = self.fixture()
        path = root / "docs/compatibility.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        row = next(row for row in document["combinations"] if row["status"] == "theoretical")
        row["status"] = "verified"
        row.pop("evidence", None)
        path.write_text(json.dumps(document), encoding="utf-8")
        with patch.object(contract, "ROOT", root), self.assertRaisesRegex(ValueError, "evidence|Verified"):
            contract.validate()

    def test_native_upgrade_requires_matrix_update(self):
        root = self.fixture()
        pom = root / "pom.xml"
        current = pom.read_text(encoding="utf-8")
        pom.write_text(current.replace("<jedis.version>8.0.2</jedis.version>", "<jedis.version>8.0.3</jedis.version>"), encoding="utf-8")
        with patch.object(contract, "ROOT", root), self.assertRaisesRegex(ValueError, "drift|actual native"):
            contract.validate()


if __name__ == "__main__":
    unittest.main()
