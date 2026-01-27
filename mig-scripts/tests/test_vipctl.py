import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import importlib.util, os

# load vipctl by file to avoid package name issues
_spec = importlib.util.spec_from_file_location("vipctl", os.path.join(os.path.dirname(__file__), "..", "vipctl.py"))
vipctl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(vipctl)


class TestVipCtl(unittest.TestCase):
    def test_set_keepalived_priority_edits_file_and_reloads(self):
        # create temp config
        content = """vrrp_instance VI_1 {
    state BACKUP
    interface eth0
    virtual_router_id 51
    priority 90
}
"""
        with tempfile.TemporaryDirectory() as td:
            conf = os.path.join(td, "keepalived.conf")
            with open(conf, "w") as f:
                f.write(content)

            # mock subprocess.run for reload
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                rc = vipctl.set_keepalived_priority(50, config_path=conf, backup_path=conf + ".bak", reload_cmd=["echo", "reload"])
                self.assertEqual(rc, 0)
                newconf = open(conf).read()
                self.assertIn("priority 50", newconf)
                # ensure reload was called
                mock_run.assert_called()

    def test_ip_add_del_and_announce_dry_run(self):
        # dry run should not call subprocess
        rc = vipctl.ip_addr_add("ethX", "10.0.0.5", dry_run=True)
        self.assertEqual(rc, 0)
        rc = vipctl.ip_addr_del("ethX", "10.0.0.5", dry_run=True)
        self.assertEqual(rc, 0)
        rc = vipctl.arping_announce("ethX", "10.0.0.5", dry_run=True)
        self.assertEqual(rc, 0)

    def test_switch_local_calls_ip_and_arping(self):
        with patch.object(vipctl, "ip_addr_add") as m_add:
            with patch.object(vipctl, "arping_announce") as m_ar:
                m_add.return_value = 0
                m_ar.return_value = 0
                rc = vipctl.switch_local_add_then_announce("ethX", "10.0.0.8", prefix=32, announce_count=2, dry_run=False)
                self.assertEqual(rc, 0)
                m_add.assert_called_once()
                m_ar.assert_called_once()


if __name__ == "__main__":
    unittest.main()