"""
Centralized defaults for which source/destination script to run.

Other scripts in mig-scripts should import and use choose_scripts(sec)
to obtain the proper `SOURCE_SCRIPT` and `DEST_SCRIPT` values so that
the `--sec` flag is handled from a single location.
"""

DEFAULT_SOURCE = "source.py"
DEFAULT_DEST = "destination.py"


def choose_scripts(sec=False):
    """Return (source_script, dest_script) depending on sec flag.

    Args:
        sec (bool): when True choose secure variants.

    Returns:
        tuple: (source_script, dest_script)
    """
    if sec:
        return "source-sec.py", "destination-sec.py"
    return DEFAULT_SOURCE, DEFAULT_DEST


# Default network addresses used by migration scripts. Edit here to change global defaults.
DEFAULT_SOURCE_IP = "192.168.2.105"
DEFAULT_DEST_IP = "192.168.2.225"
DEFAULT_CLIENT_IP = DEFAULT_DEST_IP
DEFAULT_VIP = "192.168.2.100"


def get_default_ips():
    """Return (source_ip, dest_ip, client_ip, vip) defaults."""
    return DEFAULT_SOURCE_IP, DEFAULT_DEST_IP, DEFAULT_CLIENT_IP, DEFAULT_VIP
