"""
Shared interface module
Provides access to the Meshtastic interface from any plugin.
"""

# Global interface reference - set by main.py on connection
_interface = None


def get_interface():
    """Get the current Meshtastic interface, or None if not connected."""
    return _interface


def set_interface(interface):
    """Set the Meshtastic interface reference."""
    global _interface
    _interface = interface


def clear_interface():
    """Clear the interface reference (on disconnect)."""
    global _interface
    _interface = None
