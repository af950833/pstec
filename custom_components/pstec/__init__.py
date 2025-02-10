"""PSTEC Custom Component"""
DOMAIN = "pstec"

async def async_setup_entry(hass, entry):
    """Set up the PSTEC component from a config entry."""
    hass.async_create_task(hass.config_entries.async_forward_entry_setups(entry, ["sensor"]))
    return True

async def async_unload_entry(hass, entry):
    """Unload a config entry."""
    return await hass.config_entries.async_forward_entry_unload(entry, "sensor")
