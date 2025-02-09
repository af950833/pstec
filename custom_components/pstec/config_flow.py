import voluptuous as vol
from homeassistant import config_entries
from . import DOMAIN

class PstecConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for PSTEC Sensor."""

    async def async_step_user(self, user_input=None):
        """Handle the initial step."""
        errors = {}
        if user_input is not None:
            return self.async_create_entry(title=user_input["name"], data=user_input)

        data_schema = vol.Schema({
            vol.Required("name"): str,
            vol.Required("host"): str,
            vol.Required("port", default=8899): int,
            vol.Required("payload"): str,
            vol.Optional("scan_interval", default=10): int,
        })

        return self.async_show_form(step_id="user", data_schema=data_schema, errors=errors)
