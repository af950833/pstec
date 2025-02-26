import voluptuous as vol
from homeassistant import config_entries
from homeassistant.helpers import selector
import asyncio
import logging
from . import DOMAIN

_LOGGER = logging.getLogger(__name__)

class PstecConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """PSTEC 설정 플로우 처리"""

    async def async_step_user(self, user_input=None):
        """사용자 초기 설정 단계"""
        errors = {}
        if user_input is not None:
            try:
                # EM ID 자동 조회
                reader, writer = await asyncio.open_connection(
                    user_input["host"], user_input["port"]
                )
                
                # EM ID 요청 패킷 생성
                request_packet = bytes([0x81, 0x55])  # STX(0x81) + CMD(0x55)
                bcc = request_packet[0] ^ request_packet[1]
                request_packet += bytes([bcc, 0x03])  # BCC + ETX
                
                # 패킷 전송 및 응답 대기
                writer.write(request_packet)
                await writer.drain()
                data = await reader.read(32)
                writer.close()
                await writer.wait_closed()

                # 응답 검증
                if len(data) < 8 or data[0] != 0x81 or data[1] != 0x55:
                    raise ValueError("유효하지 않은 EM ID 응답 패킷")

                # EM ID 추출 (4바이트 BCD)
                em_id = data[2:6].hex()
                user_input["em_id"] = em_id

                return self.async_create_entry(
                    title=user_input["name"], 
                    data=user_input
                )

            except (ConnectionRefusedError, asyncio.TimeoutError) as e:
                errors["base"] = "connection_error"
                _LOGGER.error(f"장치 연결 실패: {str(e)}")
            except Exception as e:
                errors["base"] = "unknown_error"
                _LOGGER.error(f"EM ID 조회 오류: {str(e)}")

        data_schema = vol.Schema({
            vol.Required("name"): str,
            vol.Required("meter_reading_day", default=25): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=1,
                    max=31,
                    step=1,
                    mode="box"
                )
            ),
            vol.Required("host"): str,
            vol.Required("port", default=8899): int,
            vol.Optional("scan_interval", default=10): int,
        })

        return self.async_show_form(
            step_id="user", 
            data_schema=data_schema, 
            errors=errors
        )
