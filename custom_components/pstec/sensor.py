import asyncio
import logging
import datetime
from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.event import async_track_time_interval

_LOGGER = logging.getLogger(__name__)

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities):
    """센서 엔터티 설정"""
    sensor = PstecTcpSensor(hass, entry)
    async_add_entities(sensor.get_sensors())

    scan_interval = datetime.timedelta(seconds=entry.data.get("scan_interval", 10))
    async_track_time_interval(hass, sensor.async_update_interval, scan_interval)

class PstecTcpSensor:
    """TCP 통신 및 데이터 처리 클래스"""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry):
        self.hass = hass
        self._entry_id = entry.entry_id
        self._name = entry.data["name"].lower().replace(" ", "_")
        self._host = entry.data["host"]
        self._port = entry.data["port"]
        self._scan_interval = entry.data.get("scan_interval", 10)
        self._em_id = bytes.fromhex(entry.data["em_id"])
        self._payload = self._build_request_packet()
        self._sensors = []

    def _build_request_packet(self):
        stx = 0x81
        cmd = 0x72
        em_id = self._em_id  # 4바이트 BCD
        data_field = bytes.fromhex("00" * 20)  # 20바이트 NULL (GM, WM, HWM, HM, SM)
        
        # BCC 계산 (STX + CMD + EM ID + DATA + STATUS)
        bcc_bytes = bytes([stx, cmd]) + em_id + data_field
        bcc = 0x00
        for byte in bcc_bytes:
            bcc ^= byte
        
        # 패킷 조립
        return bytes([stx, cmd]) + em_id + data_field + bytes([bcc, 0x03])

    def get_sensors(self):
        """센서 엔터티 리스트 생성"""
        sensor_types = [
            ("rec_dev_record", "kWh", "total_increasing"),
            ("ret_dev_record", "kWh", "total_increasing"),
            ("dev_voltage", "V", None),
            ("dev_current", "A", None),
            ("act_dev_power", "W", None),
            ("dev_frequency", "Hz", None),
            ("dev_factor", "PF", None),
            ("dev_direction", None, None),
        ]
        self._sensors = [
            PstecSensorEntity(self._name, sensor_type[0], sensor_type[1], sensor_type[2], self._entry_id)
            for sensor_type in sensor_types
        ]
        return self._sensors

    async def async_update_interval(self, _):
        """주기적 업데이트 트리거"""
        await self.async_update()

    async def async_update(self):
        max_retries = 3
        retry_delay = 3
        for attempt in range(max_retries):
            try:
                reader, writer = await asyncio.open_connection(self._host, self._port)
                _LOGGER.debug(f"연결 테스트: {self._host}:{self._port}")
                writer.write(self._payload)
                _LOGGER.debug(f"송신 패킷 (HEX): {self._payload.hex()}")  # 추가된 코드
                await writer.drain()
                data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
                writer.close()
                await writer.wait_closed()
                _LOGGER.debug(f"Raw Data (Hex): {data.hex()}")
                break
            except (asyncio.TimeoutError, ConnectionResetError) as e:
                if attempt == max_retries - 1:
                    _LOGGER.error("최대 재시도 횟수 도달. 업데이트 중단")
                    return
                _LOGGER.warning(f"연결 실패 ({attempt+1}회 재시도)...")
                await asyncio.sleep(retry_delay)
            except Exception as e:
                _LOGGER.error(f"통신 오류: {str(e)}")
                return

        # 패킷 유효성 검증
        if len(data) < 5 or data[-1] != 0x03:
            _LOGGER.error(f"잘못된 패킷: 길이={len(data)}, ETX={data[-1] if len(data)>=1 else '없음'}")
            return

        # BCC 검증
        calculated_bcc = 0
        for byte in data[:-2]:
            calculated_bcc ^= byte
        if calculated_bcc != data[-2]:
            _LOGGER.error(f"BCC 불일치: 기대값={data[-2]}, 계산값={calculated_bcc}")
            return

        # 데이터 파싱
        raw_data = data.hex()
        new_state = self._process_data(raw_data)
        if new_state:
            for sensor in self._sensors:
                key = f"{self._name}_{sensor.sensor_type}"
                if key in new_state:
                    sensor.set_state(new_state[key])

    def _process_data(self, data):
        """데이터 검증 및 값 변환"""
        if len(data) < 70:
            _LOGGER.warning("Invalid Data: Received data too short - Keeping Previous Value")
            return None

        return {
            f"{self._name}_rec_dev_record": float(int(data[4:12], 10)) / 10,
            f"{self._name}_ret_dev_record": float(int(data[12:20], 10)) / 10,
            f"{self._name}_dev_voltage": float(int(data[36:40], 10)) / 10,
            f"{self._name}_dev_current": float(int(data[40:44], 10)) / 10,
            f"{self._name}_act_dev_power": int(data[44:50], 10) * (-1 if int(data[65:66], 10) & 0x2 else 1),
            f"{self._name}_dev_frequency": float(int(data[56:60], 10)) / 100,
            f"{self._name}_dev_factor": float(int(data[60:64], 10)) / 100,
            f"{self._name}_dev_direction": "negative" if "{:b}".format(int(data[65:66], 10)).zfill(4)[1] == "1" else "positive",
        }

class PstecSensorEntity(SensorEntity):
    """개별 센서 엔터티 정의"""

    def __init__(self, prefix, sensor_type, unit, state_class, entry_id):
        self._name = f"{prefix}_{sensor_type}"
        self._unit = unit
        self._state_class = state_class
        self._state = None
        self._entry_id = entry_id
        self.sensor_type = sensor_type

    @property
    def name(self):
        return self._name

    @property
    def unique_id(self):
        return f"{self._entry_id}_{self._name}"

    @property
    def state(self):
        return self._state

    @property
    def unit_of_measurement(self):
        return self._unit

    @property
    def state_class(self):
        return self._state_class

    def set_state(self, value):
        self._state = value
        self.async_write_ha_state()