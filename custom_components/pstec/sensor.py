import asyncio
import logging
import datetime
from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.event import async_track_time_interval

_LOGGER = logging.getLogger(__name__)

DEFAULT_PAYLOAD = "81 72 52 16 98 05 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 2A 03"

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities):
    """GUI에서 추가된 설정을 기반으로 센서를 생성"""
    sensor = PstecTcpSensor(hass, entry)
    async_add_entities(sensor.get_sensors())

    scan_interval = datetime.timedelta(seconds=entry.data.get("scan_interval", 10))
    async_track_time_interval(hass, sensor.async_update_interval, scan_interval)

class PstecTcpSensor:
    """TCP 센서 (BCC 검증 및 여러 개의 센서 생성 지원)"""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry):
        """GUI에서 설정된 값 적용"""
        self.hass = hass
        self._entry_id = entry.entry_id  # ✅ 엔터티 ID 설정용
        self._name = entry.data["name"].lower().replace(" ", "_")
        self._host = entry.data["host"]
        self._port = entry.data["port"]
        self._scan_interval = entry.data.get("scan_interval", 10)
        self._payload = bytes.fromhex(entry.data["payload"].replace(" ", ""))
        self._sensors = []

    def get_sensors(self):
        """각 데이터를 개별 센서로 반환"""
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

        # ✅ `entity_id` 직접 설정 제거 → Home Assistant 자동 생성
        self._sensors = [
            PstecSensorEntity(self._name, sensor_type[0], sensor_type[1], sensor_type[2], self._entry_id)
            for sensor_type in sensor_types
        ]

        return self._sensors

    async def async_update_interval(self, _):
        """scan_interval 주기로 업데이트 실행"""
        await self.async_update()

    async def async_update(self):
        """센서 데이터를 주기적으로 업데이트"""
        try:
            reader, writer = await asyncio.open_connection(self._host, self._port)
            writer.write(self._payload)
            await writer.drain()

            data = await reader.read(1024)
            writer.close()
            await writer.wait_closed()

            raw_data = data.hex()
            _LOGGER.debug(f"Raw Data Received: {raw_data}")

            if len(data) < 4:  # ✅ 데이터 길이 검증 추가 (기존 값 유지)
                _LOGGER.warning("Invalid Data: Received packet is too short - Keeping Previous Value")
                return

            new_state = self._process_data(raw_data)
            
            if new_state:
                for sensor in self._sensors:  # ✅ 각 센서 값 개별 업데이트
                    if sensor._name in new_state:
                        sensor.set_state(new_state[sensor._name])

        except Exception as e:
            _LOGGER.error(f"TCP Sensor Error: {str(e)} - Keeping Previous Value")

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
            f"{self._name}_dev_direction": "{:b}".format(int(data[65:66], 10)).zfill(4)[1],
        }

class PstecSensorEntity(SensorEntity):
    """각 센서 값을 개별 엔터티로 관리"""

    def __init__(self, name, sensor_type, unit, state_class, entry_id):
        self._name = f"{name}_{sensor_type}"  # ✅ 센서 이름 자동 생성
        self._unit_of_measurement = unit
        self._state_class = state_class
        self._state = None
        self._entry_id = entry_id

    @property
    def name(self):
        """Home Assistant에서 엔터티 이름으로 표시될 값"""
        return self._name

    @property
    def unique_id(self):
        """✅ 엔터티 고유 ID 추가 (Device 없이 Entity만 표시)"""
        return f"{self._entry_id}_{self._name}"  # ✅ `entity_id` 대신 `unique_id` 사용

    @property
    def state(self):
        return self._state

    @property
    def unit_of_measurement(self):
        return self._unit_of_measurement

    @property
    def state_class(self):
        return self._state_class

    def set_state(self, value):
        """상태 업데이트"""
        self._state = value
        self.async_write_ha_state()
