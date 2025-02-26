import asyncio
import logging
import datetime
import json
import os
import threading
import calendar
from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.event import async_track_time_interval, async_track_time_change

# watchdog 라이브러리 사용 (requirements에 추가 필요)
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities):
    # 기본 PSTEC TCP 센서 엔터티 생성
    sensor = PstecTcpSensor(hass, entry)
    sensor_entities = sensor.get_sensors()
    async_add_entities(sensor_entities)

    usage_sensors = [
         PstecUsageSensor(hass, entry, "tday", "rec"),
         PstecUsageSensor(hass, entry, "tday", "ret"),
         PstecUsageSensor(hass, entry, "tmon", "rec"),
         PstecUsageSensor(hass, entry, "tmon", "ret"),
         PstecUsageSensor(hass, entry, "tmon", "act"),
         PstecUsageSensor(hass, entry, "tmon", "fct"),
         PstecUsageSensor(hass, entry, "tday", "act"),
         PstecUsageSensor(hass, entry, "lday", "act"),
         PstecUsageSensor(hass, entry, "lmon", "act"),
         PstecUsageSensor(hass, entry, "lmon_record", "rec"),
         PstecUsageSensor(hass, entry, "lmon_record", "ret"),
         PstecUsageSensor(hass, entry, "lmon_record", "act")
    ]
    async_add_entities(usage_sensors)

    # 센서 시작 시, 각 사용량 센서가 파일에서 기준값을 업데이트하도록 호출
    for usage_sensor in usage_sensors:
        usage_sensor.update_from_file()  # 파일을 읽어 기준값(baseline) 업데이트 (필요한 경우)
        # 파일 기반 센서("lday", "lmon", "lmon_record")는 초기 업데이트 및 파일 변경시만 업데이트
        if usage_sensor._usage_type not in ("lday", "lmon", "lmon_record"):
            await usage_sensor.async_update()  # live 값과 비교하여 상태 산출

    # 기본 센서 초기 업데이트 (실패하더라도 진행)
    try:
        await sensor.async_update()
    except Exception as ex:
        _LOGGER.error("센서 초기 업데이트 실패: %s", ex)

    scan_interval = datetime.timedelta(seconds=entry.data.get("scan_interval", 10))
    async_track_time_interval(hass, sensor.async_update_interval, scan_interval)

    # 매일 00:00:00에 파일 저장 콜백: 일간 기록 파일에 현재 수전/송전 값을 기록하고, 90일 초과 기록은 삭제
    async def file_saving_callback(now):
        _LOGGER.debug("file_saving_callback 호출됨: %s", now.isoformat())
    
        # 센서 데이터 수집
        sensor_data = {}
        for sensor_entity in sensor._sensors:
            sensor_data[sensor_entity.name] = sensor_entity.state
        _LOGGER.debug("현재 센서 데이터: %s", sensor_data)
    
        # 오늘 기록 생성
        daily_record = {
            "date": now.strftime("%Y-%m-%d"),
            "rec_dev_record": sensor_data.get(f"{sensor._name}_rec_dev_record", "N/A"),
            "ret_dev_record": sensor_data.get(f"{sensor._name}_ret_dev_record", "N/A")
        }
        _LOGGER.debug("생성된 daily_record: %s", daily_record)
    
        # 파일 경로 설정
        device_name = sensor._name
        tday_file = hass.config.path(".storage", f"{device_name}_tday_energy.json")
        _LOGGER.debug("파일 경로: %s", tday_file)
    
        # 기존 파일 내용 읽기
        records = []
        if os.path.exists(tday_file):
            try:
                with open(tday_file, "r", encoding="utf-8") as f:
                    records = json.load(f)
                if not isinstance(records, list):
                    _LOGGER.debug("파일 내용이 리스트 형식이 아님. 초기화합니다.")
                    records = []
            except Exception as e:
                _LOGGER.error("일간 파일 읽기 오류 (%s): %s", tday_file, e)
                records = []
        _LOGGER.debug("파일에서 읽어온 기존 기록: %s", records)
    
        # 오늘 날짜 기록 업데이트 또는 추가
        today_str = now.strftime("%Y-%m-%d")
        updated = False
        for record in records:
            if record.get("date") == today_str:
                _LOGGER.debug("오늘 날짜 기록 발견, 업데이트 전: %s", record)
                record.update(daily_record)
                _LOGGER.debug("오늘 날짜 기록 업데이트 후: %s", record)
                updated = True
                break
        if not updated:
            records.append(daily_record)
            _LOGGER.debug("오늘 날짜 기록이 없어서 추가: %s", daily_record)
            _LOGGER.debug("오늘 날짜 기록이 없어서 추가 후: %s", records)
    
        # 90일보다 오래된 기록 제거 (날짜 비교 시 시간 정보를 제거)
        cutoff_date = (now - datetime.timedelta(days=90)).date()
        _LOGGER.debug("컷오프 날짜: %s", cutoff_date)
        records = [
            record for record in records
            if datetime.datetime.strptime(record.get("date"), "%Y-%m-%d").date() >= cutoff_date
        ]
        _LOGGER.debug("90일 이내의 최종 기록 리스트: %s", records)
        
        # 파일에 기록 저장
        try:
            with open(tday_file, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=4)
            _LOGGER.debug("일간 JSON 파일 저장 완료: %s", tday_file)
        except Exception as e:
            _LOGGER.error("일간 JSON 파일 저장 오류: %s", e)
    
    async_track_time_change(hass, file_saving_callback, hour=0, minute=0, second=0)
    
    # 컴포넌트 시작 시, 파일이 없으면 초기 저장을 시도 (scan_interval + 5초 지연)
    async def delayed_file_save():
        delay_time = scan_interval.total_seconds() + 5
        await asyncio.sleep(delay_time)
        now = datetime.datetime.now()
        device_name = sensor._name
        tday_file = hass.config.path(".storage", f"{device_name}_tday_energy.json")
        if os.path.exists(tday_file):
            _LOGGER.debug("초기 파일 저장 건너뜀: tday 파일이 이미 존재합니다.")
            return
        else:
            _LOGGER.debug("지연 후 초기 파일 저장 수행 (delay: %s seconds)", delay_time)
            await file_saving_callback(now)
                
    hass.async_create_task(delayed_file_save())

    # 파일 변경 감지를 위한 watchdog 핸들러 정의
    class UsageFileChangeHandler(FileSystemEventHandler):
        def __init__(self, hass, usage_sensors):
            super().__init__()
            self.hass = hass
            self.usage_sensors = usage_sensors

        def on_modified(self, event):
            # 변경된 파일이 사용량 센서에서 사용하는 파일이면 해당 센서 업데이트 실행
            for sensor in self.usage_sensors:
                if os.path.abspath(event.src_path) == os.path.abspath(sensor._file):
                    _LOGGER.debug("파일 변경 감지: %s", event.src_path)
                    self.hass.async_create_task(sensor.async_update())

    storage_dir = hass.config.path(".storage")
    event_handler = UsageFileChangeHandler(hass, usage_sensors)
    observer = Observer()
    observer.schedule(event_handler, storage_dir, recursive=False)
    observer_thread = threading.Thread(target=observer.start)
    observer_thread.daemon = True
    observer_thread.start()
    hass.data.setdefault("pstec_observer", {})[entry.entry_id] = observer


class PstecTcpSensor:
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
        em_id = self._em_id
        data_field = bytes.fromhex("00" * 20)
        bcc_bytes = bytes([stx, cmd]) + em_id + data_field
        bcc = 0x00
        for byte in bcc_bytes:
            bcc ^= byte
        return bytes([stx, cmd]) + em_id + data_field + bytes([bcc, 0x03])

    def get_sensors(self):
        sensor_types = [
            ("rec_dev_record", "kWh", "total_increasing", "energy"),
            ("ret_dev_record", "kWh", "total_increasing", "energy"),
            ("act_dev_record", "kWh", "total_increasing", "energy"),
            ("dev_voltage", "V", None, "voltage"),
            ("dev_current", "A", None, "current"),
            ("act_dev_power", "W", None, "power"),
            ("dev_frequency", "Hz", None, "frequency"),
            ("dev_factor", "PF", None, "power_factor"),
            ("dev_direction", None, None, None),
        ]
        self._sensors = [
            PstecSensorEntity(self._name, sensor_type[0], sensor_type[1], sensor_type[2], sensor_type[3], self._entry_id)
            for sensor_type in sensor_types
        ]
        return self._sensors

    async def async_update_interval(self, _):
        await self.async_update()

    async def async_update(self):
        max_retries = 3
        retry_delay = 3
        for attempt in range(max_retries):
            try:
                reader, writer = await asyncio.open_connection(self._host, self._port)
                _LOGGER.debug(f"연결 테스트: {self._host}:{self._port}")
                writer.write(self._payload)
                _LOGGER.debug(f"송신 패킷 (HEX): {self._payload.hex()}")
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

        if len(data) < 5 or data[-1] != 0x03:
            _LOGGER.error(f"잘못된 패킷: 길이={len(data)}, ETX={data[-1] if len(data)>=1 else '없음'}")
            return

        calculated_bcc = 0
        for byte in data[:-2]:
            calculated_bcc ^= byte
        if calculated_bcc != data[-2]:
            _LOGGER.error(f"BCC 불일치: 기대값={data[-2]}, 계산값={calculated_bcc}")
            return

        raw_data = data.hex()
        new_state = self._process_data(raw_data)
        if new_state:
            for sensor in self._sensors:
                key = f"{self._name}_{sensor.sensor_type}"
                if key in new_state:
                    sensor.set_state(new_state[key])

    def _process_data(self, data):
        if len(data) < 70:
            _LOGGER.warning("Invalid Data: Received data too short - Keeping Previous Value")
            return None

        rec = float(int(data[4:12], 10)) / 10
        ret = float(int(data[12:20], 10)) / 10
        return {
            f"{self._name}_rec_dev_record": rec,
            f"{self._name}_ret_dev_record": ret,
            f"{self._name}_act_dev_record": round(rec - ret, 1),
            f"{self._name}_dev_voltage": float(int(data[36:40], 10)) / 10,
            f"{self._name}_dev_current": float(int(data[40:44], 10)) / 10,
            f"{self._name}_act_dev_power": int(data[44:50], 10) * (-1 if int(data[64:66], 16) & 0x04 else 1),
            f"{self._name}_dev_frequency": float(int(data[56:60], 10)) / 100,
            f"{self._name}_dev_factor": float(int(data[60:64], 10)) / 100,
            f"{self._name}_dev_direction": "negative" if int(data[64:66], 16) & 0x04 else "positive",
        }


class PstecSensorEntity(SensorEntity):
    def __init__(self, prefix, sensor_type, unit, state_class, device_class, entry_id):
        self._name = f"{prefix}_{sensor_type}"
        self._unit = unit
        self._state_class = state_class
        self._device_class = device_class
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

    @property
    def device_class(self):
        return self._device_class


class PstecUsageSensor(SensorEntity):
    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, usage_type: str, record_type: str):
        """
        usage_type: "tday", "tmon", "lday", "lmon" 또는 "lmon_record"
        record_type: "rec", "ret" 또는 "act"
        """
        self.hass = hass
        self._entry_id = entry.entry_id
        device_name = entry.data["name"].lower().replace(" ", "_")
        self._device_name = device_name
        self._usage_type = usage_type
        self._record_type = record_type
        if usage_type == "lmon_record":
            self._name = f"{device_name}_{record_type}_{usage_type}"
        else:
            self._name = f"{device_name}_{record_type}_{usage_type}_total"
        self._state = None
        self._baseline = None  # 파일에서 읽은 기준값 (rec, ret는 개별, act는 net 값)
        self._file = hass.config.path(".storage", f"{device_name}_tday_energy.json")
        self._last_mod_time = None
        self._meter_reading_day = entry.data.get("meter_reading_day")

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
        return "kWh"

    def update_from_file(self):
        # 월간 센서: 파일에서 검침일과 일치하는 기록들을 필터링하여 최신 항목을 기준값으로 사용
        if self._usage_type == "tmon":
            if os.path.exists(self._file):
                try:
                    mod_time = os.path.getmtime(self._file)
                    if self._last_mod_time != mod_time:
                        self._last_mod_time = mod_time
                        with open(self._file, "r", encoding="utf-8") as f:
                            records = json.load(f)
                        if records and isinstance(records, list):
                            filtered_records = []
                            for rec in records:
                                try:
                                    d = datetime.datetime.strptime(rec.get("date"), "%Y-%m-%d")
                                    if d.day == int(self._meter_reading_day):
                                        filtered_records.append(rec)
                                except Exception as e:
                                    _LOGGER.error("날짜 파싱 오류: %s", e)
                            if filtered_records:
                                filtered_records.sort(key=lambda r: r.get("date", ""))
                                latest_record = filtered_records[-1]
                                if self._record_type == "rec":
                                    self._baseline = float(latest_record.get("rec_dev_record", 0))
                                elif self._record_type == "ret":
                                    self._baseline = float(latest_record.get("ret_dev_record", 0))
                                elif self._record_type == "act" or self._record_type == "fct":
                                    baseline_rec = float(latest_record.get("rec_dev_record", 0))
                                    baseline_ret = float(latest_record.get("ret_dev_record", 0))
                                    self._baseline = baseline_rec - baseline_ret
                                _LOGGER.debug("%s (월간) 센서 기준값 업데이트: %s", self._name, self._baseline)
                except Exception as e:
                    _LOGGER.error("파일 읽기 오류 (%s): %s", self._file, e)
        elif self._usage_type == "lmon_record":
            # act_lmon_record 센서는 파일에서 검침일과 일치하는 기록 중 최신 항목의 net 값을 baseline으로 사용
            if os.path.exists(self._file):
                try:
                    with open(self._file, "r", encoding="utf-8") as f:
                        records = json.load(f)
                    if records and isinstance(records, list):
                        filtered = []
                        for rec in records:
                            try:
                                d = datetime.datetime.strptime(rec.get("date"), "%Y-%m-%d")
                                if d.day == int(self._meter_reading_day):
                                    filtered.append(rec)
                            except Exception as e:
                                _LOGGER.error("날짜 파싱 오류: %s", e)
                        if filtered:
                            filtered.sort(key=lambda r: r.get("date", ""))
                            latest_record = filtered[-1]
                            if self._record_type == "rec":
                                self._baseline = float(latest_record.get("rec_dev_record", 0))
                            elif self._record_type == "ret":
                                self._baseline = float(latest_record.get("ret_dev_record", 0))
                            elif self._record_type == "act":
                                self._baseline = float(latest_record.get("rec_dev_record", 0)) - float(latest_record.get("ret_dev_record", 0))
                            _LOGGER.debug("%s (전월 record) 센서 기준값 업데이트: %s", self._name, self._baseline)
                        else:
                            self._baseline = None
                    else:
                        self._baseline = None
                except Exception as e:
                    _LOGGER.error("파일 읽기 오류 (%s): %s", self._file, e)
        elif self._usage_type == "lday":
            # lday 센서는 파일에서 어제 기록을 이용 (async_update에서 처리)
            pass
        elif self._usage_type == "lmon":
            # lmon 센서는 파일에서 최근 두 검침일 기록의 차이를 이용 (async_update에서 처리)
            pass
        else:
            # tday 센서: 파일에서 최신 기록을 기준으로 업데이트
            if os.path.exists(self._file):
                try:
                    mod_time = os.path.getmtime(self._file)
                    if self._last_mod_time != mod_time:
                        self._last_mod_time = mod_time
                        with open(self._file, "r", encoding="utf-8") as f:
                            records = json.load(f)
                        if records and isinstance(records, list):
                            records.sort(key=lambda r: r.get("date", ""))
                            latest_record = records[-1]
                            if self._record_type == "rec":
                                self._baseline = float(latest_record.get("rec_dev_record", 0))
                            elif self._record_type == "ret":
                                self._baseline = float(latest_record.get("ret_dev_record", 0))
                            elif self._record_type == "act":
                                baseline_rec = float(latest_record.get("rec_dev_record", 0))
                                baseline_ret = float(latest_record.get("ret_dev_record", 0))
                                self._baseline = baseline_rec - baseline_ret
                            _LOGGER.debug("%s (일간) 센서 기준값 업데이트: %s", self._name, self._baseline)
                except Exception as e:
                    _LOGGER.error("파일 읽기 오류 (%s): %s", self._file, e)

    async def async_update(self):
        # lday와 lmon은 파일 기록 간 차이를 이용해 계산
        if self._usage_type == "lday":
            file_path = self._file
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    records = json.load(f)
                if records and isinstance(records, list):
                    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
                    yesterday_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                    today_record = None
                    yesterday_record = None
                    for rec in records:
                        if rec.get("date") == today_str:
                            today_record = rec
                        if rec.get("date") == yesterday_str:
                            yesterday_record = rec
                    if today_record is not None and yesterday_record is not None:
                        try:
                            today_net = float(today_record.get("rec_dev_record", 0)) - float(today_record.get("ret_dev_record", 0))
                            yesterday_net = float(yesterday_record.get("rec_dev_record", 0)) - float(yesterday_record.get("ret_dev_record", 0))
                            diff = today_net - yesterday_net
                            self._state = round(diff, 1)
                        except Exception as e:
                            _LOGGER.error("계산 오류 (lday): %s", e)
                            self._state = None
                    else:
                        self._state = None
                else:
                    self._state = None
            except Exception as e:
                _LOGGER.error("파일 읽기 오류 (%s): %s", file_path, e)
                self._state = None

        elif self._usage_type == "lmon":
            if os.path.exists(self._file):
                try:
                    with open(self._file, "r", encoding="utf-8") as f:
                        records = json.load(f)
                    if records and isinstance(records, list):
                        filtered = []
                        for rec in records:
                            try:
                                d = datetime.datetime.strptime(rec.get("date"), "%Y-%m-%d")
                                if d.day == int(self._meter_reading_day):
                                    filtered.append(rec)
                            except Exception as e:
                                _LOGGER.error("날짜 파싱 오류: %s", e)
                        if len(filtered) >= 2:
                            filtered.sort(key=lambda r: r.get("date", ""))
                            previous_record = filtered[-2]
                            latest_record = filtered[-1]
                            try:
                                previous_net = float(previous_record.get("rec_dev_record", 0)) - float(previous_record.get("ret_dev_record", 0))
                                latest_net = float(latest_record.get("rec_dev_record", 0)) - float(latest_record.get("ret_dev_record", 0))
                                self._state = round(latest_net - previous_net, 1)
                            except Exception as e:
                                _LOGGER.error("계산 오류 (lmon): %s", e)
                                self._state = None
                        else:
                            self._state = None
                    else:
                        self._state = None
                except Exception as e:
                    _LOGGER.error("파일 읽기 오류 (%s): %s", self._file, e)
                    self._state = None
            else:
                self._state = None

        elif self._usage_type == "lmon_record":
            if os.path.exists(self._file):
                try:
                    with open(self._file, "r", encoding="utf-8") as f:
                        records = json.load(f)
                    if records and isinstance(records, list):
                        filtered = []
                        for rec in records:
                            try:
                                d = datetime.datetime.strptime(rec.get("date"), "%Y-%m-%d")
                                if d.day == int(self._meter_reading_day):
                                    filtered.append(rec)
                            except Exception as e:
                                _LOGGER.error("날짜 파싱 오류: %s", e)
                        if filtered:
                            try:
                                filtered.sort(key=lambda r: r.get("date", ""))
                                latest_record = filtered[-1]
                                if self._record_type == "rec":
                                    net = float(latest_record.get("rec_dev_record", 0))
                                elif self._record_type == "ret":
                                    net = float(latest_record.get("ret_dev_record", 0))
                                elif self._record_type == "act":
                                    net = float(latest_record.get("rec_dev_record", 0)) - float(latest_record.get("ret_dev_record", 0))
                                self._state = round(net, 1)
                            except Exception as e:
                                _LOGGER.error("계산 오류 (lmon_record): %s", e)
                                self._state = None
                        else:
                            self._state = None
                    else:
                        self._state = None
                except Exception as e:
                    _LOGGER.error("파일 읽기 오류 (%s): %s", self._file, e)
                    self._state = None
            else:
                self._state = None

        elif self._record_type in ["rec", "ret"]:
            base_entity_id = f"sensor.{self._device_name}_{self._record_type}_dev_record"
            base_state = self.hass.states.get(base_entity_id)
            if base_state is not None and base_state.state not in (None, "unknown", "N/A"):
                try:
                    current_value = float(base_state.state)
                except ValueError:
                    current_value = 0
                if self._baseline is not None:
                    self._state = round(current_value - self._baseline, 1)
                else:
                    self._state = None
            else:
                self._state = None

        elif self._record_type == "act":
            rec_entity_id = f"sensor.{self._device_name}_rec_dev_record"
            ret_entity_id = f"sensor.{self._device_name}_ret_dev_record"
            rec_state = self.hass.states.get(rec_entity_id)
            ret_state = self.hass.states.get(ret_entity_id)
            if (rec_state is not None and ret_state is not None and
                rec_state.state not in (None, "unknown", "N/A") and
                ret_state.state not in (None, "unknown", "N/A")):
                try:
                    current_rec = float(rec_state.state)
                    current_ret = float(ret_state.state)
                except ValueError:
                    current_rec, current_ret = 0, 0
                current_net = current_rec - current_ret
                if self._baseline is not None:
                    self._state = round(current_net - self._baseline, 1)
                else:
                    self._state = None
            else:
                self._state = None
                
        elif self._record_type == "fct":
            rec_entity_id = f"sensor.{self._device_name}_rec_dev_record"
            ret_entity_id = f"sensor.{self._device_name}_ret_dev_record"
            rec_state = self.hass.states.get(rec_entity_id)
            ret_state = self.hass.states.get(ret_entity_id)
            act_lmon_total_state = self.hass.states.get(f"sensor.{self._device_name}_act_lmon_total")
            if (rec_state is not None and ret_state is not None and
                rec_state.state not in (None, "unknown", "N/A") and
                ret_state.state not in (None, "unknown", "N/A")):
                try:
                    current_rec = float(rec_state.state)
                    current_ret = float(ret_state.state)
                except ValueError:
                    current_rec, current_ret = 0, 0
                if self._baseline is not None:
                    current_net = (current_rec - current_ret) - self._baseline
                else:
                     current_net = 0
                now = datetime.datetime.now()
                meter_day = int(self._meter_reading_day)
                # 검침일 기준으로 지난 검침일과 다음 검침일 계산
                if now.day >= meter_day:
                    last_meter_date = now.replace(day=meter_day, hour=0, minute=0, second=0, microsecond=0)
                    if now.month == 12:
                        next_month = 1
                        next_year = now.year + 1
                    else:
                        next_month = now.month + 1
                        next_year = now.year
                    try:
                        next_meter_date = now.replace(year=next_year, month=next_month, day=meter_day, hour=0, minute=0, second=0, microsecond=0)
                    except ValueError:
                        last_day = calendar.monthrange(next_year, next_month)[1]
                        next_meter_date = now.replace(year=next_year, month=next_month, day=last_day, hour=0, minute=0, second=0, microsecond=0)
                else:
                    if now.month == 1:
                        prev_month = 12
                        prev_year = now.year - 1
                    else:
                        prev_month = now.month - 1
                        prev_year = now.year
                    try:
                        last_meter_date = now.replace(year=prev_year, month=prev_month, day=meter_day, hour=0, minute=0, second=0, microsecond=0)
                    except ValueError:
                        last_day = calendar.monthrange(prev_year, prev_month)[1]
                        last_meter_date = now.replace(year=prev_year, month=prev_month, day=last_day, hour=0, minute=0, second=0, microsecond=0)
                    try:
                        next_meter_date = now.replace(day=meter_day, hour=0, minute=0, second=0, microsecond=0)
                    except ValueError:
                        last_day = calendar.monthrange(now.year, now.month)[1]
                        next_meter_date = now.replace(day=last_day, hour=0, minute=0, second=0, microsecond=0)
                total_period = int((next_meter_date - last_meter_date).total_seconds() / 3600)
                elapsed = int((now - last_meter_date).total_seconds() / 3600)
                if elapsed > 18:
                    forecast = (current_net / elapsed) * total_period
                    self._state = round(forecast, 1)
                else:
                    self._state = float(act_lmon_total_state.state)
            else:
                self._state = None
                
        self.async_write_ha_state()
