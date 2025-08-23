#!/usr/bin/env python3
"""
Device Driver v2.0
ISO 62304 - Clase A (Crítica para seguridad)

Controlador para comunicación con dispositivos Holter.
"""

import serial
import time
import threading
from typing import Optional, Dict, List, Callable
from enum import Enum
import logging
from dataclasses import dataclass

class DeviceStatus(Enum):
    """Estados del dispositivo Holter"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECORDING = "recording"
    DOWNLOADING = "downloading"
    ERROR = "error"

@dataclass
class DeviceInfo:
    """Información del dispositivo Holter"""
    device_id: str
    model: str
    firmware_version: str
    battery_level: int
    memory_usage: float
    status: DeviceStatus

class HolterDriver:
    """Controlador principal para dispositivos Holter"""
    
    def __init__(self, port: str = 'COM1', baudrate: int = 9600):
        self.port = port
        self.baudrate = baudrate
        self.connection: Optional[serial.Serial] = None
        self.device_info: Optional[DeviceInfo] = None
        self.status = DeviceStatus.DISCONNECTED
        self.logger = logging.getLogger(__name__)
        
        # Callbacks para eventos
        self.on_data_received: Optional[Callable] = None
        self.on_status_changed: Optional[Callable] = None
        
        # Threading para comunicación asíncrona
        self.read_thread: Optional[threading.Thread] = None
        self.running = False
        
    def connect(self, timeout: float = 5.0) -> bool:
        """Conecta con el dispositivo Holter"""
        try:
            self.status = DeviceStatus.CONNECTING
            self._notify_status_change()
            
            self.connection = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=timeout
            )
            
            # Verificar comunicación
            if self._handshake():
                self.device_info = self._get_device_info()
                self.status = DeviceStatus.CONNECTED
                
                # Iniciar thread de lectura
                self.running = True
                self.read_thread = threading.Thread(target=self._read_loop)
                self.read_thread.daemon = True
                self.read_thread.start()
                
                self.logger.info(f"Conectado a dispositivo: {self.device_info.device_id}")
                self._notify_status_change()
                return True
            else:
                self.disconnect()
                return False
                
        except Exception as e:
            self.logger.error(f"Error en conexión: {e}")
            self.status = DeviceStatus.ERROR
            self._notify_status_change()
            return False
    
    def disconnect(self):
        """Desconecta del dispositivo"""
        self.running = False
        
        if self.read_thread and self.read_thread.is_alive():
            self.read_thread.join(timeout=2.0)
            
        if self.connection and self.connection.is_open:
            self.connection.close()
            
        self.status = DeviceStatus.DISCONNECTED
        self.device_info = None
        self.logger.info("Dispositivo desconectado")
        self._notify_status_change()
    
    def start_recording(self) -> bool:
        """Inicia grabación en el dispositivo"""
        if not self._is_connected():
            return False
            
        try:
            command = b'START_RECORDING\\r\\n'
            self.connection.write(command)
            response = self.connection.readline()
            
            if b'OK' in response:
                self.status = DeviceStatus.RECORDING
                self._notify_status_change()
                self.logger.info("Grabación iniciada")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Error iniciando grabación: {e}")
            return False
    
    def stop_recording(self) -> bool:
        """Detiene la grabación"""
        if not self._is_connected():
            return False
            
        try:
            command = b'STOP_RECORDING\\r\\n'
            self.connection.write(command)
            response = self.connection.readline()
            
            if b'OK' in response:
                self.status = DeviceStatus.CONNECTED
                self._notify_status_change()
                self.logger.info("Grabación detenida")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Error deteniendo grabación: {e}")
            return False
    
    def download_data(self, progress_callback: Optional[Callable] = None) -> List[bytes]:
        """Descarga datos del dispositivo"""
        if not self._is_connected():
            return []
            
        try:
            self.status = DeviceStatus.DOWNLOADING
            self._notify_status_change()
            
            # Solicitar datos
            command = b'DOWNLOAD_DATA\\r\\n'
            self.connection.write(command)
            
            data_blocks = []
            while True:
                block = self.connection.read(1024)
                if not block or b'END_DATA' in block:
                    break
                    
                data_blocks.append(block)
                
                if progress_callback:
                    progress_callback(len(data_blocks))
            
            self.status = DeviceStatus.CONNECTED
            self._notify_status_change()
            self.logger.info(f"Descargados {len(data_blocks)} bloques de datos")
            
            return data_blocks
            
        except Exception as e:
            self.logger.error(f"Error en descarga: {e}")
            self.status = DeviceStatus.ERROR
            self._notify_status_change()
            return []
    
    def _handshake(self) -> bool:
        """Realiza handshake con el dispositivo"""
        try:
            # Enviar comando de identificación
            self.connection.write(b'IDENTIFY\\r\\n')
            response = self.connection.readline()
            return b'HOLTER' in response
        except:
            return False
    
    def _get_device_info(self) -> DeviceInfo:
        """Obtiene información del dispositivo"""
        # Implementación simplificada
        return DeviceInfo(
            device_id="HOLTER_001",
            model="Holter Pro v2",
            firmware_version="2.0.1",
            battery_level=85,
            memory_usage=23.5,
            status=self.status
        )
    
    def _read_loop(self):
        """Loop de lectura de datos en background"""
        while self.running and self.connection and self.connection.is_open:
            try:
                if self.connection.in_waiting > 0:
                    data = self.connection.read(self.connection.in_waiting)
                    if self.on_data_received:
                        self.on_data_received(data)
                time.sleep(0.01)
            except Exception as e:
                self.logger.error(f"Error en loop de lectura: {e}")
                break
    
    def _is_connected(self) -> bool:
        """Verifica si está conectado"""
        return (self.connection and 
                self.connection.is_open and 
                self.status in [DeviceStatus.CONNECTED, DeviceStatus.RECORDING])
    
    def _notify_status_change(self):
        """Notifica cambio de estado"""
        if self.on_status_changed:
            self.on_status_changed(self.status)
