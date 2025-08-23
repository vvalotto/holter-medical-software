#!/usr/bin/env python3
"""
Data Storage Component v2.5
ISO 62304 - Clase B (No crítica)

Componente para almacenamiento y gestión de datos ECG y registros médicos.
"""

import sqlite3
import json
import pickle
import gzip
import os
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import logging

@dataclass
class PatientRecord:
    """Registro de paciente"""
    patient_id: str
    name: str
    age: int
    gender: str
    medical_record_number: str
    created_at: datetime
    updated_at: datetime

@dataclass
class ECGRecord:
    """Registro de datos ECG"""
    record_id: str
    patient_id: str
    device_id: str
    recording_date: datetime
    duration_seconds: int
    sample_rate: int
    data_file_path: str
    analysis_results: Optional[Dict] = None
    quality_metrics: Optional[Dict] = None
    annotations: Optional[List] = None

class DataStorage:
    """Gestor principal de almacenamiento de datos médicos"""
    
    def __init__(self, db_path: str = "holter_data.db", data_dir: str = "ecg_data"):
        self.db_path = db_path
        self.data_dir = data_dir
        self.logger = logging.getLogger(__name__)
        
        # Crear directorios si no existen
        os.makedirs(data_dir, exist_ok=True)
        
        # Inicializar base de datos
        self._initialize_database()
    
    def _initialize_database(self):
        """Inicializa la base de datos SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Tabla de pacientes
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patients (
                    patient_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    age INTEGER,
                    gender TEXT,
                    medical_record_number TEXT UNIQUE,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            ''')
            
            # Tabla de registros ECG
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ecg_records (
                    record_id TEXT PRIMARY KEY,
                    patient_id TEXT,
                    device_id TEXT,
                    recording_date TIMESTAMP,
                    duration_seconds INTEGER,
                    sample_rate INTEGER,
                    data_file_path TEXT,
                    analysis_results TEXT,  -- JSON
                    quality_metrics TEXT,   -- JSON
                    annotations TEXT,       -- JSON
                    FOREIGN KEY (patient_id) REFERENCES patients (patient_id)
                )
            ''')
            
            # Índices para mejorar rendimiento
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_patient_id ON ecg_records(patient_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_recording_date ON ecg_records(recording_date)')
            
            conn.commit()
            conn.close()
            
            self.logger.info("Base de datos inicializada correctamente")
            
        except Exception as e:
            self.logger.error(f"Error inicializando base de datos: {e}")
            raise
    
    def store_patient(self, patient: PatientRecord) -> bool:
        """Almacena información de paciente"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO patients 
                (patient_id, name, age, gender, medical_record_number, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                patient.patient_id,
                patient.name,
                patient.age,
                patient.gender,
                patient.medical_record_number,
                patient.created_at,
                patient.updated_at
            ))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Paciente almacenado: {patient.patient_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error almacenando paciente: {e}")
            return False
    
    def store_ecg_data(self, ecg_record: ECGRecord, ecg_data: List[float]) -> bool:
        """Almacena datos ECG con compresión"""
        try:
            # Guardar datos ECG comprimidos
            data_filename = f"{ecg_record.record_id}.ecg.gz"
            data_filepath = os.path.join(self.data_dir, data_filename)
            
            with gzip.open(data_filepath, 'wb') as f:
                pickle.dump(ecg_data, f)
            
            # Actualizar ruta en el registro
            ecg_record.data_file_path = data_filepath
            
            # Almacenar metadatos en base de datos
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO ecg_records 
                (record_id, patient_id, device_id, recording_date, duration_seconds,
                 sample_rate, data_file_path, analysis_results, quality_metrics, annotations)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ecg_record.record_id,
                ecg_record.patient_id,
                ecg_record.device_id,
                ecg_record.recording_date,
                ecg_record.duration_seconds,
                ecg_record.sample_rate,
                ecg_record.data_file_path,
                json.dumps(ecg_record.analysis_results) if ecg_record.analysis_results else None,
                json.dumps(ecg_record.quality_metrics) if ecg_record.quality_metrics else None,
                json.dumps(ecg_record.annotations) if ecg_record.annotations else None
            ))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Datos ECG almacenados: {ecg_record.record_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error almacenando datos ECG: {e}")
            return False
    
    def retrieve_patient(self, patient_id: str) -> Optional[PatientRecord]:
        """Recupera información de paciente"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT patient_id, name, age, gender, medical_record_number, created_at, updated_at
                FROM patients WHERE patient_id = ?
            ''', (patient_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return PatientRecord(
                    patient_id=row[0],
                    name=row[1],
                    age=row[2],
                    gender=row[3],
                    medical_record_number=row[4],
                    created_at=datetime.fromisoformat(row[5]),
                    updated_at=datetime.fromisoformat(row[6])
                )
            return None
            
        except Exception as e:
            self.logger.error(f"Error recuperando paciente: {e}")
            return None
    
    def retrieve_ecg_data(self, record_id: str) -> Optional[Tuple[ECGRecord, List[float]]]:
        """Recupera datos ECG y metadatos"""
        try:
            # Recuperar metadatos
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT record_id, patient_id, device_id, recording_date, duration_seconds,
                       sample_rate, data_file_path, analysis_results, quality_metrics, annotations
                FROM ecg_records WHERE record_id = ?
            ''', (record_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if not row:
                return None
            
            # Crear objeto ECGRecord
            ecg_record = ECGRecord(
                record_id=row[0],
                patient_id=row[1],
                device_id=row[2],
                recording_date=datetime.fromisoformat(row[3]),
                duration_seconds=row[4],
                sample_rate=row[5],
                data_file_path=row[6],
                analysis_results=json.loads(row[7]) if row[7] else None,
                quality_metrics=json.loads(row[8]) if row[8] else None,
                annotations=json.loads(row[9]) if row[9] else None
            )
            
            # Cargar datos ECG
            if os.path.exists(row[6]):
                with gzip.open(row[6], 'rb') as f:
                    ecg_data = pickle.load(f)
                return ecg_record, ecg_data
            else:
                self.logger.warning(f"Archivo de datos no encontrado: {row[6]}")
                return ecg_record, []
                
        except Exception as e:
            self.logger.error(f"Error recuperando datos ECG: {e}")
            return None
    
    def list_patient_records(self, patient_id: str) -> List[ECGRecord]:
        """Lista todos los registros ECG de un paciente"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT record_id, patient_id, device_id, recording_date, duration_seconds,
                       sample_rate, data_file_path, analysis_results, quality_metrics, annotations
                FROM ecg_records WHERE patient_id = ?
                ORDER BY recording_date DESC
            ''', (patient_id,))
            
            rows = cursor.fetchall()
            conn.close()
            
            records = []
            for row in rows:
                record = ECGRecord(
                    record_id=row[0],
                    patient_id=row[1],
                    device_id=row[2],
                    recording_date=datetime.fromisoformat(row[3]),
                    duration_seconds=row[4],
                    sample_rate=row[5],
                    data_file_path=row[6],
                    analysis_results=json.loads(row[7]) if row[7] else None,
                    quality_metrics=json.loads(row[8]) if row[8] else None,
                    annotations=json.loads(row[9]) if row[9] else None
                )
                records.append(record)
            
            return records
            
        except Exception as e:
            self.logger.error(f"Error listando registros: {e}")
            return []
    
    def update_analysis_results(self, record_id: str, analysis_results: Dict, quality_metrics: Dict = None):
        """Actualiza resultados de análisis de un registro"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE ecg_records 
                SET analysis_results = ?, quality_metrics = ?
                WHERE record_id = ?
            ''', (
                json.dumps(analysis_results),
                json.dumps(quality_metrics) if quality_metrics else None,
                record_id
            ))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Resultados de análisis actualizados: {record_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error actualizando análisis: {e}")
            return False
    
    def export_patient_data(self, patient_id: str, output_file: str) -> bool:
        """Exporta todos los datos de un paciente"""
        try:
            patient = self.retrieve_patient(patient_id)
            if not patient:
                return False
            
            records = self.list_patient_records(patient_id)
            
            export_data = {
                'patient': asdict(patient),
                'records': []
            }
            
            for record in records:
                # No exportar datos ECG raw por tamaño
                record_dict = asdict(record)
                record_dict['recording_date'] = record.recording_date.isoformat()
                export_data['records'].append(record_dict)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Datos exportados a: {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exportando datos: {e}")
            return False
    
    def get_storage_stats(self) -> Dict:
        """Obtiene estadísticas de almacenamiento"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Contar pacientes
            cursor.execute('SELECT COUNT(*) FROM patients')
            patient_count = cursor.fetchone()[0]
            
            # Contar registros ECG
            cursor.execute('SELECT COUNT(*) FROM ecg_records')
            record_count = cursor.fetchone()[0]
            
            # Calcular tamaño de archivos de datos
            total_size = 0
            if os.path.exists(self.data_dir):
                for root, dirs, files in os.walk(self.data_dir):
                    total_size += sum(os.path.getsize(os.path.join(root, file)) for file in files)
            
            conn.close()
            
            return {
                'patients': patient_count,
                'ecg_records': record_count,
                'storage_size_mb': total_size / (1024 * 1024),
                'database_file': self.db_path,
                'data_directory': self.data_dir
            }
            
        except Exception as e:
            self.logger.error(f"Error obteniendo estadísticas: {e}")
            return {}
