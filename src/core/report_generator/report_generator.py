#!/usr/bin/env python3
"""
Report Generator v1.9
ISO 62304 - Clase C (Sin impacto en seguridad)

Generador de reportes médicos e informes de análisis ECG.
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import logging
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
import matplotlib.pyplot as plt
import numpy as np
from io import BytesIO
import base64

@dataclass
class ReportConfig:
    """Configuración del reporte"""
    template_type: str
    include_graphs: bool = True
    include_raw_data: bool = False
    language: str = "es"
    format: str = "pdf"  # pdf, html, json

class ReportGenerator:
    """Generador principal de reportes médicos"""
    
    def __init__(self, output_dir: str = "reports"):
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)
        
        # Crear directorio de reportes si no existe
        os.makedirs(output_dir, exist_ok=True)
        
        # Estilos de documento
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        """Configura estilos personalizados para reportes médicos"""
        # Estilo para título principal
        self.styles.add(ParagraphStyle(
            name='Title',
            parent=self.styles['Heading1'],
            fontSize=18,
            spaceAfter=30,
            alignment=1,  # Centrado
            textColor=colors.darkblue
        ))
        
        # Estilo para secciones
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=14,
            spaceBefore=15,
            spaceAfter=10,
            textColor=colors.darkblue,
            borderWidth=1,
            borderColor=colors.lightblue,
            borderPadding=5
        ))
        
        # Estilo para datos clínicos
        self.styles.add(ParagraphStyle(
            name='ClinicalData',
            parent=self.styles['Normal'],
            fontSize=11,
            leftIndent=20,
            bulletIndent=10
        ))
    
    def generate_patient_report(self, patient_data: Dict, ecg_records: List[Dict], 
                              config: ReportConfig = None) -> str:
        """Genera reporte completo de paciente"""
        if config is None:
            config = ReportConfig(template_type="standard")
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"reporte_paciente_{patient_data['patient_id']}_{timestamp}.pdf"
            filepath = os.path.join(self.output_dir, filename)
            
            # Crear documento PDF
            doc = SimpleDocTemplate(filepath, pagesize=A4)
            story = []
            
            # Título del reporte
            story.append(Paragraph("REPORTE MÉDICO - MONITOREO HOLTER", self.styles['Title']))
            story.append(Spacer(1, 20))
            
            # Información del paciente
            story.extend(self._create_patient_section(patient_data))
            
            # Resumen de estudios
            story.extend(self._create_studies_summary(ecg_records))
            
            # Análisis detallado por registro
            for record in ecg_records:
                story.extend(self._create_ecg_analysis_section(record, config))
                if config.include_graphs:
                    story.extend(self._create_ecg_graphs(record))
            
            # Conclusiones y recomendaciones
            story.extend(self._create_conclusions_section(ecg_records))
            
            # Información del sistema
            story.extend(self._create_system_info_section())
            
            # Generar PDF
            doc.build(story)
            
            self.logger.info(f"Reporte generado: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Error generando reporte: {e}")
            return ""
    
    def _create_patient_section(self, patient_data: Dict) -> List:
        """Crea sección de información del paciente"""
        elements = []
        
        elements.append(Paragraph("INFORMACIÓN DEL PACIENTE", self.styles['SectionHeader']))
        
        # Tabla con datos del paciente
        data = [
            ['Nombre:', patient_data.get('name', 'N/A')],
            ['ID de Paciente:', patient_data.get('patient_id', 'N/A')],
            ['Edad:', f"{patient_data.get('age', 'N/A')} años"],
            ['Género:', patient_data.get('gender', 'N/A')],
            ['Número de Historia Clínica:', patient_data.get('medical_record_number', 'N/A')],
            ['Fecha del Reporte:', datetime.now().strftime("%d/%m/%Y %H:%M")]
        ]
        
        table = Table(data, colWidths=[2*inch, 4*inch])
        table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey)
        ]))
        
        elements.append(table)
        elements.append(Spacer(1, 20))
        
        return elements
    
    def _create_studies_summary(self, ecg_records: List[Dict]) -> List:
        """Crea resumen de estudios realizados"""
        elements = []
        
        elements.append(Paragraph("RESUMEN DE ESTUDIOS", self.styles['SectionHeader']))
        
        if not ecg_records:
            elements.append(Paragraph("No se encontraron registros ECG.", self.styles['Normal']))
            return elements
        
        # Estadísticas generales
        total_duration = sum(record.get('duration_seconds', 0) for record in ecg_records)
        avg_hr = self._calculate_average_hr(ecg_records)
        
        summary_data = [
            ['Total de Registros:', str(len(ecg_records))],
            ['Duración Total de Monitoreo:', self._format_duration(total_duration)],
            ['Frecuencia Cardíaca Promedio:', f"{avg_hr:.1f} bpm" if avg_hr else "N/A"],
            ['Período de Estudio:', self._get_study_period(ecg_records)]
        ]
        
        table = Table(summary_data, colWidths=[2.5*inch, 3.5*inch])
        table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('BACKGROUND', (0, 0), (0, -1), colors.lightblue)
        ]))
        
        elements.append(table)
        elements.append(Spacer(1, 20))
        
        return elements
    
    def _create_ecg_analysis_section(self, record: Dict, config: ReportConfig) -> List:
        """Crea sección de análisis de un registro ECG"""
        elements = []
        
        # Título del registro
        recording_date = record.get('recording_date', 'N/A')
        elements.append(Paragraph(f"ANÁLISIS ECG - {recording_date}", self.styles['SectionHeader']))
        
        # Información del registro
        record_info = [
            ['ID de Registro:', record.get('record_id', 'N/A')],
            ['Dispositivo:', record.get('device_id', 'N/A')],
            ['Duración:', self._format_duration(record.get('duration_seconds', 0))],
            ['Frecuencia de Muestreo:', f"{record.get('sample_rate', 'N/A')} Hz"]
        ]
        
        table = Table(record_info, colWidths=[2*inch, 4*inch])
        table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.gray)
        ]))
        
        elements.append(table)
        elements.append(Spacer(1, 10))
        
        # Resultados del análisis
        if record.get('analysis_results'):
            elements.extend(self._create_analysis_results_section(record['analysis_results']))
        
        return elements
    
    def _create_analysis_results_section(self, analysis_results: Dict) -> List:
        """Crea sección de resultados de análisis"""
        elements = []
        
        elements.append(Paragraph("Resultados del Análisis:", self.styles['Heading3']))
        
        # Hallazgos principales
        hr = analysis_results.get('heart_rate', 'N/A')
        rhythm = analysis_results.get('rhythm_type', 'N/A')
        confidence = analysis_results.get('confidence_score', 0)
        
        results_text = f"""
        <b>Frecuencia Cardíaca:</b> {hr} bpm<br/>
        <b>Tipo de Ritmo:</b> {rhythm}<br/>
        <b>Confianza del Análisis:</b> {confidence:.2f}<br/>
        """
        
        elements.append(Paragraph(results_text, self.styles['ClinicalData']))
        
        # Anormalidades detectadas
        abnormalities = analysis_results.get('abnormalities', [])
        if abnormalities:
            elements.append(Paragraph("Anormalidades Detectadas:", self.styles['Heading4']))
            for abnormality in abnormalities:
                elements.append(Paragraph(f"• {abnormality}", self.styles['ClinicalData']))
        else:
            elements.append(Paragraph("No se detectaron anormalidades significativas.", self.styles['ClinicalData']))
        
        elements.append(Spacer(1, 15))
        
        return elements
    
    def _create_ecg_graphs(self, record: Dict) -> List:
        """Crea gráficos ECG para el reporte"""
        elements = []
        
        try:
            # Simulación de datos ECG para gráfico
            duration = record.get('duration_seconds', 10)
            sample_rate = record.get('sample_rate', 360)
            
            # Generar señal ECG simulada
            t = np.linspace(0, min(duration, 10), min(duration * sample_rate, 3600))
            ecg_signal = self._simulate_ecg_signal(t)
            
            # Crear gráfico
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.plot(t, ecg_signal, linewidth=0.8, color='darkblue')
            ax.set_title(f'Señal ECG - {record.get("record_id", "Registro")}')
            ax.set_xlabel('Tiempo (s)')
            ax.set_ylabel('Amplitud (mV)')
            ax.grid(True, alpha=0.3)
            
            # Guardar gráfico temporalmente
            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
            img_buffer.seek(0)
            plt.close()
            
            # Añadir imagen al reporte (implementación simplificada)
            elements.append(Paragraph("Gráfico de Señal ECG:", self.styles['Heading4']))
            elements.append(Spacer(1, 10))
            # Nota: En implementación completa se añadiría la imagen aquí
            elements.append(Paragraph("[Gráfico ECG - Implementación pendiente]", self.styles['Normal']))
            elements.append(Spacer(1, 15))
            
        except Exception as e:
            self.logger.warning(f"Error creando gráfico ECG: {e}")
            
        return elements
    
    def _create_conclusions_section(self, ecg_records: List[Dict]) -> List:
        """Crea sección de conclusiones y recomendaciones"""
        elements = []
        
        elements.append(Paragraph("CONCLUSIONES Y RECOMENDACIONES", self.styles['SectionHeader']))
        
        # Análisis automático de patrones
        conclusions = self._analyze_overall_patterns(ecg_records)
        
        for conclusion in conclusions:
            elements.append(Paragraph(f"• {conclusion}", self.styles['ClinicalData']))
        
        elements.append(Spacer(1, 10))
        elements.append(Paragraph("Nota: Este reporte ha sido generado automáticamente. "
                                "Se recomienda revisión por profesional médico calificado.", 
                                self.styles['Normal']))
        
        return elements
    
    def _create_system_info_section(self) -> List:
        """Crea sección de información del sistema"""
        elements = []
        
        elements.append(PageBreak())
        elements.append(Paragraph("INFORMACIÓN DEL SISTEMA", self.styles['SectionHeader']))
        
        system_info = [
            ['Software:', 'Holter Medical Software v2.1.0'],
            ['Estándar:', 'ISO 62304 - Software de Dispositivos Médicos'],
            ['Fecha de Generación:', datetime.now().strftime("%d/%m/%Y %H:%M:%S")],
            ['Versión del Generador:', 'Report Generator v1.9']
        ]
        
        table = Table(system_info, colWidths=[2*inch, 4*inch])
        table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey)
        ]))
        
        elements.append(table)
        
        return elements
    
    def _simulate_ecg_signal(self, t: np.ndarray) -> np.ndarray:
        """Simula señal ECG para demostración"""
        # Señal ECG simplificada
        ecg = np.sin(2 * np.pi * 1.2 * t)  # Componente principal
        ecg += 0.3 * np.sin(2 * np.pi * 0.2 * t)  # Variación lenta
        ecg += 0.1 * np.random.normal(0, 1, len(t))  # Ruido
        return ecg
    
    def _calculate_average_hr(self, ecg_records: List[Dict]) -> Optional[float]:
        """Calcula frecuencia cardíaca promedio"""
        heart_rates = []
        for record in ecg_records:
            if record.get('analysis_results', {}).get('heart_rate'):
                heart_rates.append(record['analysis_results']['heart_rate'])
        
        return np.mean(heart_rates) if heart_rates else None
    
    def _format_duration(self, seconds: int) -> str:
        """Formatea duración en formato legible"""
        if seconds < 60:
            return f"{seconds} segundos"
        elif seconds < 3600:
            minutes = seconds // 60
            return f"{minutes} minutos"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}h {minutes}m"
    
    def _get_study_period(self, ecg_records: List[Dict]) -> str:
        """Obtiene período de estudio"""
        if not ecg_records:
            return "N/A"
        
        dates = []
        for record in ecg_records:
            if record.get('recording_date'):
                try:
                    dates.append(datetime.fromisoformat(record['recording_date']))
                except:
                    pass
        
        if not dates:
            return "N/A"
        
        start_date = min(dates)
        end_date = max(dates)
        
        if start_date == end_date:
            return start_date.strftime("%d/%m/%Y")
        else:
            return f"{start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}"
    
    def _analyze_overall_patterns(self, ecg_records: List[Dict]) -> List[str]:
        """Analiza patrones generales en los registros"""
        conclusions = []
        
        if not ecg_records:
            conclusions.append("No se encontraron registros para analizar.")
            return conclusions
        
        # Análisis básico de patrones
        total_abnormalities = 0
        for record in ecg_records:
            analysis = record.get('analysis_results', {})
            abnormalities = analysis.get('abnormalities', [])
            total_abnormalities += len(abnormalities)
        
        if total_abnormalities == 0:
            conclusions.append("Los registros analizados no muestran anormalidades significativas.")
        else:
            conclusions.append(f"Se detectaron {total_abnormalities} anormalidades que requieren evaluación médica.")
        
        # Añadir más análisis según disponibilidad de datos
        conclusions.append("Se recomienda seguimiento médico regular.")
        conclusions.append("Los datos deben ser interpretados en contexto clínico completo.")
        
        return conclusions
    
    def generate_summary_report(self, period_start: datetime, period_end: datetime) -> str:
        """Genera reporte resumen de un período"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"reporte_resumen_{timestamp}.pdf"
            filepath = os.path.join(self.output_dir, filename)
            
            # Implementación básica de reporte resumen
            doc = SimpleDocTemplate(filepath, pagesize=A4)
            story = []
            
            story.append(Paragraph("REPORTE RESUMEN - SISTEMA HOLTER", self.styles['Title']))
            story.append(Spacer(1, 20))
            
            # Período
            period_text = f"Período: {period_start.strftime('%d/%m/%Y')} - {period_end.strftime('%d/%m/%Y')}"
            story.append(Paragraph(period_text, self.styles['Normal']))
            story.append(Spacer(1, 20))
            
            # Estadísticas básicas (implementación simplificada)
            story.append(Paragraph("Estadísticas del Período:", self.styles['SectionHeader']))
            story.append(Paragraph("• Total de pacientes monitoreados: [Pendiente implementación]", self.styles['Normal']))
            story.append(Paragraph("• Total de horas de monitoreo: [Pendiente implementación]", self.styles['Normal']))
            story.append(Paragraph("• Anormalidades detectadas: [Pendiente implementación]", self.styles['Normal']))
            
            doc.build(story)
            
            self.logger.info(f"Reporte resumen generado: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Error generando reporte resumen: {e}")
            return ""
