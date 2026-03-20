import os
import re
import io
import json
import time
import base64
import asyncio
import sqlite3
import logging
import tempfile
import subprocess
from datetime import datetime
from contextlib import closing
from typing import Any, Dict, List, Optional

import fitz  # PyMuPDF
from fastapi import FastAPI, Request
from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from openai import AsyncOpenAI
import openai
import speech_recognition as sr
from gtts import gTTS
from pydub import AudioSegment
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

app = FastAPI()


def convert_oga_to_wav(oga_path: str, wav_path: str) -> bool:
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", oga_path, wav_path],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except Exception as e:
        logger.error(f"Error convirtiendo audio: {e}")
        return False


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return round(float(value), 2)

    text = str(value).strip()
    if not text:
        return None

    text = text.replace("€", "").replace("EUR", "").replace(" ", "")
    text = text.replace(".", "").replace(",", ".") if text.count(",") == 1 and text.count(".") >= 1 else text
    text = text.replace(",", ".") if text.count(",") == 1 and text.count(".") == 0 else text

    try:
        return round(float(text), 2)
    except Exception:
        return None


def parse_date_to_iso(date_text: Any) -> Optional[str]:
    if not date_text:
        return None

    raw = str(date_text).strip()
    if not raw:
        return None

    patterns = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d.%m.%Y",
        "%Y/%m/%d",
        "%d-%m-%y",
        "%d/%m/%y",
    ]

    for fmt in patterns:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass

    match = re.search(r"(\d{2})[\/\-.](\d{2})[\/\-.](\d{4})", raw)
    if match:
        d, m, y = match.groups()
        try:
            return datetime(int(y), int(m), int(d)).strftime("%Y-%m-%d")
        except Exception:
            return None

    return None


def normalize_estado(value: Any) -> str:
    text = str(value or "").strip().upper()
    allowed = {"COMPLETA", "VERIFICAR_DATOS", "PENDIENTE_REVISION"}
    if text in allowed:
        return text
    return "PENDIENTE_REVISION"


def build_fallback_row(page_num: int, reason: str) -> Dict[str, Any]:
    return {
        "numero_factura": None,
        "pagina": page_num,
        "fecha_literal": None,
        "fecha_iso": None,
        "total_eur": None,
        "iva_pct": 10,
        "base_eur": None,
        "cuota_eur": None,
        "estado": "PENDIENTE_REVISION",
        "observaciones": reason[:250] if reason else "No procesable",
    }


def normalize_row(row: Dict[str, Any], page_num: int) -> Dict[str, Any]:
    normalized = {
        "numero_factura": None,
        "pagina": page_num,
        "fecha_literal": row.get("fecha_literal"),
        "fecha_iso": parse_date_to_iso(row.get("fecha_iso") or row.get("fecha_literal")),
        "total_eur": safe_float(row.get("total_eur")),
        "iva_pct": 10,
        "base_eur": None,
        "cuota_eur": None,
        "estado": normalize_estado(row.get("estado")),
        "observaciones": str(row.get("observaciones") or "").strip() or "OK",
    }

    if normalized["total_eur"] is not None:
        base = round(normalized["total_eur"] / 1.10, 2)
        cuota = round(normalized["total_eur"] - base, 2)
        normalized["base_eur"] = base
        normalized["cuota_eur"] = cuota
    else:
        normalized["base_eur"] = None
        normalized["cuota_eur"] = None

    if not normalized["fecha_iso"] and normalized["estado"] == "COMPLETA":
        normalized["estado"] = "VERIFICAR_DATOS"
    if normalized["total_eur"] is None and normalized["estado"] == "COMPLETA":
        normalized["estado"] = "VERIFICAR_DATOS"

    return normalized


def sort_and_renumber_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(item: Dict[str, Any]):
        date_iso = item.get("fecha_iso")
        if date_iso:
            return (0, date_iso, item.get("pagina") or 0)
        return (1, "9999-99-99", item.get("pagina") or 0)

    rows_sorted = sorted(rows, key=sort_key)
    for idx, row in enumerate(rows_sorted, start=1):
        row["numero_factura"] = f"Z-{idx}"
    return rows_sorted


def autosize_worksheet(ws) -> None:
    for column_cells in ws.columns:
        max_length = 0
        col_letter = column_cells[0].column_letter
        for cell in column_cells:
            try:
                value = "" if cell.value is None else str(cell.value)
                max_length = max(max_length, len(value))
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max(max_length + 2, 10), 60)


def create_invoice_excel(rows: List[Dict[str, Any]]) -> str:
    wb = Workbook()
    ws = wb.active
    ws.title = "Facturas"

    headers = [
        "N°Factura",
        "Página",
        "Fecha Literal",
        "Fecha ISO",
        "Total €",
        "IVA",
        "Base €",
        "Cuota €",
        "Estado",
        "Observaciones",
    ]
    ws.append(headers)

    header_fill = PatternFill(fill_type="solid", fgColor="1F3864")
    white_font = Font(color="FFFFFF", bold=True, name="Arial", size=10)
    bold_font = Font(bold=True)
    alt_fill = PatternFill(fill_type="solid", fgColor="EBF0FA")
    total_fill = PatternFill(fill_type="solid", fgColor="D9E1F2")
    thin_border = Border(
        left=Side(style="thin", color="BFBFBF"),
        right=Side(style="thin", color="BFBFBF"),
        top=Side(style="thin", color="BFBFBF"),
        bottom=Side(style="thin", color="BFBFBF"),
    )

    for col_idx, _ in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx)
        cell.fill = header_fill
        cell.font = white_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = thin_border

    ws.row_dimensions[1].height = 30

    for idx, row in enumerate(rows, start=2):
        ws.append([
            row.get("numero_factura"),
            row.get("pagina"),
            row.get("fecha_literal"),
            row.get("fecha_iso"),
            row.get("total_eur"),
            row.get("iva_pct"),
            row.get("base_eur"),
            row.get("cuota_eur"),
            row.get("estado"),
            row.get("observaciones"),
        ])

        fill = alt_fill if idx % 2 == 0 else None
        for col_idx in range(1, 11):
            cell = ws.cell(row=idx, column=col_idx)
            cell.border = thin_border
            if fill:
                cell.fill = fill

        estado_cell = ws.cell(row=idx, column=9)
        estado = str(row.get("estado") or "")
        if estado == "COMPLETA":
            estado_cell.fill = PatternFill(fill_type="solid", fgColor="C6EFCE")
        elif estado == "VERIFICAR_DATOS":
            estado_cell.fill = PatternFill(fill_type="solid", fgColor="FFEB9C")
        elif estado == "PENDIENTE_REVISION":
            estado_cell.fill = PatternFill(fill_type="solid", fgColor="FFC7CE")

        for col in [5, 7, 8]:
            ws.cell(row=idx, column=col).number_format = '#,##0.00 €'
            ws.cell(row=idx, column=col).alignment = Alignment(horizontal="right")

        ws.cell(row=idx, column=6).alignment = Alignment(horizontal="center")
        ws.cell(row=idx, column=10).alignment = Alignment(wrap_text=True, vertical="top")

    total_row = len(rows) + 2
    ws.cell(row=total_row, column=1).value = "TOTALES"
    ws.cell(row=total_row, column=1).font = bold_font

    for col in [5, 7, 8]:
        col_letter = get_column_letter(col)
        ws.cell(row=total_row, column=col).value = f"=SUM({col_letter}2:{col_letter}{total_row-1})"
        ws.cell(row=total_row, column=col).number_format = '#,##0.00 €'
        ws.cell(row=total_row, column=col).font = bold_font
        ws.cell(row=total_row, column=col).alignment = Alignment(horizontal="right")

    for col in range(1, 11):
        ws.cell(row=total_row, column=col).fill = total_fill
        ws.cell(row=total_row, column=col).border = thin_border

    widths = [12, 10, 16, 14, 18, 8, 18, 14, 22, 55]
    for idx, width in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(idx)].width = width

    ws.freeze_panes = "A2"

    # Hoja resumen
    summary = wb.create_sheet("Resumen")
    summary.merge_cells("A1:B1")
    summary["A1"] = "Resumen de extracción"
    summary["A1"].fill = header_fill
    summary["A1"].font = white_font
    summary["A1"].alignment = Alignment(horizontal="center")

    total_paginas = len(rows)
    completas = sum(1 for r in rows if r.get("estado") == "COMPLETA")
    verificar = sum(1 for r in rows if r.get("estado") == "VERIFICAR_DATOS")
    pendientes = sum(1 for r in rows if r.get("estado") == "PENDIENTE_REVISION")
    vacias = sum(1 for r in rows if "vac" in str(r.get("observaciones", "")).lower())
    no_procesables = sum(
        1 for r in rows
        if any(x in str(r.get("observaciones", "")).lower() for x in ["ilegible", "no procesable", "no factura", "ocr", "error"])
    )
    pct_completas = round((completas / total_paginas) * 100, 2) if total_paginas else 0.0

    if pendientes >= max(2, total_paginas // 2):
        estado_general = "PROBLEMAS_MULTIPLES"
        estado_color = "FFC7CE"
    elif verificar > 0 or pendientes > 0:
        estado_general = "REQUIERE_REVISION"
        estado_color = "FFEB9C"
    else:
        estado_general = "EXITOSO"
        estado_color = "C6EFCE"

    summary_rows = [
        ("Total páginas", total_paginas),
        ("Completas", completas),
        ("Verificar", verificar),
        ("Pendientes", pendientes),
        ("Vacías", vacias),
        ("No procesables", no_procesables),
        ("% Completas", pct_completas),
        ("Estado general", estado_general),
    ]

    for idx, (label, value) in enumerate(summary_rows, start=2):
        summary.cell(row=idx, column=1, value=label)
        summary.cell(row=idx, column=2, value=value)

        fill = alt_fill if idx % 2 == 0 else None
        for col in [1, 2]:
            c = summary.cell(row=idx, column=col)
            c.border = thin_border
            if fill:
                c.fill = fill

    summary.cell(row=9, column=2).fill = PatternFill(fill_type="solid", fgColor=estado_color)
    summary.column_dimensions["A"].width = 32
    summary.column_dimensions["B"].width = 22

    temp_dir = tempfile.mkdtemp(prefix="bot_excel_")
    month_year = datetime.now().strftime("%m%Y")
    file_path = os.path.join(temp_dir, f"Facturas_{month_year}.xlsx")
    wb.save(file_path)
    return file_path


class CoachBot:
    def __init__(self):
        required_env_vars = {
            "TELEGRAM_TOKEN": os.getenv("TELEGRAM_TOKEN"),
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
            "WEBHOOK_URL": os.getenv("WEBHOOK_URL"),
        }

        missing_vars = [var for var, value in required_env_vars.items() if not value]
        if missing_vars:
            raise EnvironmentError(
                f"Faltan variables de entorno requeridas: {', '.join(missing_vars)}"
            )

        self.telegram_token = required_env_vars["TELEGRAM_TOKEN"]
        self.webhook_url = required_env_vars["WEBHOOK_URL"].strip().strip('"').strip("'")
        self.model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()
        self.client = AsyncOpenAI(api_key=required_env_vars["OPENAI_API_KEY"])

        if self.webhook_url.startswith("WEBHOOK_URL="):
            self.webhook_url = self.webhook_url.split("=", 1)[1].strip()

        if not self.webhook_url.startswith("https://"):
            raise EnvironmentError(f"WEBHOOK_URL inválida: {self.webhook_url}")

        if "/webhook" not in self.webhook_url:
            raise EnvironmentError(
                f"WEBHOOK_URL debe apuntar a /webhook. Valor actual: {self.webhook_url}"
            )

        self.started = False
        self.pending_requests = set()
        self.db_path = "bot_data.db"
        self.user_preferences: Dict[int, Dict[str, Any]] = {}
        self.locks: Dict[int, asyncio.Lock] = {}

        self.telegram_app = Application.builder().token(self.telegram_token).build()

        self._init_db()
        self._load_user_preferences()
        self.setup_handlers()

    def _init_db(self):
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    role TEXT,
                    content TEXT
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS user_preferences (
                    chat_id INTEGER PRIMARY KEY,
                    voice_responses BOOLEAN DEFAULT 0,
                    voice_speed FLOAT DEFAULT 1.0
                )
                """
            )
            conn.commit()

    def _load_user_preferences(self):
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id, voice_responses, voice_speed FROM user_preferences")
            rows = cursor.fetchall()
            for chat_id, voice_responses, voice_speed in rows:
                self.user_preferences[chat_id] = {
                    "voice_responses": bool(voice_responses),
                    "voice_speed": voice_speed,
                }

    def save_conversation(self, chat_id: int, role: str, content: str):
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO conversations (chat_id, role, content)
                VALUES (?, ?, ?)
                """,
                (chat_id, role, content),
            )
            conn.commit()

    def save_user_preference(
        self,
        chat_id: int,
        voice_responses: Optional[bool] = None,
        voice_speed: Optional[float] = None,
    ):
        pref = self.user_preferences.get(
            chat_id, {"voice_responses": False, "voice_speed": 1.0}
        )
        if voice_responses is not None:
            pref["voice_responses"] = voice_responses
        if voice_speed is not None:
            pref["voice_speed"] = voice_speed

        self.user_preferences[chat_id] = pref

        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO user_preferences (chat_id, voice_responses, voice_speed)
                VALUES (?, ?, ?)
                """,
                (chat_id, int(pref["voice_responses"]), pref["voice_speed"]),
            )
            conn.commit()

    async def enable_voice_responses(self, chat_id: int):
        self.save_user_preference(chat_id, voice_responses=True)
        return "✅ Respuestas por voz activadas."

    async def disable_voice_responses(self, chat_id: int):
        self.save_user_preference(chat_id, voice_responses=False)
        return "✅ Respuestas por voz desactivadas."

    async def set_voice_speed(self, chat_id: int, text: str):
        try:
            parts = text.lower().split("velocidad")
            if len(parts) < 2:
                return "⚠️ Indica una velocidad. Ejemplo: velocidad 1.2"

            speed = float(parts[1].strip())
            if speed < 0.5 or speed > 2.0:
                return "⚠️ La velocidad debe estar entre 0.5 y 2.0"

            self.save_user_preference(chat_id, voice_speed=speed)
            return f"✅ Velocidad de voz configurada en {speed}x"
        except ValueError:
            return "⚠️ No entendí la velocidad. Usa algo como 0.8, 1.0 o 1.5"

    async def process_voice_command(self, chat_id: int, text: str):
        text_lower = text.lower()
        if "activar voz" in text_lower or "activa voz" in text_lower:
            return await self.enable_voice_responses(chat_id)
        if "desactivar voz" in text_lower or "desactiva voz" in text_lower:
            return await self.disable_voice_responses(chat_id)
        if "velocidad" in text_lower:
            return await self.set_voice_speed(chat_id, text_lower)
        return None

    async def call_text_model(self, user_message: str) -> str:
        response = await self.client.responses.create(
            model=self.model,
            input=[
                {
                    "role": "developer",
                    "content": [
                        {
                            "type": "input_text",
                            "text": (
                                "Responde en español de forma útil y directa. "
                                "Si el usuario no está pidiendo extraer un PDF, responde normalmente."
                            ),
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": user_message}],
                },
            ],
            text={"format": {"type": "text"}},
        )

        output_text = getattr(response, "output_text", None)
        if output_text:
            return output_text.strip()

        try:
            return response.output[0].content[0].text.strip()
        except Exception:
            return "⚠️ No obtuve una respuesta válida."

    async def process_text_message(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        user_message: str,
    ) -> str:
        chat_id = update.message.chat.id
        lock = self.locks.setdefault(chat_id, asyncio.Lock())

        async with lock:
            try:
                if not user_message.strip():
                    return "⚠️ No se recibió un mensaje válido."

                voice_command_response = await self.process_voice_command(chat_id, user_message)
                if voice_command_response:
                    return voice_command_response

                await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

                response = await self.call_text_model(user_message)

                self.save_conversation(chat_id, "user", user_message)
                self.save_conversation(chat_id, "assistant", response)

                return response or "⚠️ No obtuve una respuesta válida."

            except Exception as e:
                logger.error(f"Error en process_text_message: {e}", exc_info=True)
                return "⚠️ Ocurrió un error al procesar tu mensaje."

    def pdf_to_page_images(self, pdf_path: str, dpi: int = 200) -> List[str]:
        doc = fitz.open(pdf_path)
        data_urls: List[str] = []
        zoom = dpi / 72.0
        matrix = fitz.Matrix(zoom, zoom)

        for page_index in range(len(doc)):
            page = doc.load_page(page_index)
            pix = page.get_pixmap(matrix=matrix, alpha=False)
            png_bytes = pix.tobytes("png")
            b64 = base64.b64encode(png_bytes).decode("utf-8")
            data_urls.append(f"data:image/png;base64,{b64}")

        doc.close()
        return data_urls

    async def extract_invoice_from_page(self, image_data_url: str, page_num: int) -> Dict[str, Any]:
        schema = {
            "type": "object",
            "properties": {
                "pagina": {"type": ["integer", "null"]},
                "fecha_literal": {"type": ["string", "null"]},
                "fecha_iso": {"type": ["string", "null"]},
                "total_eur": {"type": ["number", "null"]},
                "estado": {"type": "string"},
                "observaciones": {"type": "string"},
            },
            "required": ["pagina", "fecha_literal", "fecha_iso", "total_eur", "estado", "observaciones"],
            "additionalProperties": False,
        }

        developer_prompt = (
            "Eres un extractor contable especializado en facturas y tickets fotografiados o escaneados. "
            "Analiza UNA sola página de factura/ticket enviada como imagen. "
            "No inventes datos. Si un dato no puede determinarse con seguridad razonable, usa null. "
            "Busca fecha de emisión y total pagado final. "
            "Usa estado COMPLETA, VERIFICAR_DATOS o PENDIENTE_REVISION. "
            "observaciones debe ser breve y útil. "
            "No devuelvas explicaciones fuera del JSON."
        )

        user_prompt = (
            f"Analiza la página {page_num}. "
            "Extrae la fecha de emisión y el total final pagado. "
            "Si no es una factura o ticket procesable, márcalo como PENDIENTE_REVISION. "
            "Devuelve datos reales de esta página."
        )

        try:
            response = await self.client.responses.create(
                model=self.model,
                input=[
                    {
                        "role": "developer",
                        "content": [{"type": "input_text", "text": developer_prompt}],
                    },
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": user_prompt},
                            {"type": "input_image", "image_url": image_data_url, "detail": "high"},
                        ],
                    },
                ],
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "invoice_page_extraction",
                        "strict": True,
                        "schema": schema,
                    }
                },
            )

            output_text = getattr(response, "output_text", None)
            if not output_text:
                try:
                    output_text = response.output[0].content[0].text
                except Exception:
                    output_text = None

            if not output_text:
                return build_fallback_row(page_num, "Sin respuesta estructurada del modelo")

            parsed = json.loads(output_text)
            if not isinstance(parsed, dict):
                return build_fallback_row(page_num, "Respuesta JSON inválida")

            parsed["pagina"] = page_num
            return normalize_row(parsed, page_num)

        except Exception as e:
            logger.error(f"Error extrayendo página {page_num}: {e}", exc_info=True)
            return build_fallback_row(page_num, f"Error OCR/modelo en página {page_num}")

    async def handle_pdf(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        pdf_path = None

        try:
            chat_id = update.message.chat.id
            document = update.message.document

            if not document:
                await update.message.reply_text("⚠️ No recibí ningún documento.")
                return

            if document.mime_type != "application/pdf":
                await update.message.reply_text("⚠️ Solo puedo procesar archivos PDF por ahora.")
                return

            lock = self.locks.setdefault(chat_id, asyncio.Lock())
            async with lock:
                if chat_id in self.pending_requests:
                    await update.message.reply_text(
                        "⏳ Ya estoy procesando una solicitud anterior. Espera un momento."
                    )
                    return

                self.pending_requests.add(chat_id)

                try:
                    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_DOCUMENT)

                    tg_file = await document.get_file()
                    safe_name = document.file_name or f"archivo_{chat_id}.pdf"
                    pdf_path = os.path.join(tempfile.gettempdir(), safe_name)
                    await tg_file.download_to_drive(pdf_path)

                    logger.info(f"PDF descargado: {pdf_path}")
                    await update.message.reply_text("📄 PDF recibido. Procesando páginas con OCR visual...")

                    page_images = await asyncio.to_thread(self.pdf_to_page_images, pdf_path, 200)
                    if not page_images:
                        await update.message.reply_text("⚠️ No pude renderizar páginas del PDF.")
                        return

                    extracted_rows: List[Dict[str, Any]] = []
                    total_pages = len(page_images)

                    for idx, image_data_url in enumerate(page_images, start=1):
                        await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
                        row = await self.extract_invoice_from_page(image_data_url, idx)
                        extracted_rows.append(row)

                        if idx == 1 or idx == total_pages or idx % 5 == 0:
                            await update.message.reply_text(f"🔎 Procesadas {idx}/{total_pages} páginas...")

                    extracted_rows = sort_and_renumber_rows(extracted_rows)

                    excel_path = create_invoice_excel(extracted_rows)
                    with open(excel_path, "rb") as f:
                        await update.message.reply_document(
                            document=f,
                            filename=os.path.basename(excel_path),
                            caption="Aquí tienes el Excel generado desde la extracción del PDF.",
                        )

                    summary_text = (
                        f"✅ Proceso completado.\n"
                        f"Páginas procesadas: {len(extracted_rows)}\n"
                        f"Completas: {sum(1 for r in extracted_rows if r['estado'] == 'COMPLETA')}\n"
                        f"Verificar: {sum(1 for r in extracted_rows if r['estado'] == 'VERIFICAR_DATOS')}\n"
                        f"Pendientes: {sum(1 for r in extracted_rows if r['estado'] == 'PENDIENTE_REVISION')}"
                    )
                    await update.message.reply_text(summary_text)

                    try:
                        os.remove(excel_path)
                        parent_dir = os.path.dirname(excel_path)
                        if os.path.isdir(parent_dir):
                            os.rmdir(parent_dir)
                    except Exception:
                        pass

                finally:
                    self.pending_requests.discard(chat_id)

        except Exception as e:
            logger.error(f"Error procesando PDF: {e}", exc_info=True)
            await update.message.reply_text("⚠️ Ocurrió un error procesando el PDF.")
        finally:
            try:
                if pdf_path and os.path.exists(pdf_path):
                    os.remove(pdf_path)
            except Exception as e:
                logger.error(f"Error eliminando PDF temporal: {e}")

    async def handle_voice_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        oga_file_path = None
        wav_file_path = None

        try:
            chat_id = update.message.chat.id
            voice_file = await update.message.voice.get_file()

            oga_file_path = f"{chat_id}_voice_note.oga"
            wav_file_path = f"{chat_id}_voice_note.wav"

            await voice_file.download_to_drive(oga_file_path)

            if not convert_oga_to_wav(oga_file_path, wav_file_path):
                await update.message.reply_text("⚠️ No se pudo procesar el archivo de audio.")
                return

            recognizer = sr.Recognizer()
            with sr.AudioFile(wav_file_path) as source:
                audio = recognizer.record(source)

            try:
                user_message = recognizer.recognize_google(audio, language="es-ES")
                logger.info(f'Transcripción de voz: "{user_message}"')
                await update.message.reply_text(f'📝 Tu mensaje: "{user_message}"')

                response = await self.process_text_message(update, context, user_message)
                await self.deliver_text_response(update, context, response)

            except sr.UnknownValueError:
                await update.message.reply_text("⚠️ No pude entender la nota de voz.")
            except sr.RequestError as e:
                logger.error(f"Error en SpeechRecognition: {e}")
                await update.message.reply_text("⚠️ Error en el servicio de reconocimiento de voz.")

        except Exception as e:
            logger.error(f"Error manejando mensaje de voz: {e}", exc_info=True)
            await update.message.reply_text("⚠️ Ocurrió un error procesando la nota de voz.")
        finally:
            for file_path in (oga_file_path, wav_file_path):
                try:
                    if file_path and os.path.exists(file_path):
                        os.remove(file_path)
                except Exception as e:
                    logger.error(f"Error eliminando temporal {file_path}: {e}")

    async def text_to_speech(self, text: str, speed: float = 1.0) -> Optional[str]:
        try:
            temp_dir = os.path.join(os.getcwd(), "temp")
            os.makedirs(temp_dir, exist_ok=True)

            temp_filename = f"voice_{int(time.time())}.mp3"
            temp_path = os.path.join(temp_dir, temp_filename)

            tts = gTTS(text=text, lang="es")
            tts.save(temp_path)

            if abs(speed - 1.0) > 0.01:
                song = AudioSegment.from_mp3(temp_path)
                new_song = song.speedup(playback_speed=speed)
                new_song.export(temp_path, format="mp3")

            return temp_path

        except Exception as e:
            logger.error(f"Error en text_to_speech: {e}", exc_info=True)
            return None

    async def deliver_text_response(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        response: str,
    ):
        chat_id = update.message.chat.id
        pref = self.user_preferences.get(
            chat_id, {"voice_responses": False, "voice_speed": 1.0}
        )

        if pref["voice_responses"] and len(response) < 3500:
            voice_note_path = await self.text_to_speech(response, pref["voice_speed"])
            if voice_note_path and os.path.exists(voice_note_path):
                try:
                    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.RECORD_AUDIO)
                    with open(voice_note_path, "rb") as audio:
                        await update.message.reply_voice(audio)
                    os.remove(voice_note_path)
                    return
                except Exception as e:
                    logger.error(f"Error enviando voz: {e}", exc_info=True)

        await update.message.reply_text(response)

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "👋 Bienvenido. Envíame texto, voz o un PDF. "
            "Los PDFs escaneados se procesan página por página como imagen y te entrego un Excel."
        )

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        help_text = (
            "🤖 Comandos disponibles:\n\n"
            "/start - Iniciar el bot\n"
            "/help - Mostrar ayuda\n"
            "/voz - Configurar respuestas por voz\n\n"
            "Funcionalidades:\n"
            "- Mensajes de texto\n"
            "- Notas de voz\n"
            "- PDFs escaneados o fotografiados\n"
            "- Generación automática de Excel\n"
        )
        await update.message.reply_text(help_text)

    async def voice_settings_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.message.chat.id
        pref = self.user_preferences.get(
            chat_id, {"voice_responses": False, "voice_speed": 1.0}
        )
        voice_status = "activadas" if pref["voice_responses"] else "desactivadas"

        help_text = (
            "🎙 Configuración de voz\n\n"
            f"Estado actual: respuestas de voz {voice_status}\n"
            f"Velocidad actual: {pref['voice_speed']}x\n\n"
            "Comandos:\n"
            "- Activar voz\n"
            "- Desactivar voz\n"
            "- Velocidad 1.2\n"
        )
        await update.message.reply_text(help_text)

    async def route_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            await self.handle_message(update, context)
        except Exception as e:
            logger.error(f"Error en route_message: {e}", exc_info=True)
            await update.message.reply_text("❌ Ocurrió un error procesando tu mensaje.")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.message.chat.id
        try:
            user_message = (update.message.text or "").strip()
            if not user_message:
                return

            response = await asyncio.wait_for(
                self.process_text_message(update, context, user_message),
                timeout=120.0,
            )

            if response is None or not response.strip():
                raise ValueError("La respuesta del modelo está vacía")

            await self.deliver_text_response(update, context, response)

        except asyncio.TimeoutError:
            logger.error(f"Timeout procesando mensaje de {chat_id}")
            await update.message.reply_text("⏳ La operación está tomando demasiado tiempo. Inténtalo más tarde.")
        except openai.OpenAIError as e:
            logger.error(f"Error OpenAI: {e}", exc_info=True)
            await update.message.reply_text("❌ Hubo un problema con OpenAI.")
        except Exception as e:
            logger.error(f"Error inesperado en handle_message: {e}", exc_info=True)
            await update.message.reply_text("⚠️ Ocurrió un error inesperado.")

    def setup_handlers(self):
        self.telegram_app.add_handler(CommandHandler("start", self.start_command))
        self.telegram_app.add_handler(CommandHandler("help", self.help_command))
        self.telegram_app.add_handler(CommandHandler("voz", self.voice_settings_command))
        self.telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.route_message))
        self.telegram_app.add_handler(MessageHandler(filters.VOICE, self.handle_voice_message))
        self.telegram_app.add_handler(MessageHandler(filters.Document.PDF, self.handle_pdf))
        logger.info("Handlers configurados correctamente")

    async def async_init(self):
        try:
            await self.telegram_app.initialize()

            if not self.started:
                self.started = True
                await self.telegram_app.start()

            try:
                await self.telegram_app.bot.set_webhook(url=self.webhook_url)
                webhook_info = await self.telegram_app.bot.get_webhook_info()
                logger.info("Bot inicializado correctamente")
                logger.info(f"Webhook configurado en: {self.webhook_url}")
                logger.info(f"Webhook info: {webhook_info}")
            except Exception as e:
                logger.error(f"No se pudo configurar el webhook: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"Error en async_init: {e}", exc_info=True)
            raise


try:
    bot = CoachBot()
except Exception as e:
    logger.error(f"Error crítico inicializando el bot: {e}", exc_info=True)
    raise


@app.get("/")
async def root():
    return {"status": "ok", "message": "Bot activo en Render"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/webhook-info")
async def webhook_info():
    try:
        info = await bot.telegram_app.bot.get_webhook_info()
        return info.to_dict()
    except Exception as e:
        logger.error(f"Error obteniendo webhook_info: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


@app.on_event("startup")
async def startup_event():
    try:
        await bot.async_init()
        logger.info("Aplicación iniciada correctamente")
    except Exception as e:
        logger.error(f"Error al iniciar la aplicación: {e}", exc_info=True)
        raise


@app.post("/webhook")
async def webhook(request: Request):
    try:
        data = await request.json()
        logger.info(f"Update recibido en webhook: {json.dumps(data)[:1000]}")
        update = Update.de_json(data, bot.telegram_app.bot)
        await bot.telegram_app.process_update(update)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Error procesando webhook: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}
