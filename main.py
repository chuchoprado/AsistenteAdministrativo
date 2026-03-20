import os
import re
import json
import time
import asyncio
import sqlite3
import logging
import tempfile
import subprocess
from contextlib import closing
from typing import Any, Dict, List, Optional, Tuple

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


def strip_code_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def try_parse_json(text: str) -> Optional[Any]:
    if not text or not text.strip():
        return None

    candidates = []

    raw = text.strip()
    candidates.append(raw)
    candidates.append(strip_code_fences(raw))

    fenced_match = re.search(r"```json\s*(.*?)\s*```", raw, flags=re.IGNORECASE | re.DOTALL)
    if fenced_match:
        candidates.append(fenced_match.group(1).strip())

    generic_fence_match = re.search(r"```\s*(.*?)\s*```", raw, flags=re.DOTALL)
    if generic_fence_match:
        candidates.append(generic_fence_match.group(1).strip())

    object_match = re.search(r"(\{.*\})", raw, flags=re.DOTALL)
    if object_match:
        candidates.append(object_match.group(1).strip())

    array_match = re.search(r"(\[.*\])", raw, flags=re.DOTALL)
    if array_match:
        candidates.append(array_match.group(1).strip())

    seen = set()
    for candidate in candidates:
        candidate = candidate.strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        try:
            return json.loads(candidate)
        except Exception:
            continue

    return None


def flatten_dict(d: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items


def json_to_rows(data: Any) -> Tuple[List[str], List[List[Any]], str]:
    if isinstance(data, list):
        if not data:
            return ["resultado"], [["Lista vacía"]], "Resultados"

        if all(isinstance(item, dict) for item in data):
            flattened = [flatten_dict(item) for item in data]
            headers = sorted({key for row in flattened for key in row.keys()})
            rows = [[row.get(h, "") for h in headers] for row in flattened]
            return headers, rows, "Resultados"

        return ["valor"], [[json.dumps(item, ensure_ascii=False)] for item in data], "Resultados"

    if isinstance(data, dict):
        for candidate_key in ("rows", "data", "items", "resultados", "registros"):
            if candidate_key in data and isinstance(data[candidate_key], list):
                nested = data[candidate_key]
                if nested and all(isinstance(item, dict) for item in nested):
                    flattened = [flatten_dict(item) for item in nested]
                    headers = sorted({key for row in flattened for key in row.keys()})
                    rows = [[row.get(h, "") for h in headers] for row in flattened]
                    return headers, rows, "Resultados"

        flat = flatten_dict(data)
        headers = ["campo", "valor"]
        rows = [
            [k, json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v]
            for k, v in flat.items()
        ]
        return headers, rows, "Resultado"

    return ["resultado"], [[str(data)]], "Resultado"


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
        ws.column_dimensions[col_letter].width = min(max(max_length + 2, 12), 60)


def create_excel_from_json(data: Any) -> str:
    headers, rows, sheet_name = json_to_rows(data)

    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]

    ws.append(headers)
    for row in rows:
        ws.append(row)

    autosize_worksheet(ws)

    temp_dir = tempfile.mkdtemp(prefix="bot_excel_")
    file_path = os.path.join(temp_dir, f"resultado_{int(time.time())}.xlsx")
    wb.save(file_path)
    return file_path


class CoachBot:
    def __init__(self):
        required_env_vars = {
            "TELEGRAM_TOKEN": os.getenv("TELEGRAM_TOKEN"),
            "ASSISTANT_ID": os.getenv("ASSISTANT_ID"),
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
            "WEBHOOK_URL": os.getenv("WEBHOOK_URL"),
        }

        missing_vars = [var for var, value in required_env_vars.items() if not value]
        if missing_vars:
            raise EnvironmentError(
                f"Faltan variables de entorno requeridas: {', '.join(missing_vars)}"
            )

        self.telegram_token = required_env_vars["TELEGRAM_TOKEN"]
        self.assistant_id = required_env_vars["ASSISTANT_ID"]
        self.webhook_url = required_env_vars["WEBHOOK_URL"].strip().strip('"').strip("'")

        if self.webhook_url.startswith("WEBHOOK_URL="):
            self.webhook_url = self.webhook_url.split("=", 1)[1].strip()

        if not self.webhook_url.startswith("https://"):
            raise EnvironmentError(f"WEBHOOK_URL inválida: {self.webhook_url}")

        if "/webhook" not in self.webhook_url:
            raise EnvironmentError(
                f"WEBHOOK_URL debe apuntar a /webhook. Valor actual: {self.webhook_url}"
            )

        self.client = AsyncOpenAI(api_key=required_env_vars["OPENAI_API_KEY"])

        self.started = False
        self.user_threads: Dict[int, str] = {}
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

    async def get_or_create_thread(self, chat_id: int) -> Optional[str]:
        if chat_id in self.user_threads:
            return self.user_threads[chat_id]

        try:
            thread = await self.client.beta.threads.create()
            self.user_threads[chat_id] = thread.id
            return thread.id
        except Exception as e:
            logger.error(f"Error creando thread para {chat_id}: {e}", exc_info=True)
            return None

    async def wait_for_run_completion(self, thread_id: str, run_id: str, timeout: int = 180):
        start_time = time.time()
        while True:
            run_status = await self.client.beta.threads.runs.retrieve(
                thread_id=thread_id,
                run_id=run_id,
            )

            if run_status.status == "completed":
                return run_status

            if run_status.status in {"failed", "cancelled", "expired"}:
                raise RuntimeError(f"Run terminó con estado: {run_status.status}")

            if run_status.status == "requires_action":
                raise RuntimeError("El Assistant requiere una acción adicional no implementada.")

            if time.time() - start_time > timeout:
                raise TimeoutError("La consulta al asistente tardó demasiado.")

            await asyncio.sleep(2)

    async def get_latest_assistant_text(self, thread_id: str, limit: int = 10) -> Optional[str]:
        messages = await self.client.beta.threads.messages.list(
            thread_id=thread_id,
            order="desc",
            limit=limit,
        )

        for msg in messages.data:
            if msg.role != "assistant" or not msg.content:
                continue
            for part in msg.content:
                if getattr(part, "type", None) == "text":
                    text_value = part.text.value.strip()
                    if text_value:
                        return text_value
        return None

    async def send_message_to_assistant(self, chat_id: int, user_message: str) -> str:
        if chat_id in self.pending_requests:
            return "⏳ Ya estoy procesando tu solicitud anterior."

        self.pending_requests.add(chat_id)

        try:
            thread_id = await self.get_or_create_thread(chat_id)
            if not thread_id:
                return "❌ No se pudo establecer conexión con el asistente."

            await self.client.beta.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=user_message,
            )

            run = await self.client.beta.threads.runs.create(
                thread_id=thread_id,
                assistant_id=self.assistant_id,
            )

            await self.wait_for_run_completion(thread_id, run.id, timeout=120)
            response = await self.get_latest_assistant_text(thread_id, limit=10)

            if not response:
                return "⚠️ La respuesta del asistente llegó vacía."

            return response

        except Exception as e:
            logger.error(f"Error procesando mensaje con Assistant: {e}", exc_info=True)
            return "⚠️ Ocurrió un error al procesar tu mensaje."
        finally:
            self.pending_requests.discard(chat_id)

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

                response = await self.send_message_to_assistant(chat_id, user_message)

                self.save_conversation(chat_id, "user", user_message)
                self.save_conversation(chat_id, "assistant", response)

                if not response.strip():
                    return "⚠️ No obtuve una respuesta válida del asistente."

                return response

            except Exception as e:
                logger.error(f"Error en process_text_message: {e}", exc_info=True)
                return "⚠️ Ocurrió un error al procesar tu mensaje."

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
                    await context.bot.send_chat_action(
                        chat_id=chat_id, action=ChatAction.UPLOAD_DOCUMENT
                    )

                    tg_file = await document.get_file()
                    safe_name = document.file_name or f"archivo_{chat_id}.pdf"
                    pdf_path = os.path.join(tempfile.gettempdir(), safe_name)

                    await tg_file.download_to_drive(pdf_path)
                    logger.info(f"PDF descargado: {pdf_path}")

                    thread_id = await self.get_or_create_thread(chat_id)
                    if not thread_id:
                        await update.message.reply_text(
                            "❌ No se pudo crear el thread del asistente."
                        )
                        return

                    with open(pdf_path, "rb") as f:
                        uploaded_file = await self.client.files.create(
                            file=f,
                            purpose="assistants",
                        )

                    logger.info(f"Archivo subido a OpenAI: {uploaded_file.id}")

                    await update.message.reply_text("📄 PDF recibido. Extrayendo data...")

                    user_prompt = (
                        f"Lee y extrae la información del PDF '{safe_name}'. "
                        "NO respondas con explicaciones, disculpas, comentarios ni texto adicional. "
                        "NO digas que no pudiste extraer la información. "
                        "Debes devolver ÚNICAMENTE JSON válido, limpio y parseable. "
                        "Si el documento contiene una tabla o registros, devuelve una lista de objetos JSON. "
                        "Si el documento contiene campos sueltos, devuelve un objeto JSON con claves y valores. "
                        "No uses markdown. No uses ```json. No agregues texto antes ni después del JSON."
                    )

                    await self.client.beta.threads.messages.create(
                        thread_id=thread_id,
                        role="user",
                        content=user_prompt,
                        attachments=[
                            {
                                "file_id": uploaded_file.id,
                                "tools": [{"type": "file_search"}],
                            }
                        ],
                    )

                    run = await self.client.beta.threads.runs.create(
                        thread_id=thread_id,
                        assistant_id=self.assistant_id,
                        instructions=(
                            "Tu única tarea es extraer la data del archivo y devolver SOLO JSON válido. "
                            "Nunca respondas con texto conversacional, disculpas, advertencias ni explicaciones. "
                            "La salida será convertida automáticamente a Excel, así que responde únicamente con JSON."
                        ),
                    )

                    await self.wait_for_run_completion(thread_id, run.id, timeout=180)
                    assistant_message = await self.get_latest_assistant_text(thread_id, limit=10)

                    if not assistant_message:
                        await update.message.reply_text(
                            "⚠️ El asistente no devolvió una respuesta."
                        )
                        return

                    self.save_conversation(chat_id, "user", f"[PDF enviado] {safe_name}")
                    self.save_conversation(chat_id, "assistant", assistant_message)

                    await self.deliver_response(update, context, assistant_message, require_json=True)

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
                await self.deliver_response(update, context, response, require_json=False)

            except sr.UnknownValueError:
                await update.message.reply_text("⚠️ No pude entender la nota de voz.")
            except sr.RequestError as e:
                logger.error(f"Error en SpeechRecognition: {e}")
                await update.message.reply_text(
                    "⚠️ Error en el servicio de reconocimiento de voz."
                )

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

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "👋 Bienvenido. Envíame texto, voz o un PDF. "
            "Si el asistente devuelve JSON válido, te entregaré un Excel."
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
            "- PDFs para analizar\n"
            "- Generación de Excel cuando el asistente devuelva JSON válido\n"
        )
        await update.message.reply_text(help_text)

    async def route_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            await self.handle_message(update, context)
        except Exception as e:
            logger.error(f"Error en route_message: {e}", exc_info=True)
            await update.message.reply_text("❌ Ocurrió un error procesando tu mensaje.")

    async def deliver_response(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        response: str,
        require_json: bool = False,
    ):
        chat_id = update.message.chat.id
        pref = self.user_preferences.get(
            chat_id, {"voice_responses": False, "voice_speed": 1.0}
        )

        parsed_json = try_parse_json(response)
        if parsed_json is not None:
            try:
                excel_path = create_excel_from_json(parsed_json)

                await update.message.reply_text(
                    "📊 JSON detectado. Generando Excel..."
                )

                with open(excel_path, "rb") as f:
                    await update.message.reply_document(
                        document=f,
                        filename=os.path.basename(excel_path),
                        caption="Aquí tienes el archivo Excel generado desde la extracción.",
                    )

                try:
                    os.remove(excel_path)
                    parent_dir = os.path.dirname(excel_path)
                    if os.path.isdir(parent_dir):
                        os.rmdir(parent_dir)
                except Exception:
                    pass

                return

            except Exception as e:
                logger.error(f"Error generando Excel desde JSON: {e}", exc_info=True)
                await update.message.reply_text(
                    "⚠️ Se detectó JSON, pero ocurrió un error al generar el Excel."
                )
                return

        if require_json:
            await update.message.reply_text(
                "⚠️ El asistente no devolvió JSON válido desde el PDF. Revisa que el Assistant tenga habilitado file_search y que sus instrucciones estén orientadas a extracción estructurada."
            )
            return

        if pref["voice_responses"] and len(response) < 3500:
            voice_note_path = await self.text_to_speech(response, pref["voice_speed"])
            if voice_note_path and os.path.exists(voice_note_path):
                try:
                    await context.bot.send_chat_action(
                        chat_id=chat_id, action=ChatAction.RECORD_AUDIO
                    )
                    with open(voice_note_path, "rb") as audio:
                        await update.message.reply_voice(audio)
                    os.remove(voice_note_path)
                    return
                except Exception as e:
                    logger.error(f"Error enviando voz: {e}", exc_info=True)

        await update.message.reply_text(response)

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
                raise ValueError("La respuesta del asistente está vacía")

            await self.deliver_response(update, context, response, require_json=False)

        except asyncio.TimeoutError:
            logger.error(f"Timeout procesando mensaje de {chat_id}")
            await update.message.reply_text(
                "⏳ La operación está tomando demasiado tiempo. Inténtalo más tarde."
            )
        except openai.OpenAIError as e:
            logger.error(f"Error OpenAI: {e}", exc_info=True)
            await update.message.reply_text("❌ Hubo un problema con OpenAI.")
        except Exception as e:
            logger.error(f"Error inesperado en handle_message: {e}", exc_info=True)
            await update.message.reply_text("⚠️ Ocurrió un error inesperado.")

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

    def setup_handlers(self):
        self.telegram_app.add_handler(CommandHandler("start", self.start_command))
        self.telegram_app.add_handler(CommandHandler("help", self.help_command))
        self.telegram_app.add_handler(CommandHandler("voz", self.voice_settings_command))
        self.telegram_app.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self.route_message)
        )
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
