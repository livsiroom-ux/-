import asyncio
import csv
import html
import io
import logging
import re
import sqlite3

from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatType, ParseMode
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    BufferedInputFile,
    LinkPreviewOptions,
    Message,
)


# ============================================================
# НАСТРОЙКИ
# ============================================================

BOT_TOKEN = "8973976090:AAFuu8J1mcs_aAHXwyiPR9QES76nH77WIms"
ADMIN_ID = 7095354198

PRODUCT_URL = "https://funpay.com/lots/offer?id=78639026"

# Используется только при создании нового счётчика в пустой базе.
# Допустима любая ОДНА заглавная латинская буква.
CODE_LETTER = "G"

DB_PATH = Path(__file__).resolve().with_name("giveaway.sqlite3")

# Ввод и отображение дат — в московском времени, UTC+3.
# В базе время сохраняется в UTC.
EVENT_TZ = timezone(timedelta(hours=3))
EVENT_TZ_NAME = "UTC+3"
DATE_FORMAT = "%Y-%m-%d %H:%M"

BROADCAST_DELAY = 0.07

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

router = Router()

# Работаем только в личных сообщениях с ботом.
router.message.filter(F.chat.type == ChatType.PRIVATE)

broadcast_task: asyncio.Task | None = None
broadcast_lock = asyncio.Lock()


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def escape(value: str) -> str:
    return html.escape(value)


def parse_date(value: str) -> datetime:
    """Перевод введённой даты из EVENT_TZ в UTC."""
    try:
        local_date = datetime.strptime(value.strip(), DATE_FORMAT)
    except ValueError as exc:
        raise ValueError(
            "Неверная дата.\n"
            "Формат: ГГГГ-ММ-ДД ЧЧ:ММ\n"
            "Пример: 2026-12-31 20:00"
        ) from exc

    return local_date.replace(tzinfo=EVENT_TZ).astimezone(timezone.utc)


def time_left(end_time: str) -> str:
    """Время пересчитывается при каждом вызове."""
    end = datetime.fromisoformat(end_time)
    seconds = max(0, int((end - now_utc()).total_seconds()))

    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = remainder // 60

    text = f"{days} дн. {hours} ч. {minutes} мин."

    if 0 < seconds < 60:
        text += " (менее минуты)"

    return text


def is_active(event: dict) -> bool:
    return (
        bool(event["active"])
        and datetime.fromisoformat(event["end_time"]) > now_utc()
    )


def validate_event(title: str, description: str, bonus: str):
    # Ограничиваем длины, чтобы ивент помещался в сообщение.
    if not 1 <= len(title) <= 100:
        raise ValueError("Название: от 1 до 100 символов.")

    if not 1 <= len(description) <= 1000:
        raise ValueError("Описание: от 1 до 1000 символов.")

    if len(bonus) > 300:
        raise ValueError("Бонус: не более 300 символов.")


def event_text(event: dict, announcement: bool = False) -> str:
    parts = []

    if announcement:
        parts.append("🎉 <b>Новый ивент!</b>")

    parts.append(f"<b>{escape(event['title'])}</b>")
    parts.append(escape(event["description"]))

    if event["bonus"]:
        parts.append(f"🎁 Бонус: {escape(event['bonus'])}")

    if is_active(event):
        end = datetime.fromisoformat(event["end_time"])
        formatted = end.astimezone(EVENT_TZ).strftime(DATE_FORMAT)

        parts.append(f"📅 Окончание: {formatted} ({EVENT_TZ_NAME})")
        parts.append(f"⏳ Осталось: {time_left(event['end_time'])}")
    else:
        parts.append("🏁 Ивент завершён.")

    return "\n\n".join(parts)


def profile_link(user: dict) -> str:
    """HTML-ссылка на профиль участника."""
    username = user["username"]

    if username:
        url = html.escape(f"https://t.me/{username}", quote=True)
        label = escape(f"@{username}")
    else:
        url = f"tg://user?id={user['telegram_id']}"
        label = "без юзернейма"

    return f'<a href="{url}">{label}</a>'


async def send_message(
    bot: Bot,
    chat_id: int,
    text: str,
    *,
    use_html: bool = False,
):
    """Отправка с обработкой временного ограничения Telegram."""
    for attempt in range(5):
        try:
            return await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML if use_html else None,
                link_preview_options=LinkPreviewOptions(
                    is_disabled=True
                ),
            )
        except TelegramRetryAfter as exc:
            if attempt == 4:
                raise
            await asyncio.sleep(exc.retry_after + 1)


async def send_lines(bot: Bot, chat_id: int, lines: list[str]):
    """Разбиваем длинный HTML-список на несколько сообщений."""
    chunk = ""

    for line in lines:
        # Берём консервативный лимит с запасом.
        if chunk and len(chunk) + len(line) + 1 > 3000:
            await send_message(bot, chat_id, chunk, use_html=True)
            await asyncio.sleep(0.1)
            chunk = ""

        chunk += line + "\n"

    if chunk:
        await send_message(bot, chat_id, chunk, use_html=True)


async def admin_only(message: Message) -> bool:
    """Проверяем именно Telegram ID, а не username."""
    if not message.from_user or message.from_user.id != ADMIN_ID:
        await message.answer("Команда доступна только администратору.")
        return False

    return True


# ============================================================
# SQLITE
#
# Каждая операция использует отдельное соединение.
# Из обработчиков вызываем функции БД через asyncio.to_thread,
# чтобы синхронные операции SQLite не блокировали бота.
# ============================================================

def connect_db() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def init_db():
    with closing(connect_db()) as connection:
        connection.execute("PRAGMA journal_mode = WAL")

        connection.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                unique_code TEXT NOT NULL UNIQUE,
                registered_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS event (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                end_time TEXT NOT NULL,
                bonus TEXT NOT NULL DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1
                    CHECK (active IN (0, 1))
            );

            CREATE TABLE IF NOT EXISTS counter (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_letter TEXT NOT NULL,
                last_number TEXT NOT NULL
            );
        """)

        # Миграция для event без поля active.
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(event)")
        }

        if "active" not in columns:
            connection.execute(
                """
                ALTER TABLE event
                ADD COLUMN active INTEGER NOT NULL DEFAULT 1
                """
            )

        connection.execute(
            """
            INSERT OR IGNORE INTO counter (
                id, last_letter, last_number
            ) VALUES (1, ?, '0')
            """,
            (CODE_LETTER,),
        )

        connection.commit()


def increment_number(value: str) -> str:
    """
    Увеличиваем число как строку.

    Это позволяет не зависеть от предела INTEGER SQLite.
    Физически бесконечных кодов не бывает: остаются ограничения
    памяти, размера базы и длины сообщений Telegram.
    """
    if not value or any(char not in "0123456789" for char in value):
        raise RuntimeError("Повреждён числовой счётчик.")

    digits = list(value.lstrip("0") or "0")
    index = len(digits) - 1

    while index >= 0 and digits[index] == "9":
        digits[index] = "0"
        index -= 1

    if index < 0:
        digits.insert(0, "1")
    else:
        digits[index] = str(int(digits[index]) + 1)

    return "".join(digits)


def register_user(
    telegram_id: int,
    username: str | None,
) -> tuple[str, bool]:
    """
    Возвращает (код, новая_регистрация).

    Проверка пользователя, выдача кода и обновление счётчика
    происходят в ОДНОЙ транзакции.

    BEGIN IMMEDIATE сериализует конкурирующие записи:
    два одновременных /start не выдадут разные коды одному ID.
    """
    with closing(connect_db()) as connection:
        try:
            connection.execute("BEGIN IMMEDIATE")

            existing = connection.execute(
                "SELECT unique_code FROM users WHERE telegram_id = ?",
                (telegram_id,),
            ).fetchone()

            if existing:
                connection.execute(
                    "UPDATE users SET username = ? WHERE telegram_id = ?",
                    (username, telegram_id),
                )
                connection.commit()
                return existing["unique_code"], False

            counter = connection.execute(
                "SELECT * FROM counter WHERE id = 1"
            ).fetchone()

            if counter is None:
                raise RuntimeError("Счётчик не найден.")

            letter = counter["last_letter"]
            number = str(counter["last_number"])

            if not re.fullmatch(r"[A-Z]", letter):
                raise RuntimeError("Некорректная буква счётчика.")

            # Явная проверка занятости + UNIQUE в таблице users.
            while True:
                number = increment_number(number)
                code = f"{letter}{number}"

                occupied = connection.execute(
                    "SELECT 1 FROM users WHERE unique_code = ?",
                    (code,),
                ).fetchone()

                if occupied is None:
                    break

            connection.execute(
                """
                INSERT INTO users (
                    telegram_id, username, unique_code, registered_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    username,
                    code,
                    now_utc().isoformat(),
                ),
            )

            connection.execute(
                """
                UPDATE counter
                SET last_letter = ?, last_number = ?
                WHERE id = 1
                """,
                (letter, number),
            )

            connection.commit()
            return code, True

        except Exception:
            connection.rollback()
            raise


def get_user(
    telegram_id: int,
    username: str | None,
) -> dict | None:
    """Получаем участника и обновляем его текущий username."""
    with closing(connect_db()) as connection:
        with connection:
            connection.execute(
                "UPDATE users SET username = ? WHERE telegram_id = ?",
                (username, telegram_id),
            )

            row = connection.execute(
                "SELECT * FROM users WHERE telegram_id = ?",
                (telegram_id,),
            ).fetchone()

            return dict(row) if row else None


def get_users() -> list[dict]:
    with closing(connect_db()) as connection:
        rows = connection.execute(
            """
            SELECT * FROM users
            ORDER BY length(unique_code), unique_code
            """
        ).fetchall()

        return [dict(row) for row in rows]


def find_codes(codes: list[str]) -> dict[str, dict]:
    """Параметризованные запросы: ввод не подставляется в SQL."""
    result = {}

    with closing(connect_db()) as connection:
        for code in codes:
            row = connection.execute(
                "SELECT * FROM users WHERE unique_code = ?",
                (code,),
            ).fetchone()

            if row:
                result[code] = dict(row)

    return result


def get_event() -> dict | None:
    with closing(connect_db()) as connection:
        row = connection.execute(
            "SELECT * FROM event WHERE id = 1"
        ).fetchone()

        return dict(row) if row else None


def save_event(
    title: str,
    description: str,
    end_time: str,
    bonus: str,
):
    with closing(connect_db()) as connection:
        with connection:
            connection.execute(
                """
                INSERT INTO event (
                    id, title, description, end_time, bonus, active
                ) VALUES (1, ?, ?, ?, ?, 1)
                ON CONFLICT(id) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    end_time = excluded.end_time,
                    bonus = excluded.bonus,
                    active = 1
                """,
                (title, description, end_time, bonus),
            )


def update_event(field: str, value: str):
    # Только заранее разрешённые SQL-запросы.
    queries = {
        "title": "UPDATE event SET title = ? WHERE id = 1",
        "description": "UPDATE event SET description = ? WHERE id = 1",
        "bonus": "UPDATE event SET bonus = ? WHERE id = 1",
        "end_time": (
            "UPDATE event SET end_time = ?, active = 1 WHERE id = 1"
        ),
    }

    with closing(connect_db()) as connection:
        with connection:
            connection.execute(queries[field], (value,))


def finish_event() -> bool:
    with closing(connect_db()) as connection:
        with connection:
            cursor = connection.execute(
                "UPDATE event SET active = 0 WHERE id = 1"
            )
            return cursor.rowcount > 0


# ============================================================
# КОМАНДЫ ПОЛЬЗОВАТЕЛЕЙ
# ============================================================

@router.message(Command("start"))
async def start_handler(message: Message):
    user = message.from_user
    if user is None:
        return

    code, created = await asyncio.to_thread(
        register_user, user.id, user.username
    )

    if created:
        await message.answer(
            "🎉 Ты зарегистрирован!\n\n"
            f"Твой уникальный код: {code}\n\n"
            f"🛒 Купи товар на FunPay:\n{PRODUCT_URL}\n\n"
            "После покупки напиши в чате с продавцом:\n"
            f"{code} от КОД_ПРИГЛАСИВШЕГО\n\n"
            "/mycode — посмотреть свой код\n"
            "/event — посмотреть текущий ивент\n"
            "/buy — ссылка на товар"
        )
    else:
        await message.answer(
            "Ты уже зарегистрирован 🙂\n\n"
            f"Твой код: {code}\n"
            "Повторный код не выдаётся.\n\n"
            f"🛒 Купи товар на FunPay:\n{PRODUCT_URL}"
        )


@router.message(Command("buy"))
async def buy_handler(message: Message):
    if message.from_user is None:
        return

    user = await asyncio.to_thread(
        get_user,
        message.from_user.id,
        message.from_user.username,
    )

    if user is None:
        await message.answer(
            "Сначала зарегистрируйся: /start\n\n"
            f"🛒 Ссылка на товар:\n{PRODUCT_URL}"
        )
        return

    await message.answer(
        f"🛒 Купить товар на FunPay:\n{PRODUCT_URL}\n\n"
        "После покупки напиши в чате с продавцом:\n"
        f"{user['unique_code']} от КОД_ПРИГЛАСИВШЕГО"
    )


@router.message(Command("mycode"))
async def mycode_handler(message: Message):
    if message.from_user is None:
        return

    user = await asyncio.to_thread(
        get_user,
        message.from_user.id,
        message.from_user.username,
    )

    if user is None:
        await message.answer("Сначала зарегистрируйся: /start")
        return

    await message.answer(f"Твой код: {user['unique_code']}")


@router.message(Command("event"))
async def event_handler(message: Message, bot: Bot):
    if message.from_user:
        await asyncio.to_thread(
            get_user,
            message.from_user.id,
            message.from_user.username,
        )

    event = await asyncio.to_thread(get_event)

    if event is None:
        await message.answer("Ивент пока не создан.")
        return

    await send_message(
        bot, message.chat.id, event_text(event), use_html=True
    )


# ============================================================
# СОЗДАНИЕ, РЕДАКТИРОВАНИЕ И ЗАВЕРШЕНИЕ ИВЕНТА
# ============================================================

SET_EVENT_HELP = (
    "Отправь одним сообщением:\n\n"
    "/setevent Название\n"
    "2026-12-31 20:00\n"
    "Бонус или -\n"
    "Описание, можно в несколько строк\n\n"
    f"Часовой пояс: {EVENT_TZ_NAME}.\n"
    "Новый ивент заменяет предыдущий. Коды участников сохраняются."
)


@router.message(Command("setevent"))
async def setevent_handler(
    message: Message,
    command: CommandObject,
    bot: Bot,
):
    if not await admin_only(message):
        return

    lines = (command.args or "").strip().splitlines()

    if len(lines) < 4:
        await message.answer(SET_EVENT_HELP)
        return

    title = lines[0].strip()
    date_value = lines[1].strip()
    bonus = lines[2].strip()
    description = "\n".join(lines[3:]).strip()

    if bonus == "-":
        bonus = ""

    try:
        validate_event(title, description, bonus)
        end = parse_date(date_value)

        if end <= now_utc():
            raise ValueError("Дата окончания должна быть в будущем.")

    except ValueError as exc:
        await message.answer(str(exc))
        return

    await asyncio.to_thread(
        save_event,
        title,
        description,
        end.isoformat(),
        bonus,
    )

    event = await asyncio.to_thread(get_event)

    await send_message(
        bot,
        message.chat.id,
        "✅ Ивент сохранён.\n\n" + event_text(event),
        use_html=True,
    )


EDIT_EVENT_HELP = (
    "Примеры:\n\n"
    "/editevent title Новое название\n"
    "/editevent description Новое описание\n"
    "/editevent bonus Новый бонус\n"
    "/editevent bonus -\n"
    "/editevent end_time 2026-12-31 22:00\n\n"
    "Описание может занимать несколько строк.\n"
    f"Часовой пояс: {EVENT_TZ_NAME}.\n"
    "Изменение даты на будущую возобновляет ивент."
)


@router.message(Command("editevent"))
async def editevent_handler(
    message: Message,
    command: CommandObject,
    bot: Bot,
):
    if not await admin_only(message):
        return

    parts = (command.args or "").strip().split(maxsplit=1)

    if len(parts) != 2:
        await message.answer(EDIT_EVENT_HELP)
        return

    field, value = parts
    field = field.lower()
    value = value.strip()

    if field not in {"title", "description", "bonus", "end_time"}:
        await message.answer(EDIT_EVENT_HELP)
        return

    event = await asyncio.to_thread(get_event)

    if event is None:
        await message.answer("Сначала создай ивент: /setevent")
        return

    try:
        if field == "end_time":
            end = parse_date(value)

            if end <= now_utc():
                raise ValueError("Дата окончания должна быть в будущем.")

            value = end.isoformat()
        else:
            if field == "bonus" and value == "-":
                value = ""

            updated = dict(event)
            updated[field] = value

            validate_event(
                updated["title"],
                updated["description"],
                updated["bonus"],
            )

    except ValueError as exc:
        await message.answer(str(exc))
        return

    await asyncio.to_thread(update_event, field, value)
    event = await asyncio.to_thread(get_event)

    await send_message(
        bot,
        message.chat.id,
        "✅ Ивент обновлён.\n\n" + event_text(event),
        use_html=True,
    )


@router.message(Command("endevent"))
async def endevent_handler(message: Message):
    if not await admin_only(message):
        return

    exists = await asyncio.to_thread(finish_event)

    if exists:
        await message.answer(
            "🏁 Ивент завершён. Таймер остановлен.\n"
            "Все участники и их коды сохранены."
        )
    else:
        await message.answer("Ивент ещё не создан.")


@router.message(Command("timer"))
async def timer_handler(message: Message):
    if not await admin_only(message):
        return

    event = await asyncio.to_thread(get_event)

    if event is None:
        await message.answer("Ивент ещё не создан.")
    elif not is_active(event):
        await message.answer("🏁 Ивент завершён. Таймер остановлен.")
    else:
        await message.answer(
            f"⏳ Осталось: {time_left(event['end_time'])}"
        )


# ============================================================
# СПИСОК КОДОВ И ПРОВЕРКА
# ============================================================

@router.message(Command("codes"))
async def codes_handler(message: Message, bot: Bot):
    if not await admin_only(message):
        return

    users = await asyncio.to_thread(get_users)

    if not users:
        await message.answer("Участников пока нет.")
        return

    await message.answer(f"Всего участников: {len(users)}")

    lines = [
        f"{escape(user['unique_code'])} — {profile_link(user)}"
        for user in users
    ]

    await send_lines(bot, message.chat.id, lines)


# Не извлекаем код из середины слова, другого кода или username.
# Принимаем одну латинскую букву + одну или несколько цифр.
CODE_PATTERN = re.compile(
    r"(?<![\w@])[A-Za-z][0-9]+(?!\w)"
)


@router.message(Command("check"))
async def check_handler(
    message: Message,
    command: CommandObject,
    bot: Bot,
):
    if not await admin_only(message):
        return

    text = command.args or ""

    # Убираем дубли, сохраняя исходный порядок.
    # Строчные латинские буквы приводим к заглавным.
    codes = list(dict.fromkeys(
        match.group().upper()
        for match in CODE_PATTERN.finditer(text)
    ))

    if not codes:
        await message.answer(
            "Не найдены коды для проверки.\n\n"
            "Примеры:\n"
            "/check A25\n"
            "/check A25 от A3\n\n"
            "Используй латинские буквы."
        )
        return

    found = await asyncio.to_thread(find_codes, codes)

    lines = []

    for code in codes:
        if code in found:
            lines.append(
                f"✅ {escape(code)} — {profile_link(found[code])}"
            )
        else:
            lines.append(
                f"❌ {escape(code)} — не найден в базе"
            )

    await send_lines(bot, message.chat.id, lines)


# ============================================================
# ЭКСПОРТ CSV / TXT
# ============================================================

def csv_safe(value) -> str:
    """
    Защита от интерпретации текстового поля как формулы
    при открытии CSV в табличном редакторе.
    """
    text = "" if value is None else str(value)

    if text.startswith(("=", "+", "-", "@", "\t", "\r", "\n")):
        return "'" + text

    return text


def make_export(users: list[dict], file_format: str) -> bytes:
    if file_format == "csv":
        output = io.StringIO(newline="")
        writer = csv.writer(output, delimiter=";")

        writer.writerow([
            "unique_code",
            "telegram_id",
            "username",
            "profile_url",
            "registered_at",
        ])

        for user in users:
            username = user["username"]
            url = (
                f"https://t.me/{username}"
                if username
                else f"tg://user?id={user['telegram_id']}"
            )

            writer.writerow([
                csv_safe(user["unique_code"]),
                user["telegram_id"],
                csv_safe(username),
                url,
                user["registered_at"],
            ])

        # BOM помогает Excel распознать UTF-8.
        return output.getvalue().encode("utf-8-sig")

    lines = [
        "Код\tTelegram ID\tUsername\tПрофиль\tРегистрация (UTC)"
    ]

    for user in users:
        username = user["username"]
        label = f"@{username}" if username else "без юзернейма"
        url = (
            f"https://t.me/{username}"
            if username
            else f"tg://user?id={user['telegram_id']}"
        )

        lines.append(
            f"{user['unique_code']}\t"
            f"{user['telegram_id']}\t"
            f"{label}\t"
            f"{url}\t"
            f"{user['registered_at']}"
        )

    return "\n".join(lines).encode("utf-8")


@router.message(Command("export"))
async def export_handler(
    message: Message,
    command: CommandObject,
):
    if not await admin_only(message):
        return

    file_format = (command.args or "csv").strip().lower()

    if file_format not in {"csv", "txt"}:
        await message.answer("Используй /export csv или /export txt.")
        return

    users = await asyncio.to_thread(get_users)
    data = await asyncio.to_thread(make_export, users, file_format)

    # Запас относительно лимита стандартного облачного Bot API.
    if len(data) > 49 * 1024 * 1024:
        await message.answer(
            "Файл больше 49 МБ. Для такой базы "
            "нужно добавить экспорт частями."
        )
        return

    timestamp = now_utc().strftime("%Y%m%d_%H%M%S")
    filename = f"users_{timestamp}.{file_format}"

    await message.answer_document(
        document=BufferedInputFile(data, filename=filename),
        caption=f"Участников в выгрузке: {len(users)}",
    )


# ============================================================
# РАССЫЛКА
# ============================================================

async def broadcast_worker(
    bot: Bot,
    admin_chat_id: int,
    text: str,
):
    """
    Отправляем снимок ивента участникам из базы.

    Недоступных пользователей НЕ удаляем:
    их коды должны сохраняться навсегда.
    """
    sent = 0
    unavailable = 0
    failed = 0

    try:
        users = await asyncio.to_thread(get_users)

        for user in users:
            try:
                await send_message(
                    bot,
                    user["telegram_id"],
                    text,
                    use_html=True,
                )
                sent += 1

            except (TelegramForbiddenError, TelegramBadRequest):
                unavailable += 1

            except TelegramAPIError:
                failed += 1
                logger.exception(
                    "Ошибка рассылки для ID %s",
                    user["telegram_id"],
                )

            await asyncio.sleep(BROADCAST_DELAY)

        await send_message(
            bot,
            admin_chat_id,
            "✅ Рассылка завершена.\n\n"
            f"Всего получателей: {len(users)}\n"
            f"Отправлено: {sent}\n"
            f"Недоступны: {unavailable}\n"
            f"Другие ошибки: {failed}",
        )

    except asyncio.CancelledError:
        logger.info("Рассылка остановлена при выключении бота.")
        raise

    except Exception:
        logger.exception("Рассылка прервана.")

        try:
            await send_message(
                bot,
                admin_chat_id,
                "⚠️ Рассылка прервана из-за ошибки.\n"
                f"Успешно отправлено: {sent}\n"
                "Подробности — в журнале бота.",
            )
        except TelegramAPIError:
            logger.exception("Не удалось уведомить администратора.")


@router.message(Command("broadcast"))
async def broadcast_handler(message: Message, bot: Bot):
    global broadcast_task

    if not await admin_only(message):
        return

    async with broadcast_lock:
        if broadcast_task and not broadcast_task.done():
            await message.answer("Рассылка уже выполняется.")
            return

        event = await asyncio.to_thread(get_event)

        if event is None:
            await message.answer("Сначала создай ивент: /setevent")
            return

        if not is_active(event):
            await message.answer(
                "Ивент завершён. Создай новый или измени дату окончания."
            )
            return

        text = event_text(event, announcement=True)

        broadcast_task = asyncio.create_task(
            broadcast_worker(bot, message.chat.id, text)
        )

    await message.answer(
        "📣 Рассылка запущена. После завершения пришлю отчёт."
    )


# ============================================================
# МЕНЮ КОМАНД И ЗАПУСК
# ============================================================

async def setup_commands(bot: Bot):
    user_commands = [
        BotCommand(command="start", description="Регистрация"),
        BotCommand(command="mycode", description="Мой код"),
        BotCommand(command="event", description="Текущий ивент"),
        BotCommand(command="buy", description="Купить товар"),
    ]

    admin_commands = user_commands + [
        BotCommand(command="setevent", description="Создать ивент"),
        BotCommand(command="editevent", description="Редактировать ивент"),
        BotCommand(command="endevent", description="Завершить ивент"),
        BotCommand(command="timer", description="Таймер"),
        BotCommand(command="broadcast", description="Рассылка"),
        BotCommand(command="codes", description="Все коды"),
        BotCommand(command="export", description="Экспорт CSV/TXT"),
        BotCommand(command="check", description="Проверить коды"),
    ]

    await bot.set_my_commands(
        user_commands,
        scope=BotCommandScopeDefault(),
    )

    try:
        await bot.set_my_commands(
            admin_commands,
            scope=BotCommandScopeChat(chat_id=ADMIN_ID),
        )
    except TelegramBadRequest:
        logger.warning(
            "Не удалось установить меню администратора. "
            "Проверь ADMIN_ID, отправь боту /start и перезапусти его. "
            "Доступ к командам всё равно проверяется по ID."
        )


async def main():
    if BOT_TOKEN.startswith("ВСТАВЬ"):
        raise RuntimeError("Вставь токен бота в BOT_TOKEN.")

    if not re.fullmatch(r"[A-Z]", CODE_LETTER):
        raise RuntimeError(
            "CODE_LETTER должна быть одной заглавной латинской буквой."
        )

    await asyncio.to_thread(init_db)

    bot = Bot(token=BOT_TOKEN)
    dispatcher = Dispatcher()
    dispatcher.include_router(router)

    try:
        await setup_commands(bot)

        # Для long polling отключаем webhook.
        # Накопленные обновления не удаляем.
        await bot.delete_webhook(drop_pending_updates=False)

        logger.info("Бот запущен. SQLite: %s", DB_PATH)

        await dispatcher.start_polling(
            bot,
            allowed_updates=dispatcher.resolve_used_update_types(),
            close_bot_session=False,
        )

    finally:
        if broadcast_task and not broadcast_task.done():
            broadcast_task.cancel()

            try:
                await broadcast_task
            except asyncio.CancelledError:
                pass

        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
