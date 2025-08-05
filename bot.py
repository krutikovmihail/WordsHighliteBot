import logging
import sqlite3
import os
import re
import random
import asyncio
import sys
import pytz
from datetime import time
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ChatMemberHandler,
    MessageHandler,
    filters
)

# logging configs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

noisy_loggers = [
    "telegram.ext.Updater",
    "telegram.bot",
    "httpcore",
    "httpx"
]
for logger_name in noisy_loggers:
    logging.getLogger(logger_name).setLevel(logging.WARNING)

# Configs
DB_NAME = "words_database.db"
CONFIG_FILE = "bot_config.env"
MOSCOW_TIMEZONE = pytz.timezone('Europe/Moscow')  # UTC+3

def load_config():
    """Bot loading"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    
    if not token and os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                for line in f:
                    if line.startswith("TELEGRAM_BOT_TOKEN="):
                        token = line.split('=')[1].strip()
                        break
        except Exception as e:
            logger.error(f"Cant open config file: {e}")
    
    if not token:
        logger.critical("Cant find a token!")
        sys.exit(1)
    
    return token

def init_database():
    """DB init"""
    if not os.path.exists(DB_NAME):
        with sqlite3.connect(DB_NAME) as conn:
            logger.info(f"Created DB: {DB_NAME}")

def safe_table_name(chat_id: int) -> str:
    """Table safe name generation"""
    return f"chat_{re.sub(r'[^0-9]', '', str(chat_id))}_words"

def create_chat_table(chat_id: int):
    table_name = safe_table_name(chat_id)
    
    try:
        with sqlite3.connect(DB_NAME) as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    word TEXT NOT NULL UNIQUE
                )
            """)
        logger.info(f"Table for chat created: {chat_id}")
    except Exception as e:
        logger.error(f"Table creating error: {e}", exc_info=True)

def get_active_chats():
    active_chats = []
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[0]
                if table_name.startswith("chat_") and table_name.endswith("_words"):
                    chat_id_str = table_name.replace("chat_", "").replace("_words", "")
                    
                    try:
                        chat_id = int(chat_id_str)
                        if chat_id > 0:
                            chat_id = -chat_id
                    except ValueError:
                        continue
                    
                    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                    if cursor.fetchone()[0] > 0:
                        active_chats.append(chat_id)
    
    except Exception as e:
        logger.error(f"Cant get active chats: {e}")
    
    return active_chats

def get_random_words(chat_id: int, count=5):
    table_name = safe_table_name(chat_id)
    words = []
    
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            total_words = cursor.fetchone()[0]
            
            if total_words == 0:
                return []
            
            if total_words <= count:
                cursor.execute(f'SELECT word FROM "{table_name}"')
                words = [row[0] for row in cursor.fetchall()]
            else:
                cursor.execute(f'SELECT word FROM "{table_name}" ORDER BY RANDOM() LIMIT {count}')
                words = [row[0] for row in cursor.fetchall()]
    
    except Exception as e:
        logger.error(f"Cant get words for a chat {chat_id}: {e}")
    
    return words

def add_words_to_table(chat_id: int, words: list) -> int:
    table_name = safe_table_name(chat_id)
    added_count = 0
    
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if not cursor.fetchone():
                logger.info(f"Creating table for a chat: {chat_id}")
                create_chat_table(chat_id)
            
            for word in words:
                clean_word = word.strip()
                if not clean_word:
                    continue
                
                try:
                    cursor.execute(f'INSERT INTO "{table_name}" (word) VALUES (?)', (clean_word,))
                    added_count += 1
                except sqlite3.IntegrityError:
                    pass
                except Exception as e:
                    logger.error(f"Adding word error: {e}")
            
            conn.commit()
        
        logger.info(f"Adding word: {added_count}/{len(words)} in chat {chat_id}")
        return added_count
        
    except Exception as e:
        logger.error(f"Adding word critical error: {e}", exc_info=True)
        return 0

async def send_random_words(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Daily messaging")
    active_chats = get_active_chats()
    
    if not active_chats:
        logger.info("Cant find active chats")
        return
    
    logger.info(f"Active chats count: {len(active_chats)}")
    
    for chat_id in active_chats:
        try:
            # only group chats
            if chat_id > 0:
                continue
                
            words = get_random_words(chat_id)
            
            if not words:
                logger.info(f"Emty word list for chat: {chat_id}")
                continue
                
            message = "📚 Random words for repetition:\n\n"
            for i, word in enumerate(words, 1):
                message += f"{i}. {word}\n"
            
            await context.bot.send_message(
                chat_id=chat_id,
                text=message
            )
            logger.info(f"Sent words: {len(words)} into the chat {chat_id}")
            
            await asyncio.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Error sending words to the chat {chat_id}: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    logger.info(f"Bot start by user: {user.id}")
    await update.message.reply_text(
        f"Hello {user.first_name}! I'm a bot who will help you with words in chats.\n\n"
    )

async def handle_chat_addition(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.my_chat_member:
        change = update.my_chat_member
        new_status = change.new_chat_member.status
        
        if new_status in ["member", "administrator"]:
            chat = change.chat
            chat_id = chat.id
            chat_name = chat.title or "группа"
            
            logger.info(f"Bot has been added to the chat: {chat_id} ({chat_name})")
            
            if chat_id < 0:
                create_chat_table(chat_id)

async def handle_word_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if not message or not message.text:
        return
        
    chat_id = message.chat_id
    text = message.text.strip()
    user_id = message.from_user.id
    
    # Only group chats
    if chat_id > 0:
        return
    
    if not re.search(r'^\s*#WordsToLearn\b', text, re.IGNORECASE):
        return
    
    lines = text.split('\n')
    words = []
    found_hashtag = False
    
    for line in lines:
        clean_line = line.strip()
        if not clean_line:
            continue
            
        if "#WordsToLearn" in clean_line:
            found_hashtag = True
            continue
            
        if found_hashtag:
            words.append(clean_line)
    
    if not words:
        logger.info(f"Empty message from {user_id} in the {chat_id}")
        return
        
    added_count = add_words_to_table(chat_id, words)
    logger.info(f"Message processed: adding words {added_count} in chat {chat_id}")

def main() -> None:
    TOKEN = load_config()
    logger.info(f"Using DB: {os.path.abspath(DB_NAME)}")
    
    if not os.path.exists(DB_NAME):
        logger.info("Creating DB file")
    
    init_database()
    
    try:
        application = Application.builder().token(TOKEN).build()
        
        if not application.job_queue:
            logger.critical("Task queue is not available! Make sure it is installed: pip install \"python-telegram-bot[job-queue]\"")
            sys.exit(1)
            
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(ChatMemberHandler(handle_chat_addition))
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, 
            handle_word_messages
        ))
        
        moscow_time = time(hour=12, minute=0, tzinfo=MOSCOW_TIMEZONE)
        application.job_queue.run_daily(
            callback=send_random_words,
            time=moscow_time,
            days=(0, 1, 2, 3, 4, 5, 6),
            name="daily_word_distribution"
        )
        logger.info(f"The word distribution is scheduled for 12:00 UTC+3")
        
        # Запуск бота
        logger.info("The bot has been launched")
        application.run_polling()
        
    except Exception as e:
        logger.critical(f"Error starting bot: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()