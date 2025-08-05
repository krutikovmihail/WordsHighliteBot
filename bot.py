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

# Configure logging to file
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DB_NAME = "words_database.db"
CONFIG_FILE = "bot_config.env"
MOSCOW_TIMEZONE = pytz.timezone('Europe/Moscow')  # UTC+3

def load_config():
    """Load bot token from environment or config file"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    
    if not token and os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                for line in f:
                    if line.startswith("TELEGRAM_BOT_TOKEN="):
                        token = line.split('=')[1].strip()
                        break
        except Exception as e:
            logger.error(f"Config file error: {e}")
    
    if not token:
        logger.critical("Bot token not found!")
        sys.exit(1)
    
    return token

def init_database():
    """Initialize database file if missing"""
    if not os.path.exists(DB_NAME):
        with sqlite3.connect(DB_NAME) as conn:
            logger.info(f"Created new database: {DB_NAME}")

def safe_table_name(chat_id: int) -> str:
    """Generate safe SQL table name from chat ID"""
    # Remove sign and keep only digits
    return f"chat_{re.sub(r'[^0-9]', '', str(chat_id))}_words"

def create_chat_table(chat_id: int):
    """Create word table for chat if not exists"""
    table_name = safe_table_name(chat_id)
    
    try:
        with sqlite3.connect(DB_NAME) as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    word TEXT NOT NULL UNIQUE
                )
            """)
        logger.info(f"Created table for chat {chat_id}: {table_name}")
    except Exception as e:
        logger.error(f"Table creation error: {e}", exc_info=True)

def get_active_chats():
    """Retrieve all group chats with non-empty word tables"""
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
                    
                    # Only process group chats (negative IDs)
                    try:
                        chat_id = int(chat_id_str)
                        # Convert to negative ID for groups
                        if chat_id > 0:
                            chat_id = -chat_id
                    except ValueError:
                        continue
                    
                    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                    if cursor.fetchone()[0] > 0:
                        active_chats.append(chat_id)
    
    except Exception as e:
        logger.error(f"Active chats error: {e}")
    
    return active_chats

def get_random_words(chat_id: int, count=5):
    """Retrieve random words from chat's table"""
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
        logger.error(f"Word retrieval error for chat {chat_id}: {e}")
    
    return words

def add_words_to_table(chat_id: int, words: list) -> int:
    """Add words to chat table, ignore duplicates"""
    table_name = safe_table_name(chat_id)
    added_count = 0
    
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            
            # Verify table exists
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if not cursor.fetchone():
                logger.warning(f"Table {table_name} missing! Creating...")
                create_chat_table(chat_id)
            
            # Insert words
            for word in words:
                clean_word = word.strip()
                if not clean_word:
                    continue
                
                try:
                    cursor.execute(f'INSERT INTO "{table_name}" (word) VALUES (?)', (clean_word,))
                    added_count += 1
                except sqlite3.IntegrityError:
                    pass  # Duplicate handling
                except Exception as e:
                    logger.error(f"Word insertion error '{clean_word}': {e}")
            
            conn.commit()
        
        logger.info(f"Added {added_count}/{len(words)} words to chat {chat_id}")
        return added_count
        
    except Exception as e:
        logger.error(f"Critical word addition error: {e}", exc_info=True)
        return 0

async def send_random_words(context: ContextTypes.DEFAULT_TYPE):
    """Send random words to all active group chats"""
    logger.info("Starting daily word distribution")
    active_chats = get_active_chats()
    
    if not active_chats:
        logger.info("No active group chats found")
        return
    
    logger.info(f"Found {len(active_chats)} active group chats")
    
    for chat_id in active_chats:
        try:
            # Only send to group chats (negative IDs)
            if chat_id > 0:
                logger.info(f"Skipping personal chat {chat_id}")
                continue
                
            words = get_random_words(chat_id)
            
            if not words:
                logger.info(f"No words for chat {chat_id}")
                continue
                
            # Format message in Russian
            message = "📚 Случайные слова дня для повторения:\n\n"
            for i, word in enumerate(words, 1):
                message += f"{i}. {word}\n"
            
            await context.bot.send_message(
                chat_id=chat_id,
                text=message
            )
            logger.info(f"Sent {len(words)} words to group chat {chat_id}")
            
            # Short delay between messages
            await asyncio.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Message send error to chat {chat_id}: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command in private chats"""
    user = update.effective_user
    logger.info(f"User {user.id} started bot")
    await update.message.reply_text(
        f"Привет {user.first_name}! Я бот для работы с группами.\n\n"
        "Добавь меня в группу, и я буду автоматически сохранять слова из сообщений "
        "с пометкой #WordsToLearn.\n\n"
        "Каждый день в 12:00 по МСК я буду отправлять 5 случайных слов для повторения!"
    )

async def handle_chat_addition(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process bot being added to chats"""
    if update.my_chat_member:
        change = update.my_chat_member
        new_status = change.new_chat_member.status
        
        if new_status in ["member", "administrator"]:
            chat = change.chat
            chat_id = chat.id
            chat_name = chat.title or "this chat"
            
            logger.info(f"Bot added to chat: {chat_id} ({chat_name})")
            
            # Only create tables for group chats
            if chat_id < 0:
                create_chat_table(chat_id)
            else:
                logger.info(f"Skipping table creation for personal chat {chat_id}")

async def handle_word_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process messages with #WordsToLearn tag"""
    message = update.message
    if not message or not message.text:
        return
        
    chat_id = message.chat_id
    text = message.text.strip()
    user_id = message.from_user.id
    
    # Only process messages from group chats
    if chat_id > 0:
        logger.info(f"Ignoring message from personal chat {chat_id}")
        return
    
    # Log all messages for debugging
    logger.debug(f"Received message in group {chat_id}: {text[:50]}...")
    
    # Check for tag
    if not re.search(r'^\s*#WordsToLearn\b', text, re.IGNORECASE):
        logger.debug(f"Message does not contain #WordsToLearn tag")
        return
    
    # Extract words
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
        logger.info(f"Empty word message from {user_id} in {chat_id}")
        return
        
    # Add to database
    added_count = add_words_to_table(chat_id, words)
    logger.info(f"Processed message: added {added_count} words in group chat {chat_id}")

def main() -> None:
    """Launch the bot"""
    TOKEN = load_config()
    db_path = os.path.abspath(DB_NAME)
    logger.info(f"Using database: {db_path}")
    
    if not os.path.exists(DB_NAME):
        logger.warning("Creating new database file")
    
    init_database()
    
    try:
        # Create application with job queue support
        application = Application.builder().token(TOKEN).build()
        
        # Verify job queue availability
        if not application.job_queue:
            logger.critical("Job queue not initialized! Make sure you installed with: pip install \"python-telegram-bot[job-queue]\"")
            sys.exit(1)
            
        # Register handlers
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(ChatMemberHandler(handle_chat_addition))
        
        # Fixed message handler - removed ChatType filter
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, 
            handle_word_messages
        ))
        
        # Schedule daily word distribution at 12:00 Moscow time (UTC+3)
        # Create time object for 12:00 in Moscow timezone
        moscow_time = time(hour=12, minute=0, tzinfo=MOSCOW_TIMEZONE)
        
        application.job_queue.run_daily(
            callback=send_random_words,
            time=moscow_time,
            days=(0, 1, 2, 3, 4, 5, 6),  # Every day of the week
            name="daily_word_distribution"
        )
        logger.info(f"Scheduled daily word distribution at 12:00 Moscow time (UTC+3)")
        
        # Start polling
        logger.info("Bot starting")
        application.run_polling()
        
    except Exception as e:
        logger.critical(f"Bot initialization failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()