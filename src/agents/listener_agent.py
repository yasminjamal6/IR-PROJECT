"""
The Watch: Telegram Listener Agent (Module A)

This module handles real-time ingestion of messages from monitored Telegram channels.
Uses Telethon for async Telegram client operations.

Usage:
    python -m src.agents.listener_agent

Environment Variables Required:
    TELEGRAM_API_ID: Your Telegram API ID
    TELEGRAM_API_HASH: Your Telegram API Hash
    TELEGRAM_PHONE: Your phone number (for first-time authentication)
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import List, Optional, Callable, Awaitable
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import Channel, Message

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.models.schemas import TelegramMessage, TelegramChannelConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("the-watch.listener")

# Load environment variables
load_dotenv()


class TelegramListener:
    """
    Async Telegram client for monitoring safety-related channels.
    
    This is the core of Module A - it listens to predefined channels
    and pushes messages to the processing pipeline.
    """
    
    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_name: str = "the_watch_session",
        message_handler: Optional[Callable[[TelegramMessage], Awaitable[None]]] = None
    ):
        """
        Initialize the Telegram listener.
        
        Args:
            api_id: Telegram API ID from my.telegram.org
            api_hash: Telegram API Hash from my.telegram.org
            session_name: Name for the session file (stores auth)
            message_handler: Async callback for processing messages
        """
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name
        self.message_handler = message_handler
        
        # Initialize client
        self.client = TelegramClient(
            session_name,
            api_id,
            api_hash,
            system_version="4.16.30-vxTHE_WATCH"
        )
        
        # Tracked channels (will be populated via add_channel)
        self.monitored_channels: dict[int, TelegramChannelConfig] = {}
        
        # Statistics
        self.stats = {
            "messages_received": 0,
            "messages_processed": 0,
            "errors": 0,
            "started_at": None
        }
        
        logger.info("TelegramListener initialized")

    def add_channel(self, config: TelegramChannelConfig) -> None:
        """Add a channel to monitor."""
        self.monitored_channels[config.channel_id] = config
        logger.info(f"Added channel to monitor: {config.channel_name} (ID: {config.channel_id})")

    def add_channels(self, configs: List[TelegramChannelConfig]) -> None:
        """Add multiple channels to monitor."""
        for config in configs:
            self.add_channel(config)

    def _normalize_channel_id(self, channel_id: int) -> int:
        """
        Normalize channel ID to match our stored format.
        Telegram uses different ID formats:
        - Internal: 1277927787
        - Bot API: -1001277927787
        """
        # Convert to positive for comparison
        abs_id = abs(channel_id)
        
        # Remove -100 prefix if present (when id is like 1001277927787)
        if abs_id > 10**12:  # Has -100 prefix
            abs_id = abs_id - 10**12
        
        return abs_id

    async def _on_new_message(self, event: events.NewMessage.Event) -> None:
        """
        Internal handler for new messages.
        Filters by monitored channels and forwards to message_handler.
        """
        try:
            message: Message = event.message
            chat = await event.get_chat()
            
            # Log all incoming messages (INFO level so it's visible)
            chat_name = getattr(chat, 'title', getattr(chat, 'username', str(chat.id)))
            logger.info(f"📩 Incoming: {chat_name} (ID: {chat.id})")
            
            # Only process messages from channels (not private chats/groups)
            if not isinstance(chat, Channel):
                logger.info(f"⏭️  Skipping: {chat_name} - not a channel (type: {type(chat).__name__})")
                return
            
            # Normalize both the incoming ID and stored IDs for matching
            incoming_normalized = self._normalize_channel_id(chat.id)
            
            # Find matching channel config
            channel_config = None
            matched_id = None
            
            for stored_id, config in self.monitored_channels.items():
                stored_normalized = self._normalize_channel_id(stored_id)
                if incoming_normalized == stored_normalized:
                    channel_config = config
                    matched_id = stored_id
                    break
            
            if not channel_config:
                logger.info(f"⏭️  Skipping: {chat_name} (ID: {chat.id}) - not in monitored list")
                return
            
            channel_id = matched_id
            
            if not channel_config.enabled:
                return
            
            # Skip non-text messages for now
            if not message.text:
                logger.debug(f"Skipping non-text message from {channel_config.channel_name}")
                return
            
            self.stats["messages_received"] += 1
            
            # Create structured message object
            telegram_msg = TelegramMessage(
                message_id=message.id,
                channel_id=channel_id,
                channel_name=channel_config.channel_name,
                text=message.text,
                timestamp=message.date,
                has_media=message.media is not None,
                media_type=type(message.media).__name__ if message.media else None,
                reply_to_message_id=message.reply_to.reply_to_msg_id if message.reply_to else None
            )
            
            # Log the message
            logger.info(
                f"📨 NEW MESSAGE | Channel: {channel_config.channel_name} | "
                f"ID: {message.id} | Length: {len(message.text)} chars"
            )
            logger.debug(f"Message text preview: {message.text[:200]}...")
            
            # Forward to handler if configured
            if self.message_handler:
                await self.message_handler(telegram_msg)
                self.stats["messages_processed"] += 1
            else:
                # Default behavior: print to console
                self._print_message(telegram_msg)
                self.stats["messages_processed"] += 1
                
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Error processing message: {e}", exc_info=True)

    def _print_message(self, msg: TelegramMessage) -> None:
        """Pretty print a message to console (default handler)."""
        print("\n" + "=" * 80)
        print(f"🚨 INCOMING MESSAGE FROM: {msg.channel_name}")
        print(f"   Timestamp: {msg.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"   Message ID: {msg.message_id}")
        print("-" * 80)
        print(f"📝 TEXT:")
        print(msg.text)
        print("=" * 80 + "\n")

    async def start(self, phone: str = None) -> None:
        """
        Start the listener.
        Connects to Telegram and begins monitoring channels.
        
        Args:
            phone: Phone number for authentication (first time only)
        """
        logger.info("🚀 Starting The Watch Telegram Listener...")
        
        # Connect and authorize
        # Phone is only needed for first-time authentication
        phone = phone or os.getenv("TELEGRAM_PHONE")
        await self.client.start(phone=phone)
        
        me = await self.client.get_me()
        logger.info(f"✅ Authenticated as: {me.first_name} (@{me.username})")
        
        # Register message handler
        self.client.add_event_handler(
            self._on_new_message,
            events.NewMessage()
        )
        
        self.stats["started_at"] = datetime.utcnow()
        
        # Log monitored channels
        if self.monitored_channels:
            logger.info(f"👁️  Monitoring {len(self.monitored_channels)} channels:")
            for channel in self.monitored_channels.values():
                status = "✓" if channel.enabled else "✗"
                logger.info(f"   {status} {channel.channel_name} (ID: {channel.channel_id})")
        else:
            logger.warning("⚠️  No channels configured for monitoring!")
        
        logger.info("📡 Listener active. Waiting for messages...")

    async def run_forever(self) -> None:
        """Start and run until disconnected."""
        await self.start()
        await self.client.run_until_disconnected()

    async def stop(self) -> None:
        """Gracefully stop the listener."""
        logger.info("🛑 Stopping listener...")
        await self.client.disconnect()
        
        # Log statistics
        runtime = datetime.utcnow() - self.stats["started_at"] if self.stats["started_at"] else None
        logger.info(
            f"📊 Session Stats: "
            f"Received: {self.stats['messages_received']} | "
            f"Processed: {self.stats['messages_processed']} | "
            f"Errors: {self.stats['errors']} | "
            f"Runtime: {runtime}"
        )

    async def fetch_channel_history(
        self,
        channel_id: int,
        limit: int = 100
    ) -> List[TelegramMessage]:
        """
        Fetch historical messages from a channel.
        Useful for initial data population.
        
        Args:
            channel_id: Telegram channel ID
            limit: Maximum number of messages to fetch
            
        Returns:
            List of TelegramMessage objects
        """
        messages = []
        
        try:
            entity = await self.client.get_entity(channel_id)
            channel_name = getattr(entity, 'title', str(channel_id))
            
            logger.info(f"Fetching up to {limit} messages from {channel_name}...")
            
            async for message in self.client.iter_messages(entity, limit=limit):
                if message.text:
                    telegram_msg = TelegramMessage(
                        message_id=message.id,
                        channel_id=channel_id,
                        channel_name=channel_name,
                        text=message.text,
                        timestamp=message.date,
                        has_media=message.media is not None,
                        media_type=type(message.media).__name__ if message.media else None,
                        reply_to_message_id=message.reply_to.reply_to_msg_id if message.reply_to else None
                    )
                    messages.append(telegram_msg)
            
            logger.info(f"Fetched {len(messages)} text messages from {channel_name}")
            
        except Exception as e:
            logger.error(f"Error fetching history from channel {channel_id}: {e}")
            
        return messages


DEFAULT_CHANNELS = [
    TelegramChannelConfig(
        channel_id=-1001177174722,
        channel_name="Magen David Adom (MDA)",
        enabled=True,
        priority=1
    ),
    TelegramChannelConfig(
        channel_id=-1001352866222,
        channel_name="United Hatzalah",
        enabled=True,
        priority=1
    ),
    TelegramChannelConfig(
        channel_id=-1001277927787,
        channel_name="Amar Assadi News",
        enabled=True,
        priority=2
    ),
    TelegramChannelConfig(
        channel_id=-1001872012288,
        channel_name="Ariel Idan",
        enabled=True,
        priority=2
    ),
    TelegramChannelConfig(
        channel_id=-1001601174656,
        channel_name="News Channel",
        enabled=True,
        priority=2
    ),
    TelegramChannelConfig(
        channel_id=-1003766716578,
        channel_name="Test Channel",
        enabled=True,
        priority=3
    ),
]


async def main():
    """Main entry point for standalone listener execution."""
    
    # Load credentials from environment
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    
    if not api_id or not api_hash:
        logger.error(
            "❌ Missing Telegram credentials!\n"
            "   Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in your .env file.\n"
            "   Get these from https://my.telegram.org"
        )
        sys.exit(1)
    
    # Initialize listener
    listener = TelegramListener(
        api_id=int(api_id),
        api_hash=api_hash,
        session_name="the_watch_session"
    )
    
    # Add channels to monitor
    listener.add_channels(DEFAULT_CHANNELS)
    
    logger.info("📡 Monitoring Amar Assadi channel for safety incidents...")
    
    try:
        # Start listening
        await listener.run_forever()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await listener.stop()


if __name__ == "__main__":
    asyncio.run(main())
