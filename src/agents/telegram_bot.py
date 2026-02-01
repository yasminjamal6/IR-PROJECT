"""
The Watch: Telegram Bot Interface

A Telegram bot that allows users to query safety information
directly from Telegram. Supports:
- /start - Welcome message
- /safety <location> - Get safety status for a location
- /news - Get breaking news (last 24h)
- /stats - Database statistics
- Natural language queries
"""

import os
import logging
import json
from datetime import datetime
from typing import Optional, Dict, List

from dotenv import load_dotenv
from telethon import TelegramClient, events, Button
from telethon.tl.types import User
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
)
logger = logging.getLogger("the-watch.bot")

# Bot token from BotFather
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


class TheWatchBot:
    """
    Telegram bot interface for The Watch safety queries.
    """
    
    def __init__(self, bot_token: str = None):
        """
        Initialize the bot.
        
        Args:
            bot_token: Telegram bot token from @BotFather
        """
        self.bot_token = bot_token or BOT_TOKEN
        
        if not self.bot_token:
            raise ValueError(
                "Missing TELEGRAM_BOT_TOKEN. "
                "Create a bot via @BotFather and add token to .env"
            )
        
        # Initialize bot client
        self.client = TelegramClient(
            'the_watch_bot',
            api_id=int(os.getenv('TELEGRAM_API_ID')),
            api_hash=os.getenv('TELEGRAM_API_HASH')
        )
        
        # Note: Handlers will be registered in start() after client is connected
        self._handlers_registered = False
        
        # Conversation state for preferences (user_id -> state)
        self._preferences_conversations = {}
        
        logger.info("TheWatchBot initialized")
    
    def _register_handlers(self):
        """Register all command and message handlers."""
        
        @self.client.on(events.NewMessage(pattern='/start'))
        async def start_handler(event):
            """Welcome message with instructions."""
            logger.info(f"📨 /start command received from user {event.sender_id}")
            welcome = """
👁️ **Welcome to The Watch**

I'm your safety intelligence assistant for communities in Israel.

**Commands:**
• `/safety <location>` - Check safety status
• `/news` - Breaking news (last 24h)
• `/stats` - Database statistics
• `/help` - Show this message

**Or just ask me naturally:**
• "מה המצב ברחוב הרצל בתל אביב?"
• "מה קרה היום בירושלים?"
• "האם בטוח ברחוב יפו בחיפה?"
• "מה המצב בשדרות רוטשילד?"

Stay safe! 🙏
"""
            await event.respond(welcome, parse_mode='md')
        
        @self.client.on(events.NewMessage(pattern='/help'))
        async def help_handler(event):
            """Show help message."""
            await start_handler(event)
        
        @self.client.on(events.NewMessage(pattern='/safety(.*)'))
        async def safety_handler(event):
            """Handle /safety <location> command."""
            # Extract location from command
            location = event.pattern_match.group(1).strip()
            
            if not location:
                await event.respond(
                    "⚠️ אנא ציין מיקום:\n"
                    "`/safety רחוב הרצל, תל אביב`\n"
                    "`/safety נצרת`",
                    parse_mode='md'
                )
                return
            
            # Send analyzing message
            analyzing_msg = await event.respond(f"🔍 **בודק בטיחות עבור {location}...**", parse_mode='md')
            
            try:
                from src.agents.graph_orchestrator import query_safety_status
                
                result = query_safety_status(f"מה המצב ב{location}?")
                
                response = result.get('response', 'לא הצלחתי לנתח.')
                
                # Add risk badge
                risk = result.get('risk_assessment')
                if risk:
                    score = risk.get('risk_score', 0)
                    level = risk.get('risk_level', 'unknown').upper()
                    badge = self._get_risk_badge(score)
                    response = f"{badge} **{level}** ({score}/10)\n\n{response}"
                
                # Edit the analyzing message with the response
                try:
                    await analyzing_msg.edit(response, parse_mode='md')
                except Exception as edit_error:
                    logger.warning(f"Failed to edit /safety message: {edit_error}")
                    await event.respond(response, parse_mode='md')
                
            except Exception as e:
                logger.error(f"Safety query error: {e}")
                try:
                    await analyzing_msg.edit(f"❌ שגיאה: {str(e)}")
                except:
                    await event.respond(f"❌ שגיאה: {str(e)}")
        
        @self.client.on(events.NewMessage(pattern='/news'))
        async def news_handler(event):
            """Get breaking news filtered by user preferences."""
            from src.utils.user_preferences import get_preferences_manager
            from src.agents.graph_orchestrator import get_breaking_news
            
            user_id = event.sender_id
            prefs_manager = get_preferences_manager()
            prefs = prefs_manager.get_preferences(user_id)
            
            # Check if user has preferences
            if not prefs.has_preferences():
                # Ask user to set preferences first
                buttons = [
                    [Button.inline("⚙️ Set Preferences", data="set_preferences")],
                    [Button.inline("📰 Show All News", data="news_all")]
                ]
                await event.respond(
                    "**Personalized News**\n\n"
                    "To get news for your preferred locations, please set your preferences first.\n\n"
                    "You can:\n"
                    "• Set specific cities, streets, or neighborhoods\n"
                    "• Or view all news without filtering\n\n"
                    "Use `/prefs` to configure your settings.",
                    parse_mode='md',
                    buttons=buttons
                )
                return
            
            # User has preferences - fetch and filter news
            analyzing_msg = await event.respond("**מביא חדשות מותאמות אישית...**", parse_mode='md')
            
            try:
                # Get last 24 hours of news
                news = get_breaking_news(hours=24)
                all_incidents = news.get('incidents', [])
                
                # Filter by user preferences
                filtered_incidents = prefs_manager.filter_incidents_by_preferences(
                    user_id, all_incidents
                )
                
                if not filtered_incidents:
                    await analyzing_msg.edit(
                        f"**No incidents in your preferred areas** (Last 24h)\n\n"
                        f"Total incidents found: {len(all_incidents)}\n"
                        f"Filtered by your preferences: {len(filtered_incidents)}\n\n"
                        f"Your preferences:\n"
                        f"• Cities: {', '.join(prefs.preferred_cities) if prefs.preferred_cities else 'None'}\n"
                        f"• Streets: {', '.join(prefs.preferred_streets) if prefs.preferred_streets else 'None'}\n"
                        f"• Neighborhoods: {', '.join(prefs.preferred_neighborhoods) if prefs.preferred_neighborhoods else 'None'}\n\n"
                        f"Use `/prefs` to update your settings.",
                        parse_mode='md'
                    )
                    return
                
                # Format filtered incidents
                response = f"**Personalized News** (Last 24h)\n"
                response += f"Found **{len(filtered_incidents)}** incidents in your areas:\n"
                response += f"(Out of {len(all_incidents)} total incidents)\n\n"
                
                # Sort incidents: preferred street matches first, then by severity
                def sort_key(inc):
                    street = inc.get('street', '')
                    street_matches = False
                    if street:
                        street_lower = street.lower()
                        street_matches = any(
                            pref_street.lower() in street_lower or street_lower in pref_street.lower()
                            for pref_street in prefs.preferred_streets
                        )
                    severity = inc.get('severity_score', 0)
                    # Return tuple: (not street_match, -severity) so matches come first and higher severity comes first
                    return (not street_matches, -severity)
                
                sorted_incidents = sorted(filtered_incidents[:15], key=sort_key)
                
                # Helper function to remove emojis from text
                def remove_emojis(text: str) -> str:
                    """Remove emoji characters from text."""
                    import re
                    # Remove emoji patterns
                    emoji_pattern = re.compile("["
                        u"\U0001F600-\U0001F64F"  # emoticons
                        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                        u"\U0001F680-\U0001F6FF"  # transport & map symbols
                        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                        u"\U00002702-\U000027B0"
                        u"\U000024C2-\U0001F251"
                        "]+", flags=re.UNICODE)
                    return emoji_pattern.sub('', text).strip()
                
                for i, inc in enumerate(sorted_incidents, 1):
                    severity = inc.get('severity_score', '?')
                    city = inc.get('city', 'Unknown')
                    street = inc.get('street', '')
                    summary = inc.get('summary', 'No details')
                    
                    # Remove emojis from summary and ensure it's complete
                    summary = remove_emojis(summary)
                    # Ensure summary ends with proper punctuation (not mid-sentence)
                    summary = summary.rstrip()
                    if summary and not summary[-1] in ['.', '!', '?', ':', ';']:
                        # If summary doesn't end properly, try to find last complete sentence
                        last_period = summary.rfind('.')
                        if last_period > len(summary) * 0.5:  # If period is in second half
                            summary = summary[:last_period + 1]
                    
                    # Check if this incident matches a preferred street
                    street_matches = False
                    if street and street.lower() not in ['unknown', 'לא ידוע', '']:
                        street_lower = street.lower()
                        street_matches = any(
                            pref_street.lower() in street_lower or street_lower in pref_street.lower()
                            for pref_street in prefs.preferred_streets
                        )
                    
                    # Build location string - only show street if it exists and is not "Unknown"
                    location_str = f"{city}"
                    if street and street.lower() not in ['unknown', 'לא ידוע', '']:
                        location_str += f", {street}"
                    location_str += f" ({severity}/10)"
                    
                    # Format without emojis
                    if street_matches:
                        response += f"{i}. **{location_str}**\n"
                        response += f"   {summary}\n\n"
                    else:
                        response += f"{i}. **{location_str}**\n"
                        response += f"   {summary}\n\n"
                
                if len(filtered_incidents) > 15:
                    response += f"\n_... and {len(filtered_incidents) - 15} more incidents_"
                
                buttons = [
                    [Button.inline("⚙️ Update Preferences", data="set_preferences")],
                    [Button.inline("🔄 Refresh", data="news")]
                ]
                
                await analyzing_msg.edit(response, parse_mode='md', buttons=buttons)
                
            except Exception as e:
                logger.error(f"News query error: {e}")
                try:
                    await analyzing_msg.edit(f"❌ Error: {str(e)}")
                except:
                    await event.respond(f"❌ Error: {str(e)}")
        
        @self.client.on(events.NewMessage(pattern='/prefs'))
        async def preferences_handler(event):
            """Handle preferences command - conversational LLM-based setup."""
            from src.utils.user_preferences import get_preferences_manager
            
            user_id = event.sender_id
            prefs_manager = get_preferences_manager()
            prefs = prefs_manager.get_preferences(user_id)
            
            # Check if user is already in a preferences conversation
            if user_id in self._preferences_conversations:
                # Continue conversation
                await self._handle_preferences_conversation(event)
                return
            
            # Show current preferences or start conversation
            if prefs.has_preferences():
                response = "⚙️ **Your Current Preferences**\n\n"
                response += f"🏙️ Cities: {', '.join(prefs.preferred_cities) if prefs.preferred_cities else 'None'}\n"
                response += f"🛣️ Streets: {', '.join(prefs.preferred_streets) if prefs.preferred_streets else 'None'}\n"
                response += f"🏘️ Neighborhoods: {', '.join(prefs.preferred_neighborhoods) if prefs.preferred_neighborhoods else 'None'}\n\n"
                response += "Would you like to update your preferences?"
                
                buttons = [
                    [Button.inline("✏️ Update Preferences", data="start_preferences_conversation"),
                     Button.inline("🗑️ Clear", data="clear_preferences")]
                ]
                await event.respond(response, parse_mode='md', buttons=buttons)
            else:
                # Start new conversation
                await self._start_preferences_conversation(event)
        
        @self.client.on(events.NewMessage(pattern='/stats'))
        async def stats_handler(event):
            """Show database statistics."""
            try:
                from src.database.chroma_manager import get_chroma_manager
                
                chroma = get_chroma_manager()
                stats = chroma.get_statistics()
                
                response = "📊 **The Watch Database Statistics**\n\n"
                response += f"• Total incidents: **{stats.get('total_incidents', 0)}**\n"
                response += f"• Avg severity: **{stats.get('avg_severity', 0)}**/10\n"
                response += f"• Max severity: **{stats.get('max_severity', 0)}**/10\n"
                
                cities = stats.get('incidents_by_city', {})
                if cities:
                    response += "\n**Top Cities:**\n"
                    for city, count in list(cities.items())[:5]:
                        response += f"  • {city}: {count}\n"
                
                types = stats.get('incidents_by_type', {})
                if types:
                    response += "\n**By Type:**\n"
                    for t, count in types.items():
                        emoji = self._get_event_emoji(t)
                        response += f"  • {emoji} {t}: {count}\n"
                
                await event.respond(response, parse_mode='md')
                
            except Exception as e:
                logger.error(f"Stats error: {e}")
                await event.respond(f"❌ Error: {str(e)}")
        
        @self.client.on(events.NewMessage())
        async def natural_query_handler(event):
            """Handle natural language queries (non-commands)."""
            from telethon.tl.functions.messages import SetTypingRequest
            from telethon.tl.types import SendMessageTypingAction
            import asyncio
            
            user_id = event.sender_id
            
            # Check if user is in preferences conversation
            if user_id in self._preferences_conversations:
                await self._handle_preferences_conversation(event)
                return
            
            # Debug logging
            logger.debug(f"📨 Message received: from={event.sender_id}, chat={event.chat_id}, is_private={event.is_private}, text={event.text[:50] if event.text else 'None'}")
            
            # Skip commands
            if event.text and event.text.startswith('/'):
                logger.debug(f"⏭️ Skipping command: {event.text[:20]}")
                return
            
            # Skip messages from channels/groups (only respond to private)
            if not event.is_private:
                logger.debug(f"⏭️ Skipping non-private message from chat {event.chat_id}")
                return
            
            query = event.text.strip() if event.text else ""
            if not query:
                logger.debug("⏭️ Skipping empty message")
                return
            
            logger.info(f"✅ Processing natural query from {event.sender_id}: {query[:50]}")
            
            # Show typing indicator
            await self.client(SetTypingRequest(
                peer=event.chat_id,
                action=SendMessageTypingAction()
            ))
            
            # Send "analyzing" message
            analyzing_msg = await event.respond(
                "🔍 **מנתח את השאילתה שלך...**\n\n"
                "⏳ מחפש במסד הנתונים...\n"
                "📍 ממיר כתובת...\n"
                "📊 מחשב הערכת סיכון..."
            )
            
            try:
                from src.agents.graph_orchestrator import query_safety_status
                
                # Update analyzing message with progress
                await asyncio.sleep(0.3)
                await analyzing_msg.edit(
                    "🔍 **מנתח את השאילתה שלך...**\n\n"
                    "✅ מחפש במסד הנתונים...\n"
                    "📍 ממיר כתובת...\n"
                    "📊 מחשב הערכת סיכון..."
                )
                
                await asyncio.sleep(0.3)
                await analyzing_msg.edit(
                    "🔍 **מנתח את השאילתה שלך...**\n\n"
                    "✅ מחפש במסד הנתונים...\n"
                    "✅ ממיר כתובת...\n"
                    "📊 מחשב הערכת סיכון..."
                )
                
                result = query_safety_status(query)
                response_text = result.get('response', 'לא הצלחתי להבין את השאילתה שלך.')
                
                # Build response with risk badge
                risk = result.get('risk_assessment')
                location = result.get('location', '')
                
                if risk:
                    score = risk.get('risk_score', 0)
                    badge = self._get_risk_badge(score)
                    full_response = f"{badge}\n\n{response_text}"
                else:
                    full_response = response_text
                
                # Add buttons for follow-up actions
                buttons = [
                    [Button.inline("🔄 רענון", data=f"refresh:{location or query}")],
                    [Button.inline("📰 חדשות אחרונות", data="news"), 
                     Button.inline("📊 סטטיסטיקות", data="stats")]
                ]
                
                # Edit the analyzing message with the full response (NOT sending a new message)
                try:
                    await analyzing_msg.edit(full_response, parse_mode='md', buttons=buttons)
                except Exception as edit_error:
                    # If edit fails (e.g., message too old), send as new message
                    logger.warning(f"Failed to edit message, sending new: {edit_error}")
                    await event.respond(full_response, parse_mode='md', buttons=buttons)
                
            except Exception as e:
                logger.error(f"Query error: {e}")
                # Update analyzing message with error (edit, don't send new)
                try:
                    await analyzing_msg.edit(
                        "❌ **הניתוח נכשל**\n\n"
                        f"שגיאה: {str(e)[:100]}\n\n"
                        "אנא נסה שוב או השתמש ב-`/help` לפקודות זמינות.",
                        buttons=[[Button.inline("🏠 התחל מחדש", data="start")]]
                    )
                except Exception as edit_error:
                    # If edit fails, send as new message
                    logger.warning(f"Failed to edit error message: {edit_error}")
                    await event.respond(
                        f"❌ **הניתוח נכשל**\n\nשגיאה: {str(e)[:100]}",
                        buttons=[[Button.inline("🏠 התחל מחדש", data="start")]]
                    )
        
        @self.client.on(events.CallbackQuery())
        async def callback_handler(event):
            """Handle button clicks."""
            data = event.data.decode('utf-8')
            logger.info(f"Button clicked: {data}")
            
            try:
                if data == "start":
                    # Show start message
                    await event.answer("Returning to start...")
                    await event.respond(
                        "👁️ **The Watch Ready**\n\n"
                        "Send me a location or question about safety in Israel.\n"
                        "Example: מה המצב בלוד?"
                    )
                
                elif data == "news":
                    # Refresh personalized news
                    await event.answer("Refreshing news...")
                    from src.utils.user_preferences import get_preferences_manager
                    from src.agents.graph_orchestrator import get_breaking_news
                    
                    user_id = event.sender_id
                    prefs_manager = get_preferences_manager()
                    
                    news = get_breaking_news(hours=24)
                    all_incidents = news.get('incidents', [])
                    filtered_incidents = prefs_manager.filter_incidents_by_preferences(
                        user_id, all_incidents
                    )
                    
                    # Helper function to remove emojis from text
                    import re
                    def remove_emojis(text: str) -> str:
                        """Remove emoji characters from text."""
                        emoji_pattern = re.compile("["
                            u"\U0001F600-\U0001F64F"  # emoticons
                            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                            u"\U0001F680-\U0001F6FF"  # transport & map symbols
                            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                            u"\U00002702-\U000027B0"
                            u"\U000024C2-\U0001F251"
                            "]+", flags=re.UNICODE)
                        return emoji_pattern.sub('', text).strip()
                    
                    if not filtered_incidents:
                        await event.respond("No incidents in your preferred areas (last 24h).")
                    else:
                        response = f"**Personalized News** (Last 24h)\n"
                        response += f"Found **{len(filtered_incidents)}** incidents\n\n"
                        for i, inc in enumerate(filtered_incidents[:10], 1):
                            city = inc.get('city', 'Unknown')
                            summary = inc.get('summary', '')
                            summary = remove_emojis(summary)
                            # Ensure complete sentence
                            summary = summary.rstrip()
                            if summary and not summary[-1] in ['.', '!', '?', ':', ';']:
                                last_period = summary.rfind('.')
                                if last_period > len(summary) * 0.5:
                                    summary = summary[:last_period + 1]
                            severity = inc.get('severity_score', '?')
                            street = inc.get('street', '')
                            location_str = city
                            if street and street.lower() not in ['unknown', 'לא ידוע', '']:
                                location_str += f", {street}"
                            response += f"{i}. **{location_str}** ({severity}/10)\n   {summary}\n\n"
                        await event.respond(response, parse_mode='md')
                
                elif data == "news_all":
                    # Show all news without filtering
                    await event.answer("Fetching all news...")
                    from src.agents.graph_orchestrator import get_breaking_news
                    
                    news = get_breaking_news(hours=24)
                    incidents = news.get('incidents', [])
                    
                    if not incidents:
                        await event.respond("No incidents in the last 24 hours.")
                    else:
                        # Helper function to remove emojis from text
                        import re
                        def remove_emojis(text: str) -> str:
                            """Remove emoji characters from text."""
                            emoji_pattern = re.compile("["
                                u"\U0001F600-\U0001F64F"  # emoticons
                                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                u"\U00002702-\U000027B0"
                                u"\U000024C2-\U0001F251"
                                "]+", flags=re.UNICODE)
                            return emoji_pattern.sub('', text).strip()
                        
                        response = f"**All News** (Last 24h)\n"
                        response += f"Found **{len(incidents)}** incidents\n\n"
                        for i, inc in enumerate(incidents[:10], 1):
                            city = inc.get('city', 'Unknown')
                            summary = inc.get('summary', '')
                            summary = remove_emojis(summary)
                            # Ensure complete sentence
                            summary = summary.rstrip()
                            if summary and not summary[-1] in ['.', '!', '?', ':', ';']:
                                last_period = summary.rfind('.')
                                if last_period > len(summary) * 0.5:
                                    summary = summary[:last_period + 1]
                            severity = inc.get('severity_score', '?')
                            street = inc.get('street', '')
                            location_str = city
                            if street and street.lower() not in ['unknown', 'לא ידוע', '']:
                                location_str += f", {street}"
                            response += f"{i}. **{location_str}** ({severity}/10)\n   {summary}\n\n"
                        await event.respond(response, parse_mode='md')
                
                elif data == "set_preferences" or data == "start_preferences_conversation":
                    # Start preferences conversation
                    await event.answer("Starting preferences setup...")
                    await self._start_preferences_conversation(event)
                
                elif data == "save_preferences":
                    # Save preferences from conversation state
                    await event.answer("Saving preferences...")
                    from src.utils.user_preferences import get_preferences_manager
                    
                    user_id = event.sender_id
                    
                    if user_id not in self._preferences_conversations:
                        await event.answer("No pending preferences to save.")
                        return
                    
                    conversation = self._preferences_conversations[user_id]
                    prefs_data = conversation.get("pending_preferences")
                    
                    if not prefs_data:
                        await event.answer("No preferences to save.")
                        return
                    
                    try:
                        prefs_manager = get_preferences_manager()
                        
                        prefs_manager.set_preferences(
                            user_id,
                            cities=prefs_data.get('cities'),
                            streets=prefs_data.get('streets'),
                            neighborhoods=prefs_data.get('neighborhoods')
                        )
                        
                        # Clear conversation state
                        del self._preferences_conversations[user_id]
                        
                        cities_str = ', '.join(prefs_data.get('cities', [])) if prefs_data.get('cities') else 'None'
                        streets_str = ', '.join(prefs_data.get('streets', [])) if prefs_data.get('streets') else 'None'
                        neighborhoods_str = ', '.join(prefs_data.get('neighborhoods', [])) if prefs_data.get('neighborhoods') else 'None'
                        
                        await event.respond(
                            "✅ **Preferences Saved!**\n\n"
                            f"🏙️ Cities: {cities_str}\n"
                            f"🛣️ Streets: {streets_str}\n"
                            f"🏘️ Neighborhoods: {neighborhoods_str}\n\n"
                            "Use `/news` to see personalized updates! 📰",
                            parse_mode='md'
                        )
                    except Exception as e:
                        logger.error(f"Error saving preferences: {e}")
                        await event.answer(f"Error: {str(e)}")
                
                elif data == "edit_preferences":
                    # Continue editing preferences
                    await event.answer("Continue editing...")
                    user_id = event.sender_id
                    if user_id in self._preferences_conversations:
                        # Reset to asking stage
                        self._preferences_conversations[user_id]["stage"] = "asking"
                        await event.respond(
                            "✏️ **Let's try again**\n\n"
                            "Tell me which locations you'd like to monitor.\n\n"
                            "**Examples:**\n"
                            "• \"תל אביב וירושלים\"\n"
                            "• \"רחוב הרצל בתל אביב\"\n"
                            "• \"Tel Aviv, Jerusalem, and Haifa\"",
                            parse_mode='md'
                        )
                
                elif data == "cancel_preferences":
                    # Cancel preferences conversation
                    await event.answer("Cancelled")
                    user_id = event.sender_id
                    if user_id in self._preferences_conversations:
                        del self._preferences_conversations[user_id]
                    await event.respond("❌ Preferences setup cancelled.")
                
                elif data == "clear_preferences":
                    # Clear user preferences
                    await event.answer("Clearing preferences...")
                    from src.utils.user_preferences import get_preferences_manager
                    
                    prefs_manager = get_preferences_manager()
                    prefs_manager.clear_preferences(event.sender_id)
                    
                    # Also clear any active conversation
                    user_id = event.sender_id
                    if user_id in self._preferences_conversations:
                        del self._preferences_conversations[user_id]
                    
                    await event.respond(
                        "✅ **Preferences Cleared**\n\n"
                        "Your preferences have been reset. Use `/prefs` to set new ones.",
                        parse_mode='md'
                    )
                
                elif data == "stats":
                    # Show stats
                    await event.answer("Loading statistics...")
                    from src.database.chroma_manager import get_chroma_manager
                    
                    chroma = get_chroma_manager()
                    stats = chroma.get_statistics()
                    
                    response = f"📊 **Database Stats**\n\n"
                    response += f"• Total: **{stats.get('total_incidents', 0)}** incidents\n"
                    response += f"• Avg severity: **{stats.get('avg_severity', 0):.1f}**/10\n"
                    await event.respond(response, parse_mode='md')
                
                elif data.startswith("refresh:"):
                    # Refresh query for location
                    location = data.split(":", 1)[1]
                    await event.answer(f"Refreshing {location}...")
                    
                    from src.agents.graph_orchestrator import query_safety_status
                    result = query_safety_status(f"מה המצב ב{location}?")
                    response_text = result.get('response', 'Could not refresh.')
                    
                    risk = result.get('risk_assessment')
                    if risk:
                        badge = self._get_risk_badge(risk.get('risk_score', 0))
                        response_text = f"{badge}\n\n{response_text}"
                    
                    await event.respond(response_text, parse_mode='md')
                
                else:
                    await event.answer("Unknown action")
                    
            except Exception as e:
                logger.error(f"Callback error: {e}")
                await event.answer(f"Error: {str(e)[:50]}")
    
    def _get_risk_badge(self, score: float) -> str:
        """Get risk level badge emoji."""
        if score >= 9:
            return "🔴 CRITICAL"
        elif score >= 7:
            return "🟠 HIGH"
        elif score >= 5:
            return "🟡 MODERATE"
        elif score >= 3:
            return "🟢 LOW"
        else:
            return "✅ MINIMAL"
    
    def _get_event_emoji(self, event_type: str) -> str:
        """Get emoji for event type."""
        emojis = {
            'shooting': '🔫',
            'stabbing': '🔪',
            'explosion': '💥',
            'arson': '🔥',
            'brawl': '👊',
            'police_activity': '🚔',
            'roadblock': '🚧',
            'accident': '🚗',
            'unknown': '❓'
        }
        return emojis.get(event_type, '❓')
    
    async def _start_preferences_conversation(self, event):
        """Start a conversational preferences setup."""
        user_id = event.sender_id
        
        # Initialize conversation state
        self._preferences_conversations[user_id] = {
            "stage": "asking",
            "messages": []
        }
        
        welcome_msg = (
            "👋 **Let's set up your preferences!**\n\n"
            "I'll help you configure which locations you want to monitor for news.\n\n"
            "**Just tell me naturally, for example:**\n"
            "• \"I want to monitor Tel Aviv and Jerusalem\"\n"
            "• \"רחוב הרצל בתל אביב ושדרות רוטשילד\"\n"
            "• \"תל אביב, ירושלים, וחיפה\"\n"
            "• \"I'm interested in Herzl Street in Tel Aviv and the Old City in Jerusalem\"\n\n"
            "What locations would you like to monitor? 🗺️"
        )
        
        buttons = [
            [Button.inline("❌ Cancel", data="cancel_preferences")]
        ]
        
        await event.respond(welcome_msg, parse_mode='md', buttons=buttons)
    
    async def _handle_preferences_conversation(self, event):
        """Handle ongoing preferences conversation."""
        user_id = event.sender_id
        
        if user_id not in self._preferences_conversations:
            return
        
        conversation = self._preferences_conversations[user_id]
        user_message = event.text.strip()
        
        # Check for cancel keywords
        if user_message.lower() in ['cancel', 'ביטול', 'exit', 'יציאה']:
            del self._preferences_conversations[user_id]
            await event.respond("❌ Preferences setup cancelled.")
            return
        
        # Add user message to conversation
        conversation["messages"].append({"role": "user", "content": user_message})
        
        # Extract preferences using LLM
        extracted = await self._extract_preferences_with_llm(user_message)
        
        if extracted:
            # Show confirmation
            cities_str = ', '.join(extracted.get('cities', [])) if extracted.get('cities') else 'None'
            streets_str = ', '.join(extracted.get('streets', [])) if extracted.get('streets') else 'None'
            neighborhoods_str = ', '.join(extracted.get('neighborhoods', [])) if extracted.get('neighborhoods') else 'None'
            
            confirmation_msg = (
                "✅ **I understood your preferences:**\n\n"
                f"🏙️ **Cities:** {cities_str}\n"
                f"🛣️ **Streets:** {streets_str}\n"
                f"🏘️ **Neighborhoods:** {neighborhoods_str}\n\n"
                "Is this correct? I'll save these preferences."
            )
            
            # Encode preferences data for button (Telegram has 64-byte limit, so we'll store in conversation state)
            conversation["pending_preferences"] = extracted
            buttons = [
                [Button.inline("✅ Yes, Save", data="save_preferences"),
                 Button.inline("✏️ Edit", data="edit_preferences")],
                [Button.inline("❌ Cancel", data="cancel_preferences")]
            ]
            
            conversation["stage"] = "confirming"
            conversation["pending_preferences"] = extracted
            
            await event.respond(confirmation_msg, parse_mode='md', buttons=buttons)
        else:
            # Ask for clarification
            clarification_msg = (
                "🤔 **I need a bit more information**\n\n"
                "Could you tell me which cities, streets, or neighborhoods you'd like to monitor?\n\n"
                "**Examples:**\n"
                "• \"תל אביב וירושלים\"\n"
                "• \"רחוב הרצל בתל אביב\"\n"
                "• \"Tel Aviv, Jerusalem, and Haifa\"\n\n"
                "Or type 'cancel' to stop."
            )
            await event.respond(clarification_msg, parse_mode='md')
    
    async def _extract_preferences_with_llm(self, user_input: str) -> Optional[Dict]:
        """Extract preferences from natural language using LLM."""
        try:
            llm = ChatGoogleGenerativeAI(
                model="gemini-2.0-flash",
                google_api_key=os.getenv("GOOGLE_API_KEY"),
                temperature=0.2
            )
            
            prompt = ChatPromptTemplate.from_template("""
You are a helpful assistant extracting location preferences from user messages.

The user wants to set up preferences for monitoring safety news. Extract cities, streets, and neighborhoods from their message.

USER MESSAGE: {user_input}

Extract locations mentioned. Return ONLY a JSON object with this structure:
{{
    "cities": ["תל אביב", "ירושלים"],  // List of city names (Hebrew or English)
    "streets": ["רחוב הרצל", "שדרות רוטשילד"],  // List of street names
    "neighborhoods": ["שכונת התקווה", "עיר עתיקה"]  // List of neighborhood names
}}

**Guidelines:**
- Extract city names in Hebrew when possible (תל אביב, ירושלים, חיפה, etc.)
- Extract street names as mentioned (רחוב הרצל, שדרות רוטשילד, etc.)
- Extract neighborhood names if mentioned
- If a street is mentioned with a city (e.g., "רחוב הרצל בתל אביב"), extract both
- Return empty arrays [] if nothing found in that category
- Be flexible with language - accept Hebrew, English, or mixed

**Common Israeli cities:** תל אביב, ירושלים, חיפה, באר שבע, נתניה, אשדוד, ראשון לציון, פתח תקווה, נצרת, כפר קאסם, רהט, אום אל-פחם

Return ONLY the JSON object, no additional text.
""")
            
            chain = prompt | llm
            response = chain.invoke({"user_input": user_input})
            
            # Parse JSON
            content = response.content.strip()
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            content = content.strip()
            
            data = json.loads(content)
            
            # Clean and validate
            result = {
                "cities": [c.strip() for c in data.get("cities", []) if c.strip()],
                "streets": [s.strip() for s in data.get("streets", []) if s.strip()],
                "neighborhoods": [n.strip() for n in data.get("neighborhoods", []) if n.strip()]
            }
            
            # Return None if nothing extracted
            if not (result["cities"] or result["streets"] or result["neighborhoods"]):
                return None
            
            return result
            
        except Exception as e:
            logger.error(f"LLM extraction error: {e}")
            return None
    
    async def start(self):
        """Start the bot."""
        import asyncio
        
        logger.info("🤖 Starting The Watch Bot...")
        logger.info(f"📝 Bot token: {self.bot_token[:10]}...{self.bot_token[-5:] if len(self.bot_token) > 15 else '***'}")
        
        # For bots, use start() with bot_token directly
        # This handles authentication automatically
        try:
            await self.client.start(bot_token=self.bot_token)
        except Exception as e:
            logger.error(f"❌ Failed to start bot: {e}")
            raise
        
        me = await self.client.get_me()
        logger.info(f"✅ Bot authenticated: @{me.username} (ID: {me.id})")
        
        # Register handlers AFTER client is started (important for bots)
        if not self._handlers_registered:
            self._register_handlers()
            self._handlers_registered = True
            logger.info("✅ Event handlers registered")
        
        logger.info(f"📡 Bot is listening for messages...")
        logger.info(f"💡 Users can now message @{me.username} to query safety info")
        
        # Run until disconnected
        try:
            await self.client.run_until_disconnected()
        except asyncio.CancelledError:
            logger.info("Bot task cancelled")
        except Exception as e:
            logger.error(f"Bot error: {e}")
            raise
    
    async def stop(self):
        """Stop the bot."""
        logger.info("🛑 Stopping bot...")
        try:
            await self.client.disconnect()
        except Exception as e:
            logger.warning(f"Error during disconnect (safe to ignore): {e}")


async def run_bot():
    """Run The Watch bot."""
    bot = TheWatchBot()
    
    try:
        await bot.start()
    except KeyboardInterrupt:
        pass
    finally:
        await bot.stop()


if __name__ == "__main__":
    import asyncio
    asyncio.run(run_bot())
