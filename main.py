"""
The Watch - Main Entry Point

A real-time autonomous agent that monitors safety incidents in Israel.

Usage:
    # Run the full system (listener + processor)
    python main.py
    
    # Run listener only (for testing Telegram connection)
    python main.py --listener-only
    
    # Run analyst CLI (interactive query mode)
    python main.py --analyst-cli
    
    # Test processing pipeline with sample data
    python main.py --test-pipeline
"""

import asyncio
import argparse
import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.table import Table

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("the-watch.main")

console = Console()


def print_banner():
    """Print The Watch banner."""
    banner_text = """
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║   👁️  THE WATCH - Real-time Safety Intelligence                  ║
║                                                                  ║
║   Monitoring • Processing • Analyzing                            ║
║                                                                  ║
║   Powered by: Google Gemini • LangGraph • ChromaDB              ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
"""
    console.print(banner_text, style="cyan")


def check_environment():
    """Check required environment variables."""
    required_vars = {
        "GOOGLE_API_KEY": "Required for Gemini LLM and Geocoding"
    }
    
    optional_vars = {
        "TELEGRAM_API_ID": "Required for Telegram listener",
        "TELEGRAM_API_HASH": "Required for Telegram listener"
    }
    
    missing_required = []
    missing_optional = []
    
    for var, desc in required_vars.items():
        if not os.getenv(var):
            missing_required.append((var, desc))
    
    for var, desc in optional_vars.items():
        if not os.getenv(var):
            missing_optional.append((var, desc))
    
    if missing_required:
        console.print("\n[red]❌ Missing REQUIRED environment variables:[/red]")
        for var, desc in missing_required:
            console.print(f"   • {var}: {desc}")
        console.print("\n[dim]Please set these in your .env file[/dim]")
        return False
    
    if missing_optional:
        console.print("\n[yellow]⚠️  Missing optional environment variables:[/yellow]")
        for var, desc in missing_optional:
            console.print(f"   • {var}: {desc}")
    
    console.print("\n[green]✅ Environment configured correctly[/green]")
    return True


async def run_listener_only():
    """Run only the Telegram listener (Module A) for testing."""
    from src.agents.listener_agent import TelegramListener, DEFAULT_CHANNELS
    from src.agents.graph_orchestrator import process_telegram_message
    from src.models.schemas import TelegramMessage
    
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    
    if not api_id or not api_hash:
        console.print("[red]❌ Missing Telegram credentials![/red]")
        console.print("   Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env")
        sys.exit(1)
    
    # Handler that processes messages through the pipeline
    async def message_handler(msg: TelegramMessage):
        console.print(f"\n[cyan]📨 Processing message from {msg.channel_name}...[/cyan]")
        console.print(f"[dim]Text: {msg.text[:200]}...[/dim]" if len(msg.text) > 200 else f"[dim]Text: {msg.text}[/dim]")
        
        # Process through pipeline
        result = process_telegram_message(msg)
        
        if result["success"]:
            console.print(f"[green]✅ Stored incident: {result['incident_id']}[/green]")
            console.print(f"   Summary: {result['summary']}")
            console.print(f"   City: {result['city']}")
            console.print(f"   Type: {result['event_type']}")
            console.print(f"   Severity: {result['severity']}/10")
        else:
            console.print(f"[red]❌ Processing failed: {result.get('error', 'Unknown error')}[/red]")
    
    listener = TelegramListener(
        api_id=int(api_id),
        api_hash=api_hash,
        session_name="the_watch_session",
        message_handler=message_handler
    )
    
    # Add configured channels
    listener.add_channels(DEFAULT_CHANNELS)
    
    console.print("[cyan]🎧 Starting Listener Mode...[/cyan]")
    console.print("[dim]Messages will be processed and stored. Press Ctrl+C to stop.[/dim]\n")
    
    try:
        await listener.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        await listener.stop()


async def run_analyst_cli():
    """Run the analyst CLI for testing queries (Module C)."""
    from src.agents.graph_orchestrator import query_safety_status, get_breaking_news
    from src.database.chroma_manager import get_chroma_manager
    
    console.print("[cyan]🔍 Starting Analyst CLI Mode...[/cyan]")
    console.print("[dim]Type your safety questions. Commands: /stats, /news, /quit[/dim]\n")
    
    while True:
        try:
            query = console.input("\n[bold cyan]🛡️ Ask about safety > [/bold cyan]")
            query = query.strip()
            
            if not query:
                continue
            
            if query.lower() in ['/quit', '/exit', '/q', 'exit', 'quit']:
                break
            
            if query.lower() == '/stats':
                # Show database statistics
                chroma = get_chroma_manager()
                stats = chroma.get_statistics()
                
                table = Table(title="📊 Database Statistics")
                table.add_column("Metric", style="cyan")
                table.add_column("Value", style="green")
                
                table.add_row("Total Incidents", str(stats.get("total_incidents", 0)))
                table.add_row("Avg Severity", str(stats.get("avg_severity", 0)))
                table.add_row("Max Severity", str(stats.get("max_severity", 0)))
                
                if stats.get("incidents_by_city"):
                    cities = ", ".join(f"{c}: {n}" for c, n in list(stats["incidents_by_city"].items())[:5])
                    table.add_row("Top Cities", cities)
                
                console.print(table)
                continue
            
            if query.lower() == '/news':
                # Show breaking news
                console.print("\n[yellow]📰 Breaking News (Last 24 Hours):[/yellow]")
                news = get_breaking_news(hours=24)
                
                if news["incidents"]:
                    for inc in news["incidents"][:10]:
                        console.print(f"  • [{inc.get('event_type', '?')}] {inc.get('summary', 'No summary')}")
                        console.print(f"    [dim]{inc.get('city', 'Unknown')} - Severity: {inc.get('severity_score', '?')}/10[/dim]")
                else:
                    console.print("  [dim]No recent incidents found[/dim]")
                continue
            
            # Process safety query
            console.print("\n[dim]Analyzing...[/dim]")
            result = query_safety_status(query)
            
            if result.get("error"):
                console.print(f"[red]Error: {result['error']}[/red]")
            else:
                console.print(f"\n{result['response']}")
                
                if result.get("risk_assessment"):
                    ra = result["risk_assessment"]
                    console.print(f"\n[dim]📍 Location: {result.get('location', 'Unknown')}[/dim]")
                    console.print(f"[dim]📊 Incidents analyzed: {result.get('incident_count', 0)}[/dim]")
            
        except (KeyboardInterrupt, EOFError):
            break
    
    console.print("\n[cyan]👋 Analyst CLI closed.[/cyan]")


async def test_pipeline():
    """Test the processing pipeline with sample data."""
    from src.agents.graph_orchestrator import process_telegram_message, query_safety_status
    from src.models.schemas import TelegramMessage
    from datetime import datetime
    
    console.print("[cyan]🧪 Testing Processing Pipeline...[/cyan]\n")
    
    # Sample messages
    test_messages = [
        TelegramMessage(
            message_id=1001,
            channel_id=-1001234567890,
            channel_name="Test Channel",
            text="🚨 דיווח על ירי באום אל פחם ליד המאפייה הישנה. כוחות משטרה במקום.",
            timestamp=datetime.utcnow(),
            has_media=False
        ),
        TelegramMessage(
            message_id=1002,
            channel_id=-1001234567890,
            channel_name="Test Channel",
            text="מחסום משטרתי בכניסה לכפר קאסם. עיכובים בתנועה.",
            timestamp=datetime.utcnow(),
            has_media=False
        ),
        TelegramMessage(
            message_id=1003,
            channel_id=-1001234567890,
            channel_name="Test Channel",
            text="תאונת דרכים ליד הכניסה לטמרה. פצועים קל. כוחות הצלה במקום.",
            timestamp=datetime.utcnow(),
            has_media=False
        )
    ]
    
    # Process each message
    for i, msg in enumerate(test_messages, 1):
        console.print(f"[yellow]Test {i}:[/yellow] {msg.text[:80]}...")
        result = process_telegram_message(msg)
        
        if result["success"]:
            console.print(f"  [green]✅ Processed:[/green]")
            console.print(f"     Summary: {result['summary']}")
            console.print(f"     City: {result['city']}")
            console.print(f"     Type: {result['event_type']}")
            console.print(f"     Severity: {result['severity']}/10")
            console.print(f"     Coordinates: {result['coordinates']}")
        else:
            console.print(f"  [red]❌ Failed: {result.get('error', 'Unknown')}[/red]")
        console.print()
    
    # Test queries
    console.print("\n[cyan]🧪 Testing Query Pipeline...[/cyan]\n")
    
    test_queries = [
        "Is Tel Aviv safe right now?",
        "What happened in Kafr Qasim today?",
        "Show me recent incidents in the Triangle region"
    ]
    
    for query in test_queries:
        console.print(f"[yellow]Query:[/yellow] {query}")
        result = query_safety_status(query)
        
        console.print(f"[green]Response:[/green]")
        console.print(f"  {result['response'][:500]}..." if len(result.get('response', '')) > 500 else f"  {result.get('response', 'No response')}")
        console.print()
    
    console.print("[green]✅ Pipeline tests complete![/green]")


async def run_telegram_bot():
    """Run the Telegram bot interface."""
    from src.agents.telegram_bot import TheWatchBot
    
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    
    if not bot_token:
        console.print("[red]❌ Missing TELEGRAM_BOT_TOKEN![/red]")
        console.print("   1. Create a bot via @BotFather on Telegram")
        console.print("   2. Add TELEGRAM_BOT_TOKEN=your_token to .env")
        sys.exit(1)
    
    console.print("[cyan]🤖 Starting The Watch Telegram Bot...[/cyan]")
    console.print("[dim]Users can now message your bot to query safety info.[/dim]\n")
    
    bot = TheWatchBot(bot_token=bot_token)
    
    try:
        await bot.start()
    except KeyboardInterrupt:
        pass
    finally:
        await bot.stop()


async def run_full_system():
    """Run the complete The Watch system."""
    from src.agents.listener_agent import TelegramListener, DEFAULT_CHANNELS
    from src.agents.graph_orchestrator import process_telegram_message
    from src.models.schemas import TelegramMessage
    
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    
    if not api_id or not api_hash:
        console.print("[yellow]⚠️  Telegram credentials not set. Running in demo mode.[/yellow]")
        console.print("[dim]Set TELEGRAM_API_ID and TELEGRAM_API_HASH for full functionality.[/dim]\n")
        
        # Demo mode - just run analyst CLI
        await run_analyst_cli()
        return
    
    console.print("[cyan]🚀 Starting Full System...[/cyan]")
    console.print("[dim]Listener active. Use /stats, /news in another terminal to query.[/dim]\n")
    
    # Handler for processing messages
    async def message_handler(msg: TelegramMessage):
        logger.info(f"Processing message {msg.message_id} from {msg.channel_name}")
        result = process_telegram_message(msg)
        
        if result["success"]:
            logger.info(f"Stored: {result['summary'][:50]}... [{result['event_type']}]")
        else:
            logger.warning(f"Failed to process message: {result.get('error')}")
    
    listener = TelegramListener(
        api_id=int(api_id),
        api_hash=api_hash,
        session_name="the_watch_session",
        message_handler=message_handler
    )
    
    listener.add_channels(DEFAULT_CHANNELS)
    
    try:
        await listener.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        await listener.stop()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="The Watch - Real-time Safety Intelligence System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                    # Run full system
  python main.py --listener-only    # Test Telegram connection
  python main.py --analyst-cli      # Interactive query mode
  python main.py --test-pipeline    # Test with sample data
        """
    )
    parser.add_argument(
        "--listener-only",
        action="store_true",
        help="Run only the Telegram listener"
    )
    parser.add_argument(
        "--analyst-cli",
        action="store_true",
        help="Run interactive analyst CLI"
    )
    parser.add_argument(
        "--test-pipeline",
        action="store_true",
        help="Test the pipeline with sample data"
    )
    parser.add_argument(
        "--bot",
        action="store_true",
        help="Run the Telegram bot interface"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    print_banner()
    
    if not check_environment():
        sys.exit(1)
    
    if args.test_pipeline:
        asyncio.run(test_pipeline())
    elif args.listener_only:
        asyncio.run(run_listener_only())
    elif args.analyst_cli:
        asyncio.run(run_analyst_cli())
    elif args.bot:
        asyncio.run(run_telegram_bot())
    else:
        asyncio.run(run_full_system())


if __name__ == "__main__":
    main()
