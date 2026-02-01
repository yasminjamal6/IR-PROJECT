"""
The Watch: LangGraph Orchestrator

This module defines the LangGraph workflows for:
1. Processing Pipeline: Telegram message → Extract → Geocode → Store
2. Analyst Pipeline: User query → Classify → Retrieve → Calculate Risk → Respond

Uses Google Gemini (gemini-2.0-flash) for LLM operations.
"""

import os
import uuid
from datetime import datetime
from typing import Literal
from dotenv import load_dotenv

from langgraph.graph import StateGraph, END
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from src.models.schemas import (
    TheWatchState,
    AnalystState,
    EventType,
    QueryIntent,
    ExtractedIncident,
    TelegramMessage
)
from src.database.chroma_manager import get_chroma_manager
from src.tools.geocoder import get_geocoder
from src.tools.risk_calculator import get_risk_calculator

load_dotenv()

llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash",
    google_api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.2  # Low temperature for factual extraction
)

creative_llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash",
    google_api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.7  # Higher for natural responses
)


EXTRACT_INCIDENT_PROMPT = ChatPromptTemplate.from_template("""
You are an expert analyst extracting CRIME and SECURITY incident information from Israeli emergency service reports (MDA, United Hatzalah) and news channels.

Analyze this message and extract structured information:

MESSAGE:
{raw_text}

SOURCE: {source_channel}

**IMPORTANT - FILTERING RULES:**
- ✅ INCLUDE: Shootings, stabbings, terrorist attacks, violent crimes, security incidents, suspicious objects, active threats
- ❌ EXCLUDE: Regular car accidents, medical emergencies (heart attacks, births), fires without crime, weather events, international news
- ❌ EXCLUDE: If the message is about events OUTSIDE Israel

If the message should be EXCLUDED, return:
{{"skip": true, "reason": "Not a crime/security incident"}}

Otherwise, extract:
{{
    "skip": false,
    "summary": "Brief Hebrew summary (תקציר קצר)",
    "summary_en": "Brief English summary",
    "location_description": "Original location text exactly as written",
    "street": "Street/road name if mentioned (e.g., 'שדרות הציונות', 'רחוב הרצל', 'כביש 6') or null if not specified",
    "neighborhood": "Neighborhood/area name if mentioned (e.g., 'שכונת התקווה', 'עיר עתיקה') or null if not specified",
    "city": "City name in Hebrew (e.g., 'תל אביב', 'ירושלים', 'חיפה', 'אום אל-פחם')",
    "city_en": "City name in English transliteration",
    "event_type": "One of: shooting, police_activity, terrorist_attack, stabbing, arson, explosion, suspicious_object, violent_crime, unknown",
    "severity": "Integer 1-10 where: 1=minor, 5=significant, 7=serious injuries, 10=active mass-casualty",
    "confidence": "Your confidence in this extraction (0.0-1.0)"
}}

Guidelines:
- Severity: terrorist attacks/mass shootings=9-10, shootings with injuries=7-8, stabbings=6-8, suspicious objects=4-6
- Common cities: Tel Aviv, Jerusalem, Haifa, Beer Sheva, Netanya, Ashdod, Rishon LeZion, Petah Tikva, Nazareth, Tel Aviv, Kafr Qasim, Rahat
- For MDA/Hatzalah messages: focus on crime-related calls, ignore routine medical

Return ONLY the JSON object, no additional text.
""")

CLASSIFY_QUERY_PROMPT = ChatPromptTemplate.from_template("""
Classify this user query about safety/incidents in Israel:

QUERY: {user_query}

Determine:
1. Intent: Is the user asking about:
   - "breaking_news": Latest/current incidents (keywords: now, happening, current, latest, breaking)
   - "safety_status": Safety assessment of a location (keywords: safe, risk, should I go, dangerous)
   - "historical": Past incidents/trends (keywords: history, last month, trend, how many)
   - "general": General question about the system

2. Location: Extract any location/city mentioned (return null if none)

3. Time Range: Relevant time period in days (default 30, use 1 for "today", 7 for "this week")

Return JSON:
{{
    "intent": "breaking_news|safety_status|historical|general",
    "location": "City name or null",
    "time_range_days": integer
}}

Return ONLY the JSON object.
""")

GENERATE_RESPONSE_PROMPT = ChatPromptTemplate.from_template("""
You are The Watch (השומר), a safety intelligence assistant for ALL of Israel.

Generate a response based on the risk assessment data.

USER QUERY: {user_query}
QUERY INTENT: {intent}
LOCATION: {location}

RISK ASSESSMENT:
{risk_summary}

RECENT INCIDENTS:
{incidents_text}

**LANGUAGE RULES:**
- If query is in Hebrew → respond in Hebrew
- If query is in Arabic → respond in Arabic  
- If query is in English → respond in English
- Default to Hebrew if unclear

**RESPONSE GUIDELINES:**
- Be direct but compassionate - these are real safety concerns
- For risk levels, use appropriate warnings:
  * CRITICAL (8+): "🚨 אזהרה קריטית" - Strongly advise avoiding the area
  * HIGH (6-8): "⚠️ אזהרה" - Elevated risk, exercise extreme caution
  * MODERATE (4-6): "⚡ שימו לב" - Notable incidents, stay alert
  * LOW/MINIMAL (<4): Normal conditions, general awareness
- **IMPORTANT: Keep summaries SHORT and AGGREGATED**
- Instead of listing every incident, summarize by type: "Street X had 3 violence crimes and 1 murder in the last 7 days"
- Focus on patterns and trends, not individual incident details
- If there are ANY violent incidents in the last 7 days, clearly state the area has recent safety concerns
- Mention data sources (MDA, איחוד הצלה) for credibility
- End with practical safety advice
- Keep response concise - maximum 3-4 sentences for the summary

Generate response:
""")


def extract_incident_node(state: TheWatchState) -> dict:
    """
    Node 1: Extract structured incident data from raw Telegram message.
    Uses LLM to parse Hebrew/Arabic text.
    """
    raw_text = state.get("raw_message", "")
    source_channel = state.get("source_channel", "unknown")
    
    if not raw_text:
        return {
            "error": "No message text provided",
            "current_step": "error"
        }
    
    try:
        # Use LLM to extract
        chain = EXTRACT_INCIDENT_PROMPT | llm
        response = chain.invoke({
            "raw_text": raw_text,
            "source_channel": source_channel
        })
        
        # Parse JSON response
        import json
        content = response.content.strip()
        # Remove markdown code blocks if present
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        content = content.strip()
        
        data = json.loads(content)
        
        # Check if message should be skipped (not crime-related)
        if data.get("skip", False):
            return {
                "error": f"Skipped: {data.get('reason', 'Not relevant')}",
                "current_step": "skipped",
                "stored_successfully": False
            }
        
        # Map event type
        event_type_str = data.get("event_type", "unknown").lower().replace(" ", "_")
        
        # Use Hebrew summary if available, fallback to English
        summary = data.get("summary", data.get("summary_en", ""))
        city = data.get("city", data.get("city_en", ""))
        street = data.get("street") or ""
        neighborhood = data.get("neighborhood") or ""
        
        return {
            "extracted_summary": summary,
            "extracted_location": data.get("location_description", ""),
            "extracted_city": city,
            "extracted_street": street,
            "extracted_neighborhood": neighborhood,
            "extracted_event_type": event_type_str,
            "extracted_severity": int(data.get("severity", 5)),
            "extraction_confidence": float(data.get("confidence", 0.8)),
            "current_step": "extracted",
            "error": None
        }
        
    except json.JSONDecodeError as e:
        return {
            "error": f"Failed to parse LLM response: {str(e)}",
            "current_step": "error"
        }
    except Exception as e:
        return {
            "error": f"Extraction failed: {str(e)}",
            "current_step": "error"
        }


def geocode_incident_node(state: TheWatchState) -> dict:
    """
    Node 2: Geocode the extracted location.
    Tries Google Geocoding, then Places, then fallback database.
    Uses street+city for more precise coordinates.
    """
    location_desc = state.get("extracted_location", "")
    city = state.get("extracted_city", "")
    street = state.get("extracted_street", "")
    neighborhood = state.get("extracted_neighborhood", "")
    
    if not location_desc and not city:
        return {
            "error": "No location to geocode",
            "current_step": "error"
        }
    
    # Build precise address: street + neighborhood + city
    address_parts = []
    if street:
        address_parts.append(street)
    if neighborhood:
        address_parts.append(neighborhood)
    if city:
        address_parts.append(city)
    
    # Use full address if available, otherwise fall back to location_desc
    geocode_query = ", ".join(address_parts) if address_parts else location_desc
    
    try:
        geocoder = get_geocoder()
        result = geocoder.geocode(geocode_query, city)
        
        return {
            "latitude": result.latitude,
            "longitude": result.longitude,
            "formatted_address": result.formatted_address or "",
            "geocode_method": result.geocode_method,
            "geocode_confidence": result.confidence,
            "current_step": "geocoded",
            "error": None
        }
        
    except Exception as e:
        return {
            "error": f"Geocoding failed: {str(e)}",
            "current_step": "error"
        }


def store_incident_node(state: TheWatchState) -> dict:
    """
    Node 3: Store the processed incident in ChromaDB.
    """
    # Validate required fields
    if not state.get("extracted_summary"):
        return {"error": "No summary to store", "current_step": "error"}
    
    if state.get("latitude", 0) == 0 and state.get("longitude", 0) == 0:
        return {"error": "No valid coordinates", "current_step": "error"}
    
    try:
        chroma = get_chroma_manager()
        
        # Check for exact duplicate (same message_id + source_channel)
        message_id = state.get("message_id", 0)
        source_channel = state.get("source_channel", "unknown")
        
        if chroma.check_duplicate(message_id, source_channel):
            return {
                "stored_successfully": False,
                "error": "Duplicate incident - already stored",
                "current_step": "duplicate"
            }
        
        # Parse timestamp
        timestamp_str = state.get("message_timestamp", "")
        try:
            timestamp = datetime.fromisoformat(timestamp_str) if timestamp_str else datetime.utcnow()
        except:
            timestamp = datetime.utcnow()
        
        # Check for similar incidents from different channels (semantic similarity)
        event_type = EventType(state.get("extracted_event_type", "unknown"))
        similar_incident_id = chroma.check_similar_incident(
            summary=state["extracted_summary"],
            city=state.get("extracted_city", "Unknown"),
            lat=state["latitude"],
            lon=state["longitude"],
            event_type=event_type,
            timestamp=timestamp,
            street=state.get("extracted_street", ""),
            time_window_hours=6,  # Check within 6 hours
            embedding_distance_threshold=0.4,  # Embedding distance threshold (lower = more similar)
            distance_threshold_km=2.0  # Within 2km
        )
        
        if similar_incident_id:
            # Similar incident already exists - skip storing this duplicate
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"Skipping duplicate incident (similar to {similar_incident_id}): "
                       f"{state.get('extracted_city')} - {state.get('extracted_summary', '')[:50]}")
            return {
                "stored_successfully": False,
                "error": f"Similar incident already exists (ID: {similar_incident_id})",
                "current_step": "duplicate",
                "similar_incident_id": similar_incident_id
            }
        
        # Store incident with street and neighborhood
        incident_id = chroma.store_incident(
            summary=state["extracted_summary"],
            raw_text=state.get("raw_message", ""),
            timestamp=timestamp,
            severity=state.get("extracted_severity", 5),
            event_type=event_type,
            lat=state["latitude"],
            lon=state["longitude"],
            city=state.get("extracted_city", "Unknown"),
            source_channel=source_channel,
            message_id=message_id,
            street=state.get("extracted_street", ""),
            neighborhood=state.get("extracted_neighborhood", "")
        )
        
        return {
            "incident_id": incident_id,
            "stored_successfully": True,
            "current_step": "stored",
            "error": None
        }
        
    except Exception as e:
        return {
            "error": f"Storage failed: {str(e)}",
            "stored_successfully": False,
            "current_step": "error"
        }


def classify_query_node(state: AnalystState) -> dict:
    """
    Node 1: Classify user query intent and extract location.
    """
    user_query = state.get("user_query", "")
    
    if not user_query:
        return {"error": "No query provided", "current_step": "error"}
    
    try:
        chain = CLASSIFY_QUERY_PROMPT | llm
        response = chain.invoke({"user_query": user_query})
        
        # Parse JSON
        import json
        content = response.content.strip()
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        content = content.strip()
        
        data = json.loads(content)
        
        intent_str = data.get("intent", "general")
        try:
            intent = QueryIntent(intent_str)
        except:
            intent = QueryIntent.GENERAL
        
        # For safety queries, use minimum 7 days to catch recent incidents
        time_range = int(data.get("time_range_days", 30))
        if intent in [QueryIntent.BREAKING_NEWS, QueryIntent.SAFETY_STATUS]:
            time_range = max(time_range, 7)  # Minimum 7 days for safety-related queries
        
        return {
            "query_intent": intent.value,
            "query_location": data.get("location") or "",
            "query_time_range_days": time_range,
            "current_step": "classified",
            "error": None
        }
        
    except Exception as e:
        return {
            "query_intent": "general",
            "query_location": "",
            "query_time_range_days": 30,
            "error": f"Classification failed: {str(e)}",
            "current_step": "classified"
        }


def geocode_query_node(state: AnalystState) -> dict:
    """
    Node 2: Geocode the location from user query.
    """
    location = state.get("query_location", "")
    
    if not location:
        return {
            "query_latitude": 0.0,
            "query_longitude": 0.0,
            "current_step": "geocoded_query",
            "error": None
        }
    
    try:
        geocoder = get_geocoder()
        result = geocoder.geocode(location)
        
        return {
            "query_latitude": result.latitude,
            "query_longitude": result.longitude,
            "current_step": "geocoded_query",
            "error": None
        }
        
    except Exception as e:
        return {
            "query_latitude": 0.0,
            "query_longitude": 0.0,
            "current_step": "geocoded_query",
            "error": f"Query geocoding failed: {str(e)}"
        }


def retrieve_incidents_node(state: AnalystState) -> dict:
    """
    Node 3: Retrieve relevant incidents from ChromaDB.
    """
    lat = state.get("query_latitude", 0)
    lon = state.get("query_longitude", 0)
    days = state.get("query_time_range_days", 30)
    intent = state.get("query_intent", "general")
    
    try:
        chroma = get_chroma_manager()
        
        if lat != 0 and lon != 0:
            # Location-based retrieval
            incidents = chroma.get_incidents_in_area(
                center_lat=lat,
                center_lon=lon,
                radius_km=2.0,
                days=days,
                limit=50
            )
        elif intent == "breaking_news":
            # Get recent incidents everywhere
            incidents = chroma.get_incidents_by_time(hours=24)
        else:
            # General retrieval
            incidents = chroma.get_incidents_by_time(hours=days * 24)
        
        return {
            "retrieved_incidents": incidents,
            "current_step": "retrieved",
            "error": None
        }
        
    except Exception as e:
        return {
            "retrieved_incidents": [],
            "error": f"Retrieval failed: {str(e)}",
            "current_step": "error"
        }


def calculate_risk_node(state: AnalystState) -> dict:
    """
    Node 4: Calculate risk score for the queried location.
    """
    incidents = state.get("retrieved_incidents", [])
    lat = state.get("query_latitude", 0)
    lon = state.get("query_longitude", 0)
    
    if lat == 0 and lon == 0:
        # No specific location - skip risk calculation
        return {
            "risk_assessment": None,
            "current_step": "risk_calculated",
            "error": None
        }
    
    try:
        calculator = get_risk_calculator()
        assessment = calculator.calculate_risk(
            incidents=incidents,
            center_lat=lat,
            center_lon=lon,
            radius_km=2.0
        )
        
        return {
            "risk_assessment": assessment.to_dict(),
            "current_step": "risk_calculated",
            "error": None
        }
        
    except Exception as e:
        return {
            "risk_assessment": None,
            "error": f"Risk calculation failed: {str(e)}",
            "current_step": "error"
        }


def generate_response_node(state: AnalystState) -> dict:
    """
    Node 5: Generate natural language response for the user.
    """
    user_query = state.get("user_query", "")
    intent = state.get("query_intent", "general")
    location = state.get("query_location", "Unknown location")
    risk_assessment = state.get("risk_assessment")
    incidents = state.get("retrieved_incidents", [])
    
    try:
        # Format risk summary
        if risk_assessment:
            calculator = get_risk_calculator()
            from src.models.schemas import RiskAssessment as RA, RiskLevel
            
            risk_summary = f"""
Risk Score: {risk_assessment.get('risk_score', 0)}/10
Risk Level: {risk_assessment.get('risk_level', 'unknown').upper()}
Total Incidents (30 days): {risk_assessment.get('total_events', 0)}
Last 24 Hours: {risk_assessment.get('events_last_24h', 0)}
Last 7 Days: {risk_assessment.get('events_last_7d', 0)}
"""
        else:
            risk_summary = "No specific location risk assessment available."
        
        # Format incidents - aggregate by type and location instead of listing each one
        if incidents:
            # Group incidents by event type and location
            from collections import defaultdict
            grouped = defaultdict(lambda: {'count': 0, 'max_severity': 0, 'locations': set(), 'recent_days': []})
            
            for inc in incidents:
                event_type = inc.get('event_type', 'unknown')
                city = inc.get('city', 'Unknown')
                street = inc.get('street', '')
                severity = inc.get('severity_score', 0)
                
                # Skip Unknown cities
                if not city or city.lower() in ['unknown', 'לא ידוע']:
                    continue
                
                # Build location key
                if street and street.lower() not in ['unknown', 'לא ידוע', '']:
                    location_key = f"{street}, {city}"
                else:
                    location_key = city
                
                grouped[event_type]['count'] += 1
                grouped[event_type]['max_severity'] = max(grouped[event_type]['max_severity'], severity)
                grouped[event_type]['locations'].add(location_key)
            
            # Format aggregated summary
            incident_summaries = []
            for event_type, data in grouped.items():
                count = data['count']
                max_sev = data['max_severity']
                locations = list(data['locations'])[:3]  # Limit to 3 locations
                
                if count == 1:
                    summary = f"{event_type} incident"
                else:
                    summary = f"{count} {event_type} incidents"
                
                if locations:
                    loc_str = ", ".join(locations)
                    summary += f" in {loc_str}"
                
                if max_sev >= 7:
                    summary += f" (max severity: {max_sev}/10)"
                
                incident_summaries.append(f"- {summary}")
            
            incidents_text = "\n".join(incident_summaries) if incident_summaries else "No recent incidents found."
        else:
            incidents_text = "No recent incidents found."
        
        # Generate response
        chain = GENERATE_RESPONSE_PROMPT | creative_llm
        response = chain.invoke({
            "user_query": user_query,
            "intent": intent,
            "location": location or "the area",
            "risk_summary": risk_summary,
            "incidents_text": incidents_text
        })
        
        return {
            "response_text": response.content,
            "current_step": "complete",
            "error": None
        }
        
    except Exception as e:
        # Fallback response
        return {
            "response_text": f"I apologize, but I encountered an error processing your query. "
                            f"Please try rephrasing your question. Error: {str(e)}",
            "current_step": "error",
            "error": str(e)
        }


def should_continue_processing(state: TheWatchState) -> Literal["continue", "end"]:
    """Router for processing pipeline."""
    if state.get("error"):
        return "end"
    return "continue"


def should_continue_analysis(state: AnalystState) -> Literal["continue", "end"]:
    """Router for analyst pipeline."""
    if state.get("error"):
        return "end"
    return "continue"


def create_processing_graph():
    """
    Create the incident processing pipeline.
    
    Flow: Extract → Geocode → Store
    """
    workflow = StateGraph(TheWatchState)
    
    # Add nodes
    workflow.add_node("extract", extract_incident_node)
    workflow.add_node("geocode", geocode_incident_node)
    workflow.add_node("store", store_incident_node)
    
    # Set entry point
    workflow.set_entry_point("extract")
    
    # Define edges
    workflow.add_edge("extract", "geocode")
    workflow.add_edge("geocode", "store")
    workflow.add_edge("store", END)
    
    return workflow.compile()


def create_analyst_graph():
    """
    Create the user query analysis pipeline.
    
    Flow: Classify → Geocode Query → Retrieve → Calculate Risk → Respond
    """
    workflow = StateGraph(AnalystState)
    
    # Add nodes
    workflow.add_node("classify", classify_query_node)
    workflow.add_node("geocode_query", geocode_query_node)
    workflow.add_node("retrieve", retrieve_incidents_node)
    workflow.add_node("calculate_risk", calculate_risk_node)
    workflow.add_node("generate_response", generate_response_node)
    
    # Set entry point
    workflow.set_entry_point("classify")
    
    # Define edges
    workflow.add_edge("classify", "geocode_query")
    workflow.add_edge("geocode_query", "retrieve")
    workflow.add_edge("retrieve", "calculate_risk")
    workflow.add_edge("calculate_risk", "generate_response")
    workflow.add_edge("generate_response", END)
    
    return workflow.compile()


processing_pipeline = create_processing_graph()
analyst_pipeline = create_analyst_graph()


def process_telegram_message(
    message: TelegramMessage
) -> dict:
    """
    Process a raw Telegram message through the full pipeline.
    
    Args:
        message: TelegramMessage object
        
    Returns:
        dict with processing results
    """
    initial_state = {
        "messages": [],
        "raw_message": message.text,
        "source_channel": message.channel_name,
        "message_id": message.message_id,
        "message_timestamp": message.timestamp.isoformat(),
        "extracted_summary": "",
        "extracted_location": "",
        "extracted_city": "",
        "extracted_event_type": "",
        "extracted_severity": 0,
        "extraction_confidence": 0.0,
        "latitude": 0.0,
        "longitude": 0.0,
        "formatted_address": "",
        "geocode_method": "",
        "geocode_confidence": 0.0,
        "incident_id": "",
        "stored_successfully": False,
        "user_query": "",
        "query_intent": "",
        "query_location": "",
        "risk_assessment": None,
        "relevant_incidents": [],
        "response_text": "",
        "current_step": "starting",
        "error": None
    }
    
    # Run the pipeline
    final_state = processing_pipeline.invoke(initial_state)
    
    return {
        "success": final_state.get("stored_successfully", False),
        "incident_id": final_state.get("incident_id", ""),
        "summary": final_state.get("extracted_summary", ""),
        "city": final_state.get("extracted_city", ""),
        "event_type": final_state.get("extracted_event_type", ""),
        "severity": final_state.get("extracted_severity", 0),
        "coordinates": (final_state.get("latitude", 0), final_state.get("longitude", 0)),
        "error": final_state.get("error")
    }


def query_safety_status(user_query: str) -> dict:
    """
    Process a user query about safety.
    
    Args:
        user_query: Natural language query
        
    Returns:
        dict with response and analysis data
    """
    initial_state = {
        "messages": [],
        "user_query": user_query,
        "query_intent": "",
        "query_location": "",
        "query_time_range_days": 30,
        "query_latitude": 0.0,
        "query_longitude": 0.0,
        "retrieved_incidents": [],
        "risk_assessment": None,
        "response_text": "",
        "current_step": "starting",
        "error": None
    }
    
    # Run the pipeline
    final_state = analyst_pipeline.invoke(initial_state)
    
    return {
        "response": final_state.get("response_text", ""),
        "intent": final_state.get("query_intent", ""),
        "location": final_state.get("query_location", ""),
        "risk_assessment": final_state.get("risk_assessment"),
        "incident_count": len(final_state.get("retrieved_incidents", [])),
        "error": final_state.get("error")
    }


def get_breaking_news(hours: int = 24) -> dict:
    """
    Get breaking news / recent incidents.
    
    Args:
        hours: Time window
        
    Returns:
        dict with recent incidents
    """
    try:
        chroma = get_chroma_manager()
        incidents = chroma.get_incidents_by_time(hours=hours)
        
        return {
            "incidents": incidents,
            "count": len(incidents),
            "time_window_hours": hours,
            "error": None
        }
        
    except Exception as e:
        return {
            "incidents": [],
            "count": 0,
            "error": str(e)
        }
