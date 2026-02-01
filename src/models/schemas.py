"""
The Watch: Pydantic Schemas & TypedDict State Definitions

This module defines the core data structures for:
- LangGraph state management
- Incident extraction from raw Telegram messages
- User query classification and processing
- Risk assessment outputs
- ChromaDB document storage
"""

from datetime import datetime
from enum import Enum
from typing import TypedDict, List, Optional, Annotated, Sequence
from pydantic import BaseModel, Field, field_validator
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage


class EventType(str, Enum):
    """Classification of safety incident types."""
    SHOOTING = "shooting"
    POLICE_ACTIVITY = "police_activity"
    ROADBLOCK = "roadblock"
    ACCIDENT = "accident"
    BRAWL = "brawl"
    STABBING = "stabbing"
    ARSON = "arson"
    EXPLOSION = "explosion"
    TERRORIST_ATTACK = "terrorist_attack"
    VIOLENT_CRIME = "violent_crime"
    SUSPICIOUS_OBJECT = "suspicious_object"
    UNKNOWN = "unknown"


class QueryIntent(str, Enum):
    """Classification of user query intent."""
    BREAKING_NEWS = "breaking_news"  # User wants latest incidents
    SAFETY_STATUS = "safety_status"  # User wants risk assessment for location
    HISTORICAL = "historical"  # User wants historical analysis
    GENERAL = "general"  # General question about the system


class RiskLevel(str, Enum):
    """Risk level classification."""
    CRITICAL = "critical"  # 9-10
    HIGH = "high"  # 7-8
    MODERATE = "moderate"  # 5-6
    LOW = "low"  # 3-4
    MINIMAL = "minimal"  # 1-2


class ProcessedIncidentDict(TypedDict):
    """Structured incident data for storage."""
    id: str
    raw_text: str
    summary: str
    location_description: str
    city: str
    event_type: str
    severity: int
    latitude: float
    longitude: float
    timestamp: str
    source_channel: str
    message_id: int


class RiskAssessmentDict(TypedDict):
    """Risk assessment result structure."""
    location: str
    latitude: float
    longitude: float
    radius_km: float
    risk_score: float
    risk_level: str
    total_events: int
    events_last_24h: int
    events_last_7d: int
    most_recent_event: Optional[str]
    event_breakdown: dict


class TheWatchState(TypedDict):
    """
    The main state that flows through the LangGraph workflow.
    Similar to AgentState in your tailor-job-agent.
    """
    # Message history for agent reasoning
    messages: Annotated[Sequence[BaseMessage], add_messages]
    
    # Input data
    raw_message: str                      # Raw Telegram message text
    source_channel: str                   # Source channel name/ID
    message_id: int                       # Telegram message ID
    message_timestamp: str                # Original message timestamp
    
    # Extraction results (Module B, Step 1)
    extracted_summary: str
    extracted_location: str
    extracted_city: str
    extracted_event_type: str
    extracted_severity: int
    extraction_confidence: float
    
    # Geocoding results (Module B, Step 2)
    latitude: float
    longitude: float
    formatted_address: str
    geocode_method: str
    geocode_confidence: float
    
    # Storage result
    incident_id: str
    stored_successfully: bool
    
    # User query data (Module C)
    user_query: str
    query_intent: str
    query_location: str
    
    # Risk assessment (Module C, Step 2)
    risk_assessment: Optional[RiskAssessmentDict]
    relevant_incidents: List[dict]
    
    # Final response
    response_text: str
    
    # Workflow control
    current_step: str
    error: Optional[str]


class AnalystState(TypedDict):
    """
    State for the Analyst module (user query handling).
    Separate from processing state for cleaner separation.
    """
    messages: Annotated[Sequence[BaseMessage], add_messages]
    
    # User query
    user_query: str
    query_intent: str
    query_location: str
    query_time_range_days: int
    
    # Geocoded query location
    query_latitude: float
    query_longitude: float
    
    # Retrieved data
    retrieved_incidents: List[dict]
    
    # Risk calculation
    risk_assessment: Optional[RiskAssessmentDict]
    
    # Response
    response_text: str
    
    # Control
    current_step: str
    error: Optional[str]


class ExtractedIncident(BaseModel):
    """
    Structured output from LLM extraction of raw Telegram messages.
    Used with Gemini's structured output parsing.
    """
    summary: str = Field(
        ...,
        description="Brief English summary of the incident (1-2 sentences)",
        min_length=10,
        max_length=500
    )
    location_description: str = Field(
        ...,
        description="Original location text as mentioned in the message (Hebrew/Arabic)",
        min_length=1
    )
    city: str = Field(
        ...,
        description="City or village name (normalized to English transliteration)",
        examples=["Tel Aviv", "Kafr Qasim", "Nazareth", "Tamra", "Baqa al-Gharbiyye"]
    )
    event_type: EventType = Field(
        ...,
        description="Classification of the incident type"
    )
    severity: int = Field(
        ...,
        ge=1,
        le=10,
        description="Severity score: 1=minor disturbance, 5=significant incident, 10=life-threatening active incident"
    )
    confidence: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Confidence in the extraction accuracy (0.0-1.0)"
    )

    @field_validator('severity')
    @classmethod
    def validate_severity(cls, v: int) -> int:
        if not 1 <= v <= 10:
            raise ValueError('Severity must be between 1 and 10')
        return v


class GeocodedLocation(BaseModel):
    """Result of geocoding operation."""
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    formatted_address: Optional[str] = Field(
        default=None,
        description="Google Maps formatted address if available"
    )
    geocode_method: str = Field(
        default="google_geocoding",
        description="Method used: google_geocoding, google_places, or fallback"
    )
    confidence: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Confidence in geocoding accuracy"
    )


class ClassifiedQuery(BaseModel):
    """Result of user query intent classification."""
    original_query: str
    intent: QueryIntent
    location_mentioned: Optional[str] = Field(
        default=None,
        description="Location extracted from query if mentioned"
    )
    time_range_days: int = Field(
        default=30,
        description="Relevant time range in days based on query context"
    )
    confidence: float = Field(default=0.8, ge=0.0, le=1.0)


class RiskAssessment(BaseModel):
    """
    Output of the Risk Engine.
    Contains calculated risk metrics for a location.
    """
    location: str = Field(..., description="Queried location")
    latitude: float
    longitude: float
    radius_km: float = Field(default=2.0)
    
    # Risk metrics
    risk_score: float = Field(..., ge=0.0, le=10.0)
    risk_level: RiskLevel
    
    # Event statistics
    total_events: int = Field(default=0)
    events_last_24h: int = Field(default=0)
    events_last_7d: int = Field(default=0)
    total_severity_sum: int = Field(default=0)
    weighted_severity: float = Field(default=0.0)
    
    # Event breakdown
    event_type_counts: dict = Field(default_factory=dict)
    most_recent_event: Optional[datetime] = None
    
    # Time range
    analysis_start: datetime
    analysis_end: datetime

    @classmethod
    def calculate_risk_level(cls, score: float) -> RiskLevel:
        """Map numeric score to risk level."""
        if score >= 9:
            return RiskLevel.CRITICAL
        elif score >= 7:
            return RiskLevel.HIGH
        elif score >= 5:
            return RiskLevel.MODERATE
        elif score >= 3:
            return RiskLevel.LOW
        else:
            return RiskLevel.MINIMAL

    def to_dict(self) -> RiskAssessmentDict:
        """Convert to TypedDict for state management."""
        return {
            "location": self.location,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "radius_km": self.radius_km,
            "risk_score": self.risk_score,
            "risk_level": self.risk_level.value,
            "total_events": self.total_events,
            "events_last_24h": self.events_last_24h,
            "events_last_7d": self.events_last_7d,
            "most_recent_event": self.most_recent_event.isoformat() if self.most_recent_event else None,
            "event_breakdown": self.event_type_counts
        }


class TelegramMessage(BaseModel):
    """Raw message received from Telegram channel."""
    message_id: int
    channel_id: int
    channel_name: str
    text: str
    timestamp: datetime
    has_media: bool = False
    media_type: Optional[str] = None
    reply_to_message_id: Optional[int] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class TelegramChannelConfig(BaseModel):
    """Configuration for a monitored Telegram channel."""
    channel_id: int
    channel_name: str
    enabled: bool = True
    priority: int = Field(default=1, ge=1, le=5)


class IncidentMetadata(TypedDict):
    """Metadata structure for ChromaDB documents."""
    timestamp: str           # ISO format datetime
    severity_score: int      # 1-10
    event_type: str          # EventType value
    lat: float               # Latitude
    lon: float               # Longitude
    city: str                # City name
    source_channel: str      # Telegram channel
    message_id: int          # Original message ID


def create_incident_metadata(
    timestamp: datetime,
    severity: int,
    event_type: EventType,
    lat: float,
    lon: float,
    city: str,
    source_channel: str,
    message_id: int
) -> IncidentMetadata:
    """Helper to create properly typed metadata."""
    return {
        "timestamp": timestamp.isoformat(),
        "severity_score": severity,
        "event_type": event_type.value if isinstance(event_type, EventType) else event_type,
        "lat": lat,
        "lon": lon,
        "city": city,
        "source_channel": source_channel,
        "message_id": message_id
    }
