"""
The Watch: ChromaDB Vector Store Manager

Handles all vector database operations for incident storage and retrieval.
Uses Google Embeddings (models/embedding-001) for consistency with your existing project.
"""

import os
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dotenv import load_dotenv

load_dotenv()

from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_chroma import Chroma
from langchain_core.documents import Document

from src.models.schemas import (
    EventType,
    IncidentMetadata,
    create_incident_metadata
)

# Initialize embeddings (same as your existing project)
embeddings = GoogleGenerativeAIEmbeddings(
    model="models/embedding-001",
    google_api_key=os.getenv("GOOGLE_API_KEY")
)

# Persistence directory
PERSIST_DIR = os.getenv(
    "CHROMA_PERSIST_DIRECTORY",
    os.path.join(os.path.dirname(__file__), "../../data/chroma_db")
)
COLLECTION_NAME = os.getenv("CHROMA_COLLECTION_NAME", "the_watch_incidents")


class ChromaManager:
    """
    Manages ChromaDB operations for The Watch incident storage.
    
    Supports:
    - Storing processed incidents with rich metadata
    - Semantic search for similar incidents
    - Geospatial filtering (via metadata)
    - Time-based filtering
    - Event type filtering
    """
    
    def __init__(self, persist_directory: str = None, collection_name: str = None):
        """
        Initialize ChromaDB vector store.
        
        Args:
            persist_directory: Path to store the database
            collection_name: Name of the collection
        """
        self.persist_directory = persist_directory or PERSIST_DIR
        self.collection_name = collection_name or COLLECTION_NAME
        
        # Ensure directory exists
        os.makedirs(self.persist_directory, exist_ok=True)
        
        # Initialize vector store
        self.vectorstore = Chroma(
            collection_name=self.collection_name,
            embedding_function=embeddings,
            persist_directory=self.persist_directory
        )
        
        print(f"✅ ChromaManager initialized: {self.persist_directory}/{self.collection_name}")
    
    def store_incident(
        self,
        summary: str,
        raw_text: str,
        timestamp: datetime,
        severity: int,
        event_type: EventType,
        lat: float,
        lon: float,
        city: str,
        source_channel: str,
        message_id: int,
        incident_id: str = None,
        street: str = None,
        neighborhood: str = None
    ) -> str:
        """
        Store a processed incident in the vector database.
        
        Args:
            summary: English summary of the incident (this gets embedded)
            raw_text: Original message text
            timestamp: When the incident occurred
            severity: Severity score 1-10
            event_type: Type of incident
            lat: Latitude
            lon: Longitude
            city: City name
            source_channel: Telegram channel
            message_id: Original message ID
            incident_id: Optional custom ID
            street: Street name (optional)
            neighborhood: Neighborhood name (optional)
            
        Returns:
            The incident ID
        """
        incident_id = incident_id or str(uuid.uuid4())
        
        # Create metadata
        metadata = create_incident_metadata(
            timestamp=timestamp,
            severity=severity,
            event_type=event_type,
            lat=lat,
            lon=lon,
            city=city,
            source_channel=source_channel,
            message_id=message_id
        )
        
        # Add raw_text and id to metadata for retrieval
        metadata["raw_text"] = raw_text[:1000]  # Limit size
        metadata["incident_id"] = incident_id
        
        # Always add street and neighborhood fields (even if empty) for consistent retrieval
        # This ensures the field exists in metadata for filtering
        metadata["street"] = street if street else ""
        metadata["neighborhood"] = neighborhood if neighborhood else ""
        
        # Create document (summary is embedded)
        doc = Document(
            page_content=summary,
            metadata=metadata
        )
        
        # Store
        self.vectorstore.add_documents([doc], ids=[incident_id])
        
        return incident_id
    
    def search_similar(
        self,
        query: str,
        k: int = 10,
        min_severity: int = None,
        event_types: List[EventType] = None,
        city: str = None
    ) -> List[Tuple[Document, float]]:
        """
        Search for similar incidents.
        
        Args:
            query: Search query (semantic search)
            k: Number of results
            min_severity: Minimum severity filter
            event_types: Filter by event types
            city: Filter by city
            
        Returns:
            List of (Document, score) tuples
        """
        # Build filter
        where_filter = {}
        
        if min_severity:
            where_filter["severity_score"] = {"$gte": min_severity}
        
        if event_types:
            type_values = [et.value if isinstance(et, EventType) else et for et in event_types]
            where_filter["event_type"] = {"$in": type_values}
        
        if city:
            where_filter["city"] = city
        
        # Search
        if where_filter:
            results = self.vectorstore.similarity_search_with_score(
                query,
                k=k,
                filter=where_filter
            )
        else:
            results = self.vectorstore.similarity_search_with_score(query, k=k)
        
        return results
    
    def get_incidents_in_area(
        self,
        center_lat: float,
        center_lon: float,
        radius_km: float,
        days: int = 30,
        limit: int = 100
    ) -> List[Dict]:
        """
        Get all incidents within a radius of a point.
        
        Note: This performs a post-filter since ChromaDB doesn't support
        native geospatial queries. For production, consider using
        a specialized geospatial index alongside ChromaDB.
        
        Args:
            center_lat: Center latitude
            center_lon: Center longitude
            radius_km: Search radius in kilometers
            days: Time window in days
            limit: Maximum results
            
        Returns:
            List of incident dicts within the area
        """
        from src.tools.risk_calculator import haversine_distance
        
        # Calculate time boundary
        cutoff_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        # Get all recent incidents (ChromaDB doesn't support complex geospatial queries)
        # We'll filter by time in the query and then post-filter by distance
        try:
            # Get collection directly for filtering
            collection = self.vectorstore._collection
            results = collection.get(
                where={"timestamp": {"$gte": cutoff_date}},
                include=["documents", "metadatas"]
            )
        except Exception:
            # Fallback: get all and filter
            results = self.vectorstore._collection.get(
                include=["documents", "metadatas"]
            )
        
        if not results or not results.get("metadatas"):
            return []
        
        # Filter by distance
        incidents = []
        for i, metadata in enumerate(results["metadatas"]):
            if not metadata:
                continue
                
            lat = metadata.get("lat", 0)
            lon = metadata.get("lon", 0)
            
            # Calculate distance
            distance = haversine_distance(center_lat, center_lon, lat, lon)
            
            if distance <= radius_km:
                # Check time filter
                incident_time = metadata.get("timestamp", "")
                if incident_time >= cutoff_date:
                    incidents.append({
                        "summary": results["documents"][i] if results.get("documents") else "",
                        "distance_km": round(distance, 2),
                        **metadata
                    })
        
        # Sort by timestamp (most recent first)
        incidents.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        
        return incidents[:limit]
    
    def get_incidents_by_time(
        self,
        hours: int = 24,
        city: str = None,
        event_types: List[EventType] = None
    ) -> List[Dict]:
        """
        Get recent incidents within a time window.
        
        Args:
            hours: Time window in hours
            city: Filter by city
            event_types: Filter by event types
            
        Returns:
            List of incident dicts
        """
        # Calculate cutoff time
        cutoff_dt = datetime.utcnow() - timedelta(hours=hours)
        
        # Build filter (without time - filter in Python for reliability)
        where_filter = {}
        
        if city:
            where_filter["city"] = city
        
        if event_types:
            type_values = [et.value if isinstance(et, EventType) else et for et in event_types]
            where_filter["event_type"] = {"$in": type_values}
        
        try:
            collection = self.vectorstore._collection
            # Get all matching incidents (we'll filter by time in Python)
            if where_filter:
                results = collection.get(
                    where=where_filter,
                    include=["documents", "metadatas"]
                )
            else:
                # Get all incidents if no filters
                results = collection.get(
                    include=["documents", "metadatas"]
                )
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error fetching incidents: {e}")
            return []
        
        if not results or not results.get("metadatas"):
            return []
        
        incidents = []
        for i, metadata in enumerate(results["metadatas"]):
            if not metadata:
                continue
                
            timestamp_str = metadata.get("timestamp", "")
            
            # Parse timestamp and compare with cutoff
            try:
                # Handle different timestamp formats
                if "+" in timestamp_str:
                    # Has timezone info (e.g., "2026-01-31T19:11:21+00:00")
                    ts_dt = datetime.fromisoformat(timestamp_str)
                elif timestamp_str.endswith("Z"):
                    # UTC timezone marker
                    ts_dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                else:
                    # No timezone - assume UTC
                    ts_dt = datetime.fromisoformat(timestamp_str)
                    from datetime import timezone
                    if ts_dt.tzinfo is None:
                        ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                
                # Make cutoff timezone-aware for comparison
                from datetime import timezone
                if cutoff_dt.tzinfo is None:
                    cutoff_dt_aware = cutoff_dt.replace(tzinfo=timezone.utc)
                else:
                    cutoff_dt_aware = cutoff_dt
                
                # Compare with cutoff
                if ts_dt >= cutoff_dt_aware:
                    incidents.append({
                        "summary": results["documents"][i] if results.get("documents") else "",
                        **metadata
                    })
            except Exception as e:
                # If timestamp parsing fails, skip this incident
                import logging
                logger = logging.getLogger(__name__)
                logger.debug(f"Failed to parse timestamp {timestamp_str}: {e}")
                continue
        
        # Sort by timestamp (most recent first)
        incidents.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        
        return incidents
    
    def get_incident_by_id(self, incident_id: str) -> Optional[Dict]:
        """Get a specific incident by ID."""
        try:
            results = self.vectorstore._collection.get(
                ids=[incident_id],
                include=["documents", "metadatas"]
            )
            
            if results and results.get("metadatas") and results["metadatas"][0]:
                return {
                    "summary": results["documents"][0] if results.get("documents") else "",
                    **results["metadatas"][0]
                }
        except Exception:
            pass
        
        return None
    
    def delete_incident(self, incident_id: str) -> bool:
        """Delete an incident by ID."""
        try:
            self.vectorstore._collection.delete(ids=[incident_id])
            return True
        except Exception:
            return False
    
    def get_statistics(self) -> Dict:
        """Get database statistics."""
        try:
            collection = self.vectorstore._collection
            count = collection.count()
            
            # Get all for statistics
            results = collection.get(include=["metadatas"])
            
            if not results or not results.get("metadatas"):
                return {"total_incidents": count}
            
            # Calculate stats
            cities = {}
            event_types = {}
            severities = []
            
            for metadata in results["metadatas"]:
                if not metadata:
                    continue
                    
                city = metadata.get("city", "Unknown")
                cities[city] = cities.get(city, 0) + 1
                
                et = metadata.get("event_type", "unknown")
                event_types[et] = event_types.get(et, 0) + 1
                
                sev = metadata.get("severity_score", 0)
                if sev:
                    severities.append(sev)
            
            return {
                "total_incidents": count,
                "incidents_by_city": cities,
                "incidents_by_type": event_types,
                "avg_severity": round(sum(severities) / len(severities), 2) if severities else 0,
                "max_severity": max(severities) if severities else 0
            }
        except Exception as e:
            return {"total_incidents": 0, "error": str(e)}
    
    def check_duplicate(self, message_id: int, source_channel: str) -> bool:
        """Check if an incident from this message already exists."""
        try:
            results = self.vectorstore._collection.get(
                where={
                    "$and": [
                        {"message_id": message_id},
                        {"source_channel": source_channel}
                    ]
                }
            )
            return bool(results and results.get("ids"))
        except Exception:
            return False
    
    def check_similar_incident(
        self,
        summary: str,
        city: str,
        lat: float,
        lon: float,
        event_type: EventType,
        timestamp: datetime,
        street: str = None,
        time_window_hours: int = 6,
        embedding_distance_threshold: float = 0.4,
        distance_threshold_km: float = 2.0
    ) -> Optional[str]:
        """
        Check if a similar incident already exists using semantic similarity.
        
        This catches duplicates from different channels reporting the same incident.
        
        Args:
            summary: Incident summary to check
            city: City name
            lat: Latitude
            lon: Longitude
            event_type: Event type
            timestamp: Incident timestamp
            street: Street name (optional)
            time_window_hours: Time window to search within (default 6 hours)
            embedding_distance_threshold: Maximum embedding distance (lower = more similar, default 0.4)
            distance_threshold_km: Maximum distance in km to consider same location (default 2.0)
            
        Returns:
            Incident ID if similar incident found, None otherwise
        """
        try:
            from datetime import timezone
            
            # Calculate time window
            time_window_start = timestamp - timedelta(hours=time_window_hours)
            time_window_end = timestamp + timedelta(hours=1)  # Small buffer for future incidents
            
            # Search for similar incidents using semantic search
            # Use summary as query to find semantically similar incidents
            similar_results = self.vectorstore.similarity_search_with_score(
                summary,
                k=10  # Check top 10 most similar
            )
            
            if not similar_results:
                return None
            
            # Filter results by time window, location, and event type
            for doc, score in similar_results:
                # Check embedding distance (ChromaDB returns distance, lower = more similar)
                # Score of 0.0 = identical, higher = less similar
                if score > embedding_distance_threshold:
                    continue
                
                metadata = doc.metadata
                if not metadata:
                    continue
                
                # Check event type matches
                existing_event_type = metadata.get("event_type", "")
                if existing_event_type != event_type.value:
                    continue
                
                # Check time window
                existing_timestamp_str = metadata.get("timestamp", "")
                if not existing_timestamp_str:
                    continue
                
                try:
                    # Parse timestamp
                    if "+" in existing_timestamp_str:
                        existing_timestamp = datetime.fromisoformat(existing_timestamp_str)
                    else:
                        existing_timestamp = datetime.fromisoformat(existing_timestamp_str)
                        if existing_timestamp.tzinfo is None:
                            existing_timestamp = existing_timestamp.replace(tzinfo=timezone.utc)
                    
                    # Normalize timezone for comparison
                    if timestamp.tzinfo is None:
                        timestamp_aware = timestamp.replace(tzinfo=timezone.utc)
                    else:
                        timestamp_aware = timestamp
                    
                    if existing_timestamp < time_window_start or existing_timestamp > time_window_end:
                        continue
                except Exception:
                    continue
                
                # Check location proximity
                existing_lat = metadata.get("lat", 0)
                existing_lon = metadata.get("lon", 0)
                existing_city = metadata.get("city", "")
                existing_street = metadata.get("street", "")
                
                # Calculate distance using Haversine formula
                from math import radians, sin, cos, sqrt, atan2
                
                def haversine_distance(lat1, lon1, lat2, lon2):
                    """Calculate distance between two points in km."""
                    R = 6371  # Earth radius in km
                    lat1_rad = radians(lat1)
                    lat2_rad = radians(lat2)
                    delta_lat = radians(lat2 - lat1)
                    delta_lon = radians(lon2 - lon1)
                    
                    a = sin(delta_lat/2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(delta_lon/2)**2
                    c = 2 * atan2(sqrt(a), sqrt(1-a))
                    return R * c
                
                distance_km = haversine_distance(lat, lon, existing_lat, existing_lon)
                
                # Check if same city
                city_match = existing_city.lower() == city.lower() or \
                           existing_city.lower() in city.lower() or \
                           city.lower() in existing_city.lower()
                
                # Check if same street (if street is provided)
                street_match = True  # Default to True if no street info
                if street and existing_street:
                    street_lower = street.lower().strip()
                    existing_street_lower = existing_street.lower().strip()
                    if street_lower and existing_street_lower:
                        street_match = street_lower == existing_street_lower or \
                                      street_lower in existing_street_lower or \
                                      existing_street_lower in street_lower
                
                # If same city and street, or very close location (< 2km), consider duplicate
                if city_match and (street_match or distance_km < distance_threshold_km):
                    # Found a similar incident
                    return metadata.get("incident_id")
            
            return None
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error checking similar incident: {e}")
            return None


# Singleton instance
_manager: Optional[ChromaManager] = None


def get_chroma_manager() -> ChromaManager:
    """Get or create the ChromaManager singleton."""
    global _manager
    if _manager is None:
        _manager = ChromaManager()
    return _manager
