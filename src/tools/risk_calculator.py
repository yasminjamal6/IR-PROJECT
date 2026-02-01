"""
The Watch: Risk Calculator Module

Calculates risk scores for locations based on:
- Number of incidents in radius
- Severity of incidents
- Recency (time decay - recent events weighted higher)
- Event type (shootings weighted higher than accidents)

Risk Score: 0-10 scale
"""

import math
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

from src.models.schemas import (
    RiskAssessment,
    RiskLevel,
    EventType,
    RiskAssessmentDict
)


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great-circle distance between two points on Earth.
    
    Args:
        lat1, lon1: First point coordinates
        lat2, lon2: Second point coordinates
        
    Returns:
        Distance in kilometers
    """
    R = 6371  # Earth's radius in kilometers
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = (math.sin(delta_lat / 2) ** 2 +
         math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


@dataclass
class RiskConfig:
    """Configuration for risk calculation."""
    # Time windows
    recent_hours: int = 24          # What counts as "recent"
    week_hours: int = 168           # 7 days
    analysis_days: int = 30         # Default analysis window
    
    # Radius
    default_radius_km: float = 2.0
    
    # Time decay weights (multipliers for recency)
    weight_last_24h: float = 3.0    # Triple weight for last 24h
    weight_last_week: float = 2.0   # Double weight for last week
    weight_older: float = 1.0       # Base weight for older events
    
    # Event type severity multipliers
    event_weights: Dict[str, float] = None
    
    # Thresholds for risk levels
    events_for_base_score: int = 5  # This many events = base concern
    max_base_score: float = 4.0     # Maximum score from event count alone
    max_severity_score: float = 4.0 # Maximum score from severity
    max_recency_score: float = 2.0  # Maximum score from recency
    
    def __post_init__(self):
        if self.event_weights is None:
            self.event_weights = {
                EventType.SHOOTING.value: 1.5,
                EventType.STABBING.value: 1.4,
                EventType.EXPLOSION.value: 1.5,
                EventType.ARSON.value: 1.3,
                EventType.BRAWL.value: 1.0,
                EventType.POLICE_ACTIVITY.value: 0.8,
                EventType.ROADBLOCK.value: 0.5,
                EventType.ACCIDENT.value: 0.6,
                EventType.UNKNOWN.value: 0.7
            }


DEFAULT_CONFIG = RiskConfig()


class RiskCalculator:
    """
    Calculates location risk scores based on incident data.
    
    Algorithm:
    1. Count events in radius
    2. Sum weighted severities (recent events weighted higher)
    3. Apply event type multipliers
    4. Normalize to 0-10 scale
    """
    
    def __init__(self, config: RiskConfig = None):
        self.config = config or DEFAULT_CONFIG
    
    def calculate_risk(
        self,
        incidents: List[Dict],
        center_lat: float,
        center_lon: float,
        radius_km: float = None,
        analysis_days: int = None
    ) -> RiskAssessment:
        """
        Calculate risk score for a location.
        
        Args:
            incidents: List of incident dicts with metadata
            center_lat: Center latitude
            center_lon: Center longitude
            radius_km: Search radius in km
            analysis_days: Time window in days
            
        Returns:
            RiskAssessment with detailed metrics
        """
        radius_km = radius_km or self.config.default_radius_km
        analysis_days = analysis_days or self.config.analysis_days
        
        # Use timezone-aware datetime for proper comparison
        from datetime import timezone
        now = datetime.now(timezone.utc)
        analysis_start = now - timedelta(days=analysis_days)
        
        # Filter incidents by radius and time
        filtered = self._filter_incidents(
            incidents,
            center_lat,
            center_lon,
            radius_km,
            analysis_start
        )
        
        if not filtered:
            return RiskAssessment(
                location=f"{center_lat:.4f}, {center_lon:.4f}",
                latitude=center_lat,
                longitude=center_lon,
                radius_km=radius_km,
                risk_score=0.0,
                risk_level=RiskLevel.MINIMAL,
                total_events=0,
                events_last_24h=0,
                events_last_7d=0,
                total_severity_sum=0,
                weighted_severity=0.0,
                event_type_counts={},
                most_recent_event=None,
                analysis_start=analysis_start,
                analysis_end=now
            )
        
        # Calculate metrics
        metrics = self._calculate_metrics(filtered, now)
        
        # Calculate final risk score
        risk_score = self._calculate_final_score(metrics, len(filtered))
        risk_level = RiskAssessment.calculate_risk_level(risk_score)
        
        return RiskAssessment(
            location=f"{center_lat:.4f}, {center_lon:.4f}",
            latitude=center_lat,
            longitude=center_lon,
            radius_km=radius_km,
            risk_score=round(risk_score, 1),
            risk_level=risk_level,
            total_events=len(filtered),
            events_last_24h=metrics["events_24h"],
            events_last_7d=metrics["events_7d"],
            total_severity_sum=metrics["severity_sum"],
            weighted_severity=round(metrics["weighted_severity"], 2),
            event_type_counts=metrics["type_counts"],
            most_recent_event=metrics["most_recent"],
            analysis_start=analysis_start,
            analysis_end=now
        )
    
    def _filter_incidents(
        self,
        incidents: List[Dict],
        center_lat: float,
        center_lon: float,
        radius_km: float,
        cutoff: datetime
    ) -> List[Dict]:
        """Filter incidents by radius and time."""
        filtered = []
        
        for incident in incidents:
            # Parse timestamp
            timestamp_str = incident.get("timestamp", "")
            try:
                if isinstance(timestamp_str, datetime):
                    timestamp = timestamp_str
                else:
                    timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            except:
                continue
            
            # Check time (make both timezone-aware for comparison)
            from datetime import timezone
            ts_aware = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
            cutoff_aware = cutoff if cutoff.tzinfo else cutoff.replace(tzinfo=timezone.utc)
            if ts_aware < cutoff_aware:
                continue
            
            # Check distance
            lat = incident.get("lat", 0)
            lon = incident.get("lon", 0)
            distance = haversine_distance(center_lat, center_lon, lat, lon)
            
            if distance <= radius_km:
                incident["_distance_km"] = distance
                incident["_timestamp"] = timestamp
                filtered.append(incident)
        
        return filtered
    
    def _calculate_metrics(
        self,
        incidents: List[Dict],
        now: datetime
    ) -> Dict:
        """Calculate detailed metrics from filtered incidents."""
        events_24h = 0
        events_7d = 0
        severity_sum = 0
        weighted_severity = 0.0
        type_counts = {}
        most_recent = None
        
        cutoff_24h = now - timedelta(hours=self.config.recent_hours)
        cutoff_7d = now - timedelta(hours=self.config.week_hours)
        
        for incident in incidents:
            timestamp = incident.get("_timestamp", now)
            severity = incident.get("severity_score", 5)
            event_type = incident.get("event_type", "unknown")
            
            # Track most recent (use timezone-aware timestamp)
            from datetime import timezone
            ts_aware = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
            if most_recent is None or ts_aware > most_recent:
                most_recent = ts_aware
            
            # Count by time window (use timezone-aware comparison)
            from datetime import timezone
            ts_aware = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
            if ts_aware >= cutoff_24h:
                events_24h += 1
                time_weight = self.config.weight_last_24h
            elif ts_aware >= cutoff_7d:
                events_7d += 1
                time_weight = self.config.weight_last_week
            else:
                time_weight = self.config.weight_older
            
            # Sum severities
            severity_sum += severity
            
            # Apply event type weight
            type_weight = self.config.event_weights.get(event_type, 1.0)
            weighted_severity += severity * time_weight * type_weight
            
            # Count by type
            type_counts[event_type] = type_counts.get(event_type, 0) + 1
        
        # Events in last 7d includes last 24h
        events_7d += events_24h
        
        return {
            "events_24h": events_24h,
            "events_7d": events_7d,
            "severity_sum": severity_sum,
            "weighted_severity": weighted_severity,
            "type_counts": type_counts,
            "most_recent": most_recent
        }
    
    def _calculate_final_score(
        self,
        metrics: Dict,
        total_events: int
    ) -> float:
        """
        Calculate final risk score (0-10).
        
        Components:
        1. Event count score (0-4): More events = higher risk
        2. Severity score (0-4): Higher average severity = higher risk
        3. Recency score (0-2): More recent events = higher risk
        """
        if total_events == 0:
            return 0.0
        
        # 1. Event count score (logarithmic scaling)
        # 5 events = 2.0, 10 events = 3.0, 20+ events = 4.0
        event_ratio = total_events / self.config.events_for_base_score
        event_score = min(
            self.config.max_base_score,
            math.log2(1 + event_ratio) * 2
        )
        
        # 2. Severity score (based on weighted average)
        avg_weighted = metrics["weighted_severity"] / total_events
        # Normalize: avg of 5 = 2.0, avg of 10 = 4.0
        severity_score = min(
            self.config.max_severity_score,
            (avg_weighted / 5) * 2
        )
        
        # 3. Recency score (boost for very recent events)
        recency_score = 0.0
        if metrics["events_24h"] > 0:
            # Strong boost for events in last 24h
            recency_score = min(
                self.config.max_recency_score,
                math.log2(1 + metrics["events_24h"]) * 1.5
            )
        elif metrics["events_7d"] > 0:
            # Moderate boost for events in last week
            recency_score = min(
                self.config.max_recency_score / 2,
                math.log2(1 + metrics["events_7d"]) * 0.5
            )
        
        # Combine scores
        final_score = event_score + severity_score + recency_score
        
        # Cap at 10
        return min(10.0, max(0.0, final_score))
    
    def get_risk_summary(
        self,
        assessment: RiskAssessment,
        location_name: str = None
    ) -> str:
        """
        Generate a natural language summary of the risk assessment.
        
        Args:
            assessment: RiskAssessment object
            location_name: Optional location name for personalization
            
        Returns:
            Natural language summary string
        """
        location = location_name or assessment.location
        level = assessment.risk_level
        score = assessment.risk_score
        
        # Risk level descriptions
        level_descriptions = {
            RiskLevel.CRITICAL: "extremely dangerous with active incidents",
            RiskLevel.HIGH: "significantly elevated with recent serious incidents",
            RiskLevel.MODERATE: "moderately elevated with some recent activity",
            RiskLevel.LOW: "relatively calm with minimal recent activity",
            RiskLevel.MINIMAL: "very safe with no significant recent incidents"
        }
        
        level_desc = level_descriptions.get(level, "unknown")
        
        # Build summary
        parts = [
            f"🛡️ **Risk Assessment for {location}**",
            f"",
            f"**Risk Level:** {level.value.upper()} ({score}/10)",
            f"The area is currently {level_desc}.",
            f""
        ]
        
        # Add event details
        if assessment.total_events > 0:
            parts.append(f"**Incident Summary (last {30} days):**")
            parts.append(f"• Total incidents: {assessment.total_events}")
            
            if assessment.events_last_24h > 0:
                parts.append(f"• ⚠️ Last 24 hours: {assessment.events_last_24h} incidents")
            
            if assessment.events_last_7d > 0:
                parts.append(f"• Last 7 days: {assessment.events_last_7d} incidents")
            
            # Event type breakdown
            if assessment.event_type_counts:
                type_strs = [
                    f"{t.replace('_', ' ').title()}: {c}"
                    for t, c in assessment.event_type_counts.items()
                ]
                parts.append(f"• Types: {', '.join(type_strs)}")
            
            # Most recent
            if assessment.most_recent_event:
                time_ago = datetime.utcnow() - assessment.most_recent_event.replace(tzinfo=None)
                if time_ago.total_seconds() < 3600:
                    ago_str = f"{int(time_ago.total_seconds() / 60)} minutes ago"
                elif time_ago.total_seconds() < 86400:
                    ago_str = f"{int(time_ago.total_seconds() / 3600)} hours ago"
                else:
                    ago_str = f"{time_ago.days} days ago"
                parts.append(f"• Most recent incident: {ago_str}")
        else:
            parts.append("No incidents recorded in the specified area and time window.")
        
        return "\n".join(parts)


_calculator: Optional[RiskCalculator] = None


def get_risk_calculator() -> RiskCalculator:
    """Get or create the RiskCalculator singleton."""
    global _calculator
    if _calculator is None:
        _calculator = RiskCalculator()
    return _calculator


def calculate_location_risk(
    incidents: List[Dict],
    lat: float,
    lon: float,
    radius_km: float = 2.0
) -> RiskAssessment:
    """
    Calculate risk for a location.
    
    Args:
        incidents: List of incident dicts
        lat: Latitude
        lon: Longitude
        radius_km: Search radius
        
    Returns:
        RiskAssessment object
    """
    return get_risk_calculator().calculate_risk(incidents, lat, lon, radius_km)


def quick_risk_check(
    incidents: List[Dict],
    lat: float,
    lon: float
) -> Tuple[float, str]:
    """
    Quick risk check returning just score and level.
    
    Returns:
        Tuple of (risk_score, risk_level_string)
    """
    assessment = calculate_location_risk(incidents, lat, lon)
    return assessment.risk_score, assessment.risk_level.value
