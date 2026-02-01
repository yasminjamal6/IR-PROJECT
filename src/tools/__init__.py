"""Tools and utilities for The Watch."""

from .geocoder import (
    Geocoder,
    get_geocoder,
    geocode_location,
    GeocodingResult,
    KNOWN_LOCATIONS,
)

from .risk_calculator import (
    RiskCalculator,
    RiskConfig,
    get_risk_calculator,
    calculate_location_risk,
    quick_risk_check,
    haversine_distance,
)

__all__ = [
    # Geocoder
    "Geocoder",
    "get_geocoder",
    "geocode_location",
    "GeocodingResult",
    "KNOWN_LOCATIONS",
    
    # Risk Calculator
    "RiskCalculator",
    "RiskConfig",
    "get_risk_calculator",
    "calculate_location_risk",
    "quick_risk_check",
    "haversine_distance",
]
