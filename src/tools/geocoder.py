"""
The Watch: Geocoding Module

Handles location resolution for Arab villages and cities in Israel.
Uses Google Maps Geocoding API with Google Places fallback for landmarks.

Key Challenge: Arab villages often lack standard addresses. Locations are
described by landmarks (e.g., "Near the old bakery in Tel Aviv").
This module handles multi-step geocoding with fallbacks.
"""

import os
import re
from typing import Optional, Tuple, Dict
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

import googlemaps
from googlemaps.exceptions import ApiError, Timeout

from src.models.schemas import GeocodedLocation

# Initialize Google Maps client
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

if not GOOGLE_API_KEY:
    print("⚠️  Warning: GOOGLE_API_KEY not set. Geocoding will use fallback coordinates.")
    gmaps = None
else:
    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)


KNOWN_LOCATIONS: Dict[str, Tuple[float, float]] = {
    "tel aviv": (32.0853, 34.7818),
    "תל אביב": (32.0853, 34.7818),
    "תל-אביב": (32.0853, 34.7818),
    "תא": (32.0853, 34.7818),
    "jerusalem": (31.7683, 35.2137),
    "ירושלים": (31.7683, 35.2137),
    "י-ם": (31.7683, 35.2137),
    "haifa": (32.7940, 34.9896),
    "חיפה": (32.7940, 34.9896),
    "beer sheva": (31.2518, 34.7913),
    "באר שבע": (31.2518, 34.7913),
    "ב\"ש": (31.2518, 34.7913),
    "beersheba": (31.2518, 34.7913),
    "rishon lezion": (31.9730, 34.7925),
    "ראשון לציון": (31.9730, 34.7925),
    "ראשל\"צ": (31.9730, 34.7925),
    "petah tikva": (32.0841, 34.8878),
    "פתח תקווה": (32.0841, 34.8878),
    "פ\"ת": (32.0841, 34.8878),
    "holon": (32.0117, 34.7728),
    "חולון": (32.0117, 34.7728),
    "bnei brak": (32.0833, 34.8333),
    "בני ברק": (32.0833, 34.8333),
    "ramat gan": (32.0700, 34.8236),
    "רמת גן": (32.0700, 34.8236),
    "bat yam": (32.0167, 34.7500),
    "בת ים": (32.0167, 34.7500),
    "givatayim": (32.0714, 34.8117),
    "גבעתיים": (32.0714, 34.8117),
    "herzliya": (32.1653, 34.8458),
    "הרצליה": (32.1653, 34.8458),
    "raanana": (32.1833, 34.8667),
    "רעננה": (32.1833, 34.8667),
    "kfar saba": (32.1781, 34.9069),
    "כפר סבא": (32.1781, 34.9069),
    "hod hasharon": (32.1500, 34.8833),
    "הוד השרון": (32.1500, 34.8833),
    "netanya": (32.3286, 34.8567),
    "נתניה": (32.3286, 34.8567),
    "rehovot": (31.8928, 34.8113),
    "רחובות": (31.8928, 34.8113),
    "ashdod": (31.8044, 34.6553),
    "אשדוד": (31.8044, 34.6553),
    "ashkelon": (31.6658, 34.5664),
    "אשקלון": (31.6658, 34.5664),
    "hadera": (32.4339, 34.9197),
    "חדרה": (32.4339, 34.9197),
    "caesarea": (32.5000, 34.9000),
    "קיסריה": (32.5000, 34.9000),
    "zichron yaakov": (32.5667, 34.9500),
    "זכרון יעקב": (32.5667, 34.9500),
    "nahariya": (33.0089, 35.0931),
    "נהריה": (33.0089, 35.0931),
    "kiryat shmona": (33.2075, 35.5697),
    "קרית שמונה": (33.2075, 35.5697),
    "safed": (32.9658, 35.4983),
    "צפת": (32.9658, 35.4983),
    "tiberias": (32.7922, 35.5311),
    "טבריה": (32.7922, 35.5311),
    "karmiel": (32.9136, 35.2961),
    "כרמיאל": (32.9136, 35.2961),
    "afula": (32.6100, 35.2883),
    "עפולה": (32.6100, 35.2883),
    "beit shean": (32.5000, 35.5000),
    "בית שאן": (32.5000, 35.5000),
    "yokneam": (32.6592, 35.1094),
    "יקנעם": (32.6592, 35.1094),
    "kiryat ata": (32.8000, 35.1000),
    "קרית אתא": (32.8000, 35.1000),
    "kiryat bialik": (32.8333, 35.0833),
    "קרית ביאליק": (32.8333, 35.0833),
    "kiryat motzkin": (32.8333, 35.0667),
    "קרית מוצקין": (32.8333, 35.0667),
    "kiryat yam": (32.8500, 35.0667),
    "קרית ים": (32.8500, 35.0667),
    "nesher": (32.7667, 35.0333),
    "נשר": (32.7667, 35.0333),
    "tirat carmel": (32.7667, 34.9667),
    "טירת כרמל": (32.7667, 34.9667),
    "eilat": (29.5569, 34.9517),
    "אילת": (29.5569, 34.9517),
    "dimona": (31.0667, 35.0333),
    "דימונה": (31.0667, 35.0333),
    "arad": (31.2589, 35.2128),
    "ערד": (31.2589, 35.2128),
    "sderot": (31.5250, 34.5964),
    "שדרות": (31.5250, 34.5964),
    "ofakim": (31.3167, 34.6167),
    "אופקים": (31.3167, 34.6167),
    "netivot": (31.4167, 34.5833),
    "נתיבות": (31.4167, 34.5833),
    "kiryat gat": (31.6100, 34.7644),
    "קרית גת": (31.6100, 34.7644),
    "beit shemesh": (31.7514, 34.9886),
    "בית שמש": (31.7514, 34.9886),
    "modiin": (31.8989, 35.0103),
    "מודיעין": (31.8989, 35.0103),
    "maale adumim": (31.7772, 35.3008),
    "מעלה אדומים": (31.7772, 35.3008),
    "Tel Aviv": (32.5167, 35.1500),
    "um el fahem": (32.5167, 35.1500),
    "אום אל-פחם": (32.5167, 35.1500),
    "baqa al-gharbiyye": (32.4167, 35.0333),
    "baqa": (32.4167, 35.0333),
    "בקה אל-גרבייה": (32.4167, 35.0333),
    "kafr qasim": (32.1136, 34.9786),
    "kfar qassem": (32.1136, 34.9786),
    "כפר קאסם": (32.1136, 34.9786),
    "tira": (32.2333, 34.9500),
    "טירה": (32.2333, 34.9500),
    "tayibe": (32.2667, 35.0000),
    "טייבה": (32.2667, 35.0000),
    "qalansawe": (32.2833, 34.9833),
    "קלנסווה": (32.2833, 34.9833),
    "kafr qara": (32.5000, 35.0833),
    "כפר קרע": (32.5000, 35.0833),
    "ar'ara": (32.5000, 35.1000),
    "ערערה": (32.5000, 35.1000),
    "jaljulia": (32.1500, 34.9500),
    "ג'לג'וליה": (32.1500, 34.9500),
    "nazareth": (32.6996, 35.3035),
    "נצרת": (32.6996, 35.3035),
    "الناصرة": (32.6996, 35.3035),
    "shefa-amr": (32.8056, 35.1697),
    "shfaram": (32.8056, 35.1697),
    "שפרעם": (32.8056, 35.1697),
    "sakhnin": (32.8667, 35.3000),
    "סח'נין": (32.8667, 35.3000),
    "tamra": (32.8500, 35.2000),
    "טמרה": (32.8500, 35.2000),
    "arraba": (32.8500, 35.3333),
    "עראבה": (32.8500, 35.3333),
    "deir hanna": (32.8667, 35.3667),
    "דיר חנא": (32.8667, 35.3667),
    "majd al-krum": (32.9167, 35.2500),
    "מג'ד אל-כרום": (32.9167, 35.2500),
    "kafr manda": (32.8167, 35.2667),
    "כפר מנדא": (32.8167, 35.2667),
    "kafr kanna": (32.7500, 35.3333),
    "כפר כנא": (32.7500, 35.3333),
    "yaffa an-naseriyye": (32.6833, 35.2833),
    "יפיע": (32.6833, 35.2833),
    "iksal": (32.6833, 35.3500),
    "אכסאל": (32.6833, 35.3500),
    "ibillin": (32.8333, 35.2333),
    "אבילין": (32.8333, 35.2333),
    "kabul": (32.8667, 35.2000),
    "כאבול": (32.8667, 35.2000),
    "nahef": (32.9500, 35.3167),
    "נחף": (32.9500, 35.3167),
    "judeide-maker": (32.9333, 35.2500),
    "ג'דיידה-מכר": (32.9333, 35.2500),
    "rahat": (31.3925, 34.7539),
    "רהט": (31.3925, 34.7539),
    "hura": (31.2933, 34.9300),
    "חורה": (31.2933, 34.9300),
    "tel sheva": (31.2500, 34.8167),
    "תל שבע": (31.2500, 34.8167),
    "kuseife": (31.2167, 34.9833),
    "כסייפה": (31.2167, 34.9833),
    "laqye": (31.3333, 34.8500),
    "לקייה": (31.3333, 34.8500),
    "segev shalom": (31.2500, 34.9167),
    "שגב שלום": (31.2500, 34.9167),
    "arara banegev": (31.2833, 34.9000),
    "ערערה-בנגב": (31.2833, 34.9000),
    "haifa": (32.7940, 34.9896),
    "חיפה": (32.7940, 34.9896),
    "acre": (32.9278, 35.0817),
    "akko": (32.9278, 35.0817),
    "עכו": (32.9278, 35.0817),
    "lod": (31.9514, 34.8917),
    "לוד": (31.9514, 34.8917),
    "ramle": (31.9292, 34.8628),
    "רמלה": (31.9292, 34.8628),
    "jaffa": (32.0503, 34.7597),
    "יפו": (32.0503, 34.7597),
    "jerusalem": (31.7683, 35.2137),
    "ירושלים": (31.7683, 35.2137),
    "tel aviv": (32.0853, 34.7818),
    "תל אביב": (32.0853, 34.7818),
    "beer sheva": (31.2518, 34.7913),
    "באר שבע": (31.2518, 34.7913),
}


@dataclass
class GeocodingResult:
    """Internal result from geocoding attempt."""
    success: bool
    latitude: float = 0.0
    longitude: float = 0.0
    formatted_address: str = ""
    method: str = "unknown"
    confidence: float = 0.0
    error: str = ""


class Geocoder:
    """
    Multi-strategy geocoder for Arab villages in Israel.
    
    Strategy:
    1. Try Google Geocoding API with city name
    2. If vague, try Google Places Text Search for landmarks
    3. Fall back to known locations database
    4. Last resort: use city center coordinates
    """
    
    def __init__(self):
        self.client = gmaps
        self.cache: Dict[str, GeocodingResult] = {}
    
    def geocode(
        self,
        location_description: str,
        city: str = None
    ) -> GeocodedLocation:
        """
        Main geocoding method. Tries multiple strategies.
        
        Args:
            location_description: Original location text (may include landmarks)
            city: City name if known
            
        Returns:
            GeocodedLocation with coordinates and metadata
        """
        # Check cache
        cache_key = f"{location_description}|{city}".lower()
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            return GeocodedLocation(
                latitude=cached.latitude,
                longitude=cached.longitude,
                formatted_address=cached.formatted_address,
                geocode_method=cached.method,
                confidence=cached.confidence
            )
        
        result = None
        
        # Strategy 1: Try Google Geocoding with full description + city
        if self.client:
            result = self._try_google_geocoding(location_description, city)
            
            if not result.success and city:
                # Strategy 2: Try with just the city
                result = self._try_google_geocoding(city, None)
            
            if not result.success:
                # Strategy 3: Try Google Places for landmark search
                result = self._try_google_places(location_description, city)
        
        # Strategy 4: Fallback to known locations
        if not result or not result.success:
            # First try full address (street + city) for precise match
            if location_description and city:
                full_address = f"{location_description} {city}"
                result = self._try_known_locations(full_address)
            
            # Then try just city
            if (not result or not result.success) and city:
                result = self._try_known_locations(city)
            
            # Then try location description
            if (not result or not result.success) and location_description:
                result = self._try_known_locations(location_description)
        
        # Strategy 5: Last resort - extract any city name and use its coordinates
        if not result.success:
            result = self._extract_and_fallback(location_description)
        
        # Cache result
        self.cache[cache_key] = result
        
        return GeocodedLocation(
            latitude=result.latitude,
            longitude=result.longitude,
            formatted_address=result.formatted_address,
            geocode_method=result.method,
            confidence=result.confidence
        )
    
    def _try_google_geocoding(
        self,
        query: str,
        city: str = None
    ) -> GeocodingResult:
        """Try Google Geocoding API."""
        if not self.client:
            return GeocodingResult(success=False, error="No API key")
        
        try:
            # Build query
            search_query = query
            if city and city.lower() not in query.lower():
                search_query = f"{query}, {city}"
            
            # Add Israel context
            if "israel" not in search_query.lower() and "ישראל" not in search_query:
                search_query = f"{search_query}, Israel"
            
            # Geocode
            results = self.client.geocode(
                search_query,
                region="il",
                language="en"
            )
            
            if results:
                location = results[0]["geometry"]["location"]
                formatted = results[0].get("formatted_address", "")
                
                # Check if result is in Israel (rough bounds)
                lat, lon = location["lat"], location["lng"]
                if 29.0 <= lat <= 34.0 and 34.0 <= lon <= 36.0:
                    return GeocodingResult(
                        success=True,
                        latitude=lat,
                        longitude=lon,
                        formatted_address=formatted,
                        method="google_geocoding",
                        confidence=0.9
                    )
            
            return GeocodingResult(success=False, error="No results in Israel")
            
        except (ApiError, Timeout) as e:
            return GeocodingResult(success=False, error=str(e))
        except Exception as e:
            return GeocodingResult(success=False, error=str(e))
    
    def _try_google_places(
        self,
        landmark: str,
        city: str = None
    ) -> GeocodingResult:
        """Try Google Places Text Search for landmarks."""
        if not self.client:
            return GeocodingResult(success=False, error="No API key")
        
        try:
            # Build query
            search_query = landmark
            if city:
                search_query = f"{landmark} in {city}"
            
            # Use Places text search
            results = self.client.places(
                query=search_query,
                region="il"
            )
            
            if results and results.get("results"):
                place = results["results"][0]
                location = place["geometry"]["location"]
                name = place.get("name", "")
                address = place.get("formatted_address", "")
                
                lat, lon = location["lat"], location["lng"]
                
                # Verify Israel bounds
                if 29.0 <= lat <= 34.0 and 34.0 <= lon <= 36.0:
                    return GeocodingResult(
                        success=True,
                        latitude=lat,
                        longitude=lon,
                        formatted_address=f"{name}, {address}" if name else address,
                        method="google_places",
                        confidence=0.85
                    )
            
            return GeocodingResult(success=False, error="No places found")
            
        except Exception as e:
            return GeocodingResult(success=False, error=str(e))
    
    def _try_known_locations(self, location: str) -> GeocodingResult:
        """Try matching against known locations database."""
        location_lower = location.lower().strip()
        
        # Direct match
        if location_lower in KNOWN_LOCATIONS:
            lat, lon = KNOWN_LOCATIONS[location_lower]
            return GeocodingResult(
                success=True,
                latitude=lat,
                longitude=lon,
                formatted_address=location.title(),
                method="known_locations",
                confidence=0.7
            )
        
        # Partial match
        for known_name, coords in KNOWN_LOCATIONS.items():
            if known_name in location_lower or location_lower in known_name:
                return GeocodingResult(
                    success=True,
                    latitude=coords[0],
                    longitude=coords[1],
                    formatted_address=known_name.title(),
                    method="known_locations_partial",
                    confidence=0.6
                )
        
        return GeocodingResult(success=False, error="Not in known locations")
    
    def _extract_and_fallback(self, text: str) -> GeocodingResult:
        """Extract any recognizable city name and use as fallback."""
        text_lower = text.lower()
        
        for known_name, coords in KNOWN_LOCATIONS.items():
            if known_name in text_lower:
                return GeocodingResult(
                    success=True,
                    latitude=coords[0],
                    longitude=coords[1],
                    formatted_address=f"{known_name.title()} (approximate)",
                    method="text_extraction_fallback",
                    confidence=0.4
                )
        
        # Ultimate fallback: center of Israel
        return GeocodingResult(
            success=False,
            latitude=32.0,
            longitude=35.0,
            formatted_address="Israel (location unknown)",
            method="default_fallback",
            confidence=0.1,
            error="Could not geocode location"
        )
    
    def geocode_query(self, user_query: str) -> Optional[GeocodedLocation]:
        """
        Extract and geocode location from a user query.
        
        Args:
            user_query: User's natural language query
            
        Returns:
            GeocodedLocation if location found, None otherwise
        """
        # Try to find known city names in the query
        query_lower = user_query.lower()
        
        for city_name, coords in KNOWN_LOCATIONS.items():
            if city_name in query_lower:
                return GeocodedLocation(
                    latitude=coords[0],
                    longitude=coords[1],
                    formatted_address=city_name.title(),
                    geocode_method="query_extraction",
                    confidence=0.7
                )
        
        # If no known city, try geocoding the whole query
        return self.geocode(user_query)


# Singleton instance
_geocoder: Optional[Geocoder] = None


def get_geocoder() -> Geocoder:
    """Get or create the Geocoder singleton."""
    global _geocoder
    if _geocoder is None:
        _geocoder = Geocoder()
    return _geocoder


# Convenience function
def geocode_location(location_description: str, city: str = None) -> GeocodedLocation:
    """
    Geocode a location description.
    
    Args:
        location_description: Location text (may include landmarks)
        city: City name if known
        
    Returns:
        GeocodedLocation with coordinates
    """
    return get_geocoder().geocode(location_description, city)
