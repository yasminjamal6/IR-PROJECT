"""
User Preferences Manager for The Watch Bot

Stores and retrieves user preferences for personalized news filtering.
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict

# Preferences storage file
PREFS_DIR = Path(__file__).parent.parent.parent / "data"
PREFS_DIR.mkdir(parents=True, exist_ok=True)
PREFS_FILE = PREFS_DIR / "user_preferences.json"


@dataclass
class UserPreferences:
    """User preferences for news filtering."""
    user_id: int
    preferred_cities: List[str] = None
    preferred_streets: List[str] = None
    preferred_neighborhoods: List[str] = None
    
    def __post_init__(self):
        if self.preferred_cities is None:
            self.preferred_cities = []
        if self.preferred_streets is None:
            self.preferred_streets = []
        if self.preferred_neighborhoods is None:
            self.preferred_neighborhoods = []
    
    def has_preferences(self) -> bool:
        """Check if user has any preferences set."""
        return bool(
            self.preferred_cities or 
            self.preferred_streets or 
            self.preferred_neighborhoods
        )
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON storage."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'UserPreferences':
        """Create from dictionary."""
        return cls(**data)


class UserPreferencesManager:
    """Manages user preferences storage and retrieval."""
    
    def __init__(self, prefs_file: Path = PREFS_FILE):
        self.prefs_file = prefs_file
        self._prefs_cache: Dict[int, UserPreferences] = {}
        self._load_preferences()
    
    def _load_preferences(self):
        """Load preferences from file."""
        if not self.prefs_file.exists():
            self._prefs_cache = {}
            return
        
        try:
            with open(self.prefs_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self._prefs_cache = {
                    int(user_id): UserPreferences.from_dict(prefs_data)
                    for user_id, prefs_data in data.items()
                }
        except Exception as e:
            print(f"Error loading preferences: {e}")
            self._prefs_cache = {}
    
    def _save_preferences(self):
        """Save preferences to file."""
        try:
            data = {
                str(user_id): prefs.to_dict()
                for user_id, prefs in self._prefs_cache.items()
            }
            with open(self.prefs_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Error saving preferences: {e}")
    
    def get_preferences(self, user_id: int) -> UserPreferences:
        """Get user preferences, creating default if not exists."""
        if user_id not in self._prefs_cache:
            self._prefs_cache[user_id] = UserPreferences(user_id=user_id)
        return self._prefs_cache[user_id]
    
    def set_preferences(
        self,
        user_id: int,
        cities: Optional[List[str]] = None,
        streets: Optional[List[str]] = None,
        neighborhoods: Optional[List[str]] = None
    ):
        """Update user preferences."""
        prefs = self.get_preferences(user_id)
        
        if cities is not None:
            prefs.preferred_cities = [c.strip() for c in cities if c.strip()]
        if streets is not None:
            prefs.preferred_streets = [s.strip() for s in streets if s.strip()]
        if neighborhoods is not None:
            prefs.preferred_neighborhoods = [n.strip() for n in neighborhoods if n.strip()]
        
        self._prefs_cache[user_id] = prefs
        self._save_preferences()
    
    def clear_preferences(self, user_id: int):
        """Clear all preferences for a user."""
        if user_id in self._prefs_cache:
            self._prefs_cache[user_id] = UserPreferences(user_id=user_id)
            self._save_preferences()
    
    def filter_incidents_by_preferences(
        self,
        user_id: int,
        incidents: List[Dict]
    ) -> List[Dict]:
        """Filter incidents based on user preferences."""
        prefs = self.get_preferences(user_id)
        
        if not prefs.has_preferences():
            return incidents  # Return all if no preferences
        
        filtered = []
        
        for incident in incidents:
            city = incident.get('city', '').strip()
            street = incident.get('street', '').strip() if incident.get('street') else ''
            neighborhood = incident.get('neighborhood', '').strip() if incident.get('neighborhood') else ''
            
            # Skip incidents with "Unknown" city - they don't match any preference
            if not city or city.lower() in ['unknown', 'לא ידוע', '']:
                continue
            
            city_lower = city.lower()
            street_lower = street.lower() if street else ''
            neighborhood_lower = neighborhood.lower() if neighborhood else ''
            
            # Check if incident matches any preference
            matches_city = any(
                pref_city.lower() in city_lower or city_lower in pref_city.lower()
                for pref_city in prefs.preferred_cities
            )
            
            matches_street = any(
                pref_street.lower() in street_lower or street_lower in pref_street.lower()
                for pref_street in prefs.preferred_streets
            ) if street else False
            
            matches_neighborhood = any(
                pref_neighborhood.lower() in neighborhood_lower or neighborhood_lower in pref_neighborhood.lower()
                for pref_neighborhood in prefs.preferred_neighborhoods
            ) if neighborhood else False
            
            if matches_city or matches_street or matches_neighborhood:
                filtered.append(incident)
        
        return filtered


# Global instance
_prefs_manager = None

def get_preferences_manager() -> UserPreferencesManager:
    """Get the global preferences manager instance."""
    global _prefs_manager
    if _prefs_manager is None:
        _prefs_manager = UserPreferencesManager()
    return _prefs_manager
