"""Database package for Scraper HQ."""

from .database import Database

# Create and export the global database instance
db = Database()

__all__ = ['db', 'Database'] 