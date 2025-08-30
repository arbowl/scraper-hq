"""Database module for storing LLM queries and outputs."""

import sqlite3
import json
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class Database:
    """SQLite database for storing LLM queries and outputs."""
    
    def __init__(self, db_path: str = "scraper_hq.db"):
        """Initialize the database connection."""
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the database with required tables."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create LLM queries table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS llm_queries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        query TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        source TEXT,
                        template TEXT,
                        stream TEXT,
                        tags TEXT,  -- JSON array of tags
                        user_id TEXT,
                        session_id TEXT
                    )
                """)
                
                # Create LLM outputs table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS llm_outputs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        query_id INTEGER,
                        output_text TEXT NOT NULL,
                        model_name TEXT,
                        prompt_used TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        source TEXT,
                        template TEXT,
                        stream TEXT,
                        tags TEXT,  -- JSON array of tags
                        processing_time_ms INTEGER,
                        token_count INTEGER,
                        metadata TEXT,  -- JSON object for additional data
                        FOREIGN KEY (query_id) REFERENCES llm_queries (id)
                    )
                """)
                
                # Create indexes for better performance
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_queries_created_at 
                    ON llm_queries (created_at)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_queries_source 
                    ON llm_queries (source)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_queries_template 
                    ON llm_queries (template)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_outputs_created_at 
                    ON llm_outputs (created_at)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_outputs_query_id 
                    ON llm_outputs (query_id)
                """)
                
                conn.commit()
                logger.info("Database initialized successfully")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def add_query(self, query: str, source: Optional[str] = None, 
                  template: Optional[str] = None, stream: Optional[str] = None,
                  tags: Optional[List[str]] = None, user_id: Optional[str] = None,
                  session_id: Optional[str] = None) -> int:
        """Add a new LLM query to the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                tags_json = json.dumps(tags) if tags else None
                
                cursor.execute("""
                    INSERT INTO llm_queries 
                    (query, source, template, stream, tags, user_id, session_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (query, source, template, stream, tags_json, user_id, session_id))
                
                query_id = cursor.lastrowid
                conn.commit()
                logger.info(f"Added query with ID: {query_id}")
                return query_id
                
        except Exception as e:
            logger.error(f"Failed to add query: {e}")
            raise
    
    def add_output(self, query_id: int, output_text: str, model_name: str,
                   prompt_used: str, source: Optional[str] = None,
                   template: Optional[str] = None, stream: Optional[str] = None,
                   tags: Optional[List[str]] = None, 
                   processing_time_ms: Optional[int] = None,
                   token_count: Optional[int] = None, 
                   metadata: Optional[Dict[str, Any]] = None) -> int:
        """Add a new LLM output to the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                tags_json = json.dumps(tags) if tags else None
                metadata_json = json.dumps(metadata) if metadata else None
                
                cursor.execute("""
                    INSERT INTO llm_outputs 
                    (query_id, output_text, model_name, prompt_used, source, 
                     template, stream, tags, processing_time_ms, token_count, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (query_id, output_text, model_name, prompt_used, source, 
                      template, stream, tags_json, processing_time_ms, 
                      token_count, metadata_json))
                
                output_id = cursor.lastrowid
                conn.commit()
                logger.info(f"Added output with ID: {output_id}")
                return output_id
                
        except Exception as e:
            logger.error(f"Failed to add output: {e}")
            raise
    
    def get_queries(self, limit: int = 100, offset: int = 0,
                    source: Optional[str] = None, template: Optional[str] = None,
                    stream: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get LLM queries with optional filtering."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                where_clauses = []
                params = []
                
                if source:
                    where_clauses.append("source = ?")
                    params.append(source)
                if template:
                    where_clauses.append("template = ?")
                    params.append(template)
                if stream:
                    where_clauses.append("stream = ?")
                    params.append(stream)
                
                where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
                
                cursor.execute(f"""
                    SELECT * FROM llm_queries
                    {where_sql}
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                """, params + [limit, offset])
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to get queries: {e}")
            raise
    
    def get_outputs(self, limit: int = 100, offset: int = 0,
                    query_id: Optional[int] = None, source: Optional[str] = None,
                    template: Optional[str] = None, stream: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get LLM outputs with optional filtering."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                where_clauses = []
                params = []
                
                if query_id:
                    where_clauses.append("query_id = ?")
                    params.append(query_id)
                if source:
                    where_clauses.append("source = ?")
                    params.append(source)
                if template:
                    where_clauses.append("template = ?")
                    params.append(template)
                if stream:
                    where_clauses.append("stream = ?")
                    params.append(stream)
                
                where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
                
                cursor.execute(f"""
                    SELECT * FROM llm_outputs
                    {where_sql}
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                """, params + [limit, offset])
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to get outputs: {e}")
            raise
    
    def get_query_with_outputs(self, query_id: int) -> Optional[Dict[str, Any]]:
        """Get a query with all its associated outputs."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Get the query
                cursor.execute("SELECT * FROM llm_queries WHERE id = ?", (query_id,))
                query_row = cursor.fetchone()
                
                if not query_row:
                    return None
                
                # Get all outputs for this query
                cursor.execute("SELECT * FROM llm_outputs WHERE query_id = ? ORDER BY created_at DESC", (query_id,))
                output_rows = cursor.fetchall()
                
                query_dict = dict(query_row)
                query_dict['outputs'] = [dict(row) for row in output_rows]
                
                return query_dict
                
        except Exception as e:
            logger.error(f"Failed to get query with outputs: {e}")
            raise
    
    def search_queries(self, search_term: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search queries by text content."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT * FROM llm_queries 
                    WHERE query LIKE ? 
                    ORDER BY created_at DESC 
                    LIMIT ?
                """, (f"%{search_term}%", limit))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to search queries: {e}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total queries
                cursor.execute("SELECT COUNT(*) FROM llm_queries")
                total_queries = cursor.fetchone()[0]
                
                # Total outputs
                cursor.execute("SELECT COUNT(*) FROM llm_outputs")
                total_outputs = cursor.fetchone()[0]
                
                # Queries by source
                cursor.execute("""
                    SELECT source, COUNT(*) as count 
                    FROM llm_queries 
                    WHERE source IS NOT NULL 
                    GROUP BY source 
                    ORDER BY count DESC
                """)
                queries_by_source = dict(cursor.fetchall())
                
                # Recent activity (last 24 hours)
                cursor.execute("""
                    SELECT COUNT(*) FROM llm_queries 
                    WHERE created_at >= datetime('now', '-1 day')
                """)
                recent_queries = cursor.fetchone()[0]
                
                return {
                    "total_queries": total_queries,
                    "total_outputs": total_outputs,
                    "queries_by_source": queries_by_source,
                    "recent_queries_24h": recent_queries
                }
                
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            raise
    
    def _read_config_tags(self) -> List[str]:
        """Read available tags from config.yaml file."""
        try:
            config_path = Path("config.yaml")
            if not config_path.exists():
                logger.warning("config.yaml not found, returning empty tags list")
                return []
            
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            # Extract all unique tags from sources
            all_tags = set()
            if 'sources' in config:
                for source in config['sources']:
                    if 'tags' in source and isinstance(source['tags'], list):
                        all_tags.update(source['tags'])
            
            return sorted(list(all_tags))
            
        except Exception as e:
            logger.error(f"Failed to read config tags: {e}")
            return []
    
    def get_available_tags(self) -> List[str]:
        """Get all available tags from config.yaml file."""
        return self._read_config_tags()
    
    def get_queries_by_tags(self, tags: List[str], limit: int = 100) -> List[Dict[str, Any]]:
        """Get queries that have any of the specified tags."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Create placeholders for the IN clause
                placeholders = ','.join(['?' for _ in tags])
                
                cursor.execute(f"""
                    SELECT * FROM llm_queries
                    WHERE tags IS NOT NULL AND tags != 'null'
                    AND (
                        {' OR '.join([f"tags LIKE '%\"{tag}\"%'" for tag in tags])}
                    )
                    ORDER BY created_at DESC
                    LIMIT ?
                """, [limit])
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to get queries by tags: {e}")
            return []
    
    def get_outputs_by_tags(self, tags: List[str], limit: int = 100) -> List[Dict[str, Any]]:
        """Get outputs that have any of the specified tags."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute(f"""
                    SELECT * FROM llm_outputs
                    WHERE tags IS NOT NULL AND tags != 'null'
                    AND (
                        {' OR '.join([f"tags LIKE '%\"{tag}\"%'" for tag in tags])}
                    )
                    ORDER BY created_at DESC
                    LIMIT ?
                """, [limit])
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to get outputs by tags: {e}")
            return []
    
    def migrate_from_json(self, json_file_path: str):
        """Migrate existing queries from JSON file to database."""
        try:
            json_path = Path(json_file_path)
            if not json_path.exists():
                logger.info(f"JSON file {json_file_path} not found, skipping migration")
                return
            
            with open(json_path, 'r', encoding='utf-8') as f:
                queries = json.load(f)
            
            migrated_count = 0
            for query in queries:
                if isinstance(query, str) and query.strip():
                    self.add_query(query.strip())
                    migrated_count += 1
            
            logger.info(f"Migrated {migrated_count} queries from JSON file")
            
            # Backup the original JSON file
            backup_path = json_path.with_suffix('.json.backup')
            json_path.rename(backup_path)
            logger.info(f"Backed up original JSON file to {backup_path}")
            
        except Exception as e:
            logger.error(f"Failed to migrate from JSON: {e}")
            raise
    
    def insert_sample_tags(self):
        """Insert some sample tags for testing purposes."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if we already have some tags
                cursor.execute("SELECT COUNT(*) FROM llm_queries WHERE tags IS NOT NULL AND tags != 'null'")
                existing_tags_count = cursor.fetchone()[0]
                
                if existing_tags_count == 0:
                    # Insert some sample queries with tags
                    sample_queries = [
                        ("Analyze recent technology trends", "sample", "tech", "tech_news", ["technology", "trends", "analysis"]),
                        ("Summarize business insights", "sample", "business", "insights", ["business", "insights", "summary"]),
                        ("Review latest developments", "sample", "general", "updates", ["development", "latest", "review"]),
                        ("Compare different approaches", "sample", "analysis", "comparison", ["comparison", "analysis", "approaches"]),
                        ("Identify key patterns", "sample", "research", "patterns", ["patterns", "research", "identification"])
                    ]
                    
                    for query, source, template, stream, tags in sample_queries:
                        cursor.execute("""
                            INSERT INTO llm_queries 
                            (query, source, template, stream, tags)
                            VALUES (?, ?, ?, ?, ?)
                        """, (query, source, template, stream, json.dumps(tags)))
                    
                    conn.commit()
                    logger.info("Inserted sample tags for testing")
                else:
                    logger.info("Sample tags already exist, skipping insertion")
                    
        except Exception as e:
            logger.error(f"Failed to insert sample tags: {e}")
            # Don't raise here as this is just for testing


# Global database instance
db = Database() 