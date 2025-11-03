import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
import structlog
from contextlib import contextmanager
from datetime import datetime

logger = structlog.get_logger()


class PostgresClient:
    def __init__(self, host: str = "localhost", port: int = 5432, 
                 database: str = "featurestore", user: str = "featurestore", 
                 password: str = "featurestore"):
        self.conn_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        
        # Test connection
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            logger.info("PostgreSQL client connected", 
                       host=host, database=database)
        except Exception as e:
            logger.error("Failed to connect to PostgreSQL", error=str(e))
            raise
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = psycopg2.connect(**self.conn_params)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error("Database error", error=str(e))
            raise
        finally:
            conn.close()
    
    def store_offline_feature(self, entity_id: str, entity_type: str,
                              feature_name: str, feature_value: str,
                              computed_at: Optional[datetime] = None):
        """Store a feature value for offline access"""
        if computed_at is None:
            computed_at = datetime.utcnow()
        
        query = """
            INSERT INTO offline_features 
            (entity_id, entity_type, feature_name, feature_value, computed_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (entity_id, entity_type, feature_name, 
                                   str(feature_value), computed_at))
    
    def get_offline_feature(self, entity_id: str, entity_type: str,
                           feature_name: str, 
                           timestamp: Optional[datetime] = None) -> Optional[str]:
        """
        Get offline feature value at a specific point in time
        If no timestamp provided, gets the latest value
        """
        if timestamp:
            query = """
                SELECT feature_value, computed_at 
                FROM offline_features
                WHERE entity_id = %s 
                  AND entity_type = %s 
                  AND feature_name = %s
                  AND computed_at <= %s
                ORDER BY computed_at DESC
                LIMIT 1
            """
            params = (entity_id, entity_type, feature_name, timestamp)
        else:
            query = """
                SELECT feature_value, computed_at
                FROM offline_features
                WHERE entity_id = %s 
                  AND entity_type = %s 
                  AND feature_name = %s
                ORDER BY computed_at DESC
                LIMIT 1
            """
            params = (entity_id, entity_type, feature_name)
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result['feature_value'] if result else None
    
    def record_consistency_check(self, entity_id: str, entity_type: str,
                                 feature_name: str, online_value: Any,
                                 offline_value: Any, is_consistent: bool,
                                 difference: Optional[str] = None):
        """Record the result of an online/offline consistency check"""
        query = """
            INSERT INTO consistency_checks
            (check_time, entity_id, entity_type, feature_name, 
             online_value, offline_value, is_consistent, difference)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (
                    datetime.utcnow(),
                    entity_id,
                    entity_type,
                    feature_name,
                    str(online_value),
                    str(offline_value),
                    is_consistent,
                    difference
                ))
    
    def get_consistency_stats(self, hours: int = 24) -> Dict[str, Any]:
        """Get consistency check statistics for the last N hours"""
        query = """
            SELECT 
                COUNT(*) as total_checks,
                SUM(CASE WHEN is_consistent THEN 1 ELSE 0 END) as consistent_checks,
                AVG(CASE WHEN is_consistent THEN 1.0 ELSE 0.0 END) as consistency_rate
            FROM consistency_checks
            WHERE check_time > NOW() - INTERVAL '%s hours'
        """
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (hours,))
                return dict(cur.fetchone())