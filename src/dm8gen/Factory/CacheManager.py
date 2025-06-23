"""
Optimized cache manager with TTL, size limits, and memory management.
Replaces the simple dict-based caching with proper cache eviction policies.
"""
import time
import threading
from typing import Any, Dict, Optional
from collections import OrderedDict
import logging

logger = logging.getLogger(__name__)


class CacheEntry:
    """Represents a single cache entry with metadata."""
    
    def __init__(self, value: Any, ttl: Optional[float] = None):
        self.value = value
        self.created_at = time.time()
        self.last_accessed = self.created_at
        self.access_count = 1
        self.ttl = ttl
    
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        if self.ttl is None:
            return False
        return time.time() - self.created_at > self.ttl
    
    def touch(self):
        """Update last accessed time and increment access count."""
        self.last_accessed = time.time()
        self.access_count += 1


class OptimizedCache:
    """
    Thread-safe cache with TTL, LRU eviction, and size limits.
    
    This cache provides:
    - TTL (Time To Live) for automatic expiration
    - LRU (Least Recently Used) eviction when size limit is reached
    - Thread-safe operations
    - Memory usage tracking
    - Configurable size limits
    """
    
    def __init__(self, max_size: int = 1000, default_ttl: Optional[float] = 3600):
        """
        Initialize the optimized cache.
        
        Args:
            max_size (int): Maximum number of items to store (default: 1000)
            default_ttl (Optional[float]): Default TTL in seconds (default: 1 hour)
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'expirations': 0
        }
        
        logger.debug(f"Initialized OptimizedCache with max_size={max_size}, ttl={default_ttl}")
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the cache.
        
        Args:
            key (str): Cache key
            
        Returns:
            Optional[Any]: Cached value or None if not found/expired
        """
        with self._lock:
            if key not in self._cache:
                self._stats['misses'] += 1
                return None
            
            entry = self._cache[key]
            
            # Check if expired
            if entry.is_expired():
                del self._cache[key]
                self._stats['expirations'] += 1
                self._stats['misses'] += 1
                return None
            
            # Update access info and move to end (most recently used)
            entry.touch()
            self._cache.move_to_end(key)
            self._stats['hits'] += 1
            
            return entry.value
    
    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """
        Set a value in the cache.
        
        Args:
            key (str): Cache key
            value (Any): Value to cache
            ttl (Optional[float]): TTL in seconds, uses default if None
        """
        with self._lock:
            ttl = ttl or self.default_ttl
            
            # If key exists, update it
            if key in self._cache:
                self._cache[key] = CacheEntry(value, ttl)
                self._cache.move_to_end(key)
                return
            
            # Check if we need to evict items
            while len(self._cache) >= self.max_size:
                self._evict_lru()
            
            # Add new entry
            self._cache[key] = CacheEntry(value, ttl)
    
    def _evict_lru(self) -> None:
        """Evict the least recently used item."""
        if self._cache:
            evicted_key, _ = self._cache.popitem(last=False)
            self._stats['evictions'] += 1
            logger.debug(f"Evicted LRU cache entry: {evicted_key}")
    
    def invalidate(self, key: str) -> bool:
        """
        Remove a specific key from the cache.
        
        Args:
            key (str): Cache key to remove
            
        Returns:
            bool: True if key was found and removed, False otherwise
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            cleared_count = len(self._cache)
            self._cache.clear()
            self._stats = {'hits': 0, 'misses': 0, 'evictions': 0, 'expirations': 0}
            logger.debug(f"Cleared cache with {cleared_count} entries")
    
    def cleanup_expired(self) -> int:
        """
        Remove all expired entries from the cache.
        
        Returns:
            int: Number of expired entries removed
        """
        with self._lock:
            expired_keys = []
            for key, entry in self._cache.items():
                if entry.is_expired():
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self._cache[key]
                self._stats['expirations'] += 1
            
            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
            
            return len(expired_keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dict[str, Any]: Cache statistics including hit rate, size, etc.
        """
        with self._lock:
            total_requests = self._stats['hits'] + self._stats['misses']
            hit_rate = (self._stats['hits'] / total_requests * 100) if total_requests > 0 else 0
            
            return {
                'size': len(self._cache),
                'max_size': self.max_size,
                'hit_rate': round(hit_rate, 2),
                'hits': self._stats['hits'],
                'misses': self._stats['misses'],
                'evictions': self._stats['evictions'],
                'expirations': self._stats['expirations']
            }
    
    def get_memory_usage(self) -> Dict[str, Any]:
        """
        Get approximate memory usage statistics.
        
        Returns:
            Dict[str, Any]: Memory usage information
        """
        with self._lock:
            import sys
            
            total_size = 0
            for key, entry in self._cache.items():
                total_size += sys.getsizeof(key)
                total_size += sys.getsizeof(entry)
                total_size += sys.getsizeof(entry.value)
            
            avg_entry_size = total_size / len(self._cache) if self._cache else 0
            
            return {
                'total_bytes': total_size,
                'avg_entry_bytes': round(avg_entry_size, 2),
                'entry_count': len(self._cache)
            }
    
    def __len__(self) -> int:
        """Return the number of items in the cache."""
        return len(self._cache)
    
    def __contains__(self, key: str) -> bool:
        """Check if a key exists in the cache (without updating access time)."""
        with self._lock:
            if key not in self._cache:
                return False
            return not self._cache[key].is_expired()


class CacheManager:
    """
    Global cache manager for the Model class.
    Provides optimized caches for different data types with proper management.
    """
    
    # Optimized caches replacing the simple dict caches
    _data_source_cache = OptimizedCache(max_size=500, default_ttl=3600)  # 1 hour TTL
    _attribute_types_cache = OptimizedCache(max_size=200, default_ttl=3600)
    _data_module_cache = OptimizedCache(max_size=300, default_ttl=3600)
    _data_types_cache = OptimizedCache(max_size=200, default_ttl=3600)
    
    @classmethod
    def get_data_source_cache(cls) -> OptimizedCache:
        """Get the data source cache."""
        return cls._data_source_cache
    
    @classmethod
    def get_attribute_types_cache(cls) -> OptimizedCache:
        """Get the attribute types cache."""
        return cls._attribute_types_cache
    
    @classmethod
    def get_data_module_cache(cls) -> OptimizedCache:
        """Get the data module cache."""
        return cls._data_module_cache
    
    @classmethod
    def get_data_types_cache(cls) -> OptimizedCache:
        """Get the data types cache."""
        return cls._data_types_cache
    
    @classmethod
    def clear_all_caches(cls) -> None:
        """Clear all managed caches."""
        cls._data_source_cache.clear()
        cls._attribute_types_cache.clear()
        cls._data_module_cache.clear()
        cls._data_types_cache.clear()
        logger.info("Cleared all managed caches")
    
    @classmethod
    def cleanup_all_expired(cls) -> int:
        """Clean up expired entries from all caches."""
        total_cleaned = 0
        total_cleaned += cls._data_source_cache.cleanup_expired()
        total_cleaned += cls._attribute_types_cache.cleanup_expired()
        total_cleaned += cls._data_module_cache.cleanup_expired()
        total_cleaned += cls._data_types_cache.cleanup_expired()
        
        if total_cleaned > 0:
            logger.info(f"Cleaned up {total_cleaned} expired entries from all caches")
        
        return total_cleaned
    
    @classmethod
    def get_global_stats(cls) -> Dict[str, Any]:
        """Get statistics for all managed caches."""
        return {
            'data_source': cls._data_source_cache.get_stats(),
            'attribute_types': cls._attribute_types_cache.get_stats(),
            'data_module': cls._data_module_cache.get_stats(),
            'data_types': cls._data_types_cache.get_stats()
        }
    
    @classmethod
    def get_global_memory_usage(cls) -> Dict[str, Any]:
        """Get memory usage for all managed caches."""
        memory_stats = {
            'data_source': cls._data_source_cache.get_memory_usage(),
            'attribute_types': cls._attribute_types_cache.get_memory_usage(),
            'data_module': cls._data_module_cache.get_memory_usage(),
            'data_types': cls._data_types_cache.get_memory_usage()
        }
        
        total_bytes = sum(cache['total_bytes'] for cache in memory_stats.values())
        total_entries = sum(cache['entry_count'] for cache in memory_stats.values())
        
        memory_stats['total'] = {
            'total_bytes': total_bytes,
            'total_entries': total_entries,
            'avg_bytes_per_entry': round(total_bytes / total_entries, 2) if total_entries > 0 else 0
        }
        
        return memory_stats