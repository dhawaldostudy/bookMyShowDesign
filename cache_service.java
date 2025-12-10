package com.bookmyshow.service;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * CacheService - Centralized Redis caching with patterns
 * 
 * Design Patterns Used:
 * 1. Cache-Aside (Lazy Loading)
 * 2. Write-Through Cache
 * 3. Distributed Locking (prevent cache stampede)
 * 
 * Cache Strategy:
 * - Seat availability: Short TTL (5 min), high read frequency
 * - Show timings: Medium TTL (1 hour), moderate read frequency
 * - Movie catalog: Long TTL (24 hours), low update frequency
 * 
 * Trade-offs:
 * + Pros: Reduces DB load, sub-millisecond latency
 * - Cons: Stale data risk, cache invalidation complexity
 */
@Service
public class CacheService {
    
    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);
    
    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper;
    
    // Cache TTLs (in seconds)
    private static final int SEAT_AVAILABILITY_TTL = 300; // 5 minutes
    private static final int SHOW_TIMINGS_TTL = 3600; // 1 hour
    private static final int MOVIE_CATALOG_TTL = 86400; // 24 hours
    private static final int LOCK_TTL = 10; // Distributed lock: 10 seconds
    
    public CacheService(JedisPool jedisPool, ObjectMapper objectMapper) {
        this.jedisPool = jedisPool;
        this.objectMapper = objectMapper;
    }
    
    /**
     * Cache-Aside Pattern: Get seat availability for a show
     * 
     * Flow:
     * 1. Try cache (fast path)
     * 2. Cache miss → Fetch from DB
     * 3. Populate cache
     * 4. Return data
     * 
     * Cache Stampede Protection:
     * - Use distributed lock to ensure only one thread fetches from DB
     * - Other threads wait briefly and retry
     * 
     * @param showId Show identifier
     * @param dataLoader Lambda to fetch from DB if cache miss
     * @return Map of seatId -> status (AVAILABLE, LOCKED, BOOKED)
     */
    public Map<Long, String> getSeatAvailability(Long showId, DataLoader<Map<Long, String>> dataLoader) {
        String cacheKey = "show:seats:" + showId;
        
        try (Jedis jedis = jedisPool.getResource()) {
            // 1. Try cache (O(1) lookup)
            Map<String, String> cachedData = jedis.hgetAll(cacheKey);
            
            if (!cachedData.isEmpty()) {
                logger.debug("Cache HIT for show {}", showId);
                return convertToLongMap(cachedData);
            }
            
            // 2. Cache MISS - acquire distributed lock
            logger.debug("Cache MISS for show {}", showId);
            String lockKey = "lock:" + cacheKey;
            
            // Try to acquire lock (NX = only if not exists, EX = expiry)
            String lockAcquired = jedis.set(lockKey, "1", SetParams.setParams().nx().ex(LOCK_TTL));
            
            if ("OK".equals(lockAcquired)) {
                // This thread acquired the lock - fetch from DB
                try {
                    logger.debug("Acquired lock for show {}, fetching from DB", showId);
                    Map<Long, String> data = dataLoader.load();
                    
                    // 3. Populate cache
                    if (!data.isEmpty()) {
                        Map<String, String> stringMap = convertToStringMap(data);
                        jedis.hset(cacheKey, stringMap);
                        jedis.expire(cacheKey, SEAT_AVAILABILITY_TTL);
                        logger.debug("Cached {} seats for show {}", data.size(), showId);
                    }
                    
                    return data;
                } finally {
                    // Release lock
                    jedis.del(lockKey);
                }
            } else {
                // Another thread is fetching - wait and retry
                logger.debug("Lock held by another thread, waiting...");
                Thread.sleep(100); // Wait 100ms
                return getSeatAvailability(showId, dataLoader); // Retry (will hit cache)
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while waiting for cache lock", e);
            return dataLoader.load(); // Fallback to DB
        } catch (Exception e) {
            logger.error("Cache error for show {}, falling back to DB", showId, e);
            return dataLoader.load(); // Cache failure - degrade gracefully
        }
    }
    
    /**
     * Write-Through Cache: Update seat status and invalidate cache
     * 
     * Used after booking/cancellation to ensure cache consistency
     * 
     * Alternative: Write-Behind (async cache update)
     * - Pros: Faster response time
     * - Cons: Brief inconsistency window
     * 
     * @param showId Show identifier
     */
    public void invalidateSeatAvailability(Long showId) {
        String cacheKey = "show:seats:" + showId;
        
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(cacheKey);
            logger.debug("Invalidated cache for show {}", showId);
        } catch (Exception e) {
            logger.error("Failed to invalidate cache for show {}", showId, e);
            // Non-critical: Cache will expire naturally
        }
    }
    
    /**
     * Cache show timings for a movie in a city on a specific date
     * 
     * Key pattern: movie:shows:{movieId}:{city}:{date}
     * Value: List of show details (JSON serialized)
     * 
     * @param movieId Movie identifier
     * @param city City name
     * @param date Date (YYYY-MM-DD)
     * @param dataLoader Lambda to fetch from DB/Elasticsearch
     * @return List of show details
     */
    public <T> List<T> getShowTimings(Long movieId, String city, String date, 
                                       DataLoader<List<T>> dataLoader, Class<T> clazz) {
        String cacheKey = String.format("movie:shows:%d:%s:%s", movieId, city, date);
        
        try (Jedis jedis = jedisPool.getResource()) {
            String cachedJson = jedis.get(cacheKey);
            
            if (cachedJson != null) {
                logger.debug("Cache HIT for movie {} shows in {} on {}", movieId, city, date);
                return objectMapper.readValue(
                    cachedJson, 
                    objectMapper.getTypeFactory().constructCollectionType(List.class, clazz)
                );
            }
            
            // Cache miss - fetch and store
            logger.debug("Cache MISS for movie {} shows", movieId);
            List<T> shows = dataLoader.load();
            
            if (!shows.isEmpty()) {
                String json = objectMapper.writeValueAsString(shows);
                jedis.setex(cacheKey, SHOW_TIMINGS_TTL, json);
                logger.debug("Cached {} shows for movie {}", shows.size(), movieId);
            }
            
            return shows;
        } catch (Exception e) {
            logger.error("Cache error for movie {} shows, falling back to DB", movieId, e);
            return dataLoader.load();
        }
    }
    
    /**
     * Cache movie catalog (search results)
     * 
     * Key pattern: movies:{city}:{genre}:{language}
     * Used for Browse page (high read, low write)
     * 
     * @param city City filter
     * @param genre Genre filter (can be null)
     * @param language Language filter (can be null)
     * @param dataLoader Lambda to fetch from Elasticsearch
     * @return List of movies
     */
    public <T> List<T> getMovieCatalog(String city, String genre, String language,
                                        DataLoader<List<T>> dataLoader, Class<T> clazz) {
        String cacheKey = String.format("movies:%s:%s:%s", 
            city, 
            genre != null ? genre : "all", 
            language != null ? language : "all"
        );
        
        try (Jedis jedis = jedisPool.getResource()) {
            String cachedJson = jedis.get(cacheKey);
            
            if (cachedJson != null) {
                logger.debug("Cache HIT for movie catalog: {}", cacheKey);
                return objectMapper.readValue(
                    cachedJson,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, clazz)
                );
            }
            
            logger.debug("Cache MISS for movie catalog: {}", cacheKey);
            List<T> movies = dataLoader.load();
            
            if (!movies.isEmpty()) {
                String json = objectMapper.writeValueAsString(movies);
                jedis.setex(cacheKey, MOVIE_CATALOG_TTL, json);
                logger.debug("Cached {} movies for {}", movies.size(), cacheKey);
            }
            
            return movies;
        } catch (Exception e) {
            logger.error("Cache error for movie catalog, falling back to search", e);
            return dataLoader.load();
        }
    }
    
    /**
     * Distributed rate limiting using token bucket algorithm
     * 
     * Pattern: Sliding Window Counter
     * - Each user gets N tokens per time window
     * - Each request consumes 1 token
     * - Tokens refill over time
     * 
     * Use case: Prevent abuse (100 bookings/user/minute)
     * 
     * @param userId User identifier
     * @param limit Max requests per window
     * @param windowSeconds Time window in seconds
     * @return true if request allowed, false if rate limited
     */
    public boolean checkRateLimit(Long userId, int limit, int windowSeconds) {
        String key = "ratelimit:user:" + userId;
        
        try (Jedis jedis = jedisPool.getResource()) {
            Long currentCount = jedis.incr(key);
            
            if (currentCount == 1) {
                // First request in window - set expiry
                jedis.expire(key, windowSeconds);
            }
            
            if (currentCount > limit) {
                logger.warn("Rate limit exceeded for user {}: {} requests", userId, currentCount);
                return false;
            }
            
            logger.debug("Rate limit check for user {}: {}/{}", userId, currentCount, limit);
            return true;
        } catch (Exception e) {
            logger.error("Rate limit check failed for user {}, allowing request", userId, e);
            return true; // Fail open - don't block user on cache failure
        }
    }
    
    /**
     * Store user session (JWT token validation)
     * 
     * Why cache sessions?
     * - Avoid DB lookup on every request
     * - Support token invalidation (logout)
     * 
     * @param sessionToken JWT token
     * @param userId User ID
     * @param ttlSeconds Session expiry (e.g., 30 minutes)
     */
    public void storeSession(String sessionToken, Long userId, int ttlSeconds) {
        String key = "session:" + sessionToken;
        
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(key, ttlSeconds, userId.toString());
            logger.debug("Stored session for user {}", userId);
        } catch (Exception e) {
            logger.error("Failed to store session", e);
        }
    }
    
    /**
     * Validate user session
     * 
     * @param sessionToken JWT token
     * @return userId if valid, null otherwise
     */
    public Long getSession(String sessionToken) {
        String key = "session:" + sessionToken;
        
        try (Jedis jedis = jedisPool.getResource()) {
            String userId = jedis.get(key);
            return userId != null ? Long.parseLong(userId) : null;
        } catch (Exception e) {
            logger.error("Failed to get session", e);
            return null;
        }
    }
    
    /**
     * Invalidate session (logout)
     */
    public void deleteSession(String sessionToken) {
        String key = "session:" + sessionToken;
        
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key);
            logger.debug("Deleted session {}", sessionToken);
        } catch (Exception e) {
            logger.error("Failed to delete session", e);
        }
    }
    
    // ==================== Helper Methods ====================
    
    private Map<Long, String> convertToLongMap(Map<String, String> stringMap) {
        Map<Long, String> result = new HashMap<>();
        stringMap.forEach((k, v) -> result.put(Long.parseLong(k), v));
        return result;
    }
    
    private Map<String, String> convertToStringMap(Map<Long, String> longMap) {
        Map<String, String> result = new HashMap<>();
        longMap.forEach((k, v) -> result.put(k.toString(), v));
        return result;
    }
    
    /**
     * Functional interface for data loading (lambda-friendly)
     */
    @FunctionalInterface
    public interface DataLoader<T> {
        T load();
    }
}

// ==================== Usage Example ====================

/**
 * Example: Using CacheService in ShowService
 */
@Service
class ShowServiceExample {
    
    private final CacheService cacheService;
    private final ShowSeatRepository showSeatRepository;
    
    public Map<Long, String> getSeatAvailability(Long showId) {
        return cacheService.getSeatAvailability(showId, () -> {
            // This lambda only executes on cache miss
            List<ShowSeat> seats = showSeatRepository.findByShowId(showId);
            Map<Long, String> result = new HashMap<>();
            seats.forEach(seat -> 
                result.put(seat.getSeat().getSeatId(), seat.getStatus().name())
            );
            return result;
        });
    }
}