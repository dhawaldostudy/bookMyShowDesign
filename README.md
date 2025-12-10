# 🎬 Book My Show - System Design

[![Java](https://img.shields.io/badge/Java-17+-orange.svg)](https://www.oracle.com/java/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7.0-red.svg)](https://redis.io/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> A production-grade ticket booking system designed for high concurrency, strong consistency, and fault tolerance. Inspired by FAANG/MAANG interview system design expectations.

---

## 📖 Overview

**Book My Show** is a scalable online ticket booking platform for movies, events, and concerts. The system handles millions of concurrent users, prevents double-booking through optimistic locking, and ensures ACID compliance for payments.

### Key Features
- ✅ **Real-time Seat Availability** with Redis caching (sub-millisecond latency)
- ✅ **Atomic Seat Locking** using database constraints (no double-booking)
- ✅ **Two-Phase Commit** for payment + booking consistency
- ✅ **Distributed Caching** with cache-aside and write-through strategies
- ✅ **Circuit Breakers & Rate Limiting** for resilience
- ✅ **Horizontal Scalability** via database sharding (geo-based)
- ✅ **Event-Driven Architecture** with Kafka for notifications
- ✅ **Background Jobs** for lock expiry and analytics

### System Metrics
- **Capacity**: 10M+ concurrent users
- **Throughput**: 1,157 bookings/second (peak)
- **Latency**: <200ms API response time
- **Availability**: 99.99% uptime (4 nines)
- **Storage**: 4TB data, 12TB with replication

---

## 🏗️ High-Level Design (HLD)

### Architecture Diagram

```mermaid
graph TB
    subgraph "Client Layer"
        A[Web App<br/>React]
        B[Mobile Apps<br/>iOS/Android]
        C[Admin Dashboard]
    end
    
    subgraph "Edge Layer"
        D[CDN<br/>CloudFront]
        E[Load Balancer<br/>AWS ALB]
    end
    
    subgraph "API Layer"
        F[API Gateway<br/>Rate Limiting, Auth]
    end
    
    subgraph "Service Layer"
        G[User Service]
        H[Search Service]
        I[Booking Service]
        J[Payment Service]
        K[Notification Service]
    end
    
    subgraph "Data Layer"
        L[(PostgreSQL<br/>User DB)]
        M[(PostgreSQL<br/>Booking DB)]
        N[Redis<br/>Cache]
        O[Elasticsearch<br/>Search Index]
    end
    
    subgraph "Message Layer"
        P[Kafka<br/>Event Bus]
    end
    
    A --> D
    B --> D
    C --> D
    D --> E
    E --> F
    F --> G
    F --> H
    F --> I
    F --> J
    
    G --> L
    H --> O
    I --> M
    I --> N
    J --> I
    
    I --> P
    P --> K
    
    style I fill:#f9a825
    style M fill:#42a5f5
    style N fill:#ef5350
    style P fill:#66bb6a
```

### Data Flow: Seat Booking

```mermaid
sequenceDiagram
    participant U as User
    participant API as API Gateway
    participant BS as Booking Service
    participant C as Redis Cache
    participant DB as PostgreSQL
    participant PS as Payment Service
    participant MQ as Kafka
    participant NS as Notification Service
    
    U->>API: POST /bookings/lock
    API->>BS: lockSeats(showId, seatIds)
    
    BS->>C: Check seat availability (cache)
    alt Cache Hit
        C-->>BS: Seats available
    else Cache Miss
        BS->>DB: Query show_seats table
        DB-->>BS: Seat statuses
        BS->>C: Update cache
    end
    
    BS->>DB: UPDATE show_seats SET status='LOCKED' WHERE status='AVAILABLE'
    alt Seats Available
        DB-->>BS: Rows updated (success)
        BS->>DB: INSERT INTO bookings (status='PENDING', expiry=NOW()+10min)
        BS->>C: Invalidate cache
        BS-->>API: BookingResponse (bookingId, expiryTime)
        API-->>U: 201 Created
    else Race Condition (seat taken)
        DB-->>BS: 0 rows updated (conflict)
        BS-->>API: 409 Conflict
        API-->>U: Seat unavailable
    end
    
    U->>API: POST /bookings/confirm
    API->>BS: confirmBooking(bookingId, paymentDetails)
    BS->>PS: processPayment(amount, card)
    
    alt Payment Success
        PS-->>BS: PaymentResponse (success)
        BS->>DB: UPDATE bookings SET status='CONFIRMED'
        BS->>DB: UPDATE show_seats SET status='BOOKED'
        BS->>C: Invalidate cache
        BS->>MQ: Publish 'booking.confirmed' event
        MQ->>NS: Consume event
        NS->>U: Send email/SMS
        BS-->>API: BookingResponse (tickets, QR codes)
        API-->>U: 200 OK
    else Payment Failed
        PS-->>BS: PaymentResponse (failed)
        BS->>DB: UPDATE bookings SET status='CANCELLED'
        BS->>DB: UPDATE show_seats SET status='AVAILABLE'
        BS-->>API: 402 Payment Required
        API-->>U: Payment failed
    end
```

### Component Responsibilities

| Component | Responsibility | Technology | Scaling Strategy |
|-----------|---------------|------------|------------------|
| **API Gateway** | Rate limiting, auth, routing | Kong/AWS API Gateway | Horizontal (stateless) |
| **User Service** | Registration, login, profiles | Spring Boot + PostgreSQL | Read replicas (5x) |
| **Search Service** | Movie/show search | Spring Boot + Elasticsearch | Horizontal sharding by city |
| **Booking Service** | Seat locking, booking logic | Spring Boot + PostgreSQL | Geo-sharding (city-based) |
| **Payment Service** | Payment processing, refunds | Spring Boot + Stripe | Horizontal with idempotency |
| **Notification Service** | Email/SMS/Push notifications | Spring Boot + SendGrid/Twilio | Async processing (Kafka) |
| **Redis Cache** | Seat availability, sessions | Redis Cluster | 6 nodes (3 masters + 3 replicas) |
| **PostgreSQL** | Transactional data | PostgreSQL 15 | Sharding + read replicas |
| **Kafka** | Event bus for async tasks | Kafka Cluster | Partitioned by userId |

---

## 🔧 Low-Level Design (LLD)

### Database Schema

#### Core Tables

**Users Table**
```sql
CREATE TABLE users (
    user_id BIGSERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20) UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_email ON users(email);
```

**Bookings Table**
```sql
CREATE TABLE bookings (
    booking_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(user_id),
    show_id BIGINT REFERENCES shows(show_id),
    total_amount DECIMAL(10,2),
    booking_status VARCHAR(20) DEFAULT 'PENDING',
    payment_id VARCHAR(255),
    booking_time TIMESTAMP DEFAULT NOW(),
    expiry_time TIMESTAMP, -- Lock expiry (10 minutes)
    INDEX idx_user_booking (user_id, booking_time),
    INDEX idx_expiry (booking_status, expiry_time) WHERE booking_status='PENDING'
);
```

**Show_Seats Table** (Critical for concurrency)
```sql
CREATE TABLE show_seats (
    show_seat_id BIGSERIAL PRIMARY KEY,
    show_id BIGINT REFERENCES shows(show_id),
    seat_id BIGINT REFERENCES seats(seat_id),
    status VARCHAR(20) DEFAULT 'AVAILABLE', -- AVAILABLE, LOCKED, BOOKED
    price DECIMAL(10,2),
    locked_by BIGINT REFERENCES users(user_id),
    locked_at TIMESTAMP,
    booking_id BIGINT,
    UNIQUE(show_id, seat_id), -- Prevent duplicate seat entries
    INDEX idx_show_status (show_id, status)
);
```

### Class Diagram

```mermaid
classDiagram
    class User {
        +Long userId
        +String email
        +String phone
        +String passwordHash
        +String name
        +LocalDateTime createdAt
    }
    
    class Movie {
        +Long movieId
        +String title
        +String description
        +String language
        +String genre
        +Integer durationMinutes
        +LocalDate releaseDate
        +BigDecimal rating
    }
    
    class Show {
        +Long showId
        +Movie movie
        +Screen screen
        +LocalDateTime showTime
        +BigDecimal basePrice
        +Integer availableSeats
        +ShowStatus status
    }
    
    class Booking {
        +Long bookingId
        +User user
        +Show show
        +BigDecimal totalAmount
        +BookingStatus status
        +String paymentId
        +LocalDateTime bookingTime
        +LocalDateTime expiryTime
        +List~BookingSeat~ bookingSeats
        +confirmBooking()
        +cancelBooking()
        +isExpired()
    }
    
    class ShowSeat {
        +Long showSeatId
        +Show show
        +Seat seat
        +SeatStatus status
        +BigDecimal price
        +Long lockedBy
        +LocalDateTime lockedAt
        +Long bookingId
        +lock(userId)
        +release()
    }
    
    class BookingService {
        +lockSeats(request) BookingResponse
        +confirmBooking(request) BookingResponse
        +cancelBooking(bookingId)
        +getUserBookings(userId) List~Booking~
        -rollbackLockedSeats(seats)
        -invalidateCache(showId)
    }
    
    class CacheService {
        +getSeatAvailability(showId) Map
        +invalidateSeatAvailability(showId)
        +getShowTimings(movieId, city) List
        +checkRateLimit(userId, limit) boolean
        +storeSession(token, userId)
    }
    
    User "1" --> "*" Booking : places
    Show "1" --> "*" Booking : has
    Show "1" --> "*" ShowSeat : contains
    Booking "1" --> "*" BookingSeat : includes
    BookingService --> Booking : manages
    BookingService --> CacheService : uses
    BookingService --> ShowSeat : locks/releases
```

### Key Classes & Patterns

#### 1. Optimistic Locking (Seat Reservation)

**Pattern**: Optimistic Concurrency Control  
**Problem**: Multiple users selecting same seat simultaneously  
**Solution**: Database-level atomic UPDATE with WHERE clause

```java
// Critical: This UPDATE is atomic - no locks held during user think time
@Modifying
@Query("""
    UPDATE ShowSeat ss
    SET ss.status = 'LOCKED', ss.lockedBy = :userId, ss.lockedAt = :lockedAt
    WHERE ss.show.showId = :showId
      AND ss.seat.seatId = :seatId
      AND ss.status = 'AVAILABLE'
""")
int lockSeat(@Param("showId") Long showId, 
              @Param("seatId") Long seatId, 
              @Param("userId") Long userId, 
              @Param("lockedAt") LocalDateTime lockedAt);

// Returns:
// - 1 if seat was available and locked successfully
// - 0 if seat already locked (race condition detected)
```

**Why this works:**
- `WHERE status='AVAILABLE'` ensures atomicity
- Database isolation level (READ_COMMITTED) prevents dirty reads
- If two transactions try to lock same seat, only one succeeds
- No distributed locks needed (database handles it)

**Alternative Approaches:**

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| **Pessimistic Locking** (SELECT FOR UPDATE) | Strong consistency | Poor concurrency, holds locks during user payment | ❌ Too slow |
| **Distributed Locks** (Redlock) | Scalable, fast | Complex, risk of lost locks | ⚠️ Overkill |
| **Saga Pattern** (Eventual Consistency) | Highly scalable | Complex rollback, not real-time | ❌ Not suitable |
| **Optimistic + DB Constraints** | Simple, reliable, good concurrency | Requires retry logic | ✅ **Chosen** |

---

#### 2. Cache-Aside Pattern (Seat Availability)

**Pattern**: Lazy Loading with Cache Stampede Protection  
**Problem**: 10K users checking same show → DB overload  
**Solution**: Distributed lock ensures only one thread fetches from DB

```java
public Map<Long, String> getSeatAvailability(Long showId, DataLoader<Map<Long, String>> dataLoader) {
    String cacheKey = "show:seats:" + showId;
    
    try (Jedis jedis = jedisPool.getResource()) {
        // 1. Try cache (O(1) lookup)
        Map<String, String> cachedData = jedis.hgetAll(cacheKey);
        if (!cachedData.isEmpty()) {
            return convertToLongMap(cachedData); // Cache HIT
        }
        
        // 2. Cache MISS - acquire distributed lock
        String lockKey = "lock:" + cacheKey;
        String lockAcquired = jedis.set(lockKey, "1", SetParams.setParams().nx().ex(10));
        
        if ("OK".equals(lockAcquired)) {
            // This thread acquired the lock - fetch from DB
            try {
                Map<Long, String> data = dataLoader.load(); // DB query
                
                // 3. Populate cache
                jedis.hset(cacheKey, convertToStringMap(data));
                jedis.expire(cacheKey, 300); // 5-minute TTL
                return data;
            } finally {
                jedis.del(lockKey); // Release lock
            }
        } else {
            // Another thread is fetching - wait and retry
            Thread.sleep(100);
            return getSeatAvailability(showId, dataLoader); // Retry
        }
    }
}
```

**Cache Invalidation Strategy:**
- **Write-Through**: Update DB → Invalidate cache immediately
- **TTL**: Auto-expire after 5 minutes (safety net)
- **Pub/Sub**: Broadcast `seat.updated` event to all instances

---

#### 3. Two-Phase Commit (Payment + Booking)

**Pattern**: Distributed Transaction with Idempotency  
**Problem**: Payment succeeds but booking update fails → inconsistency  
**Solution**: Idempotent payment + rollback on failure

```java
@Transactional
public BookingResponse confirmBooking(ConfirmBookingRequest request) {
    // Phase 1: Prepare (validate booking)
    Booking booking = bookingRepository.findById(request.getBookingId())
        .orElseThrow(() -> new BookingNotFoundException("Booking not found"));
    
    if (booking.getExpiryTime().isBefore(LocalDateTime.now())) {
        cancelBooking(booking.getBookingId());
        throw new BookingExpiredException("Lock expired");
    }
    
    // Phase 2: Commit (process payment)
    PaymentResponse paymentResponse;
    try {
        paymentResponse = paymentService.processPayment(
            PaymentRequest.builder()
                .bookingId(booking.getBookingId())
                .amount(booking.getTotalAmount())
                .idempotencyKey(booking.getBookingId().toString()) // Prevent double-charge
                .build()
        );
    } catch (Exception e) {
        // Rollback: Cancel booking and release seats
        cancelBooking(booking.getBookingId());
        throw new PaymentFailedException("Payment failed: " + e.getMessage());
    }
    
    if (paymentResponse.getStatus() != PaymentStatus.SUCCESS) {
        cancelBooking(booking.getBookingId());
        throw new PaymentFailedException("Payment declined");
    }
    
    // Phase 3: Finalize (update booking)
    booking.setBookingStatus(BookingStatus.CONFIRMED);
    booking.setPaymentId(paymentResponse.getPaymentId());
    bookingRepository.save(booking);
    
    // Update seats to BOOKED (permanent)
    showSeatRepository.updateSeatsStatus(
        booking.getBookingSeats().stream()
            .map(bs -> bs.getShowSeat().getShowSeatId())
            .toList(),
        SeatStatus.BOOKED,
        booking.getBookingId()
    );
    
    return buildBookingResponse(booking);
}
```

**Idempotency Key:**
- Uses `bookingId` as idempotency key
- Payment gateway deduplicates retries (network failures)
- Safe to call multiple times (returns same result)

---

#### 4. Background Job (Lock Expiry Worker)

**Pattern**: Scheduled Job with Batch Processing  
**Problem**: Users abandon payment → seats stuck in LOCKED state  
**Solution**: Cron job releases expired locks every minute

```java
@Scheduled(fixedDelay = 60000, initialDelay = 10000) // Every 60s
@Transactional
public void expireBookings() {
    // 1. Find expired bookings (batch of 100)
    List<Booking> expiredBookings = bookingRepository.findExpiredBookings(
        BookingStatus.PENDING,
        LocalDateTime.now()
    );
    
    if (expiredBookings.isEmpty()) {
        return;
    }
    
    logger.info("Found {} expired bookings to cancel", expiredBookings.size());
    
    // 2. Cancel each booking (releases seats, invalidates cache)
    for (Booking booking : expiredBookings) {
        try {
            bookingService.cancelBooking(booking.getBookingId());
        } catch (Exception e) {
            logger.error("Failed to cancel booking {}", booking.getBookingId(), e);
        }
    }
}
```

**Why Scheduled Job?**
- Simple, reliable, no external dependencies
- Acceptable delay: Seats released within 1 min of expiry
- Alternative: Redis TTL + Pub/Sub (more complex, less reliable)

---

### API Endpoints

#### Booking APIs

```http
POST /api/v1/bookings/lock
Content-Type: application/json
Authorization: Bearer <token>

{
  "showId": 12345,
  "seatIds": [101, 102, 103]
}

Response: 201 Created
{
  "success": true,
  "data": {
    "bookingId": "BKG123456",
    "status": "LOCKED",
    "expiryTime": "2024-01-15T10:15:00Z",
    "totalAmount": 450.00,
    "seats": [
      {"seatId": 101, "seatNumber": "A1", "price": 150.00},
      {"seatId": 102, "seatNumber": "A2", "price": 150.00},
      {"seatId": 103, "seatNumber": "A3", "price": 150.00}
    ]
  }
}
```

```http
POST /api/v1/bookings/confirm
Content-Type: application/json
Authorization: Bearer <token>

{
  "bookingId": "BKG123456",
  "paymentMethod": "CARD",
  "paymentDetails": {
    "cardToken": "tok_abc123",
    "cardLast4": "1234"
  }
}

Response: 200 OK
{
  "success": true,
  "data": {
    "bookingId": "BKG123456",
    "status": "CONFIRMED",
    "paymentId": "PAY789012",
    "tickets": [
      {
        "ticketId": "TKT001",
        "seatNumber": "A1",
        "qrCode": "data:image/png;base64,..."
      }
    ]
  }
}
```

---

## 🚀 Setup Instructions

### Prerequisites
- **Java 17+**
- **Maven 3.8+**
- **Docker & Docker Compose**
- **PostgreSQL 15**
- **Redis 7.0**
- **Kafka 3.5**

### Local Development Setup

```bash
# 1. Clone repository
git clone https://github.com/yourusername/bookmyshow.git
cd bookmyshow

# 2. Start infrastructure (PostgreSQL, Redis, Kafka)
docker-compose up -d

# 3. Create database and run migrations
psql -U postgres -c "CREATE DATABASE bookmyshow;"
mvn flyway:migrate

# 4. Build project
mvn clean install

# 5. Run application
mvn spring-boot:run

# 6. Access application
# API: http://localhost:8080/api/v1
# Swagger UI: http://localhost:8080/swagger-ui.html
# Actuator: http://localhost:8080/actuator
```

### Docker Compose Configuration

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15-alpine
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
      POSTGRES_DB: bookmyshow
    volumes:
      - postgres_data:/var/lib/postgresql/data
  
  redis:
    image: redis:7.0-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes
    volumes:
      - redis_data:/data
  
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

volumes:
  postgres_data:
  redis_data:
```

### Running Tests

```bash
# Unit tests
mvn test

# Integration tests (requires Docker)
mvn verify

# Load tests (using Gatling)
mvn gatling:test

# Code coverage
mvn jacoco:report
# Report: target/site/jacoco/index.html
```

---

## 📊 Observability & Monitoring

### Metrics (Prometheus + Grafana)

**Key Metrics:**
```yaml
# Golden Signals
- http_server_requests_seconds{uri="/api/v1/bookings/lock"} # Latency (P50, P95, P99)
- http_server_requests_total{status="2xx"} # Traffic (requests/sec)
- http_server_requests_total{status="5xx"} # Errors (error rate)
- system_cpu_usage # Saturation (CPU utilization)

# Business Metrics
- bookings_created_total # Total bookings
- bookings_confirmed_total # Conversion rate
- seats_locked_total # Seat locks
- seats_expired_total # Abandoned bookings
- payment_success_rate # Payment success %
```

**Grafana Dashboard:**
```json
{
  "dashboard": {
    "title": "BookMyShow - Booking Service",
    "panels": [
      {
        "title": "Booking Rate",
        "targets": ["rate(bookings_created_total[5m])"]
      },
      {
        "title": "API Latency (P95)",
        "targets": ["histogram_quantile(0.95, http_server_requests_seconds_bucket)"]
      },
      {
        "title": "Error Rate",
        "targets": ["rate(http_server_requests_total{status=~'5..'}[5m])"]
      }
    ]
  }
}
```

### Logging (ELK Stack)

**Structured Logging:**
```java
logger.info("Booking confirmed", 
    keyValue("bookingId", bookingId),
    keyValue("userId", userId),
    keyValue("amount", amount),
    keyValue("duration", duration)
);

// Output (JSON):
{
  "timestamp": "2024-01-15T10:05:23.456Z",
  "level": "INFO",
  "message": "Booking confirmed",
  "bookingId": "BKG123456",
  "userId": 67890,
  "amount": 450.00,
  "duration": 235,
  "traceId": "abc123def456",
  "spanId": "789ghi012jkl"
}
```

### Distributed Tracing (Jaeger)

**Trace Example:**
```
bookings/lock (250ms)
  ├─ validate_show (5ms)
  ├─ check_cache (2ms)
  ├─ lock_seats (120ms) ← Bottleneck!
  │   ├─ db_query (100ms)
  │   └─ cache_invalidate (20ms)
  ├─ create_booking (15ms)
  └─ response (3ms)
```

### Alerting (PagerDuty)

**Alert Rules:**
```yaml
groups:
  - name: booking_service
    rules:
      - alert: HighErrorRate
        expr: rate(http_server_requests_total{status=~"5.."}[5m]) > 0.05
        for: 5m
        labels:
          severity: P1
        annotations:
          summary: "Error rate > 5% for 5 minutes"
      
      - alert: DatabaseConnectionPoolExhausted
        expr: hikaricp_connections_active / hikaricp_connections_max > 0.9
        for: 2m
        labels:
          severity: P0
        annotations:
          summary: "DB connection pool > 90% for 2 minutes"
      
      - alert: PaymentGatewayDown
        expr: up{job="payment_service"} == 0
        for: 1m
        labels:
          severity: P0
        annotations:
          summary: "Payment service unreachable"
```

---

## 🧩 Design Patterns & Rationale

### 1. Service Layer Pattern

**Purpose:** Encapsulate business logic, coordinate between repositories and external services  
**Example:** `BookingService` orchestrates seat locking, payment, and notifications

**Alternatives:**
- **Transaction Script:** Simple, but logic scattered across controllers (hard to test)
- **Domain Model:** Rich domain objects, but complex for this use case
- **Chosen:** Service Layer - balance between simplicity and maintainability

---

### 2. Repository Pattern

**Purpose:** Abstract data access, decouple business logic from persistence  
**Example:** `BookingRepository` provides `findExpiredBookings()` method

**Alternatives:**
- **Active Record:** Models handle their own persistence (tight coupling)
- **Data Mapper:** Full ORM control, but boilerplate-heavy
- **Chosen:** Repository - clean separation, testable with mocks

---

### 3. DTO (Data Transfer Object) Pattern

**Purpose:** Define API contracts, decouple internal models from external representation  
**Example:** `LockSeatsRequest`, `BookingResponse`

**Alternatives:**
- **Expose Domain Entities:** Simple, but leaks internal structure, breaks encapsulation
- **GraphQL:** Client-defined queries, but adds complexity
- **Chosen:** DTOs - explicit contracts, versioning support

---

### 4. Circuit Breaker Pattern (Resilience4j)

**Purpose:** Fail fast when downstream service is unhealthy, prevent cascading failures  
**Example:** Payment service failures open circuit → stop calling payment API

**Configuration:**
```java
@CircuitBreaker(
    name = "paymentService",
    fallbackMethod = "paymentFallback"
)
public BookingResponse confirmBooking(ConfirmBookingRequest request) {
    // Call payment service
}

// Fallback method
public BookingResponse paymentFallback(ConfirmBookingRequest request, Exception e) {
    // Return user-friendly error
    throw new PaymentFailedException("Payment service unavailable. Please try again later.");
}
```

**States:**
- **Closed:** Normal operation (all requests pass through)
- **Open:** Service failing (fail fast, return error immediately)
- **Half-Open:** Test if service recovered (allow 1 request)

**Alternatives:**
- **Retry Logic:** Simple, but wastes resources on failing service
- **Timeout:** Prevents hanging, but still makes slow calls
- **Chosen:** Circuit Breaker - stops calling failing service, saves resources

---

### 5. Cache-Aside Pattern

**Purpose:** Improve performance by caching frequently accessed data  
**Example:** Seat availability cached in Redis (5-min TTL)

**Flow:**
1. Check cache (fast path)
2. Cache miss → Fetch from DB
3. Populate cache
4. Return data

**Alternatives:**
- **Write-Through:** Update cache + DB together (consistent but slower writes)
- **Write-Behind:** Update cache, async DB write (fastest but risky)
- **Chosen:** Cache-Aside - best read performance, simple invalidation

---

### 6. Event-Driven Architecture (Kafka)

**Purpose:** Decouple services, enable asynchronous processing  
**Example:** Booking confirmed → Publish event → Notification service sends email

**Benefits:**
- **Scalability:** Notification service scales independently
- **Reliability:** Kafka persists events (delivery guaranteed)
- **Extensibility:** Add analytics service without changing the booking service

**Alternatives:**
- **Synchronous API calls:** Simple, but tight coupling, slower response
- **HTTP webhooks:** Async, but no delivery guarantee
- **Chosen:** Kafka - reliable, scalable, industry-standard

---

## 📚 Interview Q&A

### Q1: How do you prevent double-booking?

**Answer:**  
Use **optimistic locking** with database constraints. The key is an atomic UPDATE query:

```sql
UPDATE show_seats 
SET status = 'LOCKED', locked_by = :userId 
WHERE show_id = :showId 
  AND seat_id = :seatId 
  AND status = 'AVAILABLE';
```

If two users try to book the same seat simultaneously:
- User A's UPDATE succeeds (returns 1 row updated)
- User B's UPDATE fails (returns 0 rows) because the seat is no longer 'AVAILABLE'
- User B gets a "Seat unavailable" error and must select another seat

**Why not pessimistic locking (SELECT FOR UPDATE)?**  
Pessimistic locking holds database locks during the entire payment flow (could be 5+ minutes). This kills concurrency and causes lock timeouts.

**Why not distributed locks (Redis Redlock)?**  
Adds complexity, requires external coordination. Database constraints are simpler and more reliable.
### Q2: How does your caching strategy handle cache invalidation?

**Answer:**  
We use a **multi-layered invalidation strategy**:

1. **Immediate Invalidation (Write-Through):**
   - On seat lock/unlock/booking → Delete cache key immediately
   - Ensures next read gets fresh data from DB
   
2. **TTL-Based Expiry:**
   - All cache entries have 5-minute TTL as safety net
   - Prevents stale data if invalidation fails
   
3. **Pub/Sub for Multi-Instance:**
```java
   // When seat status changes
   redisPublisher.publish("seat.updated", showId);
   
   // All instances listen
   @RedisMessageListener(channel = "seat.updated")
   public void onSeatUpdate(Long showId) {
       cacheService.invalidate("show:seats:" + showId);
   }
```

**Why not update cache instead of invalidate?**  
Updating cache risks race conditions (write-write conflicts). Invalidation is safer—next read will fetch correct data from DB.

---

### Q3: What happens if payment succeeds but the booking update fails?

**Answer:**  
This is the **distributed transaction problem**. Our solution uses **idempotent payments + compensating transactions**:

**Success Path:**
1. Lock seats → Create PENDING booking
2. Call payment service (with `idempotencyKey = bookingId`)
3. Payment succeeds → Update booking to CONFIRMED
4. Update seats to BOOKED

**Failure Scenarios:**

| Failure Point | Recovery Strategy |
|--------------|-------------------|
| Payment API call fails (network) | Retry with same `idempotencyKey` (payment gateway deduplicates) |
| Payment succeeds but DB update fails | Background reconciliation job queries payment gateway, updates DB |
| Payment fails | Rollback: Cancel booking, release seats to AVAILABLE |

**Reconciliation Job (runs every 5 min):**
```java
// Find bookings with successful payment but still PENDING
List inconsistentBookings = bookingRepository
    .findByStatusAndPaymentIdNotNull(BookingStatus.PENDING);

for (Booking booking : inconsistentBookings) {
    PaymentStatus status = paymentService.getPaymentStatus(booking.getPaymentId());
    if (status == PaymentStatus.SUCCESS) {
        booking.setBookingStatus(BookingStatus.CONFIRMED);
        bookingRepository.save(booking);
    }
}
```

**Why not 2PC (Two-Phase Commit)?**  
Traditional 2PC requires distributed locks across services (payment + booking), which is slow and brittle. Our approach uses idempotency + eventual consistency for reliability.

---

### Q4: How do you handle high traffic during movie premieres (10M concurrent users)?

**Answer:**  
Multi-pronged approach:

**1. Horizontal Scaling:**
- **API Layer:** Stateless Spring Boot instances behind ALB (auto-scale based on CPU)
- **Database:** Read replicas (5x) for search queries, write traffic to master
- **Cache:** Redis Cluster (3 masters + 3 replicas) distributes load

**2. Database Sharding:**
- **Shard Key:** `city` (geo-based)
- **Rationale:** Users typically book shows in their city
- **Example:** Mumbai users → Shard 1, Delhi users → Shard 2
- **Query Routing:** Application layer routes based on `cityId`
```java
@Bean
public DataSource dataSource() {
    Map shards = new HashMap<>();
    shards.put("MUMBAI", mumbaiDataSource());
    shards.put("DELHI", delhiDataSource());
    shards.put("BANGALORE", bangaloreDataSource());
    
    ShardingDataSource ds = new ShardingDataSource();
    ds.setTargetDataSources(shards);
    ds.setDefaultTargetDataSource(mumbaiDataSource());
    return ds;
}
```

**3. Rate Limiting:**
- **User-Level:** 10 booking attempts per minute (prevents abuse)
- **API-Level:** 1000 requests/sec per instance (circuit breaker opens beyond this)
```java
@RateLimiter(name = "bookingApi", fallbackMethod = "rateLimitFallback")
public BookingResponse lockSeats(LockSeatsRequest request) {
    // Booking logic
}
```

**4. Queue-Based Fairness:**
- Use virtual waiting room (AWS CloudFront + Lambda@Edge) for premiere shows
- Users get a queue position, admitted in batches to prevent thundering herd

**5. CDN for Static Assets:**
- Movie posters, show timings cached on CloudFront (reduces origin load by 90%)

---

### Q5: How do you ensure data consistency across microservices?

**Answer:**  
We use **Saga Pattern with Choreography** for cross-service transactions:

**Scenario:** Booking confirmation involves 3 services:
1. Booking Service (update booking)
2. Payment Service (charge card)
3. Notification Service (send email)

**Event Flow:**
```mermaid
sequenceDiagram
    participant BS as Booking Service
    participant Kafka as Kafka
    participant PS as Payment Service
    participant NS as Notification Service
    
    BS->>Kafka: Publish 'booking.initiated' event
    Kafka->>PS: Consume event
    PS->>PS: Process payment
    
    alt Payment Success
        PS->>Kafka: Publish 'payment.success' event
        Kafka->>BS: Consume event
        BS->>BS: Confirm booking
        BS->>Kafka: Publish 'booking.confirmed' event
        Kafka->>NS: Consume event
        NS->>NS: Send email/SMS
    else Payment Failure
        PS->>Kafka: Publish 'payment.failed' event
        Kafka->>BS: Consume event
        BS->>BS: Cancel booking, release seats
    end
```

**Compensating Transactions:**
- Each service maintains its own database (no shared DB)
- Failed step → Publish compensating event (e.g., `booking.cancelled`)
- Services roll back their local state independently

**Eventual Consistency Trade-offs:**
- **Pro:** High availability, services independently scalable
- **Con:** Short delay (100-500ms) before all services consistent
- **Acceptable for:** Notification delays are fine; booking state must be immediate (handled via DB transaction)

**Alternatives Considered:**

| Pattern | Pros | Cons | Verdict |
|---------|------|------|---------|
| **Distributed 2PC** | Strong consistency | Slow, single point of failure | ❌ Too rigid |
| **Orchestration (workflow engine)** | Centralized control | Single point of failure, complex | ⚠️ Overkill |
| **Choreography (events)** | Decoupled, scalable | Harder to debug | ✅ **Chosen** |

---

### Q6: How do you handle seat lock expiry at scale (millions of locks)?

**Answer:**  
**Scheduled Job with Batch Processing** (runs every 60 seconds):
```java
@Scheduled(fixedDelay = 60000)
@Transactional
public void expireBookings() {
    LocalDateTime now = LocalDateTime.now();
    
    // Find expired bookings (PENDING + expiry_time < now)
    // Index on (booking_status, expiry_time) makes this fast
    List expired = bookingRepository.findExpiredBookings(
        BookingStatus.PENDING, 
        now,
        PageRequest.of(0, 1000) // Process 1000 at a time
    );
    
    for (Booking booking : expired) {
        cancelBooking(booking.getBookingId()); // Releases seats, invalidates cache
    }
}
```

**Why not Redis TTL + Keyspace Notifications?**

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| **Redis TTL** | Real-time expiry | Keyspace notifications unreliable (can miss events), requires separate worker to update DB | ❌ Not reliable |
| **Scheduled Job** | Simple, reliable, no external dependencies | 60s delay (acceptable—seat released within 1 min) | ✅ **Chosen** |
| **Database Trigger** | Automatic, real-time | Complex, hard to test, database-specific | ❌ Too complex |

**Optimization:** Index on `(booking_status, expiry_time)` ensures query is fast even with millions of bookings.

---

### Q7: What's your database indexing strategy?

**Answer:**  
**Indexes are designed based on query patterns:**

**1. Covering Index for Seat Availability:**
```sql
CREATE INDEX idx_show_status ON show_seats(show_id, status);
-- Query: SELECT * FROM show_seats WHERE show_id = ? AND status = 'AVAILABLE'
-- This index covers the WHERE clause (no table scan needed)
```

**2. Composite Index for User Bookings:**
```sql
CREATE INDEX idx_user_booking ON bookings(user_id, booking_time DESC);
-- Query: SELECT * FROM bookings WHERE user_id = ? ORDER BY booking_time DESC
-- Index supports both filtering and sorting
```

**3. Partial Index for Lock Expiry:**
```sql
CREATE INDEX idx_expiry ON bookings(expiry_time) 
WHERE booking_status = 'PENDING';
-- Only indexes PENDING bookings (smaller index, faster queries)
```

**4. Unique Constraint (Prevents Bugs):**
```sql
CREATE UNIQUE INDEX idx_unique_show_seat ON show_seats(show_id, seat_id);
-- Prevents accidentally creating duplicate seat entries
```

**Trade-offs:**
- **Indexes speed up reads** but slow down writes (UPDATE/INSERT must update indexes)
- **Rule of thumb:** Index foreign keys and WHERE/ORDER BY columns
- **Monitor:** Use `EXPLAIN ANALYZE` to verify index usage

---

### Q8: How do you test concurrency scenarios (race conditions)?

**Answer:**  
**Multi-threaded integration tests** using JUnit + CountDownLatch:
```java
@Test
public void testConcurrentSeatLocking_ShouldPreventDoubleBooking() throws Exception {
    Long showId = 12345L;
    Long seatId = 101L;
    int numThreads = 100;
    
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);
    
    // Spawn 100 threads trying to lock same seat
    for (int i = 0; i < numThreads; i++) {
        final long userId = i;
        executor.submit(() -> {
            try {
                startLatch.await(); // All threads wait here
                
                LockSeatsRequest request = new LockSeatsRequest(
                    showId, 
                    List.of(seatId), 
                    userId
                );
                
                bookingService.lockSeats(request);
                successCount.incrementAndGet();
            } catch (SeatUnavailableException e) {
                failureCount.incrementAndGet();
            } finally {
                doneLatch.countDown();
            }
        });
    }
    
    startLatch.countDown(); // Release all threads simultaneously
    doneLatch.await(); // Wait for all to finish
    
    // Verify: Only ONE thread succeeded, 99 failed
    assertEquals(1, successCount.get());
    assertEquals(99, failureCount.get());
}
```

**Load Testing with Gatling:**
```scala
scenario("Concurrent Booking")
  .exec(http("Lock Seats")
    .post("/api/v1/bookings/lock")
    .body(StringBody("""{"showId": 12345, "seatIds": [101]}"""))
    .check(status.in(201, 409))) // 201 = success, 409 = conflict
  .inject(
    rampUsers(10000) during (60 seconds) // 10K users over 1 min
  )
```

**Why this matters:**  
Race conditions are the #1 cause of production bugs in booking systems. Testing at scale catches issues before launch.

---

### Q9: How would you extend this system to support dynamic pricing?

**Answer:**  
**Strategy Pattern + Event-Driven Pricing Engine**

**Architecture:**
```java
public interface PricingStrategy {
    BigDecimal calculatePrice(Show show, Seat seat, LocalDateTime bookingTime);
}

@Component
public class DynamicPricingStrategy implements PricingStrategy {
    @Override
    public BigDecimal calculatePrice(Show show, Seat seat, LocalDateTime bookingTime) {
        BigDecimal basePrice = seat.getBasePrice();
        
        // Factor 1: Demand (seats remaining)
        double occupancy = show.getOccupancyRate();
        double demandMultiplier = occupancy > 0.8 ? 1.5 : 1.0;
        
        // Factor 2: Time to show (surge pricing)
        long hoursToShow = Duration.between(bookingTime, show.getShowTime()).toHours();
        double urgencyMultiplier = hoursToShow < 24 ? 1.3 : 1.0;
        
        // Factor 3: Day of week (weekend premium)
        double weekendMultiplier = show.getShowTime().getDayOfWeek().getValue() >= 6 ? 1.2 : 1.0;
        
        return basePrice
            .multiply(BigDecimal.valueOf(demandMultiplier))
            .multiply(BigDecimal.valueOf(urgencyMultiplier))
            .multiply(BigDecimal.valueOf(weekendMultiplier))
            .setScale(2, RoundingMode.HALF_UP);
    }
}
```

**Real-time Price Updates:**
- Kafka streams aggregate booking events every 5 min
- Calculate new prices based on demand
- Update `show_seats.price` + invalidate cache
- Users see updated prices on refresh

**Challenges:**
- **Fairness:** Users in checkout shouldn't see price change mid-payment
  - **Solution:** Lock price at seat selection, honor for 10 minutes
- **Transparency:** Show "Price may change" warning
- **Regulations:** Some regions ban dynamic pricing (surge pricing laws)

---

### Q10: How do you handle database failover?

**Answer:**  
**PostgreSQL Streaming Replication + Automatic Failover (Patroni)**

**Setup:**
- **Primary (Master):** Handles all writes
- **Standby Replicas (5x):** Asynchronous replication for reads
- **Patroni:** Distributed consensus (etcd/Consul) monitors health

**Failover Trigger:**
- Primary becomes unresponsive (heartbeat timeout)
- Patroni promotes healthiest standby to new primary
- Application reconnects via virtual IP (no code changes)

**Downtime:** 30-60 seconds (acceptable for 99.99% SLA)

**Connection Pooling (HikariCP):**
```java
hikari.connectionTimeout=30000  // 30s timeout
hikari.maxLifetime=1800000      // 30 min max connection age
hikari.validationTimeout=5000   // Test connection before use
```

**Circuit Breaker Integration:**
- If DB unreachable → Open circuit → Fail fast
- Return cached data where possible (degraded mode)
- User sees: "Service temporarily unavailable. Please retry."

**Why not Multi-Master (Active-Active)?**  
Conflict resolution is complex for booking system (requires CRDTs or last-write-wins, unacceptable for seat booking).

---

### Q11: What metrics would you monitor in production?

**Answer:**  
**Four Golden Signals (Google SRE):**

**1. Latency:**
```promql
# P99 latency for booking API
histogram_quantile(0.99, 
  rate(http_server_requests_seconds_bucket{uri="/api/v1/bookings/lock"}[5m])
)
```
**Alert:** P99 > 500ms for 5 minutes

**2. Traffic:**
```promql
# Requests per second
rate(http_server_requests_total[1m])
```
**Alert:** Sudden drop (>50% decrease) indicates outage

**3. Errors:**
```promql
# Error rate
rate(http_server_requests_total{status=~"5.."}[5m]) 
/ rate(http_server_requests_total[5m])
```
**Alert:** Error rate > 1% for 5 minutes

**4. Saturation:**
```promql
# Database connection pool utilization
hikaricp_connections_active / hikaricp_connections_max
```
**Alert:** Utilization > 80% for 2 minutes

**Business Metrics:**
- **Conversion Rate:** `confirmed_bookings / seat_locks` (target: >60%)
- **Lock Expiry Rate:** `expired_locks / total_locks` (target: <20%)
- **Payment Success Rate:** `successful_payments / attempted_payments` (target: >95%)

**Custom Dashboards:**
- Real-time bookings per show (detect hot shows)
- Average booking amount (revenue tracking)
- Regional breakdown (Mumbai vs Delhi bookings)

---

### Q12: How would you implement a referral/discount system?

**Answer:**  
**Coupon Service + Idempotent Application**

**Database Schema:**
```sql
CREATE TABLE coupons (
    coupon_id BIGSERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    discount_type VARCHAR(20), -- PERCENTAGE, FIXED_AMOUNT
    discount_value DECIMAL(10,2),
    max_usage_per_user INT DEFAULT 1,
    expiry_date TIMESTAMP,
    min_order_amount DECIMAL(10,2),
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE coupon_usage (
    usage_id BIGSERIAL PRIMARY KEY,
    coupon_id BIGINT REFERENCES coupons(coupon_id),
    user_id BIGINT REFERENCES users(user_id),
    booking_id BIGINT REFERENCES bookings(booking_id),
    used_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(coupon_id, user_id, booking_id) -- Prevent double-application
);
```

**Application Logic:**
```java
public BigDecimal applyCoupon(String couponCode, Long userId, BigDecimal orderAmount) {
    Coupon coupon = couponRepository.findByCode(couponCode)
        .orElseThrow(() -> new CouponNotFoundException("Invalid coupon"));
    
    // Validations
    if (!coupon.isActive()) throw new CouponInactiveException();
    if (coupon.getExpiryDate().isBefore(LocalDateTime.now())) 
        throw new CouponExpiredException();
    if (orderAmount.compareTo(coupon.getMinOrderAmount()) < 0) 
        throw new MinOrderNotMetException();
    
    // Check usage limit
    int usageCount = couponUsageRepository.countByUserIdAndCouponId(userId, coupon.getCouponId());
    if (usageCount >= coupon.getMaxUsagePerUser()) 
        throw new CouponLimitExceededException();
    
    // Calculate discount
    BigDecimal discount = coupon.getDiscountType() == DiscountType.PERCENTAGE
        ? orderAmount.multiply(coupon.getDiscountValue()).divide(BigDecimal.valueOf(100))
        : coupon.getDiscountValue();
    
    return orderAmount.subtract(discount).max(BigDecimal.ZERO); // Price can't go negative
}

@Transactional
public void recordCouponUsage(Long couponId, Long userId, Long bookingId) {
    // Unique constraint prevents race condition (double-apply)
    couponUsageRepository.save(new CouponUsage(couponId, userId, bookingId));
}
```

**Referral System Extension:**
```sql
CREATE TABLE referrals (
    referral_id BIGSERIAL PRIMARY KEY,
    referrer_id BIGINT REFERENCES users(user_id),
    referee_id BIGINT REFERENCES users(user_id),
    coupon_code VARCHAR(50),
    status VARCHAR(20), -- PENDING, COMPLETED
    created_at TIMESTAMP DEFAULT NOW()
);
```

**Flow:**
1. User A shares referral code `REF-A123` with User B
2. User B signs up with code → Creates referral record
3. User B makes first booking → Mark referral `COMPLETED`
4. System generates coupons for both users (e.g., ₹100 off next booking)

**Edge Cases:**
- **Coupon Stacking:** Allow/disallow multiple coupons? (We disallow by default)
- **Partial Refunds:** If booking cancelled, restore coupon usage?
- **Fraud Prevention:** Rate limit coupon applications (max 5 attempts/min)

---

### Q13: Explain your API versioning strategy

**Answer:**  
**URI Versioning (e.g., `/api/v1/`, `/api/v2/`)** - Most straightforward for REST APIs

**Why URI Versioning?**
- **Clear:** Version visible in URL
- **Cacheable:** CDN can cache different versions independently
- **Backward Compatible:** v1 and v2 coexist (no breaking changes for old clients)

**Example Migration:**
```java
// v1: Returns flat structure
@GetMapping("/api/v1/bookings/{id}")
public BookingResponseV1 getBookingV1(@PathVariable Long id) {
    return BookingResponseV1.builder()
        .bookingId(id)
        .seatNumbers("A1, A2, A3") // Comma-separated string
        .build();
}

// v2: Returns nested structure
@GetMapping("/api/v2/bookings/{id}")
public BookingResponseV2 getBookingV2(@PathVariable Long id) {
    return BookingResponseV2.builder()
        .bookingId(id)
        .seats(List.of( // Structured array
            new SeatInfo("A1", 150.00),
            new SeatInfo("A2", 150.00)
        ))
        .build();
}
```

**Deprecation Policy:**
- v1 supported for 12 months after v2 launch
- Add `Deprecation: true` header to v1 responses
- Send in-app notifications to clients still using v1
- After 12 months → Return `410 Gone` for v1 endpoints

**Alternatives Considered:**

| Strategy | Pros | Cons | Verdict |
|----------|------|------|---------|
| **Header Versioning** (`Accept: application/vnd.bms.v1+json`) | Clean URLs | Harder to test, CDN caching issues | ❌ Complex |
| **Query Param** (`/api/bookings?version=1`) | Flexible | Not RESTful, easy to forget param | ❌ Non-standard |
| **URI Versioning** (`/api/v1/bookings`) | Simple, explicit | URL changes | ✅ **Chosen** |

---

### Q14: How do you secure the API?

**Answer:**  
**Defense in Depth (Multiple Layers):**

**1. Authentication (JWT):**
```java
// Login endpoint returns JWT
POST /api/v1/auth/login
{
  "email": "user@example.com",
  "password": "********"
}

Response:
{
  "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refreshToken": "...",
  "expiresIn": 3600 // 1 hour
}

// Subsequent requests include token
GET /api/v1/bookings
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

**JWT Validation (Spring Security):**
```java
@Bean
public SecurityFilterChain filterChain(HttpSecurity http) {
    http
        .csrf().disable() // Not needed for stateless JWT
        .authorizeHttpRequests(auth -> auth
            .requestMatchers("/api/v1/auth/**").permitAll()
            .requestMatchers("/api/v1/admin/**").hasRole("ADMIN")
            .anyRequest().authenticated()
        )
        .oauth2ResourceServer(oauth2 -> oauth2.jwt());
    return http.build();
}
```

**2. Authorization (RBAC - Role-Based Access Control):**
```java
@PreAuthorize("hasRole('USER')")
public BookingResponse lockSeats(LockSeatsRequest request) {
    // Only authenticated users can book
}

@PreAuthorize("hasRole('ADMIN')")
public void deleteShow(Long showId) {
    // Only admins can delete shows
}

@PreAuthorize("#userId == authentication.principal.userId")
public List getUserBookings(Long userId) {
    // Users can only view their own bookings
}
```

**3. Rate Limiting (Prevent Abuse):**
```java
@RateLimiter(name = "bookingApi", fallbackMethod = "rateLimitExceeded")
public BookingResponse lockSeats(LockSeatsRequest request) {
    // Limit: 10 requests per minute per user
}
```

**4. Input Validation (Prevent Injection):**
```java
@Valid
public BookingResponse lockSeats(@RequestBody @Valid LockSeatsRequest request) {
    // @Valid triggers validation
}

public class LockSeatsRequest {
    @NotNull(message = "Show ID required")
    private Long showId;
    
    @NotEmpty(message = "At least one seat required")
    @Size(max = 10, message = "Max 10 seats per booking")
    private List seatIds;
}
```

**5. HTTPS Only (TLS 1.3):**
- All traffic encrypted in transit
- HTTP Strict Transport Security (HSTS) header

**6. SQL Injection Prevention:**
- Use parameterized queries (JPA)
- Never concatenate user input into SQL

**7. Secrets Management:**
- Store API keys/passwords in AWS Secrets Manager (not in code)
- Rotate credentials every 90 days

---

### Q15: How would you handle international expansion (multiple countries)?

**Answer:**  
**Multi-Region Architecture with Geo-Sharding**

**Challenges:**
1. **Latency:** Users in India shouldn't hit US servers
2. **Data Residency:** GDPR (Europe), data localization laws (India, China)
3. **Currency/Payments:** Handle INR, USD, EUR
4. **Time Zones:** Show timings in local time

**Solution:**

**1. Geo-Distributed Deployment:**
```
Region 1: India (ap-south-1)
  - API servers, PostgreSQL, Redis
  - Handles: India, Southeast Asia
  
Region 2: US (us-east-1)
  - API servers, PostgreSQL, Redis
  - Handles: North America, South America
  
Region 3: Europe (eu-west-1)
  - API servers, PostgreSQL, Redis
  - Handles: Europe, Middle East, Africa
```

**2. Geo-Routing (AWS Route 53):**
- DNS routes users to nearest region (lowest latency)
- Failover: If primary region down → Route to secondary

**3. Data Partitioning:**
```sql
-- Each region has its own database
-- No cross-region data replication (data sovereignty)

-- Global Tables (replicated):
users, movies, theaters (metadata)

-- Regional Tables (isolated):
shows, bookings, show_seats (transactional data)
```

**4. Multi-Currency Support:**
```java
public class PriceCalculator {
    public Money calculatePrice(BigDecimal basePrice, Currency currency) {
        // Base prices stored in USD, convert at runtime
        BigDecimal exchangeRate = exchangeRateService.getRate("USD", currency);
        BigDecimal localPrice = basePrice.multiply(exchangeRate);
        return new Money(localPrice, currency);
    }
}
```

**5. Localization (i18n):**
- API responses include `Accept-Language` header
- Support for 10+ languages (English, Hindi, Spanish, French, etc.)
- Date/time formatting: ISO 8601 + timezone offset

**Challenges:**
- **No Global Transactions:** User in India can't book show in US (separate inventories)
- **Eventual Consistency:** Movie metadata updates take 5-10 sec to replicate globally
- **Complexity:** 3x infrastructure cost, harder to debug

---
