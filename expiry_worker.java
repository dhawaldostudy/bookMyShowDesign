package com.bookmyshow.worker;

import com.bookmyshow.entity.Booking;
import com.bookmyshow.entity.BookingStatus;
import com.bookmyshow.repository.BookingRepository;
import com.bookmyshow.service.BookingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/**
 * BookingExpiryWorker - Background job to release expired seat locks
 * 
 * Design Pattern: Scheduled Job (Cron Pattern)
 * 
 * Problem:
 * - Users lock seats but abandon payment (close browser, network issues)
 * - Seats remain locked forever → inventory stuck
 * 
 * Solution:
 * - Every 1 minute, find bookings with expired locks (PENDING + expiryTime < now)
 * - Cancel booking and release seats
 * - Idempotent: Safe to run multiple times
 * 
 * Alternative Approaches:
 * 1. **TTL in Database** (PostgreSQL):
 *    - Pros: Automatic cleanup
 *    - Cons: Not all DBs support TTL (need extension)
 * 
 * 2. **Redis TTL + Pub/Sub**:
 *    - Store locks in Redis with TTL
 *    - On expiry, Redis publishes event
 *    - Worker listens and updates DB
 *    - Pros: Real-time cleanup
 *    - Cons: Redis failure = lost cleanup events
 * 
 * 3. **Delayed Message Queue** (RabbitMQ with TTL):
 *    - Enqueue "expire booking" message with 10-min delay
 *    - Worker processes message and cancels booking
 *    - Pros: Reliable, distributed
 *    - Cons: More infrastructure
 * 
 * Chosen: Scheduled Job (simplest, reliable)
 * - Trade-off: Cleanup happens every 1 min (not instant)
 * - Acceptable: Seats released within 1 min of expiry
 */
@Component
public class BookingExpiryWorker {
    
    private static final Logger logger = LoggerFactory.getLogger(BookingExpiryWorker.class);
    
    private final BookingRepository bookingRepository;
    private final BookingService bookingService;
    
    public BookingExpiryWorker(BookingRepository bookingRepository, BookingService bookingService) {
        this.bookingRepository = bookingRepository;
        this.bookingService = bookingService;
    }
    
    /**
     * Scheduled job runs every 60 seconds
     * 
     * Cron expression: "0 * * * * *" = top of every minute
     * Fixed delay: 60000ms = 1 minute
     * 
     * Spring scheduling options:
     * - @Scheduled(fixedDelay = 60000): Wait 60s after previous execution completes
     * - @Scheduled(fixedRate = 60000): Start every 60s (may overlap if slow)
     * - @Scheduled(cron = "0 * * * * *"): Cron-based scheduling
     * 
     * Chosen: fixedDelay (no overlap, safe)
     */
    @Scheduled(fixedDelay = 60000, initialDelay = 10000) // Start 10s after app boot
    @Transactional
    public void expireBookings() {
        try {
            logger.info("Starting booking expiry job");
            
            // 1. Find expired bookings
            // Query: SELECT * FROM bookings WHERE status='PENDING' AND expiry_time < NOW()
            List<Booking> expiredBookings = bookingRepository.findExpiredBookings(
                BookingStatus.PENDING,
                LocalDateTime.now()
            );
            
            if (expiredBookings.isEmpty()) {
                logger.debug("No expired bookings found");
                return;
            }
            
            logger.info("Found {} expired bookings to cancel", expiredBookings.size());
            
            // 2. Cancel each booking (releases seats, invalidates cache)
            int successCount = 0;
            int failCount = 0;
            
            for (Booking booking : expiredBookings) {
                try {
                    logger.debug("Cancelling expired booking {}", booking.getBookingId());
                    bookingService.cancelBooking(booking.getBookingId());
                    successCount++;
                } catch (Exception e) {
                    logger.error("Failed to cancel booking {}", booking.getBookingId(), e);
                    failCount++;
                }
            }
            
            logger.info("Booking expiry job completed: {} cancelled, {} failed", successCount, failCount);
            
            // 3. Emit metrics for monitoring
            emitMetrics(successCount, failCount);
            
        } catch (Exception e) {
            logger.error("Booking expiry job failed", e);
            // Don't throw - job will retry in next cycle
        }
    }
    
    /**
     * Emit metrics to monitoring system (Prometheus/CloudWatch)
     * 
     * Metrics:
     * - expired_bookings_cancelled_total (counter)
     * - expired_bookings_failed_total (counter)
     * - expiry_job_duration_seconds (histogram)
     */
    private void emitMetrics(int successCount, int failCount) {
        // Example: Micrometer (Spring Boot Actuator)
        // meterRegistry.counter("expired_bookings_cancelled_total").increment(successCount);
        // meterRegistry.counter("expired_bookings_failed_total").increment(failCount);
        logger.debug("Emitted metrics: success={}, fail={}", successCount, failCount);
    }
}

// ==================== Repository Query ====================

@Repository
public interface BookingRepository extends JpaRepository<Booking, Long> {
    
    /**
     * Find bookings that are PENDING and past expiry time
     * 
     * Index required: (status, expiry_time) for efficient query
     * Without index, this becomes a full table scan (slow)
     * 
     * CREATE INDEX idx_booking_expiry ON bookings(booking_status, expiry_time)
     * WHERE booking_status = 'PENDING';
     */
    @Query("""
        SELECT b FROM Booking b
        WHERE b.bookingStatus = :status
          AND b.expiryTime < :currentTime
        ORDER BY b.expiryTime ASC
    """)
    List<Booking> findExpiredBookings(
        @Param("status") BookingStatus status,
        @Param("currentTime") LocalDateTime currentTime
    );
    
    /**
     * Batch version: Limit to 100 bookings per cycle (prevent long transactions)
     * 
     * Why limit?
     * - If 10,000 bookings expire simultaneously (e.g., major event)
     * - Processing all in one transaction = long lock, OOM risk
     * - Better: Process 100 at a time, commit, repeat
     */
    @Query("""
        SELECT b FROM Booking b
        WHERE b.bookingStatus = :status
          AND b.expiryTime < :currentTime
        ORDER BY b.expiryTime ASC
        LIMIT 100
    """)
    List<Booking> findExpiredBookingsBatch(
        @Param("status") BookingStatus status,
        @Param("currentTime") LocalDateTime currentTime
    );
}

// ==================== Distributed Locking (Advanced) ====================

/**
 * If running multiple worker instances (horizontal scaling):
 * - Need distributed lock to prevent duplicate processing
 * - Example: Redlock, ShedLock, or database-based lock
 * 
 * ShedLock Example:
 */
@Component
class DistributedExpiryWorker {
    
    @Scheduled(fixedDelay = 60000)
    @SchedulerLock(
        name = "expireBookings",
        lockAtMostFor = "5m",  // Max lock duration (safety)
        lockAtLeastFor = "30s" // Min lock duration (prevent rapid re-execution)
    )
    public void expireBookingsDistributed() {
        // Only one instance executes at a time
        // Other instances skip if lock held
        logger.info("Executing distributed expiry job");
    }
}

// ==================== Testing ====================

/**
 * Unit Test: Verify expiry logic
 */
@SpringBootTest
class BookingExpiryWorkerTest {
    
    @Autowired
    private BookingExpiryWorker worker;
    
    @Autowired
    private BookingRepository bookingRepository;
    
    @Autowired
    private ShowSeatRepository showSeatRepository;
    
    @Test
    @Transactional
    public void testExpireBookings_releasesSeats() {
        // 1. Create expired booking
        Booking booking = new Booking();
        booking.setBookingStatus(BookingStatus.PENDING);
        booking.setExpiryTime(LocalDateTime.now().minusMinutes(5)); // Expired 5 min ago
        booking = bookingRepository.save(booking);
        
        // 2. Lock seats for this booking
        showSeatRepository.lockSeats(List.of(1L, 2L, 3L), booking.getBookingId());
        
        // 3. Run expiry job
        worker.expireBookings();
        
        // 4. Verify booking cancelled
        Booking updated = bookingRepository.findById(booking.getBookingId()).get();
        assertEquals(BookingStatus.CANCELLED, updated.getBookingStatus());
        
        // 5. Verify seats released
        List<ShowSeat> seats = showSeatRepository.findByBookingId(booking.getBookingId());
        seats.forEach(seat -> assertEquals(SeatStatus.AVAILABLE, seat.getStatus()));
    }
    
    @Test
    public void testExpireBookings_ignoresConfirmedBookings() {
        // Confirmed bookings should never be expired
        Booking booking = new Booking();
        booking.setBookingStatus(BookingStatus.CONFIRMED);
        booking.setExpiryTime(LocalDateTime.now().minusHours(1));
        bookingRepository.save(booking);
        
        worker.expireBookings();
        
        // Booking should remain confirmed
        Booking updated = bookingRepository.findById(booking.getBookingId()).get();
        assertEquals(BookingStatus.CONFIRMED, updated.getBookingStatus());
    }
}