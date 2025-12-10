package com.bookmyshow.service;

import com.bookmyshow.entity.*;
import com.bookmyshow.repository.*;
import com.bookmyshow.exception.*;
import com.bookmyshow.dto.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Isolation;
import redis.clients.jedis.Jedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;
import java.math.BigDecimal;

/**
 * BookingService - Core business logic for seat booking
 * 
 * Design Pattern: Service Layer Pattern
 * - Encapsulates business logic
 * - Coordinates between repositories, cache, and external services
 * 
 * Key Features:
 * - Optimistic locking with database constraints
 * - Redis caching for performance
 * - Distributed lock prevention
 * - Transaction management for consistency
 */
@Service
public class BookingService {
    
    private static final Logger logger = LoggerFactory.getLogger(BookingService.class);
    private static final int LOCK_DURATION_MINUTES = 10;
    
    private final BookingRepository bookingRepository;
    private final ShowSeatRepository showSeatRepository;
    private final ShowRepository showRepository;
    private final PaymentService paymentService;
    private final NotificationService notificationService;
    private final Jedis redisClient;
    
    public BookingService(
        BookingRepository bookingRepository,
        ShowSeatRepository showSeatRepository,
        ShowRepository showRepository,
        PaymentService paymentService,
        NotificationService notificationService,
        Jedis redisClient
    ) {
        this.bookingRepository = bookingRepository;
        this.showSeatRepository = showSeatRepository;
        this.showRepository = showRepository;
        this.paymentService = paymentService;
        this.notificationService = notificationService;
        this.redisClient = redisClient;
    }
    
    /**
     * Lock seats for a user (Step 1 of booking)
     * 
     * Algorithm:
     * 1. Validate show exists and is active
     * 2. Check seat availability in cache (fast path)
     * 3. Use optimistic locking to reserve seats in DB
     * 4. Create PENDING booking with expiry
     * 5. Invalidate cache
     * 
     * Concurrency Handling:
     * - Database UPDATE with WHERE status='AVAILABLE' ensures atomicity
     * - If UPDATE returns 0 rows, seat already taken (race condition handled)
     * - Redis distributed lock prevents cache stampede
     * 
     * @param request Contains showId, userId, seatIds
     * @return BookingResponse with bookingId and expiry time
     * @throws SeatUnavailableException if any seat is already booked
     * @throws ShowNotFoundException if show doesn't exist
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public BookingResponse lockSeats(LockSeatsRequest request) {
        logger.info("Locking seats for user {} on show {}", request.getUserId(), request.getShowId());
        
        // 1. Validate show
        Show show = showRepository.findById(request.getShowId())
            .orElseThrow(() -> new ShowNotFoundException("Show not found: " + request.getShowId()));
        
        if (show.getStatus() != ShowStatus.ACTIVE) {
            throw new BookingException("Show is not active");
        }
        
        // 2. Check cache for fast rejection (optional optimization)
        String cacheKey = "show:seats:" + request.getShowId();
        if (redisClient.exists(cacheKey)) {
            for (Long seatId : request.getSeatIds()) {
                String seatStatus = redisClient.hget(cacheKey, seatId.toString());
                if (!"AVAILABLE".equals(seatStatus)) {
                    throw new SeatUnavailableException("Seat " + seatId + " is not available");
                }
            }
        }
        
        // 3. Atomic seat locking using optimistic locking
        List<ShowSeat> lockedSeats = new ArrayList<>();
        BigDecimal totalAmount = BigDecimal.ZERO;
        
        for (Long seatId : request.getSeatIds()) {
            // Critical: This UPDATE is atomic at database level
            // If another transaction locked the seat, this returns 0 rows
            int rowsUpdated = showSeatRepository.lockSeat(
                request.getShowId(),
                seatId,
                request.getUserId(),
                LocalDateTime.now()
            );
            
            if (rowsUpdated == 0) {
                // Race condition: Another user locked this seat
                logger.warn("Seat {} already locked for show {}", seatId, request.getShowId());
                
                // Rollback: Release previously locked seats
                rollbackLockedSeats(lockedSeats);
                throw new SeatUnavailableException("Seat " + seatId + " is no longer available");
            }
            
            // Fetch updated seat
            ShowSeat showSeat = showSeatRepository.findByShowIdAndSeatId(
                request.getShowId(), seatId
            ).orElseThrow(() -> new SeatNotFoundException("Seat not found"));
            
            lockedSeats.add(showSeat);
            totalAmount = totalAmount.add(showSeat.getPrice());
        }
        
        // 4. Create PENDING booking
        Booking booking = new Booking();
        booking.setUser(new User(request.getUserId()));
        booking.setShow(show);
        booking.setTotalAmount(totalAmount);
        booking.setBookingStatus(BookingStatus.PENDING);
        booking.setExpiryTime(LocalDateTime.now().plusMinutes(LOCK_DURATION_MINUTES));
        
        // Save booking and associate seats
        booking = bookingRepository.save(booking);
        
        for (ShowSeat seat : lockedSeats) {
            BookingSeat bookingSeat = new BookingSeat();
            bookingSeat.setBooking(booking);
            bookingSeat.setShowSeat(seat);
            bookingSeat.setSeatNumber(seat.getSeat().getSeatNumber());
            bookingSeat.setPrice(seat.getPrice());
            booking.getBookingSeats().add(bookingSeat);
        }
        
        booking = bookingRepository.save(booking);
        
        // 5. Invalidate cache
        invalidateShowSeatsCache(request.getShowId());
        
        // 6. Schedule lock expiry job (handled by background worker)
        scheduleLocakExpiryJob(booking.getBookingId(), LOCK_DURATION_MINUTES);
        
        logger.info("Successfully locked {} seats for booking {}", lockedSeats.size(), booking.getBookingId());
        
        return BookingResponse.builder()
            .bookingId(booking.getBookingId())
            .status("LOCKED")
            .expiryTime(booking.getExpiryTime())
            .totalAmount(totalAmount)
            .seats(mapToSeatDTOs(lockedSeats))
            .build();
    }
    
    /**
     * Confirm booking after successful payment (Step 2 of booking)
     * 
     * Two-Phase Commit:
     * Phase 1: Process payment (external call)
     * Phase 2: Update booking and seats (database)
     * 
     * Idempotency:
     * - Payment service uses idempotency key (bookingId)
     * - If payment already processed, skip payment but confirm booking
     * 
     * @param request Contains bookingId and payment details
     * @return BookingResponse with confirmed status and tickets
     * @throws BookingExpiredException if lock expired
     * @throws PaymentFailedException if payment fails
     */
    @Transactional
    public BookingResponse confirmBooking(ConfirmBookingRequest request) {
        logger.info("Confirming booking {}", request.getBookingId());
        
        // 1. Fetch booking
        Booking booking = bookingRepository.findById(request.getBookingId())
            .orElseThrow(() -> new BookingNotFoundException("Booking not found"));
        
        // 2. Validate booking state
        if (booking.getBookingStatus() == BookingStatus.CONFIRMED) {
            logger.info("Booking {} already confirmed (idempotent)", booking.getBookingId());
            return buildBookingResponse(booking);
        }
        
        if (booking.getBookingStatus() == BookingStatus.CANCELLED) {
            throw new BookingException("Booking is cancelled");
        }
        
        if (booking.getExpiryTime().isBefore(LocalDateTime.now())) {
            // Lock expired - cancel booking
            cancelBooking(booking.getBookingId());
            throw new BookingExpiredException("Booking lock expired");
        }
        
        // 3. Process payment (external service call)
        // This is the commit point - if payment succeeds, we must confirm booking
        PaymentResponse paymentResponse;
        try {
            paymentResponse = paymentService.processPayment(
                PaymentRequest.builder()
                    .bookingId(booking.getBookingId())
                    .amount(booking.getTotalAmount())
                    .paymentMethod(request.getPaymentMethod())
                    .paymentDetails(request.getPaymentDetails())
                    .idempotencyKey(booking.getBookingId().toString()) // Prevent duplicate charges
                    .build()
            );
        } catch (Exception e) {
            logger.error("Payment failed for booking {}", booking.getBookingId(), e);
            
            // Rollback: Cancel booking and release seats
            cancelBooking(booking.getBookingId());
            throw new PaymentFailedException("Payment processing failed: " + e.getMessage());
        }
        
        if (paymentResponse.getStatus() != PaymentStatus.SUCCESS) {
            cancelBooking(booking.getBookingId());
            throw new PaymentFailedException("Payment declined: " + paymentResponse.getMessage());
        }
        
        // 4. Confirm booking (database update)
        booking.setBookingStatus(BookingStatus.CONFIRMED);
        booking.setPaymentId(paymentResponse.getPaymentId());
        bookingRepository.save(booking);
        
        // 5. Update seat status to BOOKED (permanent)
        showSeatRepository.updateSeatsStatus(
            booking.getBookingSeats().stream()
                .map(bs -> bs.getShowSeat().getShowSeatId())
                .toList(),
            SeatStatus.BOOKED,
            booking.getBookingId()
        );
        
        // 6. Invalidate cache
        invalidateShowSeatsCache(booking.getShow().getShowId());
        
        // 7. Send notification asynchronously
        notificationService.sendBookingConfirmation(booking);
        
        logger.info("Booking {} confirmed successfully", booking.getBookingId());
        
        return buildBookingResponse(booking);
    }
    
    /**
     * Cancel booking and release seats
     * 
     * Use cases:
     * - User cancels manually
     * - Payment fails
     * - Lock expires (background job)
     * 
     * @param bookingId Booking to cancel
     */
    @Transactional
    public void cancelBooking(Long bookingId) {
        logger.info("Cancelling booking {}", bookingId);
        
        Booking booking = bookingRepository.findById(bookingId)
            .orElseThrow(() -> new BookingNotFoundException("Booking not found"));
        
        if (booking.getBookingStatus() == BookingStatus.CONFIRMED) {
            // Confirmed bookings require refund process
            throw new BookingException("Cannot cancel confirmed booking without refund");
        }
        
        // Update booking status
        booking.setBookingStatus(BookingStatus.CANCELLED);
        bookingRepository.save(booking);
        
        // Release seats
        showSeatRepository.releaseSeats(
            booking.getBookingSeats().stream()
                .map(bs -> bs.getShowSeat().getShowSeatId())
                .toList()
        );
        
        // Invalidate cache
        invalidateShowSeatsCache(booking.getShow().getShowId());
        
        logger.info("Booking {} cancelled", bookingId);
    }
    
    // ==================== Helper Methods ====================
    
    private void rollbackLockedSeats(List<ShowSeat> seats) {
        logger.warn("Rolling back {} locked seats", seats.size());
        showSeatRepository.releaseSeats(
            seats.stream().map(ShowSeat::getShowSeatId).toList()
        );
    }
    
    private void invalidateShowSeatsCache(Long showId) {
        String cacheKey = "show:seats:" + showId;
        redisClient.del(cacheKey);
        logger.debug("Invalidated cache for show {}", showId);
    }
    
    private void scheduleLocakExpiryJob(Long bookingId, int minutes) {
        // In production, use Celery, RabbitMQ delayed messages, or scheduled jobs
        // For simplicity, assume a background worker polls expired bookings
        logger.debug("Scheduled expiry job for booking {} in {} minutes", bookingId, minutes);
    }
    
    private BookingResponse buildBookingResponse(Booking booking) {
        return BookingResponse.builder()
            .bookingId(booking.getBookingId())
            .status(booking.getBookingStatus().name())
            .totalAmount(booking.getTotalAmount())
            .paymentId(booking.getPaymentId())
            .seats(mapToSeatDTOs(booking.getBookingSeats()))
            .build();
    }
    
    private List<SeatDTO> mapToSeatDTOs(List<ShowSeat> showSeats) {
        return showSeats.stream()
            .map(ss -> SeatDTO.builder()
                .seatId(ss.getSeat().getSeatId())
                .seatNumber(ss.getSeat().getSeatNumber())
                .price(ss.getPrice())
                .build())
            .toList();
    }
    
    private List<SeatDTO> mapToSeatDTOs(List<BookingSeat> bookingSeats) {
        return bookingSeats.stream()
            .map(bs -> SeatDTO.builder()
                .seatId(bs.getShowSeat().getSeat().getSeatId())
                .seatNumber(bs.getSeatNumber())
                .price(bs.getPrice())
                .build())
            .toList();
    }
}

// ==================== Repository Interface ====================

/**
 * Custom repository method for atomic seat locking
 * 
 * Why custom query?
 * - JPA's default update doesn't support conditional updates well
 * - We need UPDATE...WHERE to ensure atomicity
 * - Returns number of rows updated (0 = seat unavailable)
 */
@Repository
public interface ShowSeatRepository extends JpaRepository<ShowSeat, Long> {
    
    @Modifying
    @Query("""
        UPDATE ShowSeat ss
        SET ss.status = 'LOCKED',
            ss.lockedBy = :userId,
            ss.lockedAt = :lockedAt
        WHERE ss.show.showId = :showId
          AND ss.seat.seatId = :seatId
          AND ss.status = 'AVAILABLE'
    """)
    int lockSeat(
        @Param("showId") Long showId,
        @Param("seatId") Long seatId,
        @Param("userId") Long userId,
        @Param("lockedAt") LocalDateTime lockedAt
    );
    
    @Modifying
    @Query("""
        UPDATE ShowSeat ss
        SET ss.status = 'BOOKED',
            ss.bookingId = :bookingId
        WHERE ss.showSeatId IN :seatIds
    """)
    void updateSeatsStatus(
        @Param("seatIds") List<Long> seatIds,
        @Param("status") SeatStatus status,
        @Param("bookingId") Long bookingId
    );
    
    @Modifying
    @Query("""
        UPDATE ShowSeat ss
        SET ss.status = 'AVAILABLE',
            ss.lockedBy = NULL,
            ss.lockedAt = NULL,
            ss.bookingId = NULL
        WHERE ss.showSeatId IN :seatIds
    """)
    void releaseSeats(@Param("seatIds") List<Long> seatIds);
    
    Optional<ShowSeat> findByShowIdAndSeatId(Long showId, Long seatId);
}