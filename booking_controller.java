package com.bookmyshow.controller;

import com.bookmyshow.dto.*;
import com.bookmyshow.service.BookingService;
import com.bookmyshow.exception.*;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Map;

/**
 * BookingController - REST API endpoints for booking operations
 * 
 * Design Patterns:
 * 1. **Controller Pattern** (Spring MVC)
 * 2. **DTO Pattern** (Data Transfer Objects for API contracts)
 * 3. **Exception Handler** (Centralized error handling)
 * 
 * Resilience Patterns:
 * 1. **Rate Limiting** (prevent abuse)
 * 2. **Circuit Breaker** (fail fast on downstream failures)
 * 3. **Bulkhead** (isolate resources)
 * 
 * API Versioning: /api/v1
 * - Supports backward compatibility
 * - Can release v2 without breaking v1 clients
 */
@RestController
@RequestMapping("/api/v1/bookings")
@Tag(name = "Booking", description = "Seat booking and payment operations")
@Validated
public class BookingController {
    
    private static final Logger logger = LoggerFactory.getLogger(BookingController.class);
    
    private final BookingService bookingService;
    
    public BookingController(BookingService bookingService) {
        this.bookingService = bookingService;
    }
    
    /**
     * Lock seats for a user (Step 1 of booking flow)
     * 
     * Rate Limiting:
     * - Limit: 10 lock requests per minute per user
     * - Prevents user from spamming seat locks
     * - Fallback: Return 429 Too Many Requests
     * 
     * Circuit Breaker:
     * - If DB failures > 50% in 10 requests, open circuit
     * - Fail fast for next 30 seconds
     * - Prevents cascading failures
     * 
     * @param request Contains showId, userId, seatIds
     * @return BookingResponse with bookingId and expiry
     */
    @PostMapping("/lock")
    @Operation(summary = "Lock seats for booking", description = "Reserve seats for 10 minutes")
    @RateLimiter(name = "lockSeats", fallbackMethod = "rateLimitFallback")
    @CircuitBreaker(name = "bookingService", fallbackMethod = "circuitBreakerFallback")
    public ResponseEntity<ApiResponse<BookingResponse>> lockSeats(
        @Valid @RequestBody LockSeatsRequest request,
        @AuthenticationPrincipal UserPrincipal currentUser
    ) {
        logger.info("Lock seats request: user={}, show={}, seats={}", 
            currentUser.getUserId(), request.getShowId(), request.getSeatIds());
        
        // Validate user owns the request (prevent IDOR attacks)
        request.setUserId(currentUser.getUserId());
        
        // Execute booking
        BookingResponse response = bookingService.lockSeats(request);
        
        logger.info("Seats locked successfully: booking={}", response.getBookingId());
        
        return ResponseEntity
            .status(HttpStatus.CREATED)
            .body(ApiResponse.success(response));
    }
    
    /**
     * Confirm booking after payment (Step 2 of booking flow)
     * 
     * Idempotency:
     * - If called multiple times with same bookingId, returns same result
     * - Prevents double-charging on network retry
     * 
     * @param request Contains bookingId and payment details
     * @return BookingResponse with tickets and QR codes
     */
    @PostMapping("/confirm")
    @Operation(summary = "Confirm booking", description = "Process payment and confirm booking")
    @RateLimiter(name = "confirmBooking", fallbackMethod = "rateLimitFallback")
    @CircuitBreaker(name = "paymentService", fallbackMethod = "paymentFailureFallback")
    public ResponseEntity<ApiResponse<BookingResponse>> confirmBooking(
        @Valid @RequestBody ConfirmBookingRequest request,
        @AuthenticationPrincipal UserPrincipal currentUser
    ) {
        logger.info("Confirm booking request: booking={}, user={}", 
            request.getBookingId(), currentUser.getUserId());
        
        // Validate user owns the booking (security check)
        // This is critical - prevent user A from confirming user B's booking
        validateBookingOwnership(request.getBookingId(), currentUser.getUserId());
        
        // Process payment and confirm
        BookingResponse response = bookingService.confirmBooking(request);
        
        logger.info("Booking confirmed successfully: booking={}", response.getBookingId());
        
        return ResponseEntity.ok(ApiResponse.success(response));
    }
    
    /**
     * Get booking details
     * 
     * Use case: View ticket after booking, email link to ticket
     * 
     * @param bookingId Booking identifier
     * @return Booking details with seat info
     */
    @GetMapping("/{bookingId}")
    @Operation(summary = "Get booking details", description = "Retrieve booking by ID")
    public ResponseEntity<ApiResponse<BookingResponse>> getBooking(
        @PathVariable Long bookingId,
        @AuthenticationPrincipal UserPrincipal currentUser
    ) {
        logger.info("Get booking request: booking={}, user={}", bookingId, currentUser.getUserId());
        
        // Validate ownership
        validateBookingOwnership(bookingId, currentUser.getUserId());
        
        BookingResponse response = bookingService.getBookingDetails(bookingId);
        
        return ResponseEntity.ok(ApiResponse.success(response));
    }
    
    /**
     * Cancel booking (before confirmation)
     * 
     * Use case:
     * - User changes mind before payment
     * - User selects wrong seats
     * 
     * Constraint: Only PENDING bookings can be cancelled
     * - CONFIRMED bookings require refund process (separate endpoint)
     * 
     * @param bookingId Booking to cancel
     */
    @PostMapping("/{bookingId}/cancel")
    @Operation(summary = "Cancel booking", description = "Cancel pending booking and release seats")
    public ResponseEntity<ApiResponse<String>> cancelBooking(
        @PathVariable Long bookingId,
        @AuthenticationPrincipal UserPrincipal currentUser
    ) {
        logger.info("Cancel booking request: booking={}, user={}", bookingId, currentUser.getUserId());
        
        validateBookingOwnership(bookingId, currentUser.getUserId());
        
        bookingService.cancelBooking(bookingId);
        
        logger.info("Booking cancelled: booking={}", bookingId);
        
        return ResponseEntity.ok(ApiResponse.success("Booking cancelled successfully"));
    }
    
    /**
     * Get user's booking history
     * 
     * Pagination: Prevents loading thousands of bookings at once
     * 
     * @param userId User identifier (from auth token)
     * @param page Page number (0-indexed)
     * @param size Items per page
     * @return Paginated list of bookings
     */
    @GetMapping("/user/{userId}")
    @Operation(summary = "Get user bookings", description = "List all bookings for a user")
    public ResponseEntity<ApiResponse<PageResponse<BookingResponse>>> getUserBookings(
        @PathVariable Long userId,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "20") int size,
        @AuthenticationPrincipal UserPrincipal currentUser
    ) {
        // Only allow users to see their own bookings (privacy)
        if (!userId.equals(currentUser.getUserId()) && !currentUser.isAdmin()) {
            throw new ForbiddenException("Cannot view other user's bookings");
        }
        
        logger.info("Get user bookings: user={}, page={}, size={}", userId, page, size);
        
        PageResponse<BookingResponse> response = bookingService.getUserBookings(userId, page, size);
        
        return ResponseEntity.ok(ApiResponse.success(response));
    }
    
    // ==================== Fallback Methods ====================
    
    /**
     * Fallback when rate limit exceeded
     * 
     * HTTP 429: Too Many Requests
     * Retry-After header tells client when to retry
     */
    public ResponseEntity<ApiResponse<BookingResponse>> rateLimitFallback(
        Exception e
    ) {
        logger.warn("Rate limit exceeded", e);
        return ResponseEntity
            .status(HttpStatus.TOO_MANY_REQUESTS)
            .header("Retry-After", "60") // Retry after 60 seconds
            .body(ApiResponse.error("Rate limit exceeded. Please try again later."));
    }
    
    /**
     * Fallback when circuit breaker opens (downstream service failure)
     * 
     * HTTP 503: Service Unavailable
     * Fail fast instead of waiting for timeout
     */
    public ResponseEntity<ApiResponse<BookingResponse>> circuitBreakerFallback(
        Exception e
    ) {
        logger.error("Circuit breaker opened", e);
        return ResponseEntity
            .status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(ApiResponse.error("Booking service temporarily unavailable. Please try again in a few minutes."));
    }
    
    /**
     * Fallback for payment service failure
     * 
     * More graceful error message for payment issues
     */
    public ResponseEntity<ApiResponse<BookingResponse>> paymentFailureFallback(
        ConfirmBookingRequest request,
        UserPrincipal currentUser,
        Exception e
    ) {
        logger.error("Payment service failed for booking {}", request.getBookingId(), e);
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(ApiResponse.error("Payment processing failed. Your seats are still reserved. Please try again."));
    }
    
    // ==================== Helper Methods ====================
    
    private void validateBookingOwnership(Long bookingId, Long userId) {
        Booking booking = bookingService.getBooking(bookingId);
        if (!booking.getUser().getUserId().equals(userId)) {
            throw new ForbiddenException("You don't have access to this booking");
        }
    }
}

// ==================== DTOs ====================

/**
 * Request DTO for locking seats
 * 
 * Validation:
 * - showId: Required, must be positive
 * - seatIds: Required, non-empty, max 10 seats per booking
 */
@Data
public class LockSeatsRequest {
    
    @NotNull(message = "Show ID is required")
    private Long showId;
    
    private Long userId; // Set by controller from auth token
    
    @NotNull(message = "Seat IDs are required")
    @Size(min = 1, max = 10, message = "Must select 1-10 seats")
    private List<Long> seatIds;
}

/**
 * Request DTO for confirming booking
 */
@Data
public class ConfirmBookingRequest {
    
    @NotNull(message = "Booking ID is required")
    private Long bookingId;
    
    @NotNull(message = "Payment method is required")
    private String paymentMethod; // CARD, UPI, WALLET
    
    @NotNull(message = "Payment details are required")
    private Map<String, String> paymentDetails; // cardToken, cardLast4, etc.
}

/**
 * Response DTO for booking operations
 */
@Data
@Builder
public class BookingResponse {
    private Long bookingId;
    private String status; // LOCKED, CONFIRMED, CANCELLED
    private LocalDateTime expiryTime; // For LOCKED status
    private BigDecimal totalAmount;
    private String paymentId;
    private List<SeatDTO> seats;
    private List<TicketDTO> tickets; // QR codes for confirmed bookings
}

/**
 * Generic API response wrapper
 * 
 * Consistent response format:
 * {
 *   "success": true,
 *   "data": {...},
 *   "error": null
 * }
 */
@Data
@Builder
public class ApiResponse<T> {
    private boolean success;
    private T data;
    private String error;
    private LocalDateTime timestamp;
    
    public static <T> ApiResponse<T> success(T data) {
        return ApiResponse.<T>builder()
            .success(true)
            .data(data)
            .timestamp(LocalDateTime.now())
            .build();
    }
    
    public static <T> ApiResponse<T> error(String message) {
        return ApiResponse.<T>builder()
            .success(false)
            .error(message)
            .timestamp(LocalDateTime.now())
            .build();
    }
}

// ==================== Global Exception Handler ====================

/**
 * Centralized exception handling
 * 
 * Why?
 * - Consistent error responses across all APIs
 * - Avoid try-catch in every controller method
 * - Map domain exceptions to HTTP status codes
 */
@RestControllerAdvice
public class GlobalExceptionHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    
    @ExceptionHandler(SeatUnavailableException.class)
    public ResponseEntity<ApiResponse<Void>> handleSeatUnavailable(SeatUnavailableException e) {
        logger.warn("Seat unavailable: {}", e.getMessage());
        return ResponseEntity
            .status(HttpStatus.CONFLICT)
            .body(ApiResponse.error(e.getMessage()));
    }
    
    @ExceptionHandler(BookingExpiredException.class)
    public ResponseEntity<ApiResponse<Void>> handleBookingExpired(BookingExpiredException e) {
        logger.warn("Booking expired: {}", e.getMessage());
        return ResponseEntity
            .status(HttpStatus.GONE)
            .body(ApiResponse.error(e.getMessage()));
    }
    
    @ExceptionHandler(PaymentFailedException.class)
    public ResponseEntity<ApiResponse<Void>> handlePaymentFailed(PaymentFailedException e) {
        logger.error("Payment failed: {}", e.getMessage());
        return ResponseEntity
            .status(HttpStatus.PAYMENT_REQUIRED)
            .body(ApiResponse.error(e.getMessage()));
    }
    
    @ExceptionHandler(ForbiddenException.class)
    public ResponseEntity<ApiResponse<Void>> handleForbidden(ForbiddenException e) {
        logger.warn("Forbidden access: {}", e.getMessage());
        return ResponseEntity
            .status(HttpStatus.FORBIDDEN)
            .body(ApiResponse.error(e.getMessage()));
    }
    
    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ApiResponse<Void>> handleNotFound(ResourceNotFoundException e) {
        logger.warn("Resource not found: {}", e.getMessage());
        return ResponseEntity
            .status(HttpStatus.NOT_FOUND)
            .body(ApiResponse.error(e.getMessage()));
    }
    
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Void>> handleValidationError(MethodArgumentNotValidException e) {
        String errors = e.getBindingResult()
            .getFieldErrors()
            .stream()
            .map(FieldError::getDefaultMessage)
            .collect(Collectors.joining(", "));
        
        logger.warn("Validation error: {}", errors);
        return ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .body(ApiResponse.error(errors));
    }
    
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> handleGenericError(Exception e) {
        logger.error("Unexpected error", e);
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(ApiResponse.error("An unexpected error occurred. Please try again later."));
    }
}