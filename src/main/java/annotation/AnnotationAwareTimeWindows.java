package annotation;

import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondDuration;

/**
 * A window class identical to the {@link TimeWindows} class, but only returning a single window per record,
 * i.e., the one referenced in the record annotation
 */
public class AnnotationAwareTimeWindows  extends Windows<TimeWindow> {
    /** The size of the windows in milliseconds. */
    @SuppressWarnings("WeakerAccess")
    public final long sizeMs;

    /**
     * The size of the window's advance interval in milliseconds, i.e., by how much a window moves forward relative to
     * the previous one.
     */
    @SuppressWarnings("WeakerAccess")
    public final long advanceMs;

    private final long graceMs;

    // flag to check if the grace is already set via ofSizeAndGrace or ofSizeWithNoGrace
    private final boolean hasSetGrace;

    private AnnotationAwareTimeWindows(final long sizeMs, final long advanceMs, final long graceMs, final boolean hasSetGrace) {
        this.sizeMs = sizeMs;
        this.advanceMs = advanceMs;
        this.graceMs = graceMs;
        this.hasSetGrace = hasSetGrace;

        if (sizeMs <= 0) {
            throw new IllegalArgumentException("Window size (sizeMs) must be larger than zero.");
        }

        if (advanceMs <= 0 || advanceMs > sizeMs) {
            throw new IllegalArgumentException(String.format("Window advancement interval should be more than zero " +
                    "and less than window duration which is %d ms, but given advancement interval is: %d ms", sizeMs, advanceMs));
        }

        if (graceMs < 0) {
            throw new IllegalArgumentException("Grace period must not be negative.");
        }
    }

    /**
     * Return a window definition with the given window size, and with the advance interval being equal to the window
     * size.
     * The time interval represented by the N-th window is: {@code [N * size, N * size + size)}.
     * <p>
     * This provides the semantics of tumbling windows, which are fixed-sized, gap-less, non-overlapping windows.
     * Tumbling windows are a special case of hopping windows with {@code advance == size}.
     * <p>
     * CAUTION: Using this method implicitly sets the grace period to zero, which means that any out-of-order
     * records arriving after the window ends are considered late and will be dropped.
     *
     * @param size The size of the window
     * @return a new window definition with default no grace period. Note that this means out-of-order records arriving after the window end will be dropped
     * @throws IllegalArgumentException if the specified window size is zero or negative or can't be represented as {@code long milliseconds}
     */
    public static AnnotationAwareTimeWindows ofSizeWithNoGrace(final Duration size) throws IllegalArgumentException {
        return ofSizeAndGrace(size, ofMillis(NO_GRACE_PERIOD));
    }

    /**
     * Return a window definition with the given window size, and with the advance interval being equal to the window
     * size.
     * The time interval represented by the N-th window is: {@code [N * size, N * size + size)}.
     * <p>
     * This provides the semantics of tumbling windows, which are fixed-sized, gap-less, non-overlapping windows.
     * Tumbling windows are a special case of hopping windows with {@code advance == size}.
     * <p>
     * Using this method explicitly sets the grace period to the duration specified by {@code afterWindowEnd}, which
     * means that only out-of-order records arriving more than the grace period after the window end will be dropped.
     * The window close, after which any incoming records are considered late and will be rejected, is defined as
     * {@code windowEnd + afterWindowEnd}
     *
     * @param size The size of the window. Must be larger than zero
     * @param afterWindowEnd The grace period to admit out-of-order events to a window. Must be non-negative.
     * @return a TimeWindows object with the specified size and the specified grace period
     * @throws IllegalArgumentException if {@code afterWindowEnd} is negative or can't be represented as {@code long milliseconds}
     */
    public static AnnotationAwareTimeWindows ofSizeAndGrace(final Duration size, final Duration afterWindowEnd) throws IllegalArgumentException {
        final String sizeMsgPrefix = prepareMillisCheckFailMsgPrefix(size, "size");
        final long sizeMs = validateMillisecondDuration(size, sizeMsgPrefix);

        final String afterWindowEndMsgPrefix = prepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
        final long afterWindowEndMs = validateMillisecondDuration(afterWindowEnd, afterWindowEndMsgPrefix);

        return new AnnotationAwareTimeWindows(sizeMs, sizeMs, afterWindowEndMs, true);
    }

    /**
     * Return a window definition with the given window size, and with the advance interval being equal to the window
     * size.
     * The time interval represented by the N-th window is: {@code [N * size, N * size + size)}.
     * <p>
     * This provides the semantics of tumbling windows, which are fixed-sized, gap-less, non-overlapping windows.
     * Tumbling windows are a special case of hopping windows with {@code advance == size}.
     *
     * @param size The size of the window
     * @return a new window definition without specifying the grace period (default to 24 hours minus window {@code size})
     * @throws IllegalArgumentException if the specified window size is zero or negative or can't be represented as {@code long milliseconds}
     * @deprecated since 3.0. Use {@link #ofSizeWithNoGrace(Duration)} } instead
     */
    @Deprecated
    public static AnnotationAwareTimeWindows of(final Duration size) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(size, "size");
        final long sizeMs = validateMillisecondDuration(size, msgPrefix);

        return new AnnotationAwareTimeWindows(sizeMs, sizeMs, Math.max(DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD - sizeMs, 0), false);
    }

    /**
     * Return a window definition with the original size, but advance ("hop") the window by the given interval, which
     * specifies by how much a window moves forward relative to the previous one.
     * The time interval represented by the N-th window is: {@code [N * advance, N * advance + size)}.
     * <p>
     * This provides the semantics of hopping windows, which are fixed-sized, overlapping windows.
     *
     * @param advance The advance interval ("hop") of the window, with the requirement that {@code 0 < advance.toMillis() <= sizeMs}.
     * @return a new window definition with default maintain duration of 1 day
     * @throws IllegalArgumentException if the advance interval is negative, zero, or larger than the window size
     */
    public AnnotationAwareTimeWindows advanceBy(final Duration advance) {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(advance, "advance");
        final long advanceMs = validateMillisecondDuration(advance, msgPrefix);
        return new AnnotationAwareTimeWindows(sizeMs, advanceMs, graceMs, false);
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(final long timestamp) {
        long windowStart = (Math.max(0, timestamp - sizeMs + advanceMs) / advanceMs) * advanceMs;
        final Map<Long, TimeWindow> windows = new LinkedHashMap<>();
        final TimeWindow window = new TimeWindow(windowStart, windowStart + sizeMs);
        windows.put(windowStart, window);
        return windows;
    }

    @Override
    public long size() {
        return sizeMs;
    }

    /**
     * Reject out-of-order events that arrive more than {@code millisAfterWindowEnd}
     * after the end of its window.
     * <p>
     * Delay is defined as (stream_time - record_timestamp).
     *
     * @param afterWindowEnd The grace period to admit out-of-order events to a window.
     * @return this updated builder
     * @throws IllegalArgumentException if {@code afterWindowEnd} is negative or can't be represented as {@code long milliseconds}
     * @throws IllegalStateException if {@link #grace(Duration)} is called after {@link #ofSizeAndGrace(Duration, Duration)} or {@link #ofSizeWithNoGrace(Duration)}
     * @deprecated since 3.0. Use {@link #ofSizeAndGrace(Duration, Duration)} instead
     */
    @Deprecated
    public AnnotationAwareTimeWindows grace(final Duration afterWindowEnd) throws IllegalArgumentException {
        if (this.hasSetGrace) {
            throw new IllegalStateException(
                    "Cannot call grace() after setting grace value via ofSizeAndGrace or ofSizeWithNoGrace.");
        }

        final String msgPrefix = prepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
        final long afterWindowEndMs = validateMillisecondDuration(afterWindowEnd, msgPrefix);

        return new AnnotationAwareTimeWindows(sizeMs, advanceMs, afterWindowEndMs, false);
    }

    @Override
    public long gracePeriodMs() {
        return graceMs;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AnnotationAwareTimeWindows that = (AnnotationAwareTimeWindows) o;
        return sizeMs == that.sizeMs &&
                advanceMs == that.advanceMs &&
                graceMs == that.graceMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sizeMs, advanceMs, graceMs);
    }

    @Override
    public String toString() {
        return "TimeWindows{" +
                ", sizeMs=" + sizeMs +
                ", advanceMs=" + advanceMs +
                ", graceMs=" + graceMs +
                '}';
    }
}
