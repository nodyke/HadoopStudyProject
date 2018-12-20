package com.dbocharov.hive.udf;

import java.util.Objects;

public class Range {
   private Long start;
   private Long end;

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Range range = (Range) o;
        return Objects.equals(start, range.start) &&
                Objects.equals(end, range.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    public Range(Long start, Long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return "Range{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }
    //Use integer or long as argument, create range from number
    public static Range createRangeFromNumber(Object number) {
        if(number instanceof Integer) {
            Long n = ((Integer) number).longValue();
            return new Range(n,n);
        }
        if(number instanceof Long) {
            Long n = (Long)number;
            return new Range(n,n);
        }
        throw new IllegalArgumentException("Argument is not a decimal");
    }
}
