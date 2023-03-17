package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram<Integer> {

    private int[]   hist;
    private int     buckets;
    private int     width;
    private int     lastBucketWidth;
    private int     minVal;
    private int     maxVal;
    private int     tupleNum;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // DONE
        this.buckets            = buckets;
        this.hist               = new int[buckets];
        int interval            = max + 1 - min;
        this.width              = (int) Math.ceil(1.0 * interval / buckets);
        this.lastBucketWidth    = interval - (this.width * (buckets - 1));
        this.minVal             = min;
        this.maxVal             = max;
        this.tupleNum           = 0;
    }


    public int getBucketIdx(int v) {
        return (v - minVal) / width;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(Integer v) {
        // DONE
        assert v >= minVal && v <= maxVal : "invalid value, out of range" + minVal + " " + maxVal + " " + v;
        this.hist[getBucketIdx(v)]++;
        this.tupleNum++;
    }


    private double estimateLess(int constVal, int bucketWidth) {
        if (constVal <= minVal) {
            return 0;
        }
        if (constVal > maxVal) {
            return 1.0;
        }

        // As the lab3 doc, result = ((right - val) / bucketWidth) * (bucketTuples / totalTuples)
        int     bucketIndex     = getBucketIdx(constVal);
        double  bucketFraction  = hist[bucketIndex] * 1.0 / tupleNum;

        int     bucketLeft      = bucketIndex * width + minVal;
        double  bucketRatio     = (constVal - bucketLeft) * 1.0 / bucketWidth;
        double  result          = bucketFraction * bucketRatio;

        int sum = 0;
        for (int i = 0; i < bucketIndex; i++) {
            sum += hist[i];
        }
        return (sum * 1.0) / tupleNum + result;
    }


    private double estimateEqual(int constVal, int bucketWidth) {
        if (constVal < minVal || constVal > maxVal) {
            return 0;
        }
        // As the lab3 doc, result = (bucketHeight / bucketWidth) / totalTuples
        return 1.0 * hist[getBucketIdx(constVal)] / bucketWidth / tupleNum;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, Integer v) {
        // DONE
        final int bucketIndex = getBucketIdx(v);
        final int bucketWidth = bucketIndex < buckets - 1 ? width : lastBucketWidth;
        double ans;
        switch (op) {
            case EQUALS:
                ans = estimateEqual(v, bucketWidth);
                break;
            case NOT_EQUALS:
                ans = 1.0 - estimateEqual(v, bucketWidth);
                break;
            case LESS_THAN:
                ans = estimateLess(v, bucketWidth);
                break;
            case LESS_THAN_OR_EQ:
                ans = estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            case GREATER_THAN:
                ans = 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v) - estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            case GREATER_THAN_OR_EQ:
                ans = 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
                break;
            default:
                return -1;
        }
        return ans;
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // DONE
        return Arrays.stream(hist).average().orElse(Double.NaN);
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // DONE
        return "IntHistogram{" + "maxVal=" + maxVal + ", minVal=" + minVal + ", heights=" + Arrays.toString(hist)
                + ", buckets=" + buckets + ", totalTuples=" + tupleNum + ", width=" + width + ", lastBucketWidth="
                + lastBucketWidth + '}';
    }
}
