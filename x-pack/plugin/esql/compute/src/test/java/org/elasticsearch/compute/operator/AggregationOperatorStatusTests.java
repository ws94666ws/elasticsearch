/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class AggregationOperatorStatusTests extends AbstractWireSerializingTestCase<AggregationOperator.Status> {
    public static AggregationOperator.Status simple() {
        return new AggregationOperator.Status(200012, 400036, 123, 111, 222);
    }

    public static String simpleToJson() {
        return """
            {
              "aggregation_nanos" : 200012,
              "aggregation_time" : "200micros",
              "aggregation_finish_nanos" : 400036,
              "aggregation_finish_time" : "400micros",
              "pages_processed" : 123,
              "rows_received" : 111,
              "rows_emitted" : 222
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<AggregationOperator.Status> instanceReader() {
        return AggregationOperator.Status::new;
    }

    @Override
    public AggregationOperator.Status createTestInstance() {
        return new AggregationOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected AggregationOperator.Status mutateInstance(AggregationOperator.Status instance) {
        long aggregationNanos = instance.aggregationNanos();
        long aggregationFinishNanos = instance.aggregationFinishNanos();
        int pagesProcessed = instance.pagesProcessed();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        switch (between(0, 4)) {
            case 0 -> aggregationNanos = randomValueOtherThan(aggregationNanos, ESTestCase::randomNonNegativeLong);
            case 1 -> aggregationFinishNanos = randomValueOtherThan(aggregationFinishNanos, ESTestCase::randomNonNegativeLong);
            case 2 -> pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
            case 3 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 4 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new AggregationOperator.Status(aggregationNanos, aggregationFinishNanos, pagesProcessed, rowsReceived, rowsEmitted);
    }
}
