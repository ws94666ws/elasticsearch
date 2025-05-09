/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOError;

class ElasticsearchUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger logger = LogManager.getLogger(ElasticsearchUncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread thread, Throwable t) {
        if (isFatalUncaught(t)) {
            try {
                onFatalUncaught(thread.getName(), t);
            } finally {
                // we use specific error codes in case the above notification failed, at least we
                // will have some indication of the error bringing us down
                if (t instanceof InternalError) {
                    halt(128);
                } else if (t instanceof OutOfMemoryError) {
                    halt(127);
                } else if (t instanceof StackOverflowError) {
                    halt(126);
                } else if (t instanceof UnknownError) {
                    halt(125);
                } else if (t instanceof IOError) {
                    halt(124);
                } else {
                    halt(1);
                }
            }
        } else {
            onNonFatalUncaught(thread.getName(), t);
        }
    }

    static boolean isFatalUncaught(Throwable e) {
        return e instanceof Error;
    }

    void onFatalUncaught(final String threadName, final Throwable t) {
        final String message = "fatal error in thread [" + threadName + "], exiting";
        logger.error(message, t);
    }

    void onNonFatalUncaught(final String threadName, final Throwable t) {
        final String message = "uncaught exception in thread [" + threadName + "]";
        logger.error(message, t);
    }

    @SuppressForbidden(reason = "intentionally halting")
    void halt(int status) {
        // we halt to prevent shutdown hooks from running
        Runtime.getRuntime().halt(status);
    }
}
