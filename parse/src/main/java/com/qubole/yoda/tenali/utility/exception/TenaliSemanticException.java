package com.qubole.yoda.tenali.utility.exception;


import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * An exception to be thrown in case of failure in parsing
 *
 * @version 1.0
 * @since 22 June 2017
 */
public class TenaliSemanticException extends SemanticException {
    public TenaliSemanticException(String msg) {
        super(msg);
    }

    public TenaliSemanticException(Exception ex) {
        super(ex);
    }

}
