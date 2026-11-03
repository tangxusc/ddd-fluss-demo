package com.example.orderdemo.domain.order;

/** 领域不变量被破坏时抛出。属于业务异常，不是基础设施故障。 */
public class DomainException extends RuntimeException {

    public DomainException(String message) {
        super(message);
    }
}
