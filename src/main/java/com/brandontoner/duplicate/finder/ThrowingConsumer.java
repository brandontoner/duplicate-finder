package com.brandontoner.duplicate.finder;
public interface ThrowingConsumer<T, E extends Throwable> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     * @throws E exception type
     */
    void accept(T t) throws E;
}
