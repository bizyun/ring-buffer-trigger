package com.github.bizyun.ringbuffertrigger.impl;

/**
 * @author zhangbiyun
 */
class Event<E> {
    private static final Object MANUALLY_TRIGGER_EVENT = new Object();
    private static final Object SCHEDULER_TRIGGER_EVENT = new Object();

    private Object element;

    public E getElement() {
        return (E) element;
    }

    public void setElement(E element) {
        this.element = element;
    }

    public void markManuallyTrigger() {
        this.element = MANUALLY_TRIGGER_EVENT;
    }

    public void markScheduleTrigger() {
        this.element = SCHEDULER_TRIGGER_EVENT;
    }

    public boolean isManuallyTrigger() {
        return this.element == MANUALLY_TRIGGER_EVENT;
    }

    public boolean isSchedulerTrigger() {
        return this.element == SCHEDULER_TRIGGER_EVENT;
    }

    public void clear() {
        this.element = null;
    }
}
