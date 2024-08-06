package com.salesforce.apollo.h2;

public interface SessionServices {
    SessionServices NO_SERVICES = new SessionServices() {
        public <T> T call(String serviceName, Object... parameters) throws ServiceNotFoundException {
            throw new ServiceNotFoundException(serviceName);
        }

        @Override
        public void run(String serviceName, Object... parameters) throws ServiceNotFoundException {
            throw new ServiceNotFoundException(serviceName);
        }
    };

    <T> T call(String serviceName, Object... parameters) throws ServiceNotFoundException;

    void run(String serviceName, Object... parameters) throws ServiceNotFoundException;

    class ServiceNotFoundException extends Exception {
        public ServiceNotFoundException(String serviceName) {
            super(serviceName);
        }
    }
}
