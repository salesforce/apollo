/*
 * Copyright (c) 2013 ChiralBehaviors LLC, all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chiralbehaviors.tron;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Finite State Machine implementation.
 *
 * @param <Transitions> the transition interface
 * @param <Context>     the fsm context interface
 * @author hhildebrand
 */
public final class Fsm<Context, Transitions> {
    private static final Logger                             DEFAULT_LOG = LoggerFactory.getLogger(Fsm.class);
    private static final ThreadLocal<Fsm<?, ?>>             thisFsm     = new ThreadLocal<>();
    private final        Transitions                        proxy;
    private final        Deque<State<Context, Transitions>> stack       = new ArrayDeque<>();
    private final        Lock                               sync;
    private final        Class<Transitions>                 transitionsType;
    private              Context                            context;
    private              Transitions                        current;
    private              Logger                             log;
    private              String                             name        = "";
    private              boolean                            pendingPop  = false;
    private              State<Context, Transitions>        pendingPush;
    private              PendingTransition                  popTransition;
    private              Transitions                        previous;
    private              PendingTransition                  pushTransition;
    private              String                             transition;

    Fsm(Context context, boolean sync, Class<Transitions> transitionsType, ClassLoader transitionsCL) {
        this.setContext(context);
        this.sync = sync ? new ReentrantLock() : null;
        this.transitionsType = transitionsType;
        this.log = DEFAULT_LOG;
        @SuppressWarnings("unchecked")
        Transitions facade = (Transitions) Proxy.newProxyInstance(transitionsCL, new Class<?>[] { transitionsType },
                                                                  transitionsHandler());
        proxy = facade;
    }

    /**
     * Construct a new instance of a finite state machine.
     *
     * @param fsmContext    - the object used as the action context for this FSM
     * @param transitions   - the interface class used to define the transitions for this FSM
     * @param transitionsCL - the class loader to be used to load the transitions interface class
     * @param initialState  - the initial state of the FSM
     * @param sync          - true if this FSM is to synchronize state transitions. This is required for multi-threaded
     *                      use of the FSM
     * @return the Fsm instance
     */
    public static <Context, Transitions> Fsm<Context, Transitions> construct(Context fsmContext,
                                                                             Class<Transitions> transitions,
                                                                             ClassLoader transitionsCL,
                                                                             Enum<?> initialState, boolean sync) {
        if (!transitions.isAssignableFrom(initialState.getClass())) {
            throw new IllegalArgumentException(
            String.format("Supplied initial state '%s' does not implement the transitions interface '%s'", initialState,
                          transitions));
        }
        Fsm<Context, Transitions> fsm = new Fsm<>(fsmContext, sync, transitions, transitionsCL);
        @SuppressWarnings("unchecked")
        Transitions initial = (Transitions) initialState;
        fsm.current = initial;
        return fsm;
    }

    /**
     * Construct a new instance of a finite state machine with a default ClassLoader.
     */
    public static <Context, Transitions> Fsm<Context, Transitions> construct(Context fsmContext,
                                                                             Class<Transitions> transitions,
                                                                             Enum<?> initialState, boolean sync) {
        return construct(fsmContext, transitions, fsmContext.getClass().getClassLoader(), initialState, sync);
    }

    /**
     * @return the Context of the currently executing Fsm
     */
    public static <Context> Context thisContext() {
        @SuppressWarnings("unchecked")
        Fsm<Context, ?> fsm = (Fsm<Context, ?>) thisFsm.get();
        return fsm.getContext();
    }

    /**
     * @return the currrently executing Fsm
     */
    public static <Context, Transitions> Fsm<Context, Transitions> thisFsm() {
        @SuppressWarnings("unchecked")
        Fsm<Context, Transitions> fsm = (Fsm<Context, Transitions>) thisFsm.get();
        return fsm;
    }

    /**
     * Execute the initial state's entry action. Note that we do not guard against multiple invocations.
     */
    public void enterStartState() {
        if (log.isTraceEnabled()) {
            log.trace(String.format("[%s] Entering start state %s", name, prettyPrint(current)));
        }
        executeEntryAction();
    }

    /**
     * @return the action context object of this Fsm
     */
    public Context getContext() {
        return context;
    }

    /**
     * Set the Context of the FSM
     */
    public void setContext(Context context) {
        this.context = context;
    }

    /**
     * @return the current state of the Fsm
     */
    public Transitions getCurrentState() {
        Transitions transitions = current;
        return transitions;
    }

    /**
     * @return the logger used by this Fsm
     */
    public Logger getLog() {
        return locked(() -> {
            Logger c = log;
            return c;
        });
    }

    /**
     * Set the Logger for this Fsm.
     *
     * @param log - the Logger of this Fsm
     */
    public void setLog(Logger log) {
        this.log = log;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the previous state of the Fsm, or null if no previous state
     */
    public Transitions getPreviousState() {
        return locked(() -> {
            Transitions transitions = previous;
            return transitions;
        });
    }

    /**
     * @return the String representation of the current transition
     */
    public String getTransition() {
        return locked(() -> {
            return transition;
        });
    }

    /**
     * @return the Transitions object that drives this Fsm through its transitions
     */
    public Transitions getTransitions() {
        return proxy;
    }

    /**
     * @return the invalid transition excepiton based current transition attempt
     */
    public InvalidTransition invalidTransitionOn() {
        return new InvalidTransition(String.format("[%s] %s.%s", name, prettyPrint(current), transition));
    }

    /**
     * Pop the state off of the stack of pushed states. This state will become the current state of the Fsm. Answer the
     * Transitions object that may be used to send a transition to the popped state.
     *
     * @return the Transitions object that may be used to send a transition to the popped state.
     */
    public Transitions pop() {
        if (pendingPop) {
            throw new IllegalStateException(String.format("[%s] State has already been popped", name));
        }
        if (pendingPush != null) {
            throw new IllegalStateException(String.format("[%s] Cannot pop after pushing", name));
        }
        if (stack.size() == 0) {
            throw new IllegalStateException(
            String.format("[%s] State stack is empty, current state: %s, transition: %s", name, prettyPrint(current),
                          transition));
        }
        pendingPop = true;
        popTransition = new PendingTransition();
        @SuppressWarnings("unchecked")
        Transitions pendingTransition = (Transitions) Proxy.newProxyInstance(getContext().getClass().getClassLoader(),
                                                                             new Class<?>[] { transitionsType },
                                                                             popTransition);
        return pendingTransition;
    }

    public String prettyPrint(Transitions state) {
        if (state == null) {
            return "null";
        }
        Class<?> enclosingClass = state.getClass().getEnclosingClass();
        return String.format("%s.%s", (enclosingClass != null ? enclosingClass : state.getClass()).getSimpleName(),
                             ((Enum<?>) state).name());
    }

    /**
     * Push the current state of the Fsm on the state stack. The supplied state becomes the current state of the Fsm
     *
     * @param state - the new current state of the Fsm.
     */
    public Transitions push(Transitions state) {
        return push(state, context);
    }

    /**
     * Push the current state of the Fsm on the state stack. The supplied state becomes the current state of the Fsm
     *
     * @param state   - the new current state of the Fsm.
     * @param context - the new current context of the FSM
     */
    public Transitions push(Transitions state, Context context) {
        if (state == null) {
            throw new IllegalStateException(String.format("[%s] Cannot push a null state", name));
        }
        if (pendingPush != null) {
            throw new IllegalStateException(String.format("[%s] Cannot push state twice", name));
        }
        if (pendingPop) {
            throw new IllegalStateException(String.format("[%s] Cannot push after pop", name));
        }
        pushTransition = new PendingTransition();
        pendingPush = new State<>(context, state);
        @SuppressWarnings("unchecked")
        Transitions pendingTransition = (Transitions) Proxy.newProxyInstance(getContext().getClass().getClassLoader(),
                                                                             new Class<?>[] { transitionsType },
                                                                             pushTransition);
        return pendingTransition;
    }

    public <R> R synchonizeOnState(Callable<R> call) throws Exception {
        return locked(call);
    }

    public void synchonizeOnState(Runnable call) {
        locked(() -> {
            call.run();
            return null;
        });
    }

    @Override
    public String toString() {
        return String.format("Fsm [name = %s, current=%s, previous=%s, transition=%s]", name, prettyPrint(current),
                             prettyPrint(previous), getTransition());
    }

    private void executeEntryAction() {
        for (Method action : current.getClass().getDeclaredMethods()) {
            if (action.isAnnotationPresent(Entry.class)) {
                action.setAccessible(true);
                if (log.isTraceEnabled()) {
                    log.trace(
                    String.format("[%s] Entry action: %s.%s", name, prettyPrint(current), prettyPrint(action)));
                }
                try {
                    // For entry actions with parameters, inject the context
                    if (action.getParameterTypes().length > 0)
                        action.invoke(current, getContext());
                    else
                        action.invoke(current);
                    return;
                } catch (IllegalAccessException | IllegalArgumentException e) {
                    throw new IllegalStateException(e);
                } catch (InvocationTargetException e) {
                    Throwable targetException = e.getTargetException();
                    if (targetException instanceof RuntimeException) {
                        throw (RuntimeException) targetException;
                    }
                    throw new IllegalStateException(targetException);
                }
            }
        }
    }

    private void executeExitAction() {
        for (Method action : current.getClass().getDeclaredMethods()) {
            if (action.isAnnotationPresent(Exit.class)) {
                action.setAccessible(true);
                if (log.isTraceEnabled()) {
                    log.trace(
                    String.format("[%s] Exit action: %s.%s", name, prettyPrint(current), prettyPrint(action)));
                }
                try {
                    // For exit action with parameters, inject the context
                    if (action.getParameterTypes().length > 0)
                        action.invoke(current, getContext());
                    else
                        action.invoke(current);
                    return;
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    /**
     * The Jesus Nut
     *
     * @param t         - the transition to fire
     * @param arguments - the transition arguments
     * @return
     */
    private Object fire(Method t, Object[] arguments) {
        if (t == null) {
            return null;
        }
        Fsm<?, ?> previousFsm = thisFsm.get();
        thisFsm.set(this);
        previous = current;
        if (!transitionsType.isAssignableFrom(t.getReturnType())) {
            try {
                return t.invoke(current, arguments);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }

        try {
            transition = prettyPrint(t);
            Transitions nextState;
            Transitions pinned = current;
            try {
                nextState = fireTransition(lookupTransition(t), arguments);
            } catch (InvalidTransition e) {
                nextState = fireTransition(lookupDefaultTransition(e, t), arguments);
            }
            if (pinned == current) {
                transitionTo(nextState);
            } else {
                if (nextState != null && log.isTraceEnabled()) {
                    log.trace(
                    String.format("[%s] Eliding Transition %s -> %s, pinned state: %s", name, prettyPrint(current),
                                  prettyPrint(nextState), prettyPrint(pinned)));
                }
            }
            return null;
        } finally {
            thisFsm.set(previousFsm);
        }
    }

    /**
     * Fire the concrete transition of the current state
     *
     * @param stateTransition - the transition method to execute
     * @param arguments       - the arguments of the method
     * @return the next state
     */
    @SuppressWarnings("unchecked")
    private Transitions fireTransition(Method stateTransition, Object[] arguments) {
        if (stateTransition.isAnnotationPresent(Default.class)) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("[%s] Default transition: %s.%s", prettyPrint(current)), getTransition(), name);
            }
            try {
                return (Transitions) stateTransition.invoke(current, (Object[]) null);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new IllegalStateException(
                String.format("Unable to invoke transition %s,%s", prettyPrint(current), prettyPrint(stateTransition)),
                e);
            }
        }
        if (log.isTraceEnabled()) {
            log.trace(String.format("[%s] Transition: %s.%s", name, prettyPrint(current), getTransition()));
        }
        try {
            return (Transitions) stateTransition.invoke(current, arguments);
        } catch (IllegalAccessException | IllegalArgumentException e) {
            throw new IllegalStateException(
            String.format("Unable to invoke transition %s.%s", prettyPrint(current), prettyPrint(stateTransition)),
            e.getCause());
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof InvalidTransition) {
                if (log.isTraceEnabled()) {
                    log.trace(
                    String.format("[%s] Invalid transition %s.%s", name, prettyPrint(current), getTransition()));
                }
                throw (InvalidTransition) e.getTargetException();
            }
            if (e.getTargetException() instanceof RuntimeException) {
                throw (RuntimeException) e.getTargetException();
            }
            throw new IllegalStateException(
            String.format("[%s] Unable to invoke transition %s.%s", name, prettyPrint(current),
                          prettyPrint(stateTransition)), e.getTargetException());
        }
    }

    private <T> T locked(Callable<T> call) {
        final Lock lock = sync;
        if (lock != null) {
            lock.lock();
        }
        try {
            return call.call();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    private Method lookupDefaultTransition(InvalidTransition previousException, Method t) {
        // look for a @Default transition for the state singleton
        for (Method defaultTransition : current.getClass().getDeclaredMethods()) {
            if (defaultTransition.isAnnotationPresent(Default.class)) {
                defaultTransition.setAccessible(true);
                return defaultTransition;
            }
        }
        // look for a @Default transition for the state on the enclosing enum class
        for (Method defaultTransition : current.getClass().getMethods()) {
            if (defaultTransition.isAnnotationPresent(Default.class)) {
                defaultTransition.setAccessible(true);
                return defaultTransition;
            }
        }
        if (previousException == null) {
            throw new InvalidTransition(String.format(prettyPrint(t)));
        } else {
            throw previousException;
        }
    }

    /**
     * Lookup the transition.
     *
     * @param t - the transition defined in the interface
     * @return the transition Method for the current state matching the interface definition
     */
    private Method lookupTransition(Method t) {
        Method stateTransition = null;
        try {
            // First we try declared methods on the state
            stateTransition = current.getClass().getMethod(t.getName(), t.getParameterTypes());
        } catch (NoSuchMethodException | SecurityException e1) {
            throw new IllegalStateException(
            String.format("Inconcievable!  The state %s does not implement the transition %s", prettyPrint(current),
                          prettyPrint(t)));
        }
        stateTransition.setAccessible(true);
        return stateTransition;
    }

    /**
     * Ye olde tyme state transition
     *
     * @param nextState - the next state of the Fsm
     */
    private void normalTransition(Transitions nextState) {
        if (nextState == null) { // internal loopback transition
            if (log.isTraceEnabled()) {
                log.trace(String.format("[%s] Internal loopback: %s", name, prettyPrint(current)));
            }
            return;
        }
        executeExitAction();
        if (log.isTraceEnabled()) {
            log.trace(
            String.format("[%s] State transition:  %s -> %s", name, prettyPrint(current), prettyPrint(nextState)));
        }
        current = nextState;
        executeEntryAction();
    }

    /**
     * Execute the exit action of the current state. Set current state to popped state of the stack. Execute any pending
     * transition on the current state.
     */
    private void popTransition() {
        pendingPop = false;
        previous = current;
        State<Context, Transitions> pop = stack.pop();
        PendingTransition pendingTransition = popTransition;
        popTransition = null;

        executeExitAction();
        if (log.isTraceEnabled()) {
            log.trace(String.format("[%s] State transition:  %s -> %s - Popping(%s)", name, prettyPrint(previous),
                                    prettyPrint(pop), stack.size() + 1));
        }
        current = pop.transitions;
        if (pop.context != null) {
            setContext(pop.context);
        }
        if (pendingTransition != null) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("[%s] Pop transition: %s.%s", name, prettyPrint(current),
                                        prettyPrint(pendingTransition.method)));
            }
            fire(pendingTransition.method, pendingTransition.args);
        }
    }

    private String prettyPrint(State<Context, Transitions> state) {
        return prettyPrint(state.transitions) + " [" + state.context == null ? "<>: " + getContext()
                                                                             : state.context + "]";
    }

    private String prettyPrint(Method transition) {
        StringBuilder builder = new StringBuilder();
        if (transition != null) {
            builder.append(transition.getName());
            builder.append('(');
            Class<?>[] parameters = transition.getParameterTypes();
            for (int i = 0; i < parameters.length; i++) {
                builder.append(parameters[i].getSimpleName());
                if (i != parameters.length - 1) {
                    builder.append(", ");
                }
            }
            builder.append(')');
        } else {
            builder.append("loopback");
        }
        return builder.toString();
    }

    /**
     * Push the current state of the Fsm to the stack, with the supplied context as the new current context of the FSM,
     * if non null. Transition the Fsm to the nextState, execute the entry action of that state. Set the current state
     * of the Fsm to the pending push state, executing the entry action on that state
     *
     * @param nextState
     */
    private void pushTransition(Transitions nextState) {
        State<Context, Transitions> pushed = pendingPush;
        pendingPush = null;
        normalTransition(nextState);
        stack.push(new State<>(context, current));
        if (log.isTraceEnabled()) {
            log.trace(String.format("[%s] State transition: %s -> %s - Pushing(%s)", name, prettyPrint(current),
                                    prettyPrint(pushed), stack.size()));
        }
        current = pushed.transitions;
        if (pushed.context != null) {
            setContext(pushed.context);
        }
        Transitions pinned = current;
        PendingTransition pushTrns = pushTransition;
        pushTransition = null;
        executeEntryAction();
        if (pushTrns != null) {
            if (current != pinned) {
                log.trace(String.format("[%s] Eliding push transition %s.%s pinned: %s", name, prettyPrint(current),
                                        prettyPrint(pushTrns.method), prettyPrint(pinned)));
            } else {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("[%s] Push transition: %s.%s", name, prettyPrint(current),
                                            prettyPrint(pushTrns.method)));
                }
                fire(pushTrns.method, pushTrns.args);
            }
        }
    }

    /**
     * Transition to the next state
     *
     * @param nextState
     */
    private void transitionTo(Transitions nextState) {
        if (pendingPush != null) {
            pushTransition(nextState);
        } else if (pendingPop) {
            popTransition();
        } else {
            normalTransition(nextState);
        }
    }

    private InvocationHandler transitionsHandler() {
        return new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                return locked(() -> fire(method, args));
            }
        };
    }

    private static class State<Context, Transitions> {
        private final Context     context;
        private final Transitions transitions;

        public State(Context context, Transitions transitions) {
            this.context = context;
            this.transitions = transitions;
        }

    }

    private static class PendingTransition implements InvocationHandler {
        private volatile Object[] args;
        private volatile Method   method;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (this.method != null) {
                throw new IllegalStateException(
                String.format("Pop transition '%s' has already been established", method.toGenericString()));
            }
            this.method = method;
            this.args = args;
            return null;
        }
    }
}
