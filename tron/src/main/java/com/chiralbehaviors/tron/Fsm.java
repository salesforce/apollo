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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Finite State Machine implementation.
 * 
 * @author hhildebrand
 * 
 * @param <Transitions> the transition interface
 * @param <Context>     the fsm context interface
 */
public final class Fsm<Context, Transitions> {
    private static class PopTransition implements InvocationHandler {
        private Object[] args;
        private Method   method;

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

    private static final Logger                 DEFAULT_LOG = LoggerFactory.getLogger(Fsm.class);
    private static final ThreadLocal<Fsm<?, ?>> thisFsm     = new ThreadLocal<>();

    /**
     * Construct a new instance of a finite state machine.
     * 
     * @param fsmContext    - the object used as the action context for this FSM
     * @param transitions   - the interface class used to define the transitions for
     *                      this FSM
     * @param transitionsCL - the class loader to be used to load the transitions
     *                      interface class
     * @param initialState  - the initial state of the FSM
     * @param sync          - true if this FSM is to synchronize state transitions.
     *                      This is required for multi-threaded use of the FSM
     * @return the Fsm instance
     */
    public static <Context, Transitions> Fsm<Context, Transitions> construct(Context fsmContext,
                                                                             Class<Transitions> transitions,
                                                                             ClassLoader transitionsCL,
                                                                             Enum<?> initialState, boolean sync) {
        if (!transitions.isAssignableFrom(initialState.getClass())) {
            throw new IllegalArgumentException(
                    String.format("Supplied initial state '%s' does not implement the transitions interface '%s'",
                                  initialState, transitions));
        }
        Fsm<Context, Transitions> fsm = new Fsm<>(fsmContext, sync, transitions, transitionsCL);
        @SuppressWarnings("unchecked")
        Transitions initial = (Transitions) initialState;
        fsm.setCurrentState(initial);
        return fsm;
    }

    /**
     * Construct a new instance of a finite state machine with a default
     * ClassLoader.
     */
    public static <Context, Transitions> Fsm<Context, Transitions> construct(Context fsmContext,
                                                                             Class<Transitions> transitions,
                                                                             Enum<?> initialState, boolean sync) {
        return construct(fsmContext, transitions, fsmContext.getClass().getClassLoader(), initialState, sync);
    }

    /**
     * 
     * @return the Context of the currently executing Fsm
     */
    public static <Context> Context thisContext() {
        @SuppressWarnings("unchecked")
        Fsm<Context, ?> fsm = (Fsm<Context, ?>) thisFsm.get();
        return fsm.getContext();
    }

    /**
     * 
     * @return the currrently executing Fsm
     */
    public static <Context, Transitions> Fsm<Context, Transitions> thisFsm() {
        @SuppressWarnings("unchecked")
        Fsm<Context, Transitions> fsm = (Fsm<Context, Transitions>) thisFsm.get();
        return fsm;
    }

    private final Context        context;
    private Enum<?>              current;
    private Logger               log;
    private String               name;
    private boolean              pendingPop = false;
    private Enum<?>              pendingPush;
    private PopTransition        popTransition;
    private Enum<?>              previous;
    private final Transitions    proxy;
    private final Deque<Enum<?>> stack      = new ArrayDeque<>();

    private final Lock sync;

    private String transition;

    private final Class<Transitions> transitionsType;

    Fsm(Context context, boolean sync, Class<Transitions> transitionsType, ClassLoader transitionsCL) {
        this.context = context;
        this.sync = sync ? new ReentrantLock() : null;
        this.transitionsType = transitionsType;
        this.log = DEFAULT_LOG;
        @SuppressWarnings("unchecked")
        Transitions facade = (Transitions) Proxy.newProxyInstance(transitionsCL, new Class<?>[] { transitionsType },
                                                                  transitionsHandler());
        proxy = facade;
    }

    /**
     * Execute the initial state's entry action. Note that we do not guard against
     * multiple invocations.
     */
    public void enterStartState() {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Entering start state %s", prettyPrint(current)));
        }
        executeEntryAction();
    }

    /**
     * 
     * @return the action context object of this Fsm
     */
    public Context getContext() {
        return context;
    }

    /**
     * 
     * @return the current state of the Fsm
     */
    public Transitions getCurrentState() {
        @SuppressWarnings("unchecked")
        Transitions transitions = (Transitions) current;
        return transitions;
    }

    /**
     * 
     * @return the logger used by this Fsm
     */
    public Logger getLog() {
        return log;
    }

    public String getName() {
        return name;
    }

    /**
     * 
     * @return the previous state of the Fsm, or null if no previous state
     */
    public Transitions getPreviousState() {
        @SuppressWarnings("unchecked")
        Transitions transitions = (Transitions) previous;
        return transitions;
    }

    /**
     * 
     * @return the String representation of the current transition
     */
    public String getTransition() {
        return transition;
    }

    /**
     * 
     * @return the Transitions object that drives this Fsm through its transitions
     */
    public Transitions getTransitions() {
        return proxy;
    }

    /**
     * Pop the state off of the stack of pushed states. This state will become the
     * current state of the Fsm. Answer the Transitions object that may be used to
     * send a transition to the popped state.
     * 
     * @return the Transitions object that may be used to send a transition to the
     *         popped state.
     */
    public Transitions pop() {
        if (pendingPop) {
            throw new IllegalStateException("State has already been popped");
        }
        if (pendingPush != null) {
            throw new IllegalStateException("Cannot pop after pushing");
        }
        if (stack.size() == 0) {
            throw new IllegalStateException("State stack is empty");
        }
        pendingPop = true;
        popTransition = new PopTransition();
        @SuppressWarnings("unchecked")
        Transitions pendingTransition = (Transitions) Proxy.newProxyInstance(context.getClass().getClassLoader(),
                                                                             new Class<?>[] { transitionsType },
                                                                             popTransition);
        return pendingTransition;
    }

    /**
     * Push the current state of the Fsm on the state stack. The supplied state
     * becomes the current state of the Fsm
     * 
     * @param state - the new current state of the Fsm.
     */
    public void push(Transitions state) {
        if (state == null) {
            throw new IllegalStateException("Cannot push a null state");
        }
        if (pendingPush != null) {
            throw new IllegalStateException("Cannot push state twice");
        }
        if (pendingPop) {
            throw new IllegalStateException("Cannot push after pop");
        }
        pendingPush = (Enum<?>) state;
    }

    /**
     * Set the Logger for this Fsm.
     * 
     * @param log - the Logger of this Fsm
     */
    public void setLog(Logger log) {
        this.log = log;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return String.format("Fsm [name = %s, current=%s, previous=%s, transition=%s]", name, prettyPrint(current),
                             prettyPrint(previous), transition);
    }

    private void executeEntryAction() {
        for (Method action : current.getClass().getDeclaredMethods()) {
            if (action.isAnnotationPresent(Entry.class)) {
                action.setAccessible(true);
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Executing entry action %s for state %s", prettyPrint(action),
                                            prettyPrint(current)));
                }
                try {
                    // For entry actions with parameters, inject the context
                    if (action.getParameterTypes().length > 0)
                        action.invoke(current, context);
                    else
                        action.invoke(current, new Object[] {});
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
                    log.trace(String.format("Executing exit action %s for state %s", prettyPrint(action),
                                            prettyPrint(current)));
                }
                try {
                    // For exit action with parameters, inject the context
                    if (action.getParameterTypes().length > 0)
                        action.invoke(current, context);
                    else
                        action.invoke(current, new Object[] {});
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
        if (sync != null) {
            try {
                sync.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new RuntimeException(
                        String.format("Unable to fire transition [%s] due to thread interruption", t.getName()), e);
            }
        }
        Fsm<?, ?> previousFsm = thisFsm.get();
        thisFsm.set(this);
        previous = current;

        try {
            if (!transitionsType.isAssignableFrom(t.getReturnType())) {
                try {
                    return t.invoke(current, arguments);
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new IllegalStateException(e);
                }
            }

            transition = prettyPrint(t);
            Enum<?> nextState;
            try {
                nextState = fireTransition(lookupTransition(t), arguments);
            } catch (InvalidTransition e) {
                nextState = fireTransition(lookupDefaultTransition(e, t), arguments);
            }
            transitionTo(nextState);
        } finally {
            thisFsm.set(previousFsm);
            if (sync != null) {
                sync.unlock();
            }
        }
        return null;
    }

    /**
     * Fire the concrete transition of the current state
     * 
     * @param stateTransition - the transition method to execute
     * @param arguments       - the arguments of the method
     * 
     * @return the next state
     */
    private Enum<?> fireTransition(Method stateTransition, Object[] arguments) {
        if (stateTransition.isAnnotationPresent(Default.class)) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Executing default transition %s on state %s", transition,
                                        prettyPrint(current)));
            }
            try {
                return (Enum<?>) stateTransition.invoke(current, (Object[]) null);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new IllegalStateException(String.format("Unable to invoke transition %s on state %s",
                                                              prettyPrint(stateTransition), prettyPrint(current)),
                        e);
            }
        }
        if (log.isTraceEnabled()) {
            log.trace(String.format("Executing transition %s on state %s", transition, prettyPrint(current)));
        }
        try {
            return (Enum<?>) stateTransition.invoke(current, arguments);
        } catch (IllegalAccessException | IllegalArgumentException e) {
            throw new IllegalStateException(String.format("Unable to invoke transition %s on state %s",
                                                          prettyPrint(stateTransition), prettyPrint(current)),
                    e);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof InvalidTransition) {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Invalid transition %s on state %s", transition, prettyPrint(current)));
                }
                throw new InvalidTransition(prettyPrint(stateTransition) + " -> " + prettyPrint(current), e);
            }
            throw new IllegalStateException(String.format("Unable to invoke transition %s on state %s",
                                                          prettyPrint(stateTransition), prettyPrint(current)),
                    e);
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
     * @return the transition Method for the current state matching the interface
     *         definition
     */
    private Method lookupTransition(Method t) {
        Method stateTransition = null;
        try {
            // First we try declared methods on the state
            stateTransition = current.getClass().getMethod(t.getName(), t.getParameterTypes());
        } catch (NoSuchMethodException | SecurityException e1) {
            throw new IllegalStateException(
                    String.format("Inconcievable!  The state %s does not implement the transition %s",
                                  prettyPrint(current), prettyPrint(t)));
        }
        stateTransition.setAccessible(true);
        return stateTransition;
    }

    /**
     * Ye olde tyme state transition
     * 
     * @param nextState - the next state of the Fsm
     */
    private void normalTransition(Enum<?> nextState) {
        if (nextState == null) { // internal loopback transition
            if (log.isTraceEnabled()) {
                log.trace(String.format("Internal loopback transition to state %s", prettyPrint(current)));
            }
            return;
        }
        executeExitAction();
        if (log.isTraceEnabled()) {
            log.trace(String.format("Transitioning to state %s from %s", prettyPrint(nextState), prettyPrint(current)));
        }
        current = nextState;
        executeEntryAction();
    }

    /**
     * Execute the exit action of the current state. Set current state to popped
     * state of the stack. Execute any pending transition on the current state.
     */
    private void popTransition() {
        pendingPop = false;
        previous = current;
        Enum<?> pop = stack.pop();
        PopTransition pendingTransition = popTransition;
        popTransition = null;

        executeExitAction();
        if (log.isTraceEnabled()) {
            log.trace(String.format("Popping to state %s", prettyPrint(pop)));
        }
        current = pop;
        if (pendingTransition != null) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Pop transition %s.%s", prettyPrint(current),
                                        prettyPrint(pendingTransition.method)));
            }
            fire(pendingTransition.method, pendingTransition.args);
        }
    }

    private String prettyPrint(Enum<?> state) {
        if (state == null) {
            return "null";
        }
        Class<?> enclosingClass = state.getClass().getEnclosingClass();
        return String.format("%s.%s", (enclosingClass != null ? enclosingClass : state.getClass()).getSimpleName(),
                             state.name());
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
     * Push the current state of the Fsm to the stack. If non null, transition the
     * Fsm to the nextState, execute the entry action of that state. Set the current
     * state of the Fsm to the pending push state, executing the entry action on
     * that state
     * 
     * @param nextState
     */
    private void pushTransition(Enum<?> nextState) {
        Enum<?> pushed = pendingPush;
        pendingPush = null;
        normalTransition(nextState);
        stack.push(current);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Pushing to state %s from %s", prettyPrint(pushed), prettyPrint(current)));
        }
        current = pushed;
        executeEntryAction();
    }

    /**
     * Set the current state of the Fsm. The entry action for this state will not be
     * called.
     * 
     * @param state - the new current state of the Fsm
     */
    private void setCurrentState(Transitions state) {
        current = (Enum<?>) state;
    }

    private InvocationHandler transitionsHandler() {
        return new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                return fire(method, args);
            }
        };
    }

    /**
     * Transition to the next state
     * 
     * @param nextState
     */
    private void transitionTo(Enum<?> nextState) {
        if (pendingPush != null) {
            pushTransition(nextState);
        } else if (pendingPop) {
            popTransition();
        } else {
            normalTransition(nextState);
        }
    }
}
