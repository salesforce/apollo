
//////////////////////////////////////////////////////////////////////
//                                                                  //
//  JCSP ("CSP for Java") Libraries                                 //
//  Copyright (C) 1996-2018 Peter Welch, Paul Austin and Neil Brown //
//                2001-2004 Quickstone Technologies Limited         //
//                2005-2018 Kevin Chalmers                          //
//                                                                  //
//  You may use this work under the terms of either                 //
//  1. The Apache License, Version 2.0                              //
//  2. or (at your option), the GNU Lesser General Public License,  //
//       version 2.1 or greater.                                    //
//                                                                  //
//  Full licence texts are included in the LICENCE file with        //
//  this library.                                                   //
//                                                                  //
//  Author contacts: P.H.Welch@kent.ac.uk K.Chalmers@napier.ac.uk   //
//                                                                  //
//////////////////////////////////////////////////////////////////////

package jcsp.lang;

/**
 * This provides a <I>Concurrent Read Exclusive Write</I> (CREW) lock for
 * synchronising fair and secure access to a shared resource.
 * <P>
 * <A HREF="#constructor_summary">Shortcut to the Constructor and Method
 * Summaries.</A>
 *
 * <H2>Description</H2>
 * <H3>Concurrent Read Exclusive Write</H3> Parallel processes must ensure
 * controlled access to shared resources whose state can change as a side-effect
 * of that access. Otherwise, there will be <I>race hazards</I> resulting from
 * arbitrary interleaving of that access between competing processes. For
 * example, a <I>reader</I> of a resource may observe partially updated (and,
 * hence, invalid) state because of the activities of a concurrent
 * <I>writer</I>. Or two <I>writers</I> may interfere with each other's updating
 * to leave a resource in an invalid state (as well as confusing themselves).
 * <A NAME="Static">
 * <P>
 * Where possible, each resource should be kept wrapped up in a process and
 * accessed via a channel interface - this will always be safe. However, this
 * also <I>serialises</I> access to the resource so that it can be used by only
 * one process at a time, regardless of whether that usage is read-only or
 * read-write. This is an example of <I>Exclusive Read Exclusive Write</I>
 * (EREW).
 * <P>
 * [<I>Note:</I> the above assumes the resource process has only a serial
 * implementation. A parallel implementation, of course, will allow parallel
 * access along parallel channels. However, if that parallel implementation
 * needs to share state, that shared state is itself a resource and we have only
 * deferred the problem of secure parallel access.]
 * <P>
 * Parallel <I>reader</I> operations on a resource do not lead to race hazards,
 * so long as no <I>writer</I> is present, and many applications need to be able
 * to exploit this. Parallel <I>writer</I> operations are always dangerous and
 * must be suppressed. This principle is known as <I>Concurrent Read Exclusive
 * Write</I> (CREW).
 *
 * <H3><A NAME="Pattern">Design Pattern</H3> Suppose many processes hold a
 * reference to a shared object. Associate that shared object with a shared
 * <TT>Crew</TT> lock, reference to which must also be given to each of those
 * processes.
 * <P>
 * For example, suppose <TT>Resource</TT> is a class whose methods may be
 * classified as either <I>readers</I> (i.e. cause no state change) or
 * <I>writers</I> (i.e. cause state change). Whenever we construct a
 * <TT>Resource</TT>, construct an associated <TT>Crew</TT> lock:
 * 
 * <PRE>
 *   Resource resource = new Resource (...);
 *   Crew resourceCrew = new Crew ();
 * </PRE>
 * 
 * <A NAME="Access"> Each process holding a reference to <TT>resource</TT> must
 * also be given a reference to <TT>resourceCrew</TT>. Invocations of
 * <I>reader</I> methods must be sandwiched between a claim and release of the
 * <I>reader</I> lock within <TT>resourceCrew</TT>:
 * 
 * <PRE>
 *   resourceCrew.startRead(); // this will block until no writer is present
 *   ...                       // invoke reader methods of resource
 *   resourceCrew.endRead();   // releases this reader's lock on resource
 * </PRE>
 * 
 * Invocations of <I>writer</I> methods must be sandwiched between a claim and
 * release of the <I>writer</I> lock within <TT>resourceCrew</TT>:
 * 
 * <PRE>
 *   resourceCrew.startWrite(); // this will block until no reader
 *                              // or writer is present
 *   ...                        // invoke writer (or reader) methods
 *                              // of resource
 *   resourceCrew.endWrite();   // releases this writer's lock on resource
 * </PRE>
 * 
 * This pattern enables fair and secure access to a shared resource according to
 * CREW rules. Concurrent readers will be allowed, so long as no writer is
 * present. A single writer will be allowed, so long as no readers nor other
 * writers are present. So long as each read or write operation is finite,
 * readers will not be blocked indefinitely by writers and vice-versa
 * (<A HREF="#Caution">but see the <I>Cautionary Note</I></A>).
 *
 * <H3><A NAME="Paranoia">Access Sequences for the Worried</H3> Java is a
 * language that allows the throwing and catching of <I>exceptions</I> (and a
 * <TT>return</TT> from a method invocation anywhere). This means that the
 * normal sequential flow of control is not necessarilly what happens:
 * <I>what-you-see-is-NOT-what-you-get</I>, a worrying property. If an uncaught
 * exception is thrown (or a <TT>return</TT> executed) from the resource-using
 * part of either of the above <A HREF="#Access">access sequences</A>, the
 * corresponding <TT>Crew</TT> lock would not be released. To protect ourselves
 * from this, embed the access sequence within a <TT>try</TT>-<TT>finally</TT>
 * clause - for example:
 * 
 * <PRE>
 *   try {
 *     resourceCrew.startRead(); // this will block until no writer is present
 *     ...                       // invoke reader methods of resource
 *   } finally {
 *     resourceCrew.endRead();   // releases this reader's lock on resource
 *   }
 * </PRE>
 * 
 * and:
 * 
 * <PRE>
 *   try {
 *     resourceCrew.startWrite(); // this will block until no reader
 *                                // or writer is present
 *     ...                        // invoke writer (or reader)
 *                                // methods of resource
 *   } finally {
 *     resourceCrew.endWrite ();  // releases this writer's lock on resource
 *   }
 * </PRE>
 * 
 * Now, the lock will always be released whether the reader/writer section exits
 * normally or exceptionally. This asymmetric pattern is not very pretty, but is
 * a classic application of the <TT>try</TT>-<TT>finally</TT> facility.
 *
 * <H3><A NAME="Binding">Binding the Shared Resource to its CREW Lock</H3> In
 * JCSP, shared references may be passed to processes through constructors,
 * through mutator (<TT>set...</TT>) methods (but only, of course, before and in
 * between runs) or through channels (when running).
 * <P>
 * If a resource and its lock are held in separate objects (as above), passing
 * them <I>both</I> to the processes that share them is a little tedious and
 * error prone. It is better to combine them into a <I>single</I> object.
 * <P>
 * One way is to define the <TT>Resource</TT> class to <TT>extend</TT>
 * <TT>Crew</TT>. Then, we only need to distribute the <TT>Resource</TT> object
 * and its access is protected by inherited methods - for example:
 * 
 * <PRE>
 *   resource.startRead ();        // this will block until no writer is present
 *   ...                           // invoke reader methods of resource
 *   resource.endRead ();          // releases this reader's lock on resource
 * </PRE>
 * 
 * However, this may not be possible (since our design may require
 * <TT>Resource</TT> to <TT>extend</TT> something else).
 * <P>
 * Alternatively, we could declare (or <TT>extend</TT>) <TT>Resource</TT> to
 * contain an additional <TT>Crew</TT> attribute. Again, we need only distribute
 * the <TT>Resource</TT> object. The sharing processes recover the associated
 * lock:
 * 
 * <PRE>
 * Crew resourceCrew = resource.getCrew();
 * </PRE>
 * 
 * and can use the <A HREF="#Access">original access sequences</A>.
 * <P>
 * However, this <TT>Crew</TT> class offers the option of doing this the other
 * way around, so that no modifications are needed on the application
 * <TT>Resource</TT>. Pass the <TT>Resource</TT> object to the <TT>Crew</TT>
 * {@link #Crew(Object) constructor} and a reference will be saved as an
 * attribute of the <TT>Crew</TT> object. For example:
 * 
 * <PRE>
 *   Crew resourceCrew = new Crew (new Resource (...));
 * </PRE>
 * 
 * Processes to which this <TT>resourceCrew</TT> object are distributed can
 * recover the original resource by invoking {@link #getShared
 * <TT>getShared</TT>} - for example:
 * 
 * <PRE>
 * Resource resource = (Resource) resourceCrew.getShared();
 * </PRE>
 * 
 * and can again use the <A HREF="#Access">original access sequences</A>.
 *
 * <H3><A NAME="Binding">CREW-synchronised Methods</H3> The safest way to ensure
 * adherence to the <A HREF="#Pattern">design pattern</A> is to burn the correct
 * access sequence into each <I>reader</I> and <I>writer</I> method. For
 * example, extend <TT>Resource</TT> to contain a <TT>private</TT> <TT>Crew</TT>
 * field and override each method to sandwich its invocation between the
 * appropriate synchronisations:
 * 
 * <PRE>
 * class ResourceCrew extends Resource {
 * <I></I>
 *   private final Crew crew = new Crew ();
 * <I></I>
 *   public Thing readerMethod (...) {
 *     crew.startRead ();      // this will block until no writer is present
 *     Thing result = super.readerMethod (...);
 *     crew.endRead ();        // releases this reader's lock on resource
 *     return result;
 *   }
 * <I></I>
 *   public void writerMethod (...) {
 *     crew.startWrite ();     // this will block until no reader
 *                             // or writer is present
 *     super.writerMethod (...);
 *     crew.endWrite ();       // releases this writer's lock on resource
 *   }
 * <I></I>
 *   ...  etc. for all other methods.
 * <I></I>
 * }
 * </PRE>
 * 
 * Now, parallel processes can safely share references to an instance of
 * <TT>ResourceCrew</TT>. Invocations of its <I>reader</I> and <I>writer</I>
 * methods will be automatically CREW-synchronised.
 * <P>
 * Notes:
 * <UL>
 * <LI>this is similar to declaring all class methods to be
 * <TT>synchronized</TT> (which guarantees EREW-synchronisation for parallel
 * access to its instances);
 * <LI>it <I>may</I> have been better to have designed <TT>Resource</TT> like
 * this in the first place (rather than extending it as in the above);
 * <LI>the above access sequences may need to be of the
 * <A HREF="#Paranoia">defensive</A> variety;
 * <LI>a disadvantage of this approach is that we do not have the capability of
 * grouping a sequence of <I>reader</I> or <I>reader</I>/<I>writer</I> methods
 * within a single CREW-synchronisation envelope.
 * </UL>
 *
 * <H2><A NAME="Alternatives">Alternatives to CREW for Shared Objects</H2> The
 * proliferation of references in object-oriented design is a common source of
 * error, even for single-threaded systems. For concurrent object-oriented
 * design, we must be especially careful if we are to avoid the perils of race
 * hazard.
 * <P>
 * Protocols other than CREW may be applied to ensure the safe parallel use of
 * these references. For instance, if the shared reference is to an
 * <I>immutable</I> object (like {@link String <TT>String</TT>}), no special
 * action is needed - this is <I>Concurrent Read</I> (CR).
 * <P>
 * On the other hand, if a reference to a <I>mutable</I> object (<TT>A</TT>) is
 * passed between processes over channels, the sender may agree not to refer to
 * <TT>A</TT> (nor to any objects referred to within <TT>A</TT>) in the future -
 * unless, of course, the original reference to <TT>A</TT> is passed back. In
 * this way, the reference acts as a <I>unique token</I>, possesion of which
 * must be held before access is allowed. See also
 * {@link jcsp.plugNplay.Paraplex} for a <I>double buffering</I> adaptation of
 * this.
 * <P>
 * Such patterns give us safe, secure and dynamic forms of <I>Exclusive Read
 * Exclusive Write</I> (EREW) sharing which, along with the dynamic CREW
 * provided by this class, complement the <A HREF="#Static">static control
 * automatically conferred by the CSP process model</A>. We just have to know
 * when each is appropriate and apply them with due care.
 *
 * <H2>Implementation Note</H2> Channels, which are a specialisation of the
 * multiway CSP <I>event</I>, are a fundamental primitive for synchronising the
 * actions of concurrent processes. Common patterns of use have given rise to
 * higher-level synchronisation paradigms that (when applicable) are easier and,
 * therefore, safer to use than the raw primitives. This CREW lock is an example
 * of this. [Another is the {@link One2OneCallChannel <I>CALL</I> channel}.]
 * <P>
 * The implementation is given here because it is short and simple - an example
 * of the power of expression (and provability) that follows from the CSP/occam
 * model. Correct implementation of a CREW lock has been notoriously difficult.
 * See, for example, Hoare's short but rather tricky solution from his paper,
 * <I>`Monitors: an Operating System Structuring Concept'</I> [C.A.R.Hoare,
 * CACM, 17(10) pp. 549-557, October 1974] - a paper often quoted as one of the
 * sources for the Java monitor model. In contrast, this CSP version is easy
 * <I>and that easiness is important</I>.
 * <P>
 * Each <TT>Crew</TT> object spawns off a private and anonymous
 * <TT>CrewServer</TT> process with which it interacts only via channels:
 * <p>
 * <IMG SRC="doc-files\Crew1.gif">
 * </p>
 * All accessing processes (i.e. the {@link #startRead
 * <TT>startRead</TT>}/{@link #startWrite <TT>startWrite</TT>} methods)
 * communicate down the <I>any-1</I> <TT>request</TT> channel, identifying
 * themselves as either a <TT>READER</TT> or <TT>WRITER</TT>:
 * 
 * <PRE>
 *   public void startRead () {
 *     request.write (CrewServer.READER);
 *   }
 * <I></I>
 *   public void startWrite () {
 *     request.write (CrewServer.WRITER);
 *     writerControl.write (0);
 *     // wait for all current readers to finish
 *   }
 * </PRE>
 * 
 * For <TT>startRead</TT>, this is all that happens - <TT>CrewServer</TT> will
 * refuse the communication if there is a writer present. When accepted, there
 * will be no writer present and the reader has permission to read the shared
 * resource.
 * <P>
 * The <TT>startWrite</TT> communication will also be refused if another writer
 * is present. If there are readers present, it is accepted. This has to be the
 * case since, as far as the <TT>CrewServer</TT> is aware, it might have been
 * from a <TT>startRead</TT>. So, acceptance of this communication does not yet
 * give the writer permission to start writing - but it does let
 * <TT>CrewServer</TT> know that a writer wants to write. To get that
 * permission, the writer performs a second communication on
 * <TT>writerControl</TT> - see above.
 * <P>
 * After accepting a writer <TT>request</TT>, <TT>CrewServer</TT> refuses
 * further communications on <TT>request</TT> and <TT>writerControl</TT> until
 * all readers have finished - which they do by signalling on
 * <TT>readerRelease</TT>:
 * 
 * <PRE>
 * public void endRead() {
 *     readerRelease.write(0);
 * }
 * </PRE>
 * 
 * Refusing the <TT>request</TT> channel parks newly arriving readers and
 * writers safely. When all existing readers have gone away, <TT>CrewServer</TT>
 * accepts the outstanding communication on <TT>writerControl</TT>, which gives
 * the waiting writer permission to write. <TT>CrewServer</TT> then waits for a
 * second communication on <TT>writerControl</TT>, which is the signal from the
 * writer that it has finished writing:
 * 
 * <PRE>
 * public void endWrite() {
 *     writerControl.write(0);
 * }
 * </PRE>
 * 
 * before returning to its initial state (listening on <TT>request</TT> and
 * <TT>readerRelease</TT>).
 * <P>
 * The logic is easier to explain in JCSP:
 * 
 * <PRE>
 *
//////////////////////////////////////////////////////////////////////
//                                                                  //
//  JCSP ("CSP for Java") Libraries                                 //
//  Copyright (C) 1996-2018 Peter Welch, Paul Austin and Neil Brown //
//                2001-2004 Quickstone Technologies Limited         //
//                2005-2018 Kevin Chalmers                          //
//                                                                  //
//  You may use this work under the terms of either                 //
//  1. The Apache License, Version 2.0                              //
//  2. or (at your option), the GNU Lesser General Public License,  //
//       version 2.1 or greater.                                    //
//                                                                  //
//  Full licence texts are included in the LICENCE file with        //
//  this library.                                                   //
//                                                                  //
//  Author contacts: P.H.Welch@kent.ac.uk K.Chalmers@napier.ac.uk   //
//                                                                  //
//////////////////////////////////////////////////////////////////////

package jcsp.lang;
 * <I></I>
 * class CrewServer implements CSProcess {      // this is not a public class
 * <I></I>
 *   public static final int READER = 0;
 *   public static final int WRITER = 1;
 * <I></I>
 *   private final AltingChannelInputInt request;
 *   private final ChannelInputInt writerControl;
 *   private final AltingChannelInputInt readerRelease;
 * <I></I>
 *   public CrewServer (final AltingChannelInputInt request,
 *                      final ChannelInputInt writerControl,
 *                      final AltingChannelInputInt readerRelease) {
 *     this.request = request;
 *     this.writerControl = writerControl;
 *     this.readerRelease = readerRelease;
 *   }
 * <I></I>
 *   public void run () {
 *     int nReaders = 0;
 *     Guard[] c = {readerRelease, request};
 *     final int READER_RELEASE = 0;
 *     final int REQUEST = 1;
 *     Alternative alt = new Alternative (c);
 *     while (true) {
 *       // invariant : (nReaders is the number of current readers) and
 *       // invariant : (there are no writers)
 *       switch (alt.priSelect ()) {
 *         case READER_RELEASE:
 *           readerRelease.read ();         // always let a reader finish reading
 *           nReaders--;
 *         break;
 *         case REQUEST:
 *           switch (request.read ()) {
 *             case READER:
 *               nReaders++;                // let a reader start reading
 *             break;
 *             case WRITER:
 *               for (int i = 0; i < nReaders; i++) {
 *                 readerRelease.read ();   // wait for all readers to go away
 *               }
 *               nReaders = 0;
 *               writerControl.read ();     // let the writer start writing
 *               writerControl.read ();     // wait for writer to finish writing
 *             break;
 *           }
 *         break;
 *       }
 *     }
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * Note that a <TT>CrewServer</TT> cannot be misused by reader and writer
 * processes. It is <TT>private</TT> to each <TT>Crew</TT> lock and operated
 * correctly by the methods of that lock. Given this correct operation, it is
 * trivial to establish the declared <I>loop invariant</I>. All readers and
 * writers start by communicating on the <TT>request</TT> channel so, so long as
 * that is <A HREF="#Caution"><I>fairly</I> serviced</A> and that no read or
 * write lasts forever, processes cannot be indefinitely blocked (e.g. readers
 * by writers or writers by readers).
 *
 * <H3><A NAME="Caution">Cautionary Note</H3> <I>Fair</I> servicing of readers
 * and writers in the above depends on <I>fair</I> servicing of the
 * {@link Any2OneChannelInt <I>any-1</I>} <TT>request</TT> and
 * <TT>readerRelease</TT> channels. In turn, this depends on the <I>fair</I>
 * servicing of requests to enter a <TT>synchronized</TT> block (or method) by
 * the underlying Java Virtual Machine (JVM). Java does not specify how threads
 * waiting to synchronize should be handled. Currently, Sun's standard JDKs
 * queue these requests - which is <I>fair</I>. However, there is at least one
 * JVM that puts such competing requests on a stack - which is legal but
 * <I>unfair</I> and can lead to infinite starvation. This is a problem for
 * <I>any</I> Java system relying on good behaviour from <TT>synchronized</TT>,
 * not just for JCSP's <I>any-1</I> channels or <TT>Crew</TT> locks.
 *
 * @author P.H. Welch
 */

public class Crew {
    private final Any2OneChannelIntImpl request       = new Any2OneChannelIntImpl();
    private final One2OneChannelIntImpl writerControl = new One2OneChannelIntImpl();
    private final Any2OneChannelIntImpl readerRelease = new Any2OneChannelIntImpl();

    /// TODO make this poison the existing channels, once poison is added
    private final Any2OneChannelIntImpl poison = new Any2OneChannelIntImpl();

    private final ProcessManager manager = new ProcessManager(new CrewServer(request.in(), writerControl.in(),
                                                                             readerRelease.in(), poison.in()));

    private final Object shared;

    /**
     * Construct a lock for CREW-guarded operations on a shared resource.
     */
    public Crew() {
        manager.start();
        shared = null;
    }

    /**
     * Construct a lock for CREW-guarded operations on a shared resource.
     *
     * @param shared the shared resource for which this lock is to be used (see
     *               {@link #getShared <TT>getShared</TT>}).
     */
    public Crew(Object shared) {
        manager.start();
        this.shared = shared;
    }

    /**
     * Finalize method added to terminate the process that it spawned. The spawned
     * process holds no references to this object so this object will eventually
     * fall out of scope and gets finalized.
     */
    @Override
    protected void finalize() throws Throwable {
        poison.write(0);
    }

    /**
     * This must be invoked <I>before</I> any read operations on the associated
     * shared resource.
     */
    public void startRead() {
        request.write(CrewServer.READER);
    }

    /**
     * This must be invoked <I>after</I> any read operations on the associated
     * shared resource.
     */
    public void endRead() {
        readerRelease.write(0);
    }

    /**
     * This must be invoked <I>before</I> any write operations on the associated
     * shared resource.
     */
    public void startWrite() {
        request.write(CrewServer.WRITER);
        writerControl.write(0); // wait for all current readers to finish
    }

    /**
     * This must be invoked <I>after</I> any write operations on the associated
     * shared resource.
     */
    public void endWrite() {
        writerControl.write(0);
    }

    /**
     * This returns the shared resource associated with this lock by its
     * {@link #Crew(Object) constructor}. Note: if the {@link #Crew parameterless
     * constructor} was used, this will return <TT>null</TT>.
     *
     * @return the shared resource associated with this lock.
     */
    public Object getShared() {
        return shared;
    }
}
