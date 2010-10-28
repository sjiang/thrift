/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.thrift.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A nonblocking, threaded TServer implementation. Accepts are handled on a
 * single thread, and a configurable number of nonblocking selector threads
 * manage reading from client connections.  Each selector thread owns a
 * worker thread pool to handle processing of requests.
 * 
 * Performs better than TNonblockingServer/THsHaServer on multicore hardware
 * when the the bottleneck is CPU on the single selector thread.
 *
 * To use this server, you MUST use a TFramedTransport at the outermost
 * transport, otherwise this server will be unable to determine when a whole
 * method call has been read off the wire. Clients must also use TFramedTransport.
 */
public class TThreadedSelectorServer extends TServer {
  private static final Logger LOGGER =
    LoggerFactory.getLogger(TThreadedSelectorServer.class.getName());

  // Flag for stopping the server
  private volatile boolean stopped_ = true;
  private SelectorThread[] selectorThreads;
  private AcceptThread acceptThread;

  /**
   * The maximum amount of memory we will allocate to client IO buffers at a
   * time. Without this limit, the server will gladly allocate client buffers
   * right into an out of memory exception, rather than waiting.
   */
  private final long MAX_READ_BUFFER_BYTES;

  protected final Options options_;

  /**
   * How many bytes are currently allocated to read buffers.
   */
  private long readBufferBytesAllocated = 0;

  protected void createThreads(Options options) {

  }

  /**
   * Create server with given processor and server transport, using
   * TBinaryProtocol for the protocol, TFramedTransport.Factory on both input
   * and output transports. A TProcessorFactory will be created that always
   * returns the specified processor.
   */
  public TThreadedSelectorServer(TProcessor processor,
                           TNonblockingServerTransport serverTransport) {
    this(new TProcessorFactory(processor), serverTransport);
  }

  /**
   * Create server with specified processor factory and server transport.
   * TBinaryProtocol is assumed. TFramedTransport.Factory is used on both input
   * and output transports.
   */
  public TThreadedSelectorServer(TProcessorFactory processorFactory,
                            TNonblockingServerTransport serverTransport) {
    this(processorFactory, serverTransport,
         new TFramedTransport.Factory(),
         new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory());
  }

  public TThreadedSelectorServer(TProcessor processor,
                            TNonblockingServerTransport serverTransport,
                            TProtocolFactory protocolFactory) {
    this(processor, serverTransport,
         new TFramedTransport.Factory(),
         protocolFactory, protocolFactory);
  }

  public TThreadedSelectorServer(TProcessor processor,
                            TNonblockingServerTransport serverTransport,
                            TFramedTransport.Factory transportFactory,
                            TProtocolFactory protocolFactory) {
    this(processor, serverTransport,
         transportFactory,
         protocolFactory, protocolFactory);
  }

  public TThreadedSelectorServer(TProcessorFactory processorFactory,
                            TNonblockingServerTransport serverTransport,
                            TFramedTransport.Factory transportFactory,
                            TProtocolFactory protocolFactory) {
    this(processorFactory, serverTransport,
         transportFactory,
         protocolFactory, protocolFactory);
  }

  public TThreadedSelectorServer(TProcessor processor,
                            TNonblockingServerTransport serverTransport,
                            TFramedTransport.Factory outputTransportFactory,
                            TProtocolFactory inputProtocolFactory,
                            TProtocolFactory outputProtocolFactory) {
    this(new TProcessorFactory(processor), serverTransport,
         outputTransportFactory,
         inputProtocolFactory, outputProtocolFactory);
  }

  public TThreadedSelectorServer(TProcessorFactory processorFactory,
                            TNonblockingServerTransport serverTransport,
                            TFramedTransport.Factory outputTransportFactory,
                            TProtocolFactory inputProtocolFactory,
                            TProtocolFactory outputProtocolFactory) {
    this(processorFactory, serverTransport,
          outputTransportFactory,
          inputProtocolFactory, outputProtocolFactory,
          new Options());
  }

  public TThreadedSelectorServer(TProcessorFactory processorFactory,
                            TNonblockingServerTransport serverTransport,
                            TFramedTransport.Factory outputTransportFactory,
                            TProtocolFactory inputProtocolFactory,
                            TProtocolFactory outputProtocolFactory,
                            Options options) {
    super(processorFactory, serverTransport,
          null, outputTransportFactory,
          inputProtocolFactory, outputProtocolFactory);
    options_ = options;
    options_.validate();
    MAX_READ_BUFFER_BYTES = options.maxReadBufferBytes;
  }

  /**
   * Begin accepting connections and processing invocations.
   */
  @Override
  public void serve() {
    // start the threads and listen, or exit
    if (!startThreads()) {
      return;
    }

    // this will block while we serve
    joinThreads();

  }

  /**
   * Stop listening for connections.
   */
  protected void stopListening() {
  }

  /**
   * Start the selector thread running to deal with clients.
   *
   * @return true if everything went ok, false if we couldn't start for some
   * reason.
   */
  protected boolean startThreads() {
    try {
      selectorThreads = new SelectorThread[options_.selectorThreads];
      for (int i = 0; i < selectorThreads.length; ++i) {
        selectorThreads[i] = new SelectorThread(options_);
      }
      acceptThread =
          new AcceptThread((TNonblockingServerTransport) serverTransport_, selectorThreads);
      stopped_ = false;
      for (SelectorThread thread : selectorThreads) {
        thread.start();
      }
      // TODO listen after accept thread starts?
      serverTransport_.listen();
      acceptThread.start();
      return true;
    } catch (TTransportException ttx) {
      LOGGER.error("Failed to start listening on server socket!", ttx);
      return false;
    } catch (IOException e) {
      LOGGER.error("Failed to start selector threads!", e);
      return false;
    }
  }

  /**
   * Block until the selector exits.
   */
  protected void joinThreads() {
    // wait until the io threads exit
    try {
      acceptThread.join();
    } catch (InterruptedException e) {
      // for now, just silently ignore. technically this means we'll have less of
      // a graceful shutdown as a result.
    }
    for (SelectorThread thread : selectorThreads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        // for now, just silently ignore. technically this means we'll have less of
        // a graceful shutdown as a result.
      }
    }
  }

  /**
   * Stop serving and shut everything down.
   */
  @Override
  public void stop() {
    serverTransport_.close();
    stopped_ = true;
    if (acceptThread != null) {
      acceptThread.wakeupSelector();
    }
    if (selectorThreads != null) {
      for (SelectorThread thread : selectorThreads) {
        if (thread != null) thread.wakeupSelector();
      }
    }
  }

  /**
   * Perform an invocation. This method could behave several different ways
   * - invoke immediately inline, queue for separate execution, etc.
   */
  protected boolean requestInvoke(FrameBuffer frameBuffer, ExecutorService invoker) {
    try {
      invoker.execute(new Invocation(frameBuffer));
      return true;
    } catch (RejectedExecutionException rx) {
      LOGGER.warn("ExecutorService rejected execution!", rx);
      return false;
    }
  }


  /**
   * The thread that selects on the listen socket and accepts new connections
   * to hand off to the IO threads
   */
  protected class AcceptThread extends Thread {

    // sjn the listen socket to accept on
    private final TNonblockingServerTransport serverTransport;
    // sjn accept selector
    private final Selector acceptSelector;
    private final SelectorThread[] selectorThreads;
    private int nextSelectorThread = 0;

    /**
     * Set up the AcceptThead
     * @throws IOException 
     */
    public AcceptThread(final TNonblockingServerTransport serverTransport,
        SelectorThread[] selectorThreads) throws IOException {
      this.serverTransport = serverTransport;
      this.acceptSelector = SelectorProvider.provider().openSelector();
      serverTransport.registerSelector(acceptSelector);
      this.selectorThreads = selectorThreads;
    }    

    /**
     * The work loop. Selects on the server transport and accepts.
     * If there was a server transport that had blocking accepts, and returned
     * on blocking client transports, that should be used instead
     */
    public void run() {
      try {
        while (!stopped_) {
          select();
        }
      } catch (Throwable t) {
        LOGGER.error("run() exiting due to uncaught error", t);
      } finally {
        TThreadedSelectorServer.this.stop();
      }
    }

    /**
     * If the selector is blocked, wake it up.
     */
    public void wakeupSelector() {
      acceptSelector.wakeup();
    }
    
    /**
     * Select and process IO events appropriately:
     * If there are connections to be accepted, accept them.
     */
    private void select() {
      try {
        // wait for connect events.
        acceptSelector.select();

        // process the io events we received
        Iterator<SelectionKey> selectedKeys = acceptSelector.selectedKeys().iterator();
        while (!stopped_ && selectedKeys.hasNext()) {
          SelectionKey key = selectedKeys.next();
          selectedKeys.remove();

          // skip if not valid
          if (!key.isValid()) {
            continue;
          }
          
          if (key.isAcceptable()) {
            handleAccept();
          } else {
            LOGGER.warn("Unexpected state in select! " + key.interestOps());
          }
        }
      } catch (IOException e) {
        LOGGER.warn("Got an IOException while selecting!", e);
      }
    }

    /**
     * Accept a new connection.
     */
    private void handleAccept() {
      TNonblockingTransport client = null;
      try {
        // accept the connection
        client = (TNonblockingTransport) serverTransport.accept();
        // hand the connection to a selector thread
        // TODO make choosing a thread a strategy?
        selectorThreads[nextSelectorThread].addAcceptedConnection(client);
        nextSelectorThread++;
        nextSelectorThread %= selectorThreads.length;
      } catch (TTransportException tte) {
        // something went wrong accepting.
        LOGGER.warn("Exception trying to accept!", tte);
        tte.printStackTrace();
        if (client != null) client.close();
      }
    }
  } // AcceptThread

  /**
   * The thread(s) that will be doing all the selecting, and those that
   * still need to be read.
   */
  protected class SelectorThread extends Thread {

    // Main selector for this thread
    private final Selector selector;

    // List of FrameBuffers that want to change their selection interests.
    private final Set<FrameBuffer> selectInterestChanges =
      new HashSet<FrameBuffer>();

    // The list of accepted connections ready for processing
    private final BlockingQueue<TNonblockingTransport> acceptedQueue;

    private final ExecutorService invoker;
    private final Options options;

    /**
     * Set up the SelectorThread.
     */
    public SelectorThread(Options options) throws IOException {
      this(Executors.newFixedThreadPool(options.workerThreadsPerSelectorThread), options);
    }
    
    public SelectorThread(ExecutorService invoker, Options options) throws IOException {
      this.selector = SelectorProvider.provider().openSelector();
      this.acceptedQueue = new LinkedBlockingQueue<TNonblockingTransport>();
      this.invoker = invoker;
      this.options = options;
    }

    public void addAcceptedConnection(TNonblockingTransport a) {
      acceptedQueue.add(a);
      selector.wakeup();
    }

    /**
     * The work loop. Handles both selecting (all IO operations) and managing
     * the selection preferences of all existing connections.
     */
    public void run() {
      try {
        while (!stopped_) {
          select();
          processInterestChanges();
        }
      } catch (Throwable t) {
        LOGGER.error("run() exiting due to uncaught error", t);
      } finally {
        TThreadedSelectorServer.this.stop();
        gracefullyShutdownInvokerPool();
      }
    }

    protected void gracefullyShutdownInvokerPool() {
      // try to gracefully shut down the executor service
      invoker.shutdown();

      // Loop until awaitTermination finally does return without a interrupted
      // exception. If we don't do this, then we'll shut down prematurely. We want
      // to let the executorService clear it's task queue, closing client sockets
      // appropriately.
      long timeoutMS = options.stopTimeoutUnit.toMillis(options.stopTimeoutVal);
      long now = System.currentTimeMillis();
      while (timeoutMS >= 0) {
        try {
          invoker.awaitTermination(timeoutMS, TimeUnit.MILLISECONDS);
          break;
        } catch (InterruptedException ix) {
          long newnow = System.currentTimeMillis();
          timeoutMS -= (newnow - now);
          now = newnow;
        }
      }
    }
    
    /**
     * If the selector is blocked, wake it up.
     */
    public void wakeupSelector() {
      selector.wakeup();
    }

    /**
     * Add FrameBuffer to the list of select interest changes and wake up the
     * selector if it's blocked. When the select() call exits, it'll give the
     * FrameBuffer a chance to change its interests.
     */
    public void requestSelectInterestChange(FrameBuffer frameBuffer) {
      synchronized (selectInterestChanges) {
        selectInterestChanges.add(frameBuffer);
      }
      // wakeup the selector, if it's currently blocked.
      selector.wakeup();
    }

    /**
     * Select and process IO events appropriately:
     * If there are connections to be accepted, accept them.
     * If there are existing connections with data waiting to be read, read it,
     * buffering until a whole frame has been read.
     * If there are any pending responses, buffer them until their target client
     * is available, and then send the data.
     */
    private void select() {
      try {
        // wait for io events.
        selector.select();
        
        // process the accepted connections
        TNonblockingTransport accepted;
        while (!stopped_ && null != (accepted = acceptedQueue.poll())) {
          SelectionKey clientKey = null;
          try {
            clientKey = accepted.registerSelector(selector, SelectionKey.OP_READ);
            // add this key to the map
            FrameBuffer frameBuffer = new FrameBuffer(accepted, clientKey, this);
            clientKey.attach(frameBuffer);
          } catch (IOException e) {
            LOGGER.warn("Failed to register accepted connection with selector!", e);
            if (clientKey != null) cleanupSelectionKey(clientKey);
            accepted.close();
          }
        }

        // process the io events we received
        Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
        while (!stopped_ && selectedKeys.hasNext()) {
          SelectionKey key = selectedKeys.next();
          selectedKeys.remove();

          // skip if not valid
          if (!key.isValid()) {
            cleanupSelectionKey(key);
            continue;
          }

          // if the key is marked Accept, then it has to be the server
          // transport.
          if (key.isReadable()) {
            // deal with reads
            handleRead(key);
          } else if (key.isWritable()) {
            // deal with writes
            handleWrite(key);
          } else {
            LOGGER.warn("Unexpected state in select! " + key.interestOps());
          }
        }
      } catch (IOException e) {
        LOGGER.warn("Got an IOException while selecting!", e);
      }
    }

    /**
     * Check to see if there are any FrameBuffers that have switched their
     * interest type from read to write or vice versa.
     */
    private void processInterestChanges() {
      synchronized (selectInterestChanges) {
        for (FrameBuffer fb : selectInterestChanges) {
          fb.changeSelectInterests();
        }
        selectInterestChanges.clear();
      }
    }

    /**
     * Do the work required to read from a readable client. If the frame is
     * fully read, then invoke the method call.
     */
    private void handleRead(SelectionKey key) {
      FrameBuffer buffer = (FrameBuffer)key.attachment();
      if (!buffer.read()) {
        cleanupSelectionKey(key);
        return;
      }

      // if the buffer's frame read is complete, invoke the method.
      if (buffer.isFrameFullyRead()) {
        if (!requestInvoke(buffer, invoker)) {
          cleanupSelectionKey(key);
        }
      }
    }

    /**
     * Let a writable client get written, if there's data to be written.
     */
    private void handleWrite(SelectionKey key) {
      FrameBuffer buffer = (FrameBuffer)key.attachment();
      if (!buffer.write()) {
        cleanupSelectionKey(key);
      }
    }

    /**
     * Do connection-close cleanup on a given SelectionKey.
     */
    private void cleanupSelectionKey(SelectionKey key) {
      // remove the records from the two maps
      FrameBuffer buffer = (FrameBuffer)key.attachment();
      if (buffer != null) {
        // close the buffer
        buffer.close();
      }
      // cancel the selection key
      key.cancel();
    }
  } // SelectorThread

  /**
   * Class that implements a sort of state machine around the interaction with
   * a client and an invoker. It manages reading the frame size and frame data,
   * getting it handed off as wrapped transports, and then the writing of
   * reponse data back to the client. In the process it manages flipping the
   * read and write bits on the selection key for its client.
   */
  protected class FrameBuffer {
    //
    // Possible states for the FrameBuffer state machine.
    //
    // in the midst of reading the frame size off the wire
    private static final int READING_FRAME_SIZE = 1;
    // reading the actual frame data now, but not all the way done yet
    private static final int READING_FRAME = 2;
    // completely read the frame, so an invocation can now happen
    private static final int READ_FRAME_COMPLETE = 3;
    // waiting to get switched to listening for write events
    private static final int AWAITING_REGISTER_WRITE = 4;
    // started writing response data, not fully complete yet
    private static final int WRITING = 6;
    // another thread wants this framebuffer to go back to reading
    private static final int AWAITING_REGISTER_READ = 7;
    // we want our transport and selection key invalidated in the selector thread
    private static final int AWAITING_CLOSE = 8;

    //
    // Instance variables
    //

    // the actual transport hooked up to the client.
    private final TNonblockingTransport trans_;

    // the SelectionKey that corresponds to our transport
    private final SelectionKey selectionKey_;
    
    private final SelectorThread selectorThread_;

    // where in the process of reading/writing are we?
    private int state_ = READING_FRAME_SIZE;

    // the ByteBuffer we'll be using to write and read, depending on the state
    private ByteBuffer buffer_;

    private TByteArrayOutputStream response_;

    public FrameBuffer( final TNonblockingTransport trans,
                        final SelectionKey selectionKey,
                        final SelectorThread selectorThread) {
      trans_ = trans;
      selectionKey_ = selectionKey;
      selectorThread_ = selectorThread;
      buffer_ = ByteBuffer.allocate(4);
    }

    /**
     * Give this FrameBuffer a chance to read. The selector loop should have
     * received a read event for this FrameBuffer.
     *
     * @return true if the connection should live on, false if it should be
     * closed
     */
    public boolean read() {
      if (state_ == READING_FRAME_SIZE) {
        // try to read the frame size completely
        if (!internalRead()) {
          return false;
        }

        // if the frame size has been read completely, then prepare to read the
        // actual frame.
        if (buffer_.remaining() == 0) {
          // pull out the frame size as an integer.
          int frameSize = buffer_.getInt(0);
          if (frameSize <= 0) {
            LOGGER.error("Read an invalid frame size of " + frameSize
              + ". Are you using TFramedTransport on the client side?");
            return false;
          }

          // if this frame will always be too large for this server, log the
          // error and close the connection.
          if (frameSize > MAX_READ_BUFFER_BYTES) {
            LOGGER.error("Read a frame size of " + frameSize
              + ", which is bigger than the maximum allowable buffer size for ALL connections.");
            return false;
          }

          // if this frame will push us over the memory limit, then return.
          // with luck, more memory will free up the next time around.
          if (readBufferBytesAllocated + frameSize > MAX_READ_BUFFER_BYTES) {
            return true;
          }

          // incremement the amount of memory allocated to read buffers
          readBufferBytesAllocated += frameSize;

          // reallocate the readbuffer as a frame-sized buffer
          buffer_ = ByteBuffer.allocate(frameSize);

          state_ = READING_FRAME;
        } else {
          // this skips the check of READING_FRAME state below, since we can't
          // possibly go on to that state if there's data left to be read at
          // this one.
          return true;
        }
      }

      // it is possible to fall through from the READING_FRAME_SIZE section
      // to READING_FRAME if there's already some frame data available once
      // READING_FRAME_SIZE is complete.

      if (state_ == READING_FRAME) {
        if (!internalRead()) {
          return false;
        }

        // since we're already in the select loop here for sure, we can just
        // modify our selection key directly.
        if (buffer_.remaining() == 0) {
          // get rid of the read select interests
          selectionKey_.interestOps(0);
          state_ = READ_FRAME_COMPLETE;
        }

        return true;
      }

      // if we fall through to this point, then the state must be invalid.
      LOGGER.error("Read was called but state is invalid (" + state_ + ")");
      return false;
    }

    /**
     * Give this FrameBuffer a chance to write its output to the final client.
     */
    public boolean write() {
      if (state_ == WRITING) {
        try {
          if (trans_.write(buffer_) < 0) {
            return false;
          }
        } catch (IOException e) {
          LOGGER.warn("Got an IOException during write!", e);
          return false;
        }

        // we're done writing. now we need to switch back to reading.
        if (buffer_.remaining() == 0) {
          prepareRead();
        }
        return true;
      }

      LOGGER.error("Write was called, but state is invalid (" + state_ + ")");
      return false;
    }

    /**
     * Give this FrameBuffer a chance to set its interest to write, once data
     * has come in.
     */
    public void changeSelectInterests() {
      if (state_ == AWAITING_REGISTER_WRITE) {
        // set the OP_WRITE interest
        selectionKey_.interestOps(SelectionKey.OP_WRITE);
        state_ = WRITING;
      } else if (state_ == AWAITING_REGISTER_READ) {
        prepareRead();
      } else if (state_ == AWAITING_CLOSE){
        close();
        selectionKey_.cancel();
      } else {
        LOGGER.error(
          "changeSelectInterest was called, but state is invalid ("
          + state_ + ")");
      }
    }

    /**
     * Shut the connection down.
     */
    public void close() {
      // if we're being closed due to an error, we might have allocated a
      // buffer that we need to subtract for our memory accounting.
      if (state_ == READING_FRAME || state_ == READ_FRAME_COMPLETE) {
        readBufferBytesAllocated -= buffer_.array().length;
      }
      trans_.close();
    }

    /**
     * Check if this FrameBuffer has a full frame read.
     */
    public boolean isFrameFullyRead() {
      return state_ == READ_FRAME_COMPLETE;
    }

    /**
     * After the processor has processed the invocation, whatever thread is
     * managing invocations should call this method on this FrameBuffer so we
     * know it's time to start trying to write again. Also, if it turns out
     * that there actually isn't any data in the response buffer, we'll skip
     * trying to write and instead go back to reading.
     */
    public void responseReady() {
      // the read buffer is definitely no longer in use, so we will decrement
      // our read buffer count. we do this here as well as in close because
      // we'd like to free this read memory up as quickly as possible for other
      // clients.
      readBufferBytesAllocated -= buffer_.array().length;

      if (response_.len() == 0) {
        // go straight to reading again. this was probably an oneway method
        state_ = AWAITING_REGISTER_READ;
        buffer_ = null;
      } else {
        buffer_ = ByteBuffer.wrap(response_.get(), 0, response_.len());

        // set state that we're waiting to be switched to write. we do this
        // asynchronously through requestSelectInterestChange() because there is a
        // possibility that we're not in the main thread, and thus currently
        // blocked in select(). (this functionality is in place for the sake of
        // the HsHa server.)
        state_ = AWAITING_REGISTER_WRITE;
      }
      requestSelectInterestChange();
    }

    /**
     * Actually invoke the method signified by this FrameBuffer.
     */
    public void invoke() {
      TTransport inTrans = getInputTransport();
      TProtocol inProt = inputProtocolFactory_.getProtocol(inTrans);
      TProtocol outProt = outputProtocolFactory_.getProtocol(getOutputTransport());

      try {
        processorFactory_.getProcessor(inTrans).process(inProt, outProt);
        responseReady();
        return;
      } catch (TException te) {
        LOGGER.warn("Exception while invoking!", te);
      } catch (Exception e) {
        LOGGER.error("Unexpected exception while invoking!", e);
      }
      // This will only be reached when there is an exception.
      state_ = AWAITING_CLOSE;
      requestSelectInterestChange();
    }

    /**
     * Wrap the read buffer in a memory-based transport so a processor can read
     * the data it needs to handle an invocation.
     */
    private TTransport getInputTransport() {
      return new TMemoryInputTransport(buffer_.array());
    }

    /**
     * Get the transport that should be used by the invoker for responding.
     */
    private TTransport getOutputTransport() {
      response_ = new TByteArrayOutputStream();
      return outputTransportFactory_.getTransport(new TIOStreamTransport(response_));
    }

    /**
     * Perform a read into buffer.
     *
     * @return true if the read succeeded, false if there was an error or the
     * connection closed.
     */
    private boolean internalRead() {
      try {
        if (trans_.read(buffer_) < 0) {
          return false;
        }
        return true;
      } catch (IOException e) {
        LOGGER.warn("Got an IOException in internalRead!", e);
        return false;
      }
    }

    /**
     * We're done writing, so reset our interest ops and change state accordingly.
     */
    private void prepareRead() {
      // we can set our interest directly without using the queue because
      // we're in the select thread.
      selectionKey_.interestOps(SelectionKey.OP_READ);
      // get ready for another go-around
      buffer_ = ByteBuffer.allocate(4);
      state_ = READING_FRAME_SIZE;
    }

    /**
     * When this FrameBuffer needs to change it's select interests and execution
     * might not be in the select thread, then this method will make sure the
     * interest change gets done when the select thread wakes back up. When the
     * current thread is the select thread, then it just does the interest change
     * immediately.
     */
    private void requestSelectInterestChange() {
      if (Thread.currentThread() == selectorThread_) {
        changeSelectInterests();
      } else {
        selectorThread_.requestSelectInterestChange(this);
      }
    }
  } // FrameBuffer

  /**
   * An Invocation represents a method call that is prepared to execute, given
   * an idle worker thread. It contains the input and output protocols the
   * thread's processor should use to perform the usual Thrift invocation.
   */
  private class Invocation implements Runnable {

    private final FrameBuffer frameBuffer;

    public Invocation(final FrameBuffer frameBuffer) {
      this.frameBuffer = frameBuffer;
    }

    public void run() {
      frameBuffer.invoke();
    }
  }

  public static class Options {
    public long maxReadBufferBytes = Long.MAX_VALUE;
    public int selectorThreads = 2;
    public int workerThreadsPerSelectorThread = 4;
    public int stopTimeoutVal = 60;
    public TimeUnit stopTimeoutUnit = TimeUnit.SECONDS;
    
    public Options() {}

    public void validate() {
      if (maxReadBufferBytes <= 1024) {
        throw new IllegalArgumentException("You must allocate at least 1KB to the read buffer.");
      }
    }
  }
}

