use crate::{
    framed::{FrameConsumer, FrameProducer},
    Error, Result,
};
use core::{
    cell::UnsafeCell,
    cmp::min,
    mem::{forget, transmute},
    ops::{Deref, DerefMut},
    result::Result as CoreResult,
    slice::{self, from_raw_parts_mut},
    sync::atomic::{
        AtomicBool, AtomicUsize,
        Ordering::{AcqRel, Acquire, Release},
    },
};

const CONSUMER_BIT: usize = 1 << { usize::BITS - 1 };

/// `Storage` repersents the actual storage for a BBQueue
pub trait Storage {
    /// A type that points to the underlying storage impl
    type Pointer: StoragePointer;

    /// Returns a reference to the underyling pointer
    fn get(&self) -> &Self::Pointer;
    /// Returns a mutable reference to the underyling pointer
    fn get_mut(&mut self) -> &mut Self::Pointer;
}

/// TODO
pub trait StoragePointer: Clone + core::fmt::Debug {
    /// TODO
    const CAPACITY: usize;
    /// TODO
    fn get_buf_ptr(&self) -> *mut u8;
    /// TODO
    unsafe fn zero_buffer(&self);
    /// TODO
    fn write(&self) -> &AtomicUsize;
    /// TODO
    fn read(&self) -> &AtomicUsize;
    /// TODO
    fn last(&self) -> &AtomicUsize;
    /// TODO
    fn reserve(&self) -> &AtomicUsize;
    /// TODO
    fn read_in_progress(&self) -> &AtomicBool;
    /// TODO
    fn write_in_progress(&self) -> &AtomicBool;
    /// TODO
    fn prod_con_count(&self) -> &AtomicUsize;
    /// TODO
    fn ptr_eq(&self, other: &Self) -> bool;
}

/// TODO
#[derive(Debug)]
pub struct BufStorage<const N: usize> {
    pub(crate) buf: UnsafeCell<[u8; N]>,
    /// Where the next byte will be written
    pub(crate) write: AtomicUsize,

    /// Where the next byte will be read from
    pub(crate) read: AtomicUsize,

    /// Used in the inverted case to mark the end of the
    /// readable streak. Otherwise will == sizeof::<self.buf>().
    /// Writer is responsible for placing this at the correct
    /// place when entering an inverted condition, and Reader
    /// is responsible for moving it back to sizeof::<self.buf>()
    /// when exiting the inverted condition
    pub(crate) last: AtomicUsize,

    /// Used by the Writer to remember what bytes are currently
    /// allowed to be written to, but are not yet ready to be
    /// read from
    pub(crate) reserve: AtomicUsize,

    /// Is there an active read grant?
    pub(crate) read_in_progress: AtomicBool,

    /// Is there an active write grant?
    pub(crate) write_in_progress: AtomicBool,

    pub(crate) prod_con_count: AtomicUsize,
}

unsafe impl<const N: usize> Send for BufStorage<N> {}
unsafe impl<const N: usize> Sync for BufStorage<N> {}

impl<const N: usize> BufStorage<N> {
    /// TODO
    pub const fn new() -> Self {
        Self {
            buf: UnsafeCell::new([0; N]),
            // Owned by the writer
            write: AtomicUsize::new(0),

            // Owned by the reader
            read: AtomicUsize::new(0),

            // Cooperatively owned
            //
            // NOTE: This should generally be initialized as size_of::<self.buf>(), however
            // this would prevent the structure from being entirely zero-initialized,
            // and can cause the .data section to be much larger than necessary. By
            // forcing the `last` pointer to be zero initially, we place the structure
            // in an "inverted" condition, which will be resolved on the first commited
            // bytes that are written to the structure.
            //
            // When read == last == write, no bytes will be allowed to be read (good), but
            // write grants can be given out (also good).
            last: AtomicUsize::new(0),

            // Owned by the Writer, "private"
            reserve: AtomicUsize::new(0),

            // Owned by the Reader, "private"
            read_in_progress: AtomicBool::new(false),

            // Owned by the Writer, "private"
            write_in_progress: AtomicBool::new(false),

            prod_con_count: AtomicUsize::new(0),
        }
    }
}

impl<'a, const N: usize> Storage for &'a BufStorage<N> {
    type Pointer = Self;

    fn get(&self) -> &Self::Pointer {
        self
    }

    fn get_mut(&mut self) -> &mut Self::Pointer {
        self
    }
}

impl<'a, const N: usize> StoragePointer for &'a BufStorage<N> {
    const CAPACITY: usize = N;

    fn get_buf_ptr(&self) -> *mut u8 {
        unsafe { (*self.buf.get()).as_mut_ptr() }
    }

    unsafe fn zero_buffer(&self) {
        unsafe { (*self.buf.get()).as_mut_ptr().write_bytes(0, N) };
    }

    #[inline]
    fn write(&self) -> &AtomicUsize {
        &self.write
    }

    #[inline]
    fn read(&self) -> &AtomicUsize {
        &self.read
    }

    #[inline]
    fn last(&self) -> &AtomicUsize {
        &self.last
    }

    #[inline]
    fn reserve(&self) -> &AtomicUsize {
        &self.reserve
    }

    #[inline]
    fn read_in_progress(&self) -> &AtomicBool {
        &self.read_in_progress
    }

    #[inline]
    fn write_in_progress(&self) -> &AtomicBool {
        &self.write_in_progress
    }

    #[inline]
    fn prod_con_count(&self) -> &AtomicUsize {
        &self.prod_con_count
    }

    fn ptr_eq(&self, other: &Self) -> bool {
        core::ptr::eq(*self, *other)
    }
}

#[derive(Debug)]
/// A backing structure for a BBQueue. Can be used to create either
/// a BBQueue or a split Producer/Consumer pair
pub struct BBBuffer<S: Storage> {
    storage: S,
}

//unsafe impl<const A: usize> Sync for BBBuffer<A> {}

impl<'a, S: Storage> BBBuffer<S> {
    /// Attempt to split the `BBBuffer` into `Consumer` and `Producer` halves to gain access to the
    /// buffer. If buffer has already been split, an error will be returned.
    ///
    /// NOTE: When splitting, the underlying buffer will be explicitly initialized
    /// to zero. This may take a measurable amount of time, depending on the size
    /// of the buffer. This is necessary to prevent undefined behavior. If the buffer
    /// is placed at `static` scope within the `.bss` region, the explicit initialization
    /// will be elided (as it is already performed as part of memory initialization)
    ///
    /// NOTE:  If the `thumbv6` feature is selected, this function takes a short critical section
    /// while splitting.
    ///
    /// ```rust
    /// # // bbqueue test shim!
    /// # fn bbqtest() {
    /// use bbqueue::BBStatic;
    ///
    /// // Create and split a new buffer
    /// let buffer: BBStatic<6> = bbqueue::new_static_queue!(6);
    /// let (prod, cons) = buffer.try_split().unwrap();
    ///
    /// // Not possible to split twice
    /// assert!(buffer.try_split().is_err());
    /// # // bbqueue test shim!
    /// # }
    /// #
    /// # fn main() {
    /// # #[cfg(not(feature = "thumbv6"))]
    /// # bbqtest();
    /// # }
    /// ```
    pub fn try_split(&'a self) -> Result<(Producer<S::Pointer>, Consumer<S::Pointer>)> {
        let storage = self.storage.get();
        if atomic::fetch_or(storage.prod_con_count(), CONSUMER_BIT, Acquire) & CONSUMER_BIT
            == CONSUMER_BIT
        {
            return Err(Error::AlreadySplit);
        }

        atomic::fetch_add(storage.prod_con_count(), 1, Acquire);

        unsafe {
            // Explicitly zero the data to avoid undefined behavior.
            // This is required, because we hand out references to the buffers,
            // which mean that creating them as references is technically UB for now
            self.storage.get().zero_buffer();

            Ok((
                Producer {
                    ptr: self.storage.get().clone(),
                },
                Consumer {
                    ptr: self.storage.get().clone(),
                },
            ))
        }
    }

    /// Attempts to create a new consumer if one has not already been created. If a consumer has already been created
    /// an error will be returned.
    ///
    /// NOTE: If a producer has not already been created, the underlying buffer will be explicitly initialized
    /// to zero. This may take a measurable amount of time, depending on the size
    /// of the buffer. This is necessary to prevent undefined behavior. If the buffer
    /// is placed at `static` scope within the `.bss` region, the explicit initialization
    /// will be elided (as it is already performed as part of memory initialization)
    ///
    pub fn consumer(&'a self) -> Result<Consumer<S::Pointer>> {
        let prev = atomic::fetch_or(&self.storage.get().prod_con_count(), CONSUMER_BIT, Acquire);
        if prev & CONSUMER_BIT == CONSUMER_BIT {
            return Err(Error::AlreadySplit);
        }
        if prev & !CONSUMER_BIT == 0 {
            unsafe {
                self.storage.get().zero_buffer();
            }
        }

        Ok(Consumer {
            ptr: self.storage.get().clone(),
        })
    }

    /// Attempts to create a new [`FrameConsumer`] if one has not already been created. If a consumer has already been created
    /// an error will be returned.
    ///
    /// NOTE: If a producer has not already been created, the underlying buffer will be explicitly initialized
    /// to zero. This may take a measurable amount of time, depending on the size
    /// of the buffer. This is necessary to prevent undefined behavior. If the buffer
    /// is placed at `static` scope within the `.bss` region, the explicit initialization
    /// will be elided (as it is already performed as part of memory initialization)
    ///
    pub fn frame_consumer(&'a self) -> Result<FrameConsumer<S::Pointer>> {
        self.consumer().map(|consumer| FrameConsumer { consumer })
    }

    /// Creates a new producer with access to the underlying buffer.
    ///
    /// NOTE: If a producer has not already been created, the underlying buffer will be explicitly initialized
    /// to zero. This may take a measurable amount of time, depending on the size
    /// of the buffer. This is necessary to prevent undefined behavior. If the buffer
    /// is placed at `static` scope within the `.bss` region, the explicit initialization
    /// will be elided (as it is already performed as part of memory initialization)
    pub fn producer(&'a self) -> Producer<S::Pointer> {
        let prev = self.storage.get().prod_con_count().fetch_add(1, Acquire);
        if prev & CONSUMER_BIT != CONSUMER_BIT || prev & !CONSUMER_BIT >= 1 {
            unsafe {
                self.storage.get().zero_buffer();
            }
        }

        Producer {
            ptr: self.storage.get().clone(),
        }
    }

    /// Creates a new [`FrameProducer`] with access to the underlying buffer.
    ///
    /// NOTE: If a producer has not already been created, the underlying buffer will be explicitly initialized
    /// to zero. This may take a measurable amount of time, depending on the size
    /// of the buffer. This is necessary to prevent undefined behavior. If the buffer
    /// is placed at `static` scope within the `.bss` region, the explicit initialization
    /// will be elided (as it is already performed as part of memory initialization)
    pub fn frame_producer(&'a self) -> FrameProducer<S::Pointer> {
        let producer = self.producer();
        FrameProducer { producer }
    }

    /// Attempt to split the `BBBuffer` into `FrameConsumer` and `FrameProducer` halves
    /// to gain access to the buffer. If buffer has already been split, an error
    /// will be returned.
    ///
    /// NOTE: When splitting, the underlying buffer will be explicitly initialized
    /// to zero. This may take a measurable amount of time, depending on the size
    /// of the buffer. This is necessary to prevent undefined behavior. If the buffer
    /// is placed at `static` scope within the `.bss` region, the explicit initialization
    /// will be elided (as it is already performed as part of memory initialization)
    ///
    /// NOTE:  If the `thumbv6` feature is selected, this function takes a short critical
    /// section while splitting.
    pub fn try_split_framed(
        &'a self,
    ) -> Result<(FrameProducer<S::Pointer>, FrameConsumer<S::Pointer>)> {
        let (producer, consumer) = self.try_split()?;
        Ok((FrameProducer { producer }, FrameConsumer { consumer }))
    }

    /// Attempt to release the Producer and Consumer
    ///
    /// This re-initializes the buffer so it may be split in a different mode at a later
    /// time. There must be no read or write grants active, or an error will be returned.
    ///
    /// The `Producer` and `Consumer` must be from THIS `BBBuffer`, or an error will
    /// be returned.
    ///
    /// ```rust
    /// # // bbqueue test shim!
    /// # fn bbqtest() {
    /// use bbqueue::BBStatic;
    ///
    /// // Create and split a new buffer
    /// let buffer: BBStatic<6> = bbqueue::new_static_queue!(6);
    /// let (prod, cons) = buffer.try_split().unwrap();
    ///
    /// // Not possible to split twice
    /// assert!(buffer.try_split().is_err());
    ///
    /// // Release the producer and consumer
    /// assert!(buffer.try_release(prod, cons).is_ok());
    ///
    /// // Split the buffer in framed mode
    /// let (fprod, fcons) = buffer.try_split_framed().unwrap();
    /// # // bbqueue test shim!
    /// # }
    /// #
    /// # fn main() {
    /// # #[cfg(not(feature = "thumbv6"))]
    /// # bbqtest();
    /// # }
    /// ```
    pub fn try_release(
        &'a self,
        prod: Producer<S::Pointer>,
        cons: Consumer<S::Pointer>,
    ) -> CoreResult<(), (Producer<S::Pointer>, Consumer<S::Pointer>)> {
        // Note: Re-entrancy is not possible because we require ownership
        // of the producer and consumer, which are not cloneable. We also
        // can assume the buffer has been split, because

        // Are these our producers and consumers?
        let ptr = self.storage.get();
        let our_prod = prod.ptr.ptr_eq(self.storage.get());
        let our_cons = cons.ptr.ptr_eq(self.storage.get());

        if !(our_prod && our_cons) {
            // Can't release, not our producer and consumer
            return Err((prod, cons));
        }

        let wr_in_progress = ptr.write_in_progress().load(Acquire);
        let rd_in_progress = ptr.read_in_progress().load(Acquire);

        if wr_in_progress || rd_in_progress {
            // Can't release, active grant(s) in progress
            return Err((prod, cons));
        }

        // Drop the producer and consumer halves
        drop(prod);
        drop(cons);

        // Re-initialize the buffer (not totally needed, but nice to do)
        ptr.write().store(0, Release);
        ptr.read().store(0, Release);
        ptr.reserve().store(0, Release);
        ptr.last().store(0, Release);

        // Mark the buffer as ready to be split
        atomic::fetch_and(ptr.prod_con_count(), !CONSUMER_BIT, Release);
        atomic::fetch_sub(ptr.prod_con_count(), 1, Release);

        Ok(())
    }

    /*
    /// Attempt to release the Producer and Consumer in Framed mode
    ///
    /// This re-initializes the buffer so it may be split in a different mode at a later
    /// time. There must be no read or write grants active, or an error will be returned.
    ///
    /// The `FrameProducer` and `FrameConsumer` must be from THIS `BBBuffer`, or an error
    /// will be returned.
    pub fn try_release_framed(
        &'a self,
        prod: FrameProducer<'a, N>,
        cons: FrameConsumer<'a, N>,
    ) -> CoreResult<(), (FrameProducer<'a, N>, FrameConsumer<'a, N>)> {
        self.try_release(prod.producer, cons.consumer)
            .map_err(|(producer, consumer)| {
                // Restore the wrapper types
                (FrameProducer { producer }, FrameConsumer { consumer })
            })
    }*/
}

impl<S: Storage> BBBuffer<S> {
    /// Create a new constant inner portion of a `BBBuffer`.
    ///
    /// NOTE: This is only necessary to use when creating a `BBBuffer` at static
    /// scope, and is generally never used directly. This process is necessary to
    /// work around current limitations in `const fn`, and will be replaced in
    /// the future.
    ///
    /// ```rust,no_run
    /// use bbqueue::{BBBuffer, BufStorage};
    ///
    /// static STORAGE: BufStorage<6> = BufStorage::new();
    /// static BUF: BBBuffer<&'static BufStorage<6>> = BBBuffer::new(&STORAGE);
    ///
    /// fn main() {
    ///    let (prod, cons) = BUF.try_split().unwrap();
    /// }
    /// ```
    pub const fn new(storage: S) -> Self {
        Self { storage }
    }
}

/// Initializes a new static BBQueue
#[macro_export]
macro_rules! new_static_queue {
    ($n: expr) => {{
        static STORAGE: $crate::BufStorage<$n> = $crate::BufStorage::new();
        $crate::BBBuffer::new(&STORAGE)
    }};
}

/// A convinence type repersetning a static BBBuffer
pub type BBStatic<const N: usize> = BBBuffer<&'static BufStorage<N>>;

/// `Producer` is the primary interface for pushing data into a `BBBuffer`.
/// There are various methods for obtaining a grant to write to the buffer, with
/// different potential tradeoffs. As all grants are required to be a contiguous
/// range of data, different strategies are sometimes useful when making the decision
/// between maximizing usage of the buffer, and ensuring a given grant is successful.
///
/// As a short summary of currently possible grants:
///
/// * `grant_exact(N)`
///   * User will receive a grant `sz == N` (or receive an error)
///   * This may cause a wraparound if a grant of size N is not available
///       at the end of the ring.
///   * If this grant caused a wraparound, the bytes that were "skipped" at the
///       end of the ring will not be available until the reader reaches them,
///       regardless of whether the grant commited any data or not.
///   * Maximum possible waste due to skipping: `N - 1` bytes
/// * `grant_max_remaining(N)`
///   * User will receive a grant `0 < sz <= N` (or receive an error)
///   * This will only cause a wrap to the beginning of the ring if exactly
///       zero bytes are available at the end of the ring.
///   * Maximum possible waste due to skipping: 0 bytes
///
/// See [this github issue](https://github.com/jamesmunns/bbqueue/issues/38) for a
/// discussion of grant methods that could be added in the future.
pub struct Producer<P> {
    ptr: P,
}

//unsafe impl<'a, const N: usize> Send for Producer<'a, N> {}

impl<P: StoragePointer> Producer<P> {
    /// Request a writable, contiguous section of memory of exactly
    /// `sz` bytes. If the buffer size requested is not available,
    /// an error will be returned.
    ///
    /// This method may cause the buffer to wrap around early if the
    /// requested space is not available at the end of the buffer, but
    /// is available at the beginning
    ///
    /// ```rust
    /// # // bbqueue test shim!
    /// # fn bbqtest() {
    /// use bbqueue::BBStatic;
    ///
    /// // Create and split a new buffer of 6 elements
    /// let buffer: BBStatic<6> = bbqueue::new_static_queue!(6);
    ///
    /// let (mut prod, cons) = buffer.try_split().unwrap();
    ///
    /// // Successfully obtain and commit a grant of four bytes
    /// let mut grant = prod.grant_exact(4).unwrap();
    /// assert_eq!(grant.buf().len(), 4);
    /// grant.commit(4);
    ///
    /// // Try to obtain a grant of three bytes
    /// assert!(prod.grant_exact(3).is_err());
    /// # // bbqueue test shim!
    /// # }
    /// #
    /// # fn main() {
    /// # #[cfg(not(feature = "thumbv6"))]
    /// # bbqtest();
    /// # }
    /// ```
    pub fn grant_exact(&self, sz: usize) -> Result<GrantW<P>> {
        //let inner = unsafe { &self.bbq.as_ref() };

        if atomic::swap(self.ptr.write_in_progress(), true, AcqRel) {
            return Err(Error::GrantInProgress);
        }

        // Writer component. Must never write to `read`,
        // be careful writing to `load`
        let write = self.ptr.write().load(Acquire);
        let read = self.ptr.read().load(Acquire);
        let max = P::CAPACITY;
        let already_inverted = write < read;

        let start = if already_inverted {
            if (write + sz) < read {
                // Inverted, room is still available
                write
            } else {
                // Inverted, no room is available
                self.ptr.write_in_progress().store(false, Release);
                return Err(Error::InsufficientSize);
            }
        } else if write + sz <= max {
            // Non inverted condition
            write
        } else {
            // Not inverted, but need to go inverted

            // NOTE: We check sz < read, NOT <=, because
            // write must never == read in an inverted condition, since
            // we will then not be able to tell if we are inverted or not
            if sz < read {
                // Invertible situation
                0
            } else {
                // Not invertible, no space
                self.ptr.write_in_progress().store(false, Release);
                return Err(Error::InsufficientSize);
            }
        };

        // Safe write, only viewed by this task
        self.ptr.reserve().store(start + sz, Release);

        // This is sound, as UnsafeCell, MaybeUninit, and GenericArray
        // are all `#[repr(Transparent)]
        let start_of_buf_ptr = self.ptr.get_buf_ptr();
        let grant_slice =
            unsafe { from_raw_parts_mut(start_of_buf_ptr.offset(start as isize), sz) };

        Ok(GrantW {
            buf: grant_slice,
            ptr: self.ptr.clone(),
            to_commit: 0,
        })
    }

    /// Request a writable, contiguous section of memory of up to
    /// `sz` bytes. If a buffer of size `sz` is not available without
    /// wrapping, but some space (0 < available < sz) is available without
    /// wrapping, then a grant will be given for the remaining size at the
    /// end of the buffer. If no space is available for writing, an error
    /// will be returned.
    ///
    /// ```
    /// # // bbqueue test shim!
    /// # fn bbqtest() {
    /// use bbqueue::BBStatic;
    ///
    /// // Create and split a new buffer of 6 elements
    /// let buffer: BBStatic<6> = bbqueue::new_static_queue!(6);
    /// let (mut prod, mut cons) = buffer.try_split().unwrap();
    ///
    /// // Successfully obtain and commit a grant of four bytes
    /// let mut grant = prod.grant_max_remaining(4).unwrap();
    /// assert_eq!(grant.buf().len(), 4);
    /// grant.commit(4);
    ///
    /// // Release the four initial commited bytes
    /// let mut grant = cons.read().unwrap();
    /// assert_eq!(grant.buf().len(), 4);
    /// grant.release(4);
    ///
    /// // Try to obtain a grant of three bytes, get two bytes
    /// let mut grant = prod.grant_max_remaining(3).unwrap();
    /// assert_eq!(grant.buf().len(), 2);
    /// grant.commit(2);
    /// # // bbqueue test shim!
    /// # }
    /// #
    /// # fn main() {
    /// # #[cfg(not(feature = "thumbv6"))]
    /// # bbqtest();
    /// # }
    /// ```
    pub fn grant_max_remaining(&mut self, mut sz: usize) -> Result<GrantW<P>> {
        if atomic::swap(&self.ptr.write_in_progress(), true, AcqRel) {
            return Err(Error::GrantInProgress);
        }

        // Writer component. Must never write to `read`,
        // be careful writing to `load`
        let write = self.ptr.write().load(Acquire);
        let read = self.ptr.read().load(Acquire);
        let max = P::CAPACITY;

        let already_inverted = write < read;

        let start = if already_inverted {
            // In inverted case, read is always > write
            let remain = read - write - 1;

            if remain != 0 {
                sz = min(remain, sz);
                write
            } else {
                // Inverted, no room is available
                self.ptr.write_in_progress().store(false, Release);
                return Err(Error::InsufficientSize);
            }
        } else if write != max {
            // Some (or all) room remaining in un-inverted case
            sz = min(max - write, sz);
            write
        } else {
            // Not inverted, but need to go inverted

            // NOTE: We check read > 1, NOT read >= 1, because
            // write must never == read in an inverted condition, since
            // we will then not be able to tell if we are inverted or not
            if read > 1 {
                sz = min(read - 1, sz);
                0
            } else {
                // Not invertible, no space
                self.ptr.write_in_progress().store(false, Release);
                return Err(Error::InsufficientSize);
            }
        };

        // Safe write, only viewed by this task
        self.ptr.reserve().store(start + sz, Release);

        // This is sound, as UnsafeCell, MaybeUninit, and GenericArray
        // are all `#[repr(Transparent)]
        let start_of_buf_ptr = self.ptr.get_buf_ptr();
        let grant_slice =
            unsafe { from_raw_parts_mut(start_of_buf_ptr.offset(start as isize), sz) };

        Ok(GrantW {
            buf: grant_slice,
            ptr: self.ptr.clone(),
            to_commit: 0,
        })
    }
}
impl<P: StoragePointer> Clone for Producer<P> {
    fn clone(&self) -> Self {
        self.ptr.prod_con_count().fetch_add(1, Acquire);
        Self {
            ptr: self.ptr.clone(),
        }
    }
}

/// `Consumer` is the primary interface for reading data from a `BBBuffer`.
pub struct Consumer<P> {
    ptr: P,
}

// unsafe impl<'a, const N: usize> Send for Consumer<'a, N> {}

impl<P: StoragePointer> Consumer<P> {
    /// Obtains a contiguous slice of committed bytes. This slice may not
    /// contain ALL available bytes, if the writer has wrapped around. The
    /// remaining bytes will be available after all readable bytes are
    /// released
    ///
    /// ```rust
    /// # // bbqueue test shim!
    /// # fn bbqtest() {
    /// use bbqueue::BBStatic;
    ///
    /// // Create and split a new buffer of 6 elements
    /// let buffer: BBStatic<6> = bbqueue::new_static_queue!(6);
    /// let (mut prod, mut cons) = buffer.try_split().unwrap();
    ///
    /// // Successfully obtain and commit a grant of four bytes
    /// let mut grant = prod.grant_max_remaining(4).unwrap();
    /// assert_eq!(grant.buf().len(), 4);
    /// grant.commit(4);
    ///
    /// // Obtain a read grant
    /// let mut grant = cons.read().unwrap();
    /// assert_eq!(grant.buf().len(), 4);
    /// # // bbqueue test shim!
    /// # }
    /// #
    /// # fn main() {
    /// # #[cfg(not(feature = "thumbv6"))]
    /// # bbqtest();
    /// # }
    /// ```
    pub fn read(&mut self) -> Result<GrantR<P>> {
        if atomic::swap(self.ptr.read_in_progress(), true, AcqRel) {
            return Err(Error::GrantInProgress);
        }

        let write = self.ptr.write().load(Acquire);
        let last = self.ptr.last().load(Acquire);
        let mut read = self.ptr.read().load(Acquire);

        // Resolve the inverted case or end of read
        if (read == last) && (write < read) {
            read = 0;
            // This has some room for error, the other thread reads this
            // Impact to Grant:
            //   Grant checks if read < write to see if inverted. If not inverted, but
            //     no space left, Grant will initiate an inversion, but will not trigger it
            // Impact to Commit:
            //   Commit does not check read, but if Grant has started an inversion,
            //   grant could move Last to the prior write position
            // MOVING READ BACKWARDS!
            self.ptr.read().store(0, Release);
        }

        let sz = if write < read {
            // Inverted, only believe last
            last
        } else {
            // Not inverted, only believe write
            write
        } - read;

        if sz == 0 {
            self.ptr.read_in_progress().store(false, Release);
            return Err(Error::InsufficientSize);
        }

        // This is sound, as UnsafeCell, MaybeUninit, and GenericArray
        // are all `#[repr(Transparent)]
        let start_of_buf_ptr = self.ptr.get_buf_ptr();
        let grant_slice = unsafe { from_raw_parts_mut(start_of_buf_ptr.offset(read as isize), sz) };

        Ok(GrantR {
            buf: grant_slice,
            ptr: self.ptr.clone(),
            to_release: 0,
        })
    }

    /// Obtains two disjoint slices, which are each contiguous of committed bytes.
    /// Combined these contain all previously commited data.
    pub fn split_read(&mut self) -> Result<SplitGrantR<'_, P>> {
        if atomic::swap(self.ptr.read_in_progress(), true, AcqRel) {
            return Err(Error::GrantInProgress);
        }

        let write = self.ptr.write().load(Acquire);
        let last = self.ptr.last().load(Acquire);
        let mut read = self.ptr.read().load(Acquire);

        // Resolve the inverted case or end of read
        if (read == last) && (write < read) {
            read = 0;
            // This has some room for error, the other thread reads this
            // Impact to Grant:
            //   Grant checks if read < write to see if inverted. If not inverted, but
            //     no space left, Grant will initiate an inversion, but will not trigger it
            // Impact to Commit:
            //   Commit does not check read, but if Grant has started an inversion,
            //   grant could move Last to the prior write position
            // MOVING READ BACKWARDS!
            self.ptr.read().store(0, Release);
        }

        let (sz1, sz2) = if write < read {
            // Inverted, only believe last
            (last - read, write)
        } else {
            // Not inverted, only believe write
            (write - read, 0)
        };

        if sz1 == 0 {
            self.ptr.read_in_progress().store(false, Release);
            return Err(Error::InsufficientSize);
        }

        // This is sound, as UnsafeCell, MaybeUninit, and GenericArray
        // are all `#[repr(Transparent)]
        let start_of_buf_ptr = self.ptr.get_buf_ptr();
        let grant_slice1 =
            unsafe { from_raw_parts_mut(start_of_buf_ptr.offset(read as isize), sz1) };
        let grant_slice2 = unsafe { from_raw_parts_mut(start_of_buf_ptr, sz2) };

        Ok(SplitGrantR {
            buf1: grant_slice1,
            buf2: grant_slice2,
            ptr: &self.ptr,
            to_release: 0,
        })
    }
}

impl<S: Storage> BBBuffer<S> {
    /// Returns the size of the backing storage.
    ///
    /// This is the maximum number of bytes that can be stored in this queue.
    ///
    /// ```rust
    /// # // bbqueue test shim!
    /// # fn bbqtest() {
    /// use bbqueue::{BBStatic, new_static_queue};
    ///
    /// // Create a new buffer of 6 elements
    /// let buffer: BBStatic<6> = new_static_queue!(6);
    /// assert_eq!(buffer.capacity(), 6);
    /// # // bbqueue test shim!
    /// # }
    /// #
    /// # fn main() {
    /// # #[cfg(not(feature = "thumbv6"))]
    /// # bbqtest();
    /// # }
    /// ```
    pub const fn capacity(&self) -> usize {
        <S::Pointer as StoragePointer>::CAPACITY
    }
}

/// A structure representing a contiguous region of memory that
/// may be written to, and potentially "committed" to the queue.
///
/// NOTE: If the grant is dropped without explicitly commiting
/// the contents, or by setting a the number of bytes to
/// automatically be committed with `to_commit()`, then no bytes
/// will be comitted for writing.
///
/// If the `thumbv6` feature is selected, dropping the grant
/// without committing it takes a short critical section,
#[derive(Debug, PartialEq)]
pub struct GrantW<P: StoragePointer> {
    pub(crate) buf: *mut [u8],
    ptr: P,
    pub(crate) to_commit: usize,
}

unsafe impl<P: StoragePointer> Send for GrantW<P> {}

/// A structure representing a contiguous region of memory that
/// may be read from, and potentially "released" (or cleared)
/// from the queue
///
/// NOTE: If the grant is dropped without explicitly releasing
/// the contents, or by setting the number of bytes to automatically
/// be released with `to_release()`, then no bytes will be released
/// as read.
///
///
/// If the `thumbv6` feature is selected, dropping the grant
/// without releasing it takes a short critical section,
#[derive(Debug, PartialEq)]
pub struct GrantR<P: StoragePointer> {
    pub(crate) buf: *mut [u8],
    ptr: P,
    pub(crate) to_release: usize,
}

unsafe impl<P: StoragePointer> Send for GrantR<P> {}

/// A structure representing up to two contiguous regions of memory that
/// may be read from, and potentially "released" (or cleared)
/// from the queue
#[derive(Debug, PartialEq)]
pub struct SplitGrantR<'a, P: StoragePointer> {
    pub(crate) buf1: &'a mut [u8],
    pub(crate) buf2: &'a mut [u8],
    ptr: &'a P,
    pub(crate) to_release: usize,
}

// unsafe impl<'a, const N: usize> Send for GrantR<'a, N> {}

// unsafe impl<'a, const N: usize> Send for SplitGrantR<'a, N> {}

impl<P: StoragePointer> GrantW<P> {
    /// Finalizes a writable grant given by `grant()` or `grant_max()`.
    /// This makes the data available to be read via `read()`. This consumes
    /// the grant.
    ///
    /// If `used` is larger than the given grant, the maximum amount will
    /// be commited
    ///
    /// NOTE:  If the `thumbv6` feature is selected, this function takes a short critical
    /// section while committing.
    pub fn commit(mut self, used: usize) {
        self.commit_inner(used);
        forget(self);
    }

    /// Obtain access to the inner buffer for writing
    ///
    /// ```rust
    /// # // bbqueue test shim!
    /// # fn bbqtest() {
    /// use bbqueue::BBStatic;
    ///
    /// // Create and split a new buffer of 6 elements
    /// let buffer: BBStatic<6> = bbqueue::new_static_queue!(6);
    /// let (mut prod, mut cons) = buffer.try_split().unwrap();
    ///
    /// // Successfully obtain and commit a grant of four bytes
    /// let mut grant = prod.grant_max_remaining(4).unwrap();
    /// grant.buf().copy_from_slice(&[1, 2, 3, 4]);
    /// grant.commit(4);
    /// # // bbqueue test shim!
    /// # }
    /// #
    /// # fn main() {
    /// # #[cfg(not(feature = "thumbv6"))]
    /// # bbqtest();
    /// # }
    /// ```
    #[inline]
    pub fn buf(&mut self) -> &mut [u8] {
        unsafe { &mut *self.buf }
    }

    /// Sometimes, it's not possible for the lifetimes to check out. For example,
    /// if you need to hand this buffer to a function that expects to receive a
    /// `&'static mut [u8]`, it is not possible for the inner reference to outlive the
    /// grant itself.
    ///
    /// You MUST guarantee that in no cases, the reference that is returned here outlives
    /// the grant itself. Once the grant has been released, referencing the data contained
    /// WILL cause undefined behavior.
    ///
    /// Additionally, you must ensure that a separate reference to this data is not created
    /// to this data, e.g. using `DerefMut` or the `buf()` method of this grant.
    pub unsafe fn as_static_mut_buf(&mut self) -> &'static mut [u8] {
        &mut *self.buf
    }

    #[inline(always)]
    pub(crate) fn commit_inner(&mut self, used: usize) {
        // If there is no grant in progress, return early. This
        // generally means we are dropping the grant within a
        // wrapper structure
        if !self.ptr.write_in_progress().load(Acquire) {
            return;
        }

        // Writer component. Must never write to READ,
        // be careful writing to LAST

        // Saturate the grant commit
        let len = self.buf().len();
        let used = min(len, used);

        let write = self.ptr.write().load(Acquire);
        atomic::fetch_sub(self.ptr.reserve(), len - used, AcqRel);

        let max = P::CAPACITY;
        let last = self.ptr.last().load(Acquire);
        let new_write = self.ptr.reserve().load(Acquire);

        if (new_write < write) && (write != max) {
            // We have already wrapped, but we are skipping some bytes at the end of the ring.
            // Mark `last` where the write pointer used to be to hold the line here
            self.ptr.last().store(write, Release);
        } else if new_write > last {
            // We're about to pass the last pointer, which was previously the artificial
            // end of the ring. Now that we've passed it, we can "unlock" the section
            // that was previously skipped.
            //
            // Since new_write is strictly larger than last, it is safe to move this as
            // the other thread will still be halted by the (about to be updated) write
            // value
            self.ptr.last().store(max, Release);
        }
        // else: If new_write == last, either:
        // * last == max, so no need to write, OR
        // * If we write in the end chunk again, we'll update last to max next time
        // * If we write to the start chunk in a wrap, we'll update last when we
        //     move write backwards

        // Write must be updated AFTER last, otherwise read could think it was
        // time to invert early!
        self.ptr.write().store(new_write, Release);

        // Allow subsequent grants
        self.ptr.write_in_progress().store(false, Release);
    }

    /// Configures the amount of bytes to be commited on drop.
    pub fn to_commit(&mut self, amt: usize) {
        self.to_commit = self.buf().len().min(amt);
    }
}

impl<P: StoragePointer> GrantR<P> {
    /// Release a sequence of bytes from the buffer, allowing the space
    /// to be used by later writes. This consumes the grant.
    ///
    /// If `used` is larger than the given grant, the full grant will
    /// be released.
    ///
    /// NOTE:  If the `thumbv6` feature is selected, this function takes a short critical
    /// section while releasing.
    pub fn release(mut self, used: usize) {
        // Saturate the grant release
        let used = min(self.buf().len(), used);

        self.release_inner(used);
        forget(self);
    }

    pub(crate) fn shrink(&mut self, len: usize) {
        unsafe {
            let ptr = self.buf_mut().as_mut_ptr();
            let old_len = self.buf().len();
            assert!(len <= old_len);
            self.buf = slice::from_raw_parts_mut(ptr, len);
        }
    }

    /// Obtain access to the inner buffer for reading
    ///
    /// ```
    /// # // bbqueue test shim!
    /// # fn bbqtest() {
    /// use bbqueue::BBStatic;
    ///
    /// // Create and split a new buffer of 6 elements
    /// let buffer: BBStatic<6> = bbqueue::new_static_queue!(6);
    /// let (mut prod, mut cons) = buffer.try_split().unwrap();
    ///
    /// // Successfully obtain and commit a grant of four bytes
    /// let mut grant = prod.grant_max_remaining(4).unwrap();
    /// grant.buf().copy_from_slice(&[1, 2, 3, 4]);
    /// grant.commit(4);
    ///
    /// // Obtain a read grant, and copy to a buffer
    /// let mut grant = cons.read().unwrap();
    /// let mut buf = [0u8; 4];
    /// buf.copy_from_slice(grant.buf());
    /// assert_eq!(&buf, &[1, 2, 3, 4]);
    /// # // bbqueue test shim!
    /// # }
    /// #
    /// # fn main() {
    /// # #[cfg(not(feature = "thumbv6"))]
    /// # bbqtest();
    /// # }
    /// ```
    pub fn buf(&self) -> &[u8] {
        unsafe { &*self.buf }
    }

    /// Obtain mutable access to the read grant
    ///
    /// This is useful if you are performing in-place operations
    /// on an incoming packet, such as decryption
    pub fn buf_mut(&mut self) -> &mut [u8] {
        unsafe { &mut *self.buf }
    }

    /// Sometimes, it's not possible for the lifetimes to check out. For example,
    /// if you need to hand this buffer to a function that expects to receive a
    /// `&'static [u8]`, it is not possible for the inner reference to outlive the
    /// grant itself.
    ///
    /// You MUST guarantee that in no cases, the reference that is returned here outlives
    /// the grant itself. Once the grant has been released, referencing the data contained
    /// WILL cause undefined behavior.
    ///
    /// Additionally, you must ensure that a separate reference to this data is not created
    /// to this data, e.g. using `Deref` or the `buf()` method of this grant.
    pub unsafe fn as_static_buf(&self) -> &'static [u8] {
        transmute::<&[u8], &'static [u8]>(&*self.buf)
    }

    #[inline(always)]
    pub(crate) fn release_inner(&mut self, used: usize) {
        // If there is no grant in progress, return early. This
        // generally means we are dropping the grant within a
        // wrapper structure
        if !self.ptr.read_in_progress().load(Acquire) {
            return;
        }

        // This should always be checked by the public interfaces
        debug_assert!(used <= self.buf().len());

        // This should be fine, purely incrementing
        let _ = atomic::fetch_add(self.ptr.read(), used, Release);

        self.ptr.read_in_progress().store(false, Release);
    }

    /// Configures the amount of bytes to be released on drop.
    pub fn to_release(&mut self, amt: usize) {
        self.to_release = self.buf().len().min(amt);
    }
}

impl<'a, P: StoragePointer> SplitGrantR<'a, P> {
    /// Release a sequence of bytes from the buffer, allowing the space
    /// to be used by later writes. This consumes the grant.
    ///
    /// If `used` is larger than the given grant, the full grant will
    /// be released.
    ///
    /// NOTE:  If the `thumbv6` feature is selected, this function takes a short critical
    /// section while releasing.
    pub fn release(mut self, used: usize) {
        // Saturate the grant release
        let used = min(self.combined_len(), used);

        self.release_inner(used);
        forget(self);
    }

    /// Obtain access to both inner buffers for reading
    ///
    /// ```
    /// # // bbqueue test shim!
    /// # fn bbqtest() {
    /// use bbqueue::BBStatic;
    ///
    /// // Create and split a new buffer of 6 elements
    /// let buffer: BBStatic<6> = bbqueue::new_static_queue!(6);
    /// let (mut prod, mut cons) = buffer.try_split().unwrap();
    ///
    /// // Successfully obtain and commit a grant of four bytes
    /// let mut grant = prod.grant_max_remaining(4).unwrap();
    /// grant.buf().copy_from_slice(&[1, 2, 3, 4]);
    /// grant.commit(4);
    ///
    /// // Obtain a read grant, and copy to a buffer
    /// let mut grant = cons.read().unwrap();
    /// let mut buf = [0u8; 4];
    /// buf.copy_from_slice(grant.buf());
    /// assert_eq!(&buf, &[1, 2, 3, 4]);
    /// # // bbqueue test shim!
    /// # }
    /// #
    /// # fn main() {
    /// # #[cfg(not(feature = "thumbv6"))]
    /// # bbqtest();
    /// # }
    /// ```
    pub fn bufs(&self) -> (&[u8], &[u8]) {
        (self.buf1, self.buf2)
    }

    /// Obtain mutable access to both parts of the read grant
    ///
    /// This is useful if you are performing in-place operations
    /// on an incoming packet, such as decryption
    pub fn bufs_mut(&mut self) -> (&mut [u8], &mut [u8]) {
        (self.buf1, self.buf2)
    }

    #[inline(always)]
    pub(crate) fn release_inner(&mut self, used: usize) {
        // If there is no grant in progress, return early. This
        // generally means we are dropping the grant within a
        // wrapper structure
        if !self.ptr.read_in_progress().load(Acquire) {
            return;
        }

        // This should always be checked by the public interfaces
        debug_assert!(used <= self.combined_len());

        if used <= self.buf1.len() {
            // This should be fine, purely incrementing
            let _ = atomic::fetch_add(self.ptr.read(), used, Release);
        } else {
            // Also release parts of the second buffer
            self.ptr.read().store(used - self.buf1.len(), Release);
        }

        self.ptr.read_in_progress().store(false, Release);
    }

    /// Configures the amount of bytes to be released on drop.
    pub fn to_release(&mut self, amt: usize) {
        self.to_release = self.combined_len().min(amt);
    }

    /// The combined length of both buffers
    pub fn combined_len(&self) -> usize {
        self.buf1.len() + self.buf2.len()
    }
}

impl<P: StoragePointer> Drop for GrantW<P> {
    fn drop(&mut self) {
        self.commit_inner(self.to_commit)
    }
}

impl<P: StoragePointer> Drop for GrantR<P> {
    fn drop(&mut self) {
        self.release_inner(self.to_release)
    }
}

impl<'a, P: StoragePointer> Drop for SplitGrantR<'a, P> {
    fn drop(&mut self) {
        self.release_inner(self.to_release)
    }
}

impl<P: StoragePointer> Deref for GrantW<P> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.buf }
    }
}

impl<P: StoragePointer> DerefMut for GrantW<P> {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { &mut *self.buf }
    }
}

impl<P: StoragePointer> Deref for GrantR<P> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.buf }
    }
}

impl<P: StoragePointer> DerefMut for GrantR<P> {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { &mut *self.buf }
    }
}

#[cfg(feature = "thumbv6")]
mod atomic {
    use core::sync::atomic::{
        AtomicBool, AtomicUsize,
        Ordering::{self, Acquire, Release},
    };
    use cortex_m::interrupt::free;

    #[inline(always)]
    pub fn fetch_add(atomic: &AtomicUsize, val: usize, _order: Ordering) -> usize {
        free(|_| {
            let prev = atomic.load(Acquire);
            atomic.store(prev.wrapping_add(val), Release);
            prev
        })
    }

    #[inline(always)]
    pub fn fetch_or(atomic: &AtomicUsize, val: usize, _order: Ordering) -> usize {
        free(|_| {
            let prev = atomic.load(Acquire);
            atomic.store(prev | val, Release);
            prev
        })
    }

    #[inline(always)]
    pub fn fetch_and(atomic: &AtomicUsize, val: usize, _order: Ordering) -> usize {
        free(|_| {
            let prev = atomic.load(Acquire);
            atomic.store(prev & val, Release);
            prev
        })
    }

    #[inline(always)]
    pub fn fetch_sub(atomic: &AtomicUsize, val: usize, _order: Ordering) -> usize {
        free(|_| {
            let prev = atomic.load(Acquire);
            atomic.store(prev.wrapping_sub(val), Release);
            prev
        })
    }

    #[inline(always)]
    pub fn swap(atomic: &AtomicBool, val: bool, _order: Ordering) -> bool {
        free(|_| {
            let prev = atomic.load(Acquire);
            atomic.store(val, Release);
            prev
        })
    }
}

#[cfg(not(feature = "thumbv6"))]
mod atomic {
    use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[inline(always)]
    pub fn fetch_add(atomic: &AtomicUsize, val: usize, order: Ordering) -> usize {
        atomic.fetch_add(val, order)
    }

    #[inline(always)]
    pub fn fetch_or(atomic: &AtomicUsize, val: usize, order: Ordering) -> usize {
        atomic.fetch_or(val, order)
    }

    #[inline(always)]
    pub fn fetch_and(atomic: &AtomicUsize, val: usize, order: Ordering) -> usize {
        atomic.fetch_and(val, order)
    }

    #[inline(always)]
    pub fn fetch_sub(atomic: &AtomicUsize, val: usize, order: Ordering) -> usize {
        atomic.fetch_sub(val, order)
    }

    #[inline(always)]
    pub fn swap(atomic: &AtomicBool, val: bool, order: Ordering) -> bool {
        atomic.swap(val, order)
    }
}
