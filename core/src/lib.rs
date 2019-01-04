// #![no_std]
#![allow(unused)]

use core::cmp::min;
use core::marker::PhantomData;
use core::ptr::NonNull;
use core::result::Result as CoreResult;
use core::slice::from_raw_parts;
use core::slice::from_raw_parts_mut;
use core::sync::atomic::{
    AtomicUsize,
    Ordering::{
        Acquire,
        Relaxed,
        Release,
    },
};
use core::cell::UnsafeCell;

pub use generic_array::{GenericArray, ArrayLength};
pub use generic_array::typenum as typenum;

pub type Result<T> = CoreResult<T, Error>;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum Error {
    InsufficientSize,
    GrantInProgress,
}

#[derive(Debug)]
pub struct Track {
    /// Where the next byte will be written
    write: AtomicUsize,

    /// Where the next byte will be read from
    read: AtomicUsize,

    /// Used in the inverted case to mark the end of the
    /// readable streak. Otherwise will == unsafe { (*self.buf.get()).len() }.
    /// Writer is responsible for placing this at the correct
    /// place when entering an inverted condition, and Reader
    /// is responsible for moving it back to unsafe { (*self.buf.get()).len() }
    /// when exiting the inverted condition
    last: AtomicUsize,

    /// Used by the Writer to remember what bytes are currently
    /// allowed to be written to, but are not yet ready to be
    /// read from
    reserve: usize,

    /// Is there an active read grant?
    read_in_progress: bool,
}

impl Track {
    fn new(sz: usize) -> Self {
        Track {
            /// Owned by the writer
            write: AtomicUsize::new(0),

            /// Owned by the reader
            read: AtomicUsize::new(0),

            /// Cooperatively owned
            last: AtomicUsize::new(sz),

            /// Owned by the Writer, "private"
            reserve: 0,

            /// Owned by the Reader, "private"
            read_in_progress: false,
        }
    }
}

#[derive(Debug)]
pub struct BBQueue<'a> {
    pub buf: UnsafeCell<&'a mut [u8]>,
    is_split: bool,
    trk: Track,
    prod_token: (),
    cons_token: (),
}

impl<'a> BBQueue<'a> {
    pub fn new(buf: &'static mut [u8]) -> Self {
        BBQueue {
            trk: Track::new(buf.len()),
            buf: UnsafeCell::new(buf),
            is_split: false,
            cons_token: (),
            prod_token: (),
        }
    }

    /// Request a writable, contiguous section of memory of exactly
    /// `sz` bytes. If the buffer size requested is not available,
    /// an error will be returned.
    pub fn grant(&mut self, sz: usize) -> Result<GrantW> {
        // Writer component. Must never write to `read`,
        // be careful writing to `load`

        let write = self.trk.write.load(Relaxed);

        if self.trk.reserve != write {
            // GRANT IN PROCESS, do not allow further grants
            // until the current one has been completed
            return Err(Error::GrantInProgress);
        }

        let read = self.trk.read.load(Acquire);
        let max = unsafe { (*self.buf.get()).len() };

        let already_inverted = write < read;

        let start = if already_inverted {
            if (write + sz) < read {
                // Inverted, room is still available
                write
            } else {
                // Inverted, no room is available
                return Err(Error::InsufficientSize);
            }
        } else {
            if write + sz <= max {
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
                    return Err(Error::InsufficientSize);
                }
            }
        };

        // Safe write, only viewed by this task
        self.trk.reserve = start + sz;

        Ok(GrantW {
            buf: unsafe { from_raw_parts_mut(&mut unsafe { (*self.buf.get())[start] }, sz) },
            internal: (),
        })
    }

    /// Request a writable, contiguous section of memory of up to
    /// `sz` bytes. If a buffer of size `sz` is not available, but
    /// some space (0 < available < sz) is available, then a grant
    /// will be given for the remaining size. If no space is available
    /// for writing, an error will be returned
    pub fn grant_max(&mut self, mut sz: usize) -> Result<GrantW> {
        // Writer component. Must never write to `read`,
        // be careful writing to `load`

        let write = self.trk.write.load(Relaxed);

        if self.trk.reserve != write {
            // GRANT IN PROCESS, do not allow further grants
            // until the current one has been completed
            return Err(Error::GrantInProgress);
        }

        let read = self.trk.read.load(Acquire);
        let max = unsafe { (*self.buf.get()).len() };

        let already_inverted = write < read;

        let start = if already_inverted {
            // In inverted case, read is always > write
            let remain = read - write - 1;

            if remain != 0 {
                sz = min(remain, sz);
                write
            } else {
                // Inverted, no room is available
                return Err(Error::InsufficientSize);
            }
        } else {
            if write != max {
                // Some (or all) room remaining in un-inverted case
                sz = min(max - write, sz);
                write
            } else {
                // Not inverted, but need to go inverted

                // NOTE: We check read > 1, NOT read > 1, because
                // write must never == read in an inverted condition, since
                // we will then not be able to tell if we are inverted or not
                if read > 1 {
                    sz = min(read - 1, sz);
                    0
                } else {
                    // Not invertible, no space
                    return Err(Error::InsufficientSize);
                }
            }
        };

        // Safe write, only viewed by this task
        self.trk.reserve = start + sz;

        Ok(GrantW {
            buf: unsafe { from_raw_parts_mut(&mut unsafe { (*self.buf.get())[start] }, sz) },
            internal: (),
        })
    }

    /// Finalizes a writable grant given by `grant()` or `grant_max()`.
    /// This makes the data available to be read via `read()`.
    ///
    /// If `used` is larger than the given grant, this function will panic.
    pub fn commit(&mut self, used: usize, grant: GrantW) {
        // Writer component. Must never write to READ,
        // be careful writing to LAST

        // Verify we are not committing more than the given
        // grant
        let len = grant.buf.len();
        assert!(len >= used);
        drop(grant);

        let write = self.trk.write.load(Relaxed);
        self.trk.reserve -= len - used;

        // Inversion case, we have begun writing
        if (self.trk.reserve < write) && (write != unsafe { (*self.buf.get()).len() }) {
            // This has potential for danger. We have two writers!
            // MOVING LAST BACKWARDS
            self.trk.last.store(write, Release);
        }

        // This has some potential for danger. The other thread (READ)
        // does look at this variable!
        // MOVING WRITE FORWARDS
        self.trk.write.store(self.trk.reserve, Release);
    }

    /// Obtains a contiguous slice of committed bytes. This slice may not
    /// contain ALL available bytes, if the writer has wrapped around. The
    /// remaining bytes will be available after all readable bytes are
    /// released
    pub fn read(&mut self) -> Result<GrantR> {
        if self.trk.read_in_progress {
            return Err(Error::GrantInProgress);
        }

        let write = self.trk.write.load(Acquire);
        let mut last = self.trk.last.load(Acquire);
        let mut read = self.trk.read.load(Relaxed);
        let max = unsafe { (*self.buf.get()).len() };

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
            self.trk.read.store(0, Release);
            if last != max {
                // This is pretty tricky, we have two writers!
                // MOVING LAST FORWARDS
                self.trk.last.store(max, Release);
                last = max;
            }
        }

        let sz = if write < read {
            // Inverted, only believe last
            last
        } else {
            // Not inverted, only believe write
            write
        } - read;

        if sz == 0 {
            return Err(Error::InsufficientSize);
        }

        self.trk.read_in_progress = true;

        Ok(GrantR {
            buf: unsafe { from_raw_parts(&unsafe { (*self.buf.get())[read] }, sz) },
            internal: (),
        })
    }

    /// Release a sequence of bytes from the buffer, allowing the space
    /// to be used by later writes
    ///
    /// If `used` is larger than the given grant, this function will panic.
    pub fn release(&mut self, used: usize, grant: GrantR) {
        assert!(used <= grant.buf.len());
        drop(grant);

        // This should be fine, purely incrementing
        let _ = self.trk.read.fetch_add(used, Release);

        self.trk.read_in_progress = false;
    }
}

#[derive(Debug, PartialEq)]
pub struct GrantW {
    pub buf: &'static mut [u8],

    // Zero sized type preventing external construction
    internal: (),
}

#[derive(Debug, PartialEq)]
pub struct GrantR {
    pub buf: &'static [u8],

    // Zero sized type preventing external construction
    internal: (),
}

/// An opaque structure, capable of reading data from the queue
unsafe impl<'a> Send for Consumer<'a> {}
pub struct Consumer<'a> {
    /// The underlying `BBQueue` object`
    pub bbq: NonNull<BBQueue<'a>>,
    token: &'a mut (),
}

/// An opaque structure, capable of writing data to the queue
unsafe impl<'a> Send for Producer<'a> {}
pub struct Producer<'a> {
    /// The underlying `BBQueue` object`
    pub bbq: NonNull<BBQueue<'a>>,
    token: &'a mut (),
}

impl<'a> BBQueue<'a> {
    /// This method takes a `BBQueue`, and returns a set of SPSC handles
    /// that may be given to separate threads
    pub fn split(&'a mut self) -> (Producer<'a>, Consumer<'a>) {
        assert!(!self.is_split);

        let x = unsafe { NonNull::new_unchecked(self as *const _ as *mut _) };
        let y = unsafe { NonNull::new_unchecked(self as *const _ as *mut _) };


        let mut ret = (
            Producer {
                bbq: x,
                // ltr: PhantomData,
                token: &mut self.prod_token,
            },
            Consumer {
                bbq: y,
                // ltr: PhantomData,
                token: &mut self.cons_token,
            },
        );
        ret
    }
}

impl<'a> Producer<'a> {
    /// Request a writable, contiguous section of memory of exactly
    /// `sz` bytes. If the buffer size requested is not available,
    /// an error will be returned.
    #[inline(always)]
    pub fn grant(&mut self, sz: usize) -> Result<GrantW> {
        unsafe { self.bbq.as_mut().grant(sz) }
    }

    /// Request a writable, contiguous section of memory of up to
    /// `sz` bytes. If a buffer of size `sz` is not available, but
    /// some space (0 < available < sz) is available, then a grant
    /// will be given for the remaining size. If no space is available
    /// for writing, an error will be returned
    #[inline(always)]
    pub fn grant_max(&mut self, sz: usize) -> Result<GrantW> {
        unsafe { self.bbq.as_mut().grant_max(sz) }
    }

    /// Finalizes a writable grant given by `grant()` or `grant_max()`.
    /// This makes the data available to be read via `read()`.
    ///
    /// If `used` is larger than the given grant, this function will panic.
    #[inline(always)]
    pub fn commit(&mut self, used: usize, grant: GrantW) {
        unsafe { self.bbq.as_mut().commit(used, grant) }
    }
}

impl<'a> Consumer<'a> {
    /// Obtains a contiguous slice of committed bytes. This slice may not
    /// contain ALL available bytes, if the writer has wrapped around. The
    /// remaining bytes will be available after all readable bytes are
    /// released
    ///
    /// NOTE: For now, it is possible to have multiple read grants. However,
    /// care must be taken NOT to do something like this:
    ///
    /// ```rust,skip
    /// let grant_1 = bbq.read();
    /// let grant_2 = bbq.read();
    /// bbq.release(grant_1.buf.len(), grant1); // OK, but now `grant_2` is invalid
    /// bbq.release(grant_2.buf.len(), grant2); // UNDEFINED BEHAVIOR!
    /// ```
    ///
    /// This behavior will be fixed in later releases
    #[inline(always)]
    pub fn read(&mut self) -> Result<GrantR> {
        unsafe { self.bbq.as_mut().read() }
    }

    /// Release a sequence of bytes from the buffer, allowing the space
    /// to be used by later writes
    ///
    /// If `used` is larger than the given grant, this function will panic.
    #[inline(always)]
    pub fn release(&mut self, used: usize, grant: GrantR) {
        unsafe { self.bbq.as_mut().release(used, grant) }
    }
}

