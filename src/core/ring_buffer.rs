use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;
use std::sync::Arc;

// Cache line size padding to prevent false sharing
#[repr(align(64))]
struct CachePadded<T>(T);

pub struct RingBuffer<T> {
    buffer: Vec<UnsafeCell<Option<T>>>,
    capacity: usize,
    mask: usize,
    head: CachePadded<AtomicUsize>, // Producer index
    tail: CachePadded<AtomicUsize>, // Consumer index
}

unsafe impl<T: Send> Sync for RingBuffer<T> {}
unsafe impl<T: Send> Send for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0 && capacity.is_power_of_two(), "Capacity must be a power of two");
        
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(UnsafeCell::new(None));
        }

        RingBuffer {
            buffer,
            capacity,
            mask: capacity - 1,
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
        }
    }

    pub fn push(&self, item: T) -> Result<(), T> {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Acquire);

        if head.wrapping_sub(tail) >= self.capacity {
            return Err(item); // Buffer full
        }

        let index = head & self.mask;
        unsafe {
            *self.buffer[index].get() = Some(item);
        }

        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    pub fn pop(&self) -> Option<T> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);

        if tail == head {
            return None; // Buffer empty
        }

        let index = tail & self.mask;
        let item = unsafe {
            (*self.buffer[index].get()).take()
        };

        self.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        item
    }
}
