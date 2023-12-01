use std::path::Path;

use anyhow::{bail, Context};
use byteorder::{ByteOrder, LittleEndian};
use memmap2::{Mmap, MmapOptions};
use storage_proofs_core::{error::Result, parameter_cache::LockedFile};

const DEGREE: usize = 14;

/// u32 = 4 bytes
const NODE_BYTES: usize = 4;

#[derive(Debug)]
pub struct CacheData {
    /// This is a large list of fixed (parent) sized arrays.
    data: Mmap,
    /// Offset in nodes.
    offset: u32,
    /// Len in nodes.
    len: u32,
    /// The underlyling file.
    file: LockedFile,
}

impl CacheData {
    /// Change the cache to point to the newly passed in offset.
    ///
    /// The `new_offset` must be set, such that `new_offset + len` does not
    /// overflow the underlying data.
    pub fn shift(&mut self, new_offset: u32) -> Result<()> {
        if self.offset == new_offset {
            return Ok(());
        }

        let offset = new_offset as usize * DEGREE * NODE_BYTES;
        let len = self.len as usize * DEGREE * NODE_BYTES;

        self.data = unsafe {
            MmapOptions::new()
                .offset(offset as u64)
                .len(len)
                .map(self.file.as_ref())
                .context("could not shift mmap}")?
        };
        self.offset = new_offset;

        Ok(())
    }

    /// Returns true if this node is in the cached range.
    pub fn contains(&self, node: u32) -> bool {
        node >= self.offset && node < self.offset + self.len
    }

    /// Read the parents for the given node from cache.
    ///
    /// Panics if the `node` is not in the cache.
    pub fn read(&self, node: u32) -> [u32; DEGREE] {
        assert!(node >= self.offset, "node not in cache");
        let start = (node - self.offset) as usize * DEGREE * NODE_BYTES;
        let end = start + DEGREE * NODE_BYTES;

        let mut res = [0u32; DEGREE];
        LittleEndian::read_u32_into(&self.data[start..end], &mut res);
        res
    }

    pub fn reset(&mut self) -> Result<()> {
        if self.offset == 0 {
            return Ok(());
        }

        self.shift(0)
    }

    pub fn open(offset: u32, len: u32, path: &Path) -> Result<Self> {
        let min_cache_size = (offset + len) as usize * DEGREE * NODE_BYTES;

        let file = LockedFile::open_shared_read(path)
            .with_context(|| format!("could not open path={}", path.display()))?;

        let actual_len = file.as_ref().metadata()?.len();
        if actual_len < min_cache_size as u64 {
            bail!(
                "corrupted cache: {}, expected at least {}, got {} bytes",
                path.display(),
                min_cache_size,
                actual_len
            );
        }

        let data = unsafe {
            MmapOptions::new()
                .offset((offset as usize * DEGREE * NODE_BYTES) as u64)
                .len(len as usize * DEGREE * NODE_BYTES)
                .map(file.as_ref())
                .with_context(|| format!("could not mmap path={}", path.display()))?
        };

        Ok(Self {
            data,
            file,
            len,
            offset,
        })
    }
}
