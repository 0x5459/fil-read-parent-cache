use std::{env, path::Path};

use cache::CacheData;

mod cache;

fn main() {
    let args: Vec<_> = env::args().skip(1).collect();
    let cache_data = CacheData::open(0, args[0].parse().unwrap(), Path::new(&args[1])).unwrap();
    dbg!(cache_data.read(999));
}
