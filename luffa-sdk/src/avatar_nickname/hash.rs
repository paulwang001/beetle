use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn my_hash<T>(obj: T) -> u64
    where
        T: Hash,
{
    let mut hasher = DefaultHasher::new();
    obj.hash(&mut hasher);
    hasher.finish()
}