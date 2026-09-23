# Expected

Both paths must observe all 10,000 edges. The Rust bridge must borrow the Arrow
buffers through the Arrow C Stream interface and remain within 25 percent of
the official GraphAr reader's P95 on the same measured run.
