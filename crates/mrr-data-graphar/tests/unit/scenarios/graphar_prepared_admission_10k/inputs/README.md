# Input

The focused Rust Scenario prepares one native GraphAr dataset containing 10,000
binary-Entity MRR facts before the measured loop begins. Preparation performs
native storage I/O, Arrow C Stream import, and projection-independent identity
and context decoding exactly once.
