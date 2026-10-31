# Input

The focused Rust Scenario prepares one native GraphAr dataset containing 10,000
binary-Entity MRR facts before the measured loop begins. Preparation performs
native storage I/O, Arrow C Stream import, and projection-independent identity
and context decoding exactly once.

The first uncached projection validation is performed, checked and emitted separately before measurement. Warm samples use the bounded exact schema/catalog certificate attached to the immutable prepared source.
