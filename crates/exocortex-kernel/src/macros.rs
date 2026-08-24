// macros.rs — the `pack!` macro (§7.0).
//
// M0 carries the PRD §2.6.1 skeleton verbatim; the full expansion (enums,
// kind constants, companions, triples) is implemented at M1.

/// The `pack!` macro. Emits:
///   - A `pub const PACK_DEF: PackDef = ...;` inside the pack crate.
///   - An `inventory::submit!` block registering it.
///   - Zero-sized marker types for `MemoryType`/`EntityType` variants that
///     packs can name in their Rust code.
///
/// The macro body is straight `macro_rules!`; the coding agent should copy the
/// implementation from `crates/exocortex-kernel/src/macros.rs` verbatim.
#[macro_export]
macro_rules! pack {
    (
        name: $name:literal,
        version: $version:literal,
        kernel_min: $kernel_min:literal,
        memory_types! { $($mt:ident),* $(,)? }
        entity_types! { $($et:ident),* $(,)? }
        kinds! { $($kind:ident => bucket: $bucket:ident, inverse: $inv:tt, bi: $bi:literal, default_strength: $ds:literal),* $(,)? }
        type_triples! { $($tk:ident => ($from:tt, $to:tt)),* $(,)? }
        crepe_rules! { $($rule:tt)* }
    ) => {
        // Emitted skeleton — full expansion in macros.rs. This shape lets
        // callers write the ergonomic DSL shown in PRD §7.0.
        pub const PACK_DEF: $crate::PackDef = $crate::PackDef {
            name: ::smol_str::SmolStr::new_static($name),
            version: $crate::__parse_version!($version),
            kernel_min: $crate::__parse_version!($kernel_min),
            memory_type_names: ::std::vec![], // populated by proc-macro pass in v1.1
            entity_type_names: ::std::vec![],
            kinds: ::std::vec![],
            type_triples: ::std::vec![],
            rule_ids: ::std::vec![],
        };
        ::inventory::submit! { PACK_DEF.clone() }
    };
}
