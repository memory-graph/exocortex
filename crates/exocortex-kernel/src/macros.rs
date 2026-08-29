// macros.rs — the `pack!` macro and its helper macros (§7.0, §7.18 DSL).
//
// The DSL is declarative `macro_rules!`:
//  - `memory_types!`/`entity_types!` expand to `#[repr(u8)]` enums in the
//    pack crate whose declaration order IS the ontology u8 id order.
//  - `kinds!` expands to a const `KindRow` table (authored rows plus the
//    auto-registered inverse companion rows, R-T4). Companions carry no type
//    triples, so authoring them directly fails the R-T17 lookup in the
//    validator — they are materialized on write only.
//  - `type_triples!`/`crepe_rules!` expand to table/builder entries resolved
//    by name at pack-def build time (names are the stable identity surface).
//
// Pack-space `RelKindId`s are assigned provisionally as
// `0x8000_0000 | local`; `Ontology::from_packs` canonicalizes them with the
// pack's registry slot (`PackId`), assigned deterministically by sorted pack
// name at load time.

/// The `pack!` macro. Emits, inside the pack crate:
///   - `pub enum MemoryType` / `pub enum EntityType` (`#[repr(u8)]`,
///     declaration order == ontology id order),
///   - `pub static KIND_TABLE` (authored kinds + inverse companions),
///   - `pub static CREPE_RULES` (rule name -> source text),
///   - `pub fn pack_def() -> PackDef` (the runtime builder),
///   - an `inventory::submit!` registration hook.
///
/// See PRD §7.0 for the DSL and §7.18 for the dev-v1 invocation.
#[macro_export]
macro_rules! pack {
    (
        name: $name:literal,
        version: $version:literal,
        kernel_min: $kernel_min:literal,
        memory_types! { $($mt:ident),* $(,)? }
        entity_types! { $($et:ident),* $(,)? }
        $(computed_only_kinds! { $($ck:ident),* $(,)? })?
        kinds! { $($kentries:tt)* }
        type_triples! { $($tk:ident => $pair:tt),* $(,)? }
        crepe_rules! { $($crepe_src:tt)* }
    ) => {
        #[doc = concat!("Memory types registered by pack `", $name, "`. Declaration order == ontology u8 id order.")]
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        #[repr(u8)]
        #[allow(missing_docs)]
        pub enum MemoryType { $($mt),* }

        impl MemoryType {
            /// All variants in declaration (= id) order.
            pub const ALL: &'static [MemoryType] = &[ $( MemoryType::$mt ),* ];
            /// The u8 id assigned by the effective ontology.
            #[inline]
            pub const fn id(self) -> u8 { self as u8 }
            /// Resolve an ontology id back to a variant.
            #[inline]
            pub fn from_id(id: u8) -> ::core::option::Option<Self> {
                <Self>::ALL.get(id as usize).copied()
            }
            /// Stable display name (matches the `memory_type_names` entry).
            pub fn name(self) -> &'static str {
                match self { $( MemoryType::$mt => stringify!($mt) ),* }
            }
        }

        #[doc = concat!("Entity types registered by pack `", $name, "`. Declaration order == ontology u8 id order.")]
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        #[repr(u8)]
        #[allow(missing_docs)]
        pub enum EntityType { $($et),* }

        impl EntityType {
            /// All variants in declaration (= id) order.
            pub const ALL: &'static [EntityType] = &[ $( EntityType::$et ),* ];
            /// The u8 id assigned by the effective ontology.
            #[inline]
            pub const fn id(self) -> u8 { self as u8 }
            /// Resolve an ontology id back to a variant.
            #[inline]
            pub fn from_id(id: u8) -> ::core::option::Option<Self> {
                <Self>::ALL.get(id as usize).copied()
            }
            /// Stable display name (matches the `entity_type_names` entry).
            pub fn name(self) -> &'static str {
                match self { $( EntityType::$et => stringify!($et) ),* }
            }
        }

        /// Authored kind rows followed by their auto-registered inverse
        /// companion rows (R-T4), in declaration order.
        pub static KIND_TABLE: &[$crate::pack::KindRow] =
            $crate::__kind_rows!(@rows [] $($kentries)*);

        /// Pack-local Crepe rules, verbatim source (§7.18). Rule ids are
        /// extracted from this source deterministically at build time.
        /// The block is captured as TEXT because Crepe is not Rust and
        /// only the downstream `crepe_rules!` compiler can parse it.
        /// (PX2-S1 sharpened this note: `macro_rules!` CAN tokenize
        /// past `;` — `$($x:tt)*` and `:block` both do — the text
        /// capture here serves the downstream text compiler, which
        /// token structure would not.)
        pub static CREPE_RULES_SRC: &'static str = stringify!($($crepe_src)*);

        #[doc = concat!("Build the `PackDef` for pack `", $name, "`. The value is deterministic for a given pack version; the fingerprint over it is stable across processes.")]
        pub fn pack_def() -> $crate::PackDef {
            // --- kinds: authored rows take sequential local ids; companions
            // take 0x4000 + j so the two sequences never collide. Kernel-const
            // kinds keep their kernel-space ids.
            let mut kinds: ::std::vec::Vec<$crate::RelMeta> = ::std::vec::Vec::new();
            let mut next_authored: u32 = 0;
            let mut next_companion: u32 = 0x4000;
            for row in KIND_TABLE {
                let id = if !row.companion {
                    match $crate::pack::kernel_const_by_name(row.kernel_const_name) {
                        Some(k) => k,
                        None => {
                            next_authored += 1;
                            $crate::RelKindId(0x8000_0000 | (next_authored - 1))
                        }
                    }
                } else {
                    next_companion += 1;
                    $crate::RelKindId(0x8000_0000 | (next_companion - 1))
                };
                kinds.push($crate::RelMeta {
                    id,
                    display_name: ::smol_str::SmolStr::new_static(row.name),
                    bucket: row.bucket,
                    inverse: None, // fixed up below
                    bidirectional: row.bidirectional,
                    default_strength: row.default_strength,
                    computed_only: false,
                });
            }
            // W6 (audit): computed-only kinds are declared in the pack and
            // carried on the ontology (R-T14) — never a string literal in
            // a consumer crate.
            $(
                for k in kinds.iter_mut() {
                    if matches!(k.display_name.as_str(), $( stringify!($ck) )|*) {
                        k.computed_only = true;
                    }
                }
            )*
            let id_by_name: ::std::collections::HashMap<::std::string::String, $crate::RelKindId> =
                kinds.iter().map(|k| (k.display_name.to_string(), k.id)).collect();
            for k in kinds.iter_mut() {
                let row = KIND_TABLE
                    .iter()
                    .find(|r| r.name == k.display_name.as_str())
                    .expect("kind row for emitted kind");
                if let Some(inv) = row.inverse_name {
                    k.inverse = id_by_name.get(inv).copied();
                }
            }

            // --- type triples: names -> ids through the maps built above.
            let mt_by_name: ::std::collections::HashMap<&'static str, u8> =
                [ $( (stringify!($mt), MemoryType::$mt as u8) ),* ].into_iter().collect();
            let side_ids = |names: ::core::option::Option<&'static [&'static str]>|
                -> ::core::option::Option<::std::vec::Vec<u8>> {
                names.map(|ns| {
                    ns.iter()
                        .map(|n| {
                            *mt_by_name.get(n).unwrap_or_else(|| {
                                panic!("type_triples! references unknown memory type {n}")
                            })
                        })
                        .collect()
                })
            };
            let mut type_triples: ::std::vec::Vec<$crate::pack::TypeTriple> =
                ::std::vec::Vec::new();
            $(
                let (f_names, t_names) = $crate::__triple_sides!(($tk => $pair));
                type_triples.push($crate::pack::TypeTriple {
                    kind: id_by_name[stringify!($tk)],
                    from_types: side_ids(f_names),
                    to_types: side_ids(t_names),
                });
            )*

            $crate::PackDef {
                name: ::smol_str::SmolStr::new_static($name),
                version: $crate::__parse_version!($version),
                kernel_min: $crate::__parse_version!($kernel_min),
                memory_type_names: ::std::vec![
                    $( ::smol_str::SmolStr::new_static(stringify!($mt)) ),*
                ],
                entity_type_names: ::std::vec![
                    $( ::smol_str::SmolStr::new_static(stringify!($et)) ),*
                ],
                kinds,
                type_triples,
                rule_ids: $crate::pack::rule_ids_from_source(CREPE_RULES_SRC)
                    .into_iter()
                    .map(::smol_str::SmolStr::new_static)
                    .collect(),
            }
        }

        ::inventory::submit! {
            $crate::pack::PackRegistration { build: pack_def }
        }
    };
}

/// Internal: expand one kind entry (or a trailing fragment) into `KindRow`
/// initializers. Authored rows carry `companion: false`; every `inverse:`
/// target that is not `Self` auto-registers a read-only companion row (R-T4).
///
/// The `kernel_const:` suffix is matched by separate arms (not `$(...)?`)
/// because an optional group followed by the entry's trailing comma is
/// locally ambiguous for the macro engine.
#[doc(hidden)]
#[macro_export]
macro_rules! __kind_rows {
    (@rows [$($rows:tt)*]) => { &[ $($rows)* ] };
    // Self-inverse, kernel-const bound:
    (@rows
        [$($rows:tt)*]
        $name:ident => bucket: $bucket:ident, inverse: Self, bi: $bi:literal,
        default_strength: $ds:literal , kernel_const: $kc:ident , $($rest:tt)*
    ) => {
        $crate::__kind_rows!(@rows
            [$($rows)*
                $crate::pack::KindRow {
                    name: stringify!($name),
                    bucket: $crate::RelBucket::$bucket,
                    inverse_name: ::core::option::Option::Some(stringify!($name)),
                    bidirectional: $bi,
                    default_strength: $ds,
                    kernel_const_name: stringify!($kc),
                    companion: false,
                },
            ]
            $($rest)*
        );
    };
    // Self-inverse, plain:
    (@rows
        [$($rows:tt)*]
        $name:ident => bucket: $bucket:ident, inverse: Self, bi: $bi:literal,
        default_strength: $ds:literal , $($rest:tt)*
    ) => {
        $crate::__kind_rows!(@rows
            [$($rows)*
                $crate::pack::KindRow {
                    name: stringify!($name),
                    bucket: $crate::RelBucket::$bucket,
                    inverse_name: ::core::option::Option::Some(stringify!($name)),
                    bidirectional: $bi,
                    default_strength: $ds,
                    kernel_const_name: "",
                    companion: false,
                },
            ]
            $($rest)*
        );
    };
    // Named inverse, kernel-const bound:
    (@rows
        [$($rows:tt)*]
        $name:ident => bucket: $bucket:ident, inverse: $inv:ident, bi: $bi:literal,
        default_strength: $ds:literal , kernel_const: $kc:ident , $($rest:tt)*
    ) => {
        $crate::__kind_rows!(@rows
            [$($rows)*
                $crate::pack::KindRow {
                    name: stringify!($name),
                    bucket: $crate::RelBucket::$bucket,
                    inverse_name: ::core::option::Option::Some(stringify!($inv)),
                    bidirectional: $bi,
                    default_strength: $ds,
                    kernel_const_name: stringify!($kc),
                    companion: false,
                },
                $crate::pack::KindRow {
                    name: stringify!($inv),
                    bucket: $crate::RelBucket::$bucket,
                    inverse_name: ::core::option::Option::Some(stringify!($name)),
                    bidirectional: $bi,
                    default_strength: $ds,
                    kernel_const_name: "",
                    companion: true,
                },
            ]
            $($rest)*
        );
    };
    // Named inverse, plain:
    (@rows
        [$($rows:tt)*]
        $name:ident => bucket: $bucket:ident, inverse: $inv:ident, bi: $bi:literal,
        default_strength: $ds:literal , $($rest:tt)*
    ) => {
        $crate::__kind_rows!(@rows
            [$($rows)*
                $crate::pack::KindRow {
                    name: stringify!($name),
                    bucket: $crate::RelBucket::$bucket,
                    inverse_name: ::core::option::Option::Some(stringify!($inv)),
                    bidirectional: $bi,
                    default_strength: $ds,
                    kernel_const_name: "",
                    companion: false,
                },
                $crate::pack::KindRow {
                    name: stringify!($inv),
                    bucket: $crate::RelBucket::$bucket,
                    inverse_name: ::core::option::Option::Some(stringify!($name)),
                    bidirectional: $bi,
                    default_strength: $ds,
                    kernel_const_name: "",
                    companion: true,
                },
            ]
            $($rest)*
        );
    };
}

/// Internal: destructure one `type_triples!` entry `Kind => (from, to)` into
/// `(Option<&[names]>, Option<&[names]>)`. `_` is the any-wildcard (None).
#[doc(hidden)]
#[macro_export]
macro_rules! __triple_sides {
    (($tk:ident => ( _ , _ ))) => {
        (::core::option::Option::None, ::core::option::Option::None)
    };
    (($tk:ident => ( _ , $($t:ident)|+ ))) => {
        (
            ::core::option::Option::None,
            ::core::option::Option::<&'static [&'static str]>::Some(&[ $( stringify!($t) ),+ ]),
        )
    };
    (($tk:ident => ( $($f:ident)|+ , _ ))) => {
        (
            ::core::option::Option::<&'static [&'static str]>::Some(&[ $( stringify!($f) ),+ ]),
            ::core::option::Option::None,
        )
    };
    (($tk:ident => ( $($f:ident)|+ , $($t:ident)|+ ))) => {
        (
            ::core::option::Option::<&'static [&'static str]>::Some(&[ $( stringify!($f) ),+ ]),
            ::core::option::Option::<&'static [&'static str]>::Some(&[ $( stringify!($t) ),+ ]),
        )
    };
}

/// Internal: extract the kind ident of a `type_triples!` entry.
#[doc(hidden)]
#[macro_export]
#[allow(unused_macros)]
macro_rules! __tt_kind {
    ($tk:ident => ($($pair:tt)*)) => {
        $tk
    };
}

/// Internal: expand one `crepe_rules!` rule into `(rule_id, source)`.
#[doc(hidden)]
#[macro_export]
#[allow(unused_macros)]
macro_rules! __crepe_rule {
    ($pred:ident ($($args:tt)*) <- $($body:tt)+ ;) => {
        (stringify!($pred), stringify!( ($($args)*) <- $($body)+ ))
    };
}

/// Internal: parse a `"major.minor.patch"` literal into `PackVersion` at
/// compile time.
#[doc(hidden)]
#[macro_export]
macro_rules! __parse_version {
    ($s:literal) => {
        $crate::pack::PackVersion::parse($s)
    };
}
