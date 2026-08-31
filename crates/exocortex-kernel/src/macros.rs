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
        $(actions! { $($averbs:tt)* })?
        $(functions! { $($fverbs:tt)* })?
        $(guidance! { $($gentries:tt)* })?
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

        // ---- PX2: the optional `actions!` / `functions!` / `guidance!`
        // sections (palantir-expansion PRD §3.2, §4.1, §4.2). Each is
        // optional; a pack that declares none expands to empty tables
        // and still compiles unchanged. Signatures land in `pack_def()`
        // (and the compatibility fingerprint); bodies live only in the
        // `inventory` registrations.
        $crate::__pack_actions!(@start $name $(; $($averbs)*)?);
        $crate::__pack_functions!(@start $name $(; $($fverbs)*)?);
        $crate::__pack_guidance!(@start $(; $($gentries)*)?);

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

            // ---- PX2: verb signatures + guidance. Bodies are deliberately
            // absent (registrations only) — patching a body moves neither
            // fingerprint level. Guidance keys/links resolve against THIS
            // pack's own tables at build time (§4.2: same resolution
            // discipline as type_triples!, failure is a pack-load error).
            let mut actions: ::std::vec::Vec<$crate::verbs::PackActionDef> =
                ::std::vec::Vec::new();
            for &(verb, ceiling, input_ty, output_ty) in PACK_ACTION_SIGS {
                actions.push($crate::verbs::PackActionDef {
                    name: ::smol_str::SmolStr::new_static(verb),
                    ceiling: ceiling,
                    input_type: ::smol_str::SmolStr::new_static(input_ty),
                    output_type: ::smol_str::SmolStr::new_static(output_ty),
                });
            }
            let mut functions: ::std::vec::Vec<$crate::verbs::PackFunctionDef> =
                ::std::vec::Vec::new();
            for &(verb, engine, p50, p99, input_ty, output_ty) in PACK_FUNCTION_SIGS {
                functions.push($crate::verbs::PackFunctionDef {
                    name: ::smol_str::SmolStr::new_static(verb),
                    engine: ::smol_str::SmolStr::new_static(engine),
                    input_type: ::smol_str::SmolStr::new_static(input_ty),
                    output_type: ::smol_str::SmolStr::new_static(output_ty),
                    p50_budget_us: p50,
                    p99_budget_us: p99,
                });
            }
            let mut guidance: ::std::vec::Vec<$crate::verbs::GuidanceEntry> =
                __pack_guidance();
            let kind_names: ::std::collections::HashSet<&str> =
                KIND_TABLE.iter().map(|r| r.name).collect();
            let mt_names: ::std::collections::HashSet<&str> =
                mt_by_name.keys().copied().collect();
            for entry in &guidance {
                let key_known =
                    mt_names.contains(entry.key.as_str()) || kind_names.contains(entry.key.as_str());
                assert!(
                    key_known,
                    "guidance! key `{}` is neither a memory type nor a kind of pack {}",
                    entry.key, $name
                );
                for link in &entry.links {
                    assert!(
                        kind_names.contains(link.kind.as_str()),
                        "guidance! link kind `{}` is not a kind of pack {}",
                        link.kind, $name
                    );
                    assert!(
                        mt_names.contains(link.other.as_str()),
                        "guidance! link target `{}` is not a memory type of pack {}",
                        link.other, $name
                    );
                }
            }

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
                actions,
                functions,
                guidance,
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

// ---- PX2: the `actions!` / `functions!` / `guidance!` section munchers
// (palantir-expansion PRD §3.2/§4.1/§4.2; mechanics de-risked by the
// PX2-S1 spike — outcome (a), plain `macro_rules!`).
//
// `macro_rules!` cannot splice identifiers, so each verb expands into a
// `pub mod $verb` (the verb ident used as-is) holding the generated
// `Input` struct and typed body; signature rows pair names with
// stringified types in per-pack statics that `pack_def()` reads.

/// Internal: the `actions!` section. Invocation shape from `pack!`:
/// `__pack_actions!(@start $pack $(; $($verbs)*)?)` — the `;` marker
/// carries section presence so absent sections still emit the (empty)
/// signature table `pack_def()` reads.
#[doc(hidden)]
#[macro_export]
macro_rules! __pack_actions {
    (@start $pack:literal) => {
        #[doc = concat!("Action signatures declared by pack `", $pack, "` (empty: none declared).")]
        pub static PACK_ACTION_SIGS: &[(
            &'static str,
            $crate::Visibility,
            &'static str,
            &'static str,
        )] = &[];
    };
    (@start $pack:literal ; $($verbs:tt)*) => {
        $crate::__pack_actions!(@verbs $pack [@sigs] $($verbs)*);
    };
    (@verbs $pack:literal [@sigs $($sigs:tt)*]) => {
        #[doc = concat!("Action signatures declared by pack `", $pack, "`, in declaration order.")]
        pub static PACK_ACTION_SIGS: &[(
            &'static str,
            $crate::Visibility,
            &'static str,
            &'static str,
        )] = &[ $($sigs)* ];
    };
    // The verb module and registration are emitted DIRECTLY (never
    // through the tt accumulator): a `:block` body fragment corrupted
    // when re-matched as token trees on the recursion path. Hygiene: the
    // author's body is a CLOSURE binding its own `ctx`/`input` — params
    // written by the macro could never bind identifiers written by the
    // caller (separate syntax contexts).
    (@verbs
        $pack:literal [@sigs $($sigs:tt)*]
        $verb:ident (input: { $($fields:tt)* }, min_visibility: $ceil:ident) = $body:expr ,
        $($rest:tt)*
    ) => {
        #[doc = concat!("Typed input for pack action `", stringify!($verb), "`.")]
        #[allow(non_snake_case)]
        pub mod $verb {
            // Caller-scope names (the pack root's own imports and items)
            // resolve for the author's closure through this glob: paths in
            // macro-emitted code resolve at the expansion site.
            use super::*;
            $crate::__input_struct!(@struct [] $($fields)*);
            #[doc = concat!(
                "The `", stringify!($verb),
                "` body — a typed transform compiled and type-checked in the pack crate (PX2-S1 outcome (a))."
            )]
            pub static BODY: fn(
                &$crate::verbs::ActionContext,
                Input,
            ) -> $crate::KernelResult<$crate::verbs::ActionProduct> = $body;
        }
        ::inventory::submit! {
            $crate::verbs::PackActionRegistration {
                pack_name: $pack,
                verb_name: stringify!($verb),
                ceiling: $crate::Visibility::$ceil,
                input_schema: || $crate::verbs::__schema_of::<$verb::Input>(),
                run: |ctx, value| {
                    let input = $crate::verbs::__decode_input::<$verb::Input>(value)?;
                    ($verb::BODY)(ctx, input)
                },
            }
        }
        $crate::__pack_actions!(@verbs
            $pack
            [@sigs
                $($sigs)*
                (
                    stringify!($verb),
                    $crate::Visibility::$ceil,
                    concat!(stringify!($verb), "::Input"),
                    "ActionProduct",
                ),
            ]
            $($rest)*
        );
    };
    // Separator skip: a `,` between entries (or a trailing one) is not
    // an entry. Declared before the catch-all so it wins the ordering.
    (@verbs $pack:literal [@sigs $($sigs:tt)*] , $($rest:tt)*) => {
        $crate::__pack_actions!(@verbs $pack [@sigs $($sigs)*] $($rest)*);
    };
    // The catch-all must be declared AFTER the recursion arms (PX2-S1).
    (@verbs $pack:literal $($_:tt)*) => {
        ::core::compile_error!(
            "malformed actions! entry: expected \
             `Verb(input: { field: Type, ... }, min_visibility: Visibility) = |ctx, input| { body }`"
        );
    };
}

/// Internal: the `functions!` section. v1 executes `scheme` bodies through
/// the reasoning crate's embedded Steel interpreter; a `datalog` body is a
/// pack-compile error (Crepe compiles at build time only — Datalog rules
/// belong in `crepe_rules!`).
#[doc(hidden)]
#[macro_export]
macro_rules! __pack_functions {
    (@start $pack:literal) => {
        #[doc = concat!("Function signatures declared by pack `", $pack, "` (empty: none declared).")]
        pub static PACK_FUNCTION_SIGS: &[(
            &'static str,
            &'static str,
            u32,
            u32,
            &'static str,
            &'static str,
        )] = &[];
    };
    (@start $pack:literal ; $($verbs:tt)*) => {
        $crate::__pack_functions!(@fns $pack [@sigs] $($verbs)*);
    };
    (@fns $pack:literal [@sigs $($sigs:tt)*]) => {
        #[doc = concat!("Function signatures declared by pack `", $pack, "`, in declaration order.")]
        pub static PACK_FUNCTION_SIGS: &[(
            &'static str,
            &'static str,
            u32,
            u32,
            &'static str,
            &'static str,
        )] = &[ $($sigs)* ];
    };
    // The legal entry comes FIRST; the rejected-engine arms below it can
    // then be unambiguous (a `scheme` entry never reaches them).
    (@fns
        $pack:literal [@sigs $($sigs:tt)*]
        $verb:ident (input: { $($fields:tt)* }) -> $output:ty ,
        p50_us: $p50:literal , p99_us: $p99:literal = scheme { $($fbody:tt)* }
        $($rest:tt)*
    ) => {
        #[doc = concat!("Typed input for pack function `", stringify!($verb), "`.")]
        #[allow(non_snake_case)]
        pub mod $verb {
            $crate::__input_struct!(@struct [] $($fields)*);
        }
        ::inventory::submit! {
            $crate::verbs::PackFunctionRegistration {
                pack_name: $pack,
                verb_name: stringify!($verb),
                engine: "scheme",
                body: stringify!($($fbody)*),
                p50_budget_us: $p50,
                p99_budget_us: $p99,
                input_schema: || $crate::verbs::__schema_of::<$verb::Input>(),
                output_schema: || $crate::verbs::__schema_of::<$output>(),
            }
        }
        $crate::__pack_functions!(@fns
            $pack
            [@sigs
                $($sigs)*
                (
                    stringify!($verb),
                    "scheme",
                    $p50,
                    $p99,
                    concat!(stringify!($verb), "::Input"),
                    stringify!($output),
                ),
            ]
            $($rest)*
        );
    };
    // Separator skip (see `__pack_actions!`).
    (@fns $pack:literal [@sigs $($sigs:tt)*] , $($rest:tt)*) => {
        $crate::__pack_functions!(@fns $pack [@sigs $($sigs)*] $($rest)*);
    };
    // Rejected engines, AFTER the scheme arm: `datalog` names the build-
    // time constraint; any other ident names the one legal spelling.
    (@fns $pack:literal $acc1:tt $acc2:tt
        $verb:ident ($($sig:tt)*) -> $output:ty = datalog { $($fbody:tt)* } $($rest:tt)*
    ) => {
        ::core::compile_error!(
            "pack functions! support `scheme` bodies in v1 — Datalog rules compile at build time and belong in crepe_rules! (see PX2 in docs/master-plan.prd)"
        );
    };
    (@fns $pack:literal $acc1:tt $acc2:tt
        $verb:ident ($($sig:tt)*) -> $output:ty , p50_us: $p50:literal , p99_us: $p99:literal = $engine:ident { $($fbody:tt)* } $($rest:tt)*
    ) => {
        ::core::compile_error!(
            "pack functions! engine must be `scheme` in v1 (datalog belongs in crepe_rules!)"
        );
    };
    (@fns $pack:literal $($_:tt)*) => {
        ::core::compile_error!(
            "malformed functions! entry: expected \
             `Verb(input: { field: Type, ... }) -> Type, p50_us: N, p99_us: N = scheme { body }`"
        );
    };
}

/// Internal: the generated `Input` struct for a verb (fields are
/// `$f:ident : $ty:ty` pairs). Derives ride the kernel's re-exports so a
/// pack crate stays single-dependency.
#[doc(hidden)]
#[macro_export]
macro_rules! __input_struct {
    (@struct [$($acc:tt)*]) => {
        // The `crate =` redirects point serde's/schemars' generated helper
        // code at the kernel's re-exports, so a pack crate never needs a
        // direct serde/schemars dependency (single-dep seam preserved).
        #[derive(Clone, Debug, exocortex_kernel::serde::Serialize, exocortex_kernel::serde::Deserialize, exocortex_kernel::schemars::JsonSchema)]
        #[serde(crate = "exocortex_kernel::serde")]
        #[schemars(crate = "exocortex_kernel::schemars")]
        pub struct Input { $($acc)* }
    };
    (@struct [$($acc:tt)*] $f:ident : $ty:ty , $($rest:tt)*) => {
        $crate::__input_struct!(@struct [$($acc)* pub $f: $ty,] $($rest)*);
    };
    (@struct [$($acc:tt)*] $f:ident : $ty:ty) => {
        $crate::__input_struct!(@struct [$($acc)* pub $f: $ty,]);
    };
}

/// Internal: the `guidance!` section. Each entry is
/// `Key { when: "...", caution: "...", link: [Kind => Target | Kind <= Source, ...] }`
/// (any subset, any order). Keys and link names resolve against the
/// declaring pack's own type/kind tables at `pack_def()` build time.
#[doc(hidden)]
#[macro_export]
macro_rules! __pack_guidance {
    (@start) => {
        #[doc = "Structured agent guidance declared by this pack (§4.2), in declaration order."]
        pub fn __pack_guidance() -> ::std::vec::Vec<$crate::verbs::GuidanceEntry> {
            ::std::vec::Vec::new()
        }
    };
    (@start ; $($entries_in:tt)*) => {
        $crate::__pack_guidance!(@entries [] $($entries_in)*);
    };
    (@entries [$($entries:tt)*]) => {
        #[doc = "Structured agent guidance declared by this pack (§4.2), in declaration order."]
        pub fn __pack_guidance() -> ::std::vec::Vec<$crate::verbs::GuidanceEntry> {
            ::std::vec![ $($entries)* ]
        }
    };
    (@entries [$($entries:tt)*] $key:ident { $($attrs:tt)* } $($rest:tt)* ) => {
        $crate::__pack_guidance!(@entries
            [$($entries)*
                $crate::verbs::__guidance_entry(
                    stringify!($key),
                    ::std::vec::Vec::from($crate::__guidance_pieces!(@pieces [] $($attrs)*)),
                ),
            ]
            $($rest)*
        );
    };
    // Separator skip (see `__pack_actions!`).
    (@entries [$($entries:tt)*] , $($rest:tt)*) => {
        $crate::__pack_guidance!(@entries [$($entries)*] $($rest)*);
    };
    (@entries [$($entries:tt)*] $($_:tt)*) => {
        ::core::compile_error!(
            "malformed guidance! entry: expected `Key { when: \"...\", caution: \"...\", link: [Kind => Target] }`"
        );
    };
}

/// Internal: one guidance entry's attributes → `__GuidancePiece`s.
/// Link lists are munched through the `@lpass` inner pass (terminated by
/// `|`) so the pieces accumulator stays flat.
#[doc(hidden)]
#[macro_export]
macro_rules! __guidance_pieces {
    // The terminal returns a bracketed array — an unparenthesized comma
    // inside an expression-position macro expansion would be truncated
    // ("macro expansion ignores `,` and any tokens following").
    (@pieces [$($acc:tt)*]) => { [ $($acc)* ] };
    (@pieces [$($acc:tt)*] when: $w:literal , $($rest:tt)*) => {
        $crate::__guidance_pieces!(@pieces
            [$($acc)* $crate::verbs::__GuidancePiece::When($w),] $($rest)*);
    };
    (@pieces [$($acc:tt)*] when: $w:literal) => {
        $crate::__guidance_pieces!(@pieces [$($acc)* $crate::verbs::__GuidancePiece::When($w),]);
    };
    (@pieces [$($acc:tt)*] caution: $c:literal , $($rest:tt)*) => {
        $crate::__guidance_pieces!(@pieces
            [$($acc)* $crate::verbs::__GuidancePiece::Caution($c),] $($rest)*);
    };
    (@pieces [$($acc:tt)*] caution: $c:literal) => {
        $crate::__guidance_pieces!(@pieces [$($acc)* $crate::verbs::__GuidancePiece::Caution($c),]);
    };
    (@pieces [$($acc:tt)*] link: [ $($links:tt)* ] , $($rest:tt)*) => {
        $crate::__guidance_pieces!(@lpass [$($acc)*] [] $($links)* | $($rest)*);
    };
    (@pieces [$($acc:tt)*] link: [ $($links:tt)* ]) => {
        $crate::__guidance_pieces!(@lpass [$($acc)*] [] $($links)* |);
    };
    // @lpass: fold one link list into the piece accumulator; `|` ends it.
    (@lpass [$($acc:tt)*] [$($lacc:tt)*] $lk:ident => $lt:ident , $($more:tt)*) => {
        $crate::__guidance_pieces!(@lpass [$($acc)*] [$($lacc)*
            $crate::verbs::__GuidancePiece::Link(stringify!($lk), true, stringify!($lt)),
        ] $($more)*);
    };
    (@lpass [$($acc:tt)*] [$($lacc:tt)*] $lk:ident => $lt:ident | $($rest:tt)*) => {
        $crate::__guidance_pieces!(@pieces [$($acc)* $($lacc)*
            $crate::verbs::__GuidancePiece::Link(stringify!($lk), true, stringify!($lt)),
        ] $($rest)*);
    };
    (@lpass [$($acc:tt)*] [$($lacc:tt)*] $lk:ident <= $lt:ident , $($more:tt)*) => {
        $crate::__guidance_pieces!(@lpass [$($acc)*] [$($lacc)*
            $crate::verbs::__GuidancePiece::Link(stringify!($lk), false, stringify!($lt)),
        ] $($more)*);
    };
    (@lpass [$($acc:tt)*] [$($lacc:tt)*] $lk:ident <= $lt:ident | $($rest:tt)*) => {
        $crate::__guidance_pieces!(@pieces [$($acc)* $($lacc)*
            $crate::verbs::__GuidancePiece::Link(stringify!($lk), false, stringify!($lt)),
        ] $($rest)*);
    };
    (@lpass [$($acc:tt)*] [$($lacc:tt)*] | $($rest:tt)*) => {
        $crate::__guidance_pieces!(@pieces [$($acc)* $($lacc)*] $($rest)*);
    };
}
