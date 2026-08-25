//! Round-2 small-fix tests: H6's untested fixes.
//! - tag normalization (kernel §7.5 comment: lowercase/trim/dedupe)
//! - §14.3 `effective_strength` closed-form cases

mod kernel_tags {
    use exocortex_kernel::normalize_tags;

    #[test]
    fn tags_are_trimmed_lowercased_and_deduped() {
        let out = normalize_tags(["  Rust ", "rust", "Cargo-Build", "", "   ", "RUST"]);
        let rendered: Vec<String> = out.iter().map(|t| t.to_string()).collect();
        assert_eq!(rendered, vec!["rust", "cargo-build"]);
    }

    #[test]
    fn empty_input_yields_empty_output() {
        assert!(normalize_tags(Vec::<String>::new()).is_empty());
        assert!(normalize_tags(["", "  "]).is_empty());
    }
}

mod effective_strength {
    use exocortex_dreams::mcr2::effective_strength;

    #[test]
    fn closed_form_hand_cases() {
        // ev=1 → boost=0; sr=1 → success=1; age=0 → decay=1.
        let identity = effective_strength(0.6, 1, 1.0, 0.0);
        assert!((identity - 0.6).abs() < 1e-6, "got {identity}");

        // ev=17 → boost = 0.05*sqrt(16) = 0.20 (the cap; ev=100 stays 0.20).
        let capped = effective_strength(0.5, 17, 1.0, 0.0);
        assert!((capped - 0.7).abs() < 1e-6, "got {capped}");
        let still_capped = effective_strength(0.5, 100, 1.0, 0.0);
        assert!((still_capped - 0.7).abs() < 1e-6, "boost capped at 0.20");

        // sr=0 → success=0.5 halves the result.
        let halved = effective_strength(0.5, 1, 0.0, 0.0);
        assert!((halved - 0.25).abs() < 1e-6, "got {halved}");

        // age=100d → decay=0.5 (the floor); age=500d stays 0.5.
        let decayed = effective_strength(0.8, 1, 1.0, 100.0);
        assert!((decayed - 0.4).abs() < 1e-6, "got {decayed}");
        let floored = effective_strength(0.8, 1, 1.0, 500.0);
        assert!((floored - 0.4).abs() < 1e-6, "decay floored at 0.5");

        // Full stack: base 0.5, ev 5 (boost 0.05*2=0.1), sr 0.8 (0.9), 10d (0.9).
        let stacked = effective_strength(0.5, 5, 0.8, 10.0);
        assert!((stacked - 0.6 * 0.9 * 0.9).abs() < 1e-6, "got {stacked}");

        // Clamp: a pathological base cannot escape [0,1].
        assert_eq!(effective_strength(1.4, 100, 1.0, 0.0), 1.0);
        assert_eq!(effective_strength(0.0, 1, 0.0, 0.0), 0.0);
    }
}
