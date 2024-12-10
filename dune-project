(lang dune 3.0)

(formatting
 (enabled_for ocaml))

(authors "Łukasz Kurowski")

(maintainers "crackcomm@gmail.com")

(generate_opam_files true)

(package
 (name polars)
 (synopsis "Polars bindings for OCaml")
 (description "Polars is a blazingly fast data manipulation library in Rust. This library provides bindings for OCaml.")
 (depends
  (ocaml (>= 5.02))
  (async (= v0.17.0))
  (core (= v0.17.0))
  (ppx_typed_fields (= v0.17.0))))
