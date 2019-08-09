Changelog
=========

2.7
---

  * port to dune
    (whitequark)
  * support for OCaml 4.08 (#26)
    (Anton Kochkov, review by Gabriel Scherer)

2.6
---

  * Support for NPM packaging (#17)
    (Maxime Rasan)
  * Fix `varint` decoding (#18)
    (There was a decoding bug for integers between 2^56 and 2^63)
    (Maxime Rasan)
  * Support for OCaml 4.06 (#19)
    (Gabriel Scherer)

The homepage for the project has now moved to:
<https://github.com/ocaml-ppx/ppx_deriving_protobuf>

2.5
---
  * Compatibility with statically linked ppx drivers.

2.4
---

  * OCaml 4.03.0 compatibility.

2.3
---

  * Add support for exporting `.protoc` files.
  * Add support for hygiene.
  * Fix several bugs related to edge cases in serializing and deserializing
    integers.

2.2
---

  * Update to accomodate syntactic changes in OCaml 4.02.2.

2.1
---

  * Update for _ppx_deriving_ 2.0.

2.0
---

  * Update to accomodate syntactic changes in _ppx_deriving_ 1.0.

1.0.0
-----

  * First stable release and initial release as _ppx_deriving_protobuf_.
