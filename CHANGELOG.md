Changelog
=========

0.9.1
-----

  * Update for OCaml 4.02.
  * Add support for `String.t`, `Bytes.t` and `bytes`.
  * Fix a bug that prevented using repeated and optional fields as
    constructor arguments.
  * Accept namespaced attributes, e.g. [@protobuf.key].
  * Reject attributes likely to be misplaced, e.g. [@key] attached
    to a record field name.

0.9.0
-----

  * Initial release.
