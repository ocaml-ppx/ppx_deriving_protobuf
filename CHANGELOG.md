Changelog
=========

1.0.0
-----

  * Add support for `String.t`, `Bytes.t` and `bytes`.
  * Fix a bug that prevented using repeated and optional fields as
    constructor arguments.
  * Accept namespaced attributes, e.g. [@protobuf.key].
  * Reject attributes likely to be misplaced, e.g. [@key] attached
    to a record field name.
  * Support passing types, rather than type variables, as type parameters.
  * Add shorthands for encoding and decoding `bytes`.

0.9.0
-----

  * Initial release.
