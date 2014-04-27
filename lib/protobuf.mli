(** Type of wire format payload kinds. *)
type payload_kind =
| Varint
| Bits32
| Bits64
| Bytes

(** Type of failures possible while decoding. *)
type decoding_error =
| Incomplete
| Overflow
| Malformed_field
| Unexpected_payload  of string * payload_kind
| Missing_field       of string

exception Decoding_error of decoding_error

module Wire : sig

  (** Type of wire format readers. *)
  type reader

  (** [reader_of_string s] creates a reader positioned at start of string [s]. *)
  val reader_of_string  : string -> reader

  (** [read_varint r] reads a varint from [r].
      If [r] has exhausted its input, raises [Encoding_error Incomplete]. *)
  val read_varint       : reader -> int64

  (** [read_int64 r] reads eight bytes from [r].
      If [r] has exhausted its input, raises [Encoding_error Incomplete]. *)
  val read_int64        : reader -> int64

  (** [read_int32 r] reads four bytes from [r].
      If [r] has exhausted its input, raises [Encoding_error Incomplete]. *)
  val read_int32        : reader -> int32

  (** [read_bytes r] reads a varint from [r].
      If [r] has exhausted its input, raises [Encoding_error Incomplete]. *)
  val read_bytes        : reader -> string

  (** [read_smallint r] reads a varint from [r] and truncates it to [int].
      If the value doesn't fit in the range of [int], raises
      [Encoding_error Overflow].
      If [r] has exhausted its input, raises [Encoding_error Incomplete]. *)
  val read_smallint     : reader -> int

  (** [read_nested r] returns a reader for a message nested in [r].
      If reading the message would exhaust input of [r], raises
      [Encoding_error Incomplete]. *)
  val read_nested       : reader -> reader

  (** [read_key r] reads a key and a payload kind from [r].
      If [r] has exhausted its input when the function is called, returns [None].
      If [r] has exhausted its input while reading, raises
      [Encoding_error Incomplete].
      If the payload kind is unknown, raises [Encoding_error Malformed_field]. *)
  val read_key          : reader -> (int * payload_kind) option

  (** [skip r pk] skips the next value of kind [pk] in [r].
      If skipping the value would exhaust input of [r], raises
      [Encoding_error Incomplete]. *)
  val skip              : reader -> payload_kind -> unit

  (** [int_of_int32 v] returns [v] truncated to [int].
      If the value doesn't fit in the range of [int], raises
      [Encoding_error Overflow]. *)
  val int_of_int32      : int32 -> int

  (** [int_of_int64 v] returns [v] truncated to [int].
      If the value doesn't fit in the range of [int], raises
      [Encoding_error Overflow]. *)
  val int_of_int64      : int64 -> int

  (** [get v n] returns [v'] if [v] is [Some v'] or raises
      [Encoding_error (Missing_field n)] if [v] is [None]. *)
  val get               : 'a option -> string -> 'a
end
