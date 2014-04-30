(** Type of wire format payload kinds. *)
type payload_kind =
| Varint
| Bits32
| Bits64
| Bytes

module Decoder : sig
  (** Type of failures possible while decoding. *)
  type error =
  | Incomplete
  | Overflow
  | Malformed_field
  | Unexpected_payload of string * payload_kind
  | Missing_field      of string
  | Malformed_variant  of string

  (** [error_to_string e] converts error [e] to its string representation. *)
  val error_to_string : error -> string

  exception Failure of error

  (** Type of wire format decoders. *)
  type t

  (** [of_string s] creates a reader positioned at start of string [s]. *)
  val of_string     : string -> t

  (** [skip r pk] skips the next value of kind [pk] in [d].
      If skipping the value would exhaust input of [d], raises
      [Encoding_error Incomplete]. *)
  val skip          : t -> payload_kind -> unit

  (** [bool r] reads a varint from [d] and converts it to [bool].
      If [d] has exhausted its input, raises [Failure Incomplete].
      If the value is not [0] or [1], raises [Failure Overflow]. *)
  val bool          : t -> bool

  (** [varint r] reads a varint from [d].
      If [d] has exhausted its input, raises [Failure Incomplete]. *)
  val varint        : t -> int64

  (** [zigzag r] reads a varint from [d] and zigzag-decodes it.
      If [d] has exhausted its input, raises [Failure Incomplete]. *)
  val zigzag        : t -> int64

  (** [bits32 r] reads four bytes from [d].
      If [d] has exhausted its input, raises [Failure Incomplete]. *)
  val bits32        : t -> int32

  (** [bits64 r] reads eight bytes from [d].
      If [d] has exhausted its input, raises [Failure Incomplete]. *)
  val bits64        : t -> int64

  (** [bytes r] reads a varint from [d].
      If [d] has exhausted its input, raises [Failure Incomplete]. *)
  val bytes         : t -> string

  (** [nested r] returns a reader for a message nested in [d].
      If reading the message would exhaust input of [d], raises
      [Failure Incomplete]. *)
  val nested        : t -> t

  (** [key r] reads a key and a payload kind from [d].
      If [d] has exhausted its input when the function is called, returns [None].
      If [d] has exhausted its input while reading, raises
      [Failure Incomplete].
      If the payload kind is unknown, raises [Failure Malformed_field]. *)
  val key           : t -> (int * payload_kind) option

  (** [int_of_int32 v] returns [v] truncated to [int].
      If the value doesn't fit in the range of [int], raises
      [Failure Overflow]. *)
  val int_of_int32  : int32 -> int

  (** [int_of_int64 v] returns [v] truncated to [int].
      If the value doesn't fit in the range of [int], raises
      [Failure Overflow]. *)
  val int_of_int64  : int64 -> int
end
