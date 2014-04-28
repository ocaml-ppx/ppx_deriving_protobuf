(** Type of wire format payload kinds. *)
type payload_kind =
| Varint
| Bits32
| Bits64
| Bytes

module Reader : sig
  (** Type of wire format readers. *)
  type t

  (** Type of failures possible while decoding. *)
  type error =
  | Incomplete
  | Overflow
  | Malformed_field
  | Unexpected_payload of string * payload_kind
  | Missing_field      of string

  exception Error of error

  (** [of_string s] creates a reader positioned at start of string [s]. *)
  val of_string     : string -> t

  (** [skip r pk] skips the next value of kind [pk] in [r].
      If skipping the value would exhaust input of [r], raises
      [Encoding_error Incomplete]. *)
  val skip          : t -> payload_kind -> unit

  (** [bool r] reads a bool from [r].
      If [r] has exhausted its input, raises [Decoding_error Incomplete].
      If the value is not [0] or [1], raises [Decoding_error Overflow]. *)
  val bool          : t -> bool

  (** [varint r] reads a varint from [r].
      If [r] has exhausted its input, raises [Decoding_error Incomplete]. *)
  val varint        : t -> int64

  (** [zigzag r] reads a zigzag-encoded varint from [r].
      If [r] has exhausted its input, raises [Decoding_error Incomplete]. *)
  val zigzag        : t -> int64

  (** [bits32 r] reads four bytes from [r].
      If [r] has exhausted its input, raises [Decoding_error Incomplete]. *)
  val bits32        : t -> int32

  (** [bits64 r] reads eight bytes from [r].
      If [r] has exhausted its input, raises [Decoding_error Incomplete]. *)
  val bits64        : t -> int64

  (** [bytes r] reads a varint from [r].
      If [r] has exhausted its input, raises [Decoding_error Incomplete]. *)
  val bytes         : t -> string

  (** [nested r] returns a reader for a message nested in [r].
      If reading the message would exhaust input of [r], raises
      [Decoding_error Incomplete]. *)
  val nested        : t -> t

  (** [key r] reads a key and a payload kind from [r].
      If [r] has exhausted its input when the function is called, returns [None].
      If [r] has exhausted its input while reading, raises
      [Decoding_error Incomplete].
      If the payload kind is unknown, raises [Decoding_error Malformed_field]. *)
  val key           : t -> (int * payload_kind) option

  (** [int_of_int32 v] returns [v] truncated to [int].
      If the value doesn't fit in the range of [int], raises
      [Decoding_error Overflow]. *)
  val int_of_int32 : int32 -> int

  (** [int_of_int64 v] returns [v] truncated to [int].
      If the value doesn't fit in the range of [int], raises
      [Decoding_error Overflow]. *)
  val int_of_int64 : int64 -> int
end
