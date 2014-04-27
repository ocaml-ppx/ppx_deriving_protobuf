type payload_kind =
| Varint
| Bits32
| Bits64
| Bytes

type decoding_error =
| Incomplete
| Overflow
| Malformed_field
| Unexpected_payload  of string * payload_kind
| Missing_field       of string

exception Decoding_error of decoding_error

module Wire = struct
  type reader = {
            source : string;
            limit  : int;
    mutable offset : int;
  }

  let int_of_int32 v =
    if (Int32.compare v (Int32.of_int max_int)) =  1 ||
       (Int32.compare v (Int32.of_int min_int)) = -1 then
      raise (Decoding_error Overflow);
    Int32.to_int v

  let int_of_int64 v =
    if (Int64.compare v (Int64.of_int max_int)) =  1 ||
       (Int64.compare v (Int64.of_int min_int)) = -1 then
      raise (Decoding_error Overflow);
    Int64.to_int v

  let reader_of_string source =
    { source; offset = 0; limit = String.length source; }

  let read_byte r =
    if r.offset >= r.limit then
      raise (Decoding_error Incomplete);
    let byte = int_of_char r.source.[r.offset] in
    r.offset <- r.offset + 1;
    byte

  let read_varint r =
    let rec read s =
      let b = read_byte r in
      if b land 0x80 <> 0
      then Int64.(logor (shift_left (logand (of_int b) 0x7fL) s) (read (s + 7)))
      else Int64.(shift_left (of_int b) s)
    in
    read 0

  let read_smallint r =
    int_of_int64 (read_varint r)

  let read_int32 r =
    let b4 = read_byte r in
    let b3 = read_byte r in
    let b2 = read_byte r in
    let b1 = read_byte r in
    Int32.(add (shift_left (of_int b1) 24)
           (add (shift_left (of_int b2) 16)
            (add (shift_left (of_int b3) 8)
             (of_int b4))))

  let read_int64 r =
    let b8 = read_byte r in
    let b7 = read_byte r in
    let b6 = read_byte r in
    let b5 = read_byte r in
    let b4 = read_byte r in
    let b3 = read_byte r in
    let b2 = read_byte r in
    let b1 = read_byte r in
    Int64.(add (shift_left (of_int b1) 56)
           (add (shift_left (of_int b2) 48)
            (add (shift_left (of_int b3) 40)
             (add (shift_left (of_int b4) 32)
              (add (shift_left (of_int b5) 24)
               (add (shift_left (of_int b6) 16)
                (add (shift_left (of_int b7) 8)
                 (of_int b8))))))))

  let read_bytes r =
    let len = read_smallint r in
    if r.offset + len > r.limit then
      raise (Decoding_error Incomplete);
    let str = String.sub r.source r.offset len in
    r.offset <- r.offset + len;
    str

  let read_nested r =
    let len = read_smallint r in
    if r.offset + len > r.limit then
      raise (Decoding_error Incomplete);
    let r' = { r with limit = r.offset + len; } in
    r.offset <- r.offset + len;
    r'

  let read_key r =
    if r.offset = r.limit
    then None
    else
      let prefix  = read_smallint r in
      let key, ty = prefix lsr 3, prefix land 0x7 in
      match ty with
      | 0 -> Some (key, Varint)
      | 1 -> Some (key, Bits64)
      | 2 -> Some (key, Bytes)
      | 5 -> Some (key, Bits32)
      | _ -> raise (Decoding_error Malformed_field)

  let skip r kind =
    let skip_len n =
      if r.offset + n > r.limit then
        raise (Decoding_error Incomplete);
      r.offset <- r.offset + r.limit
    in
    let rec skip_varint () =
      let b = read_byte r in
      if b land 0x80 <> 0 then skip_varint () else ()
    in
    match kind with
    | Bits32 -> skip_len 4
    | Bits64 -> skip_len 8
    | Bytes  -> skip_len (read_smallint r)
    | Varint -> skip_varint ()

  let get v f =
    match v with
    | None    -> raise (Decoding_error (Missing_field f))
    | Some v' -> v'
end

type foo = {
  a : int;
  b : int           [@encoding fixed32];
  c : string option [@id 10];
  d : float;
} [@@protobuf]

let foo_of_protobuf r =
  let a = ref None in
  let b = ref None in
  let c = ref None in
  let d = ref None in
  let rec read () =
    match Wire.read_key r with
    | Some (0, Varint) -> a := Some (Wire.int_of_int64 (Wire.read_varint r)); read ()
    | Some (0, kind)   -> raise (Decoding_error (Unexpected_payload ("Test.foo.a", kind)))
    | Some (1, Bits32) -> b := Some (Wire.int_of_int32 (Wire.read_int32 r)); read ()
    | Some (1, kind)   -> raise (Decoding_error (Unexpected_payload ("Test.foo.b", kind)))
    | Some (2, Bytes)  -> c := Some (Wire.read_bytes r); read ()
    | Some (2, kind)   -> raise (Decoding_error (Unexpected_payload ("Test.foo.c", kind)))
    | Some (3, Bits64) -> d := Some (Int64.float_of_bits (Wire.read_int64 r)); read ()
    | Some (3, kind)   -> raise (Decoding_error (Unexpected_payload ("Test.foo.d", kind)))
    | Some (_, kind)   -> Wire.skip r kind; read ()
    | None             -> ()
  in
  read ();
  { a = Wire.get !a "Test.foo.a";
    b = Wire.get !b "Test.foo.b";
    c = !c;
    d = Wire.get !d "Test.foo.d"; }

